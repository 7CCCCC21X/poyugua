"""The web console's back end: the settings saved on the page (控制台在线改参数), the operator's actions, the order list
and the review — what preopen/web.py serves.

A saved setting beats the environment variable of the same name, takes effect at once (Engine.apply_settings: a resting
quote that no longer fits is withdrawn and quoted again, one that fits keeps its queue place) and is kept in
<PREOPEN_DB stem>.control.json, so a restart runs on it too. LIVE is the one that needs a restart: switching it saves the
value and starts the process again (leaving live trading, every order the bot signed is cancelled first; with
PREOPEN_KEY_EXCLUSIVE=on every open order of the API key).
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Callable

from . import config, journal
from .config import ConfigError

LOG = logging.getLogger(__name__)


class Console:
    def __init__(self, engine: Any, env: dict[str, str], path: Path, overrides: dict[str, str],
                 notify: Callable[[str], None] | None = None):
        self.engine = engine
        self.env = dict(env)
        self.path = path
        self.overrides = dict(overrides)
        self.notify_cb = notify

    # ------------------------------------------------------------------ reads
    def report(self) -> dict[str, Any]:
        return self.engine.report()

    def settings(self) -> dict[str, Any]:
        now = config.current_values(self.engine.s)
        try:
            env_values = config.current_values(config.from_env(self.env))
        except ConfigError:
            env_values = {}
        items = []
        for name, label, help_text in config.CONTROL_KEYS:
            form = dict(config.CONTROL_FORMS[name])
            source = "网页" if name in self.overrides else "环境变量" if self.env.get(name, "").strip() else "默认"
            items.append({"name": name, "label": label, "help": help_text, "form": form, "value": now.get(name, ""),
                          "source": source, "env_value": env_values.get(name, ""), "saved": self.overrides.get(name)})
        return {"items": items, "live": bool(self.engine.s.live), "has_key": bool(self.env.get("POLY_PRIVATE_KEY", "").strip()),
                "overrides": len(self.overrides)}

    def orders(self, tab: str, coin: str, limit: int = 300) -> dict[str, Any]:
        records = self.engine.all_records() if tab in ("all", "filled", "closed") else list(self.engine.ledger.rows.values())
        return journal.orders(records, tab, coin.lower().strip(), self.engine.now(), limit=max(1, min(limit, 2000)))

    def review(self) -> dict[str, Any]:
        return journal.journal(self.engine.all_records(), self.engine.now())

    def csv(self, kind: str) -> tuple[str, str]:
        records = self.engine.all_records()
        if kind == "daily":
            return "preopen-daily.csv", journal.daily_csv(journal.journal(records, self.engine.now()))
        return "preopen-fills.csv", journal.detail_csv(records)

    # ------------------------------------------------------------------ writes
    def notify(self, text: str) -> None:
        LOG.info(text)
        if self.notify_cb:
            try:
                self.notify_cb(text)
            except Exception as error:  # never stops a save
                LOG.warning("通知失败：%s", error)

    async def change(self, body: dict[str, Any]) -> tuple[bool, str]:
        """{"name": X, "value": V} saves X=V; {"reset": X} returns X to the environment; {"reset_all": true} clears all."""
        trial = dict(self.overrides)
        if body.get("reset_all"):
            what = "全部恢复环境变量"
            trial = {}
        elif body.get("reset"):
            name = str(body["reset"])
            if name not in config.CONTROL_KEY_SET:
                return False, f"不能在网页上改 {name}"
            what = f"{name} 恢复环境变量"
            trial.pop(name, None)
        else:
            name, value = str(body.get("name") or ""), str(body.get("value") if body.get("value") is not None else "").strip()
            if name not in config.CONTROL_KEY_SET:
                return False, f"不能在网页上改 {name or '（空）'}"
            if name == "PREOPEN_MARKETS" and value == "":
                value = "none"
            what = f"{name}={value or '（空）'}"
            trial[name] = value
        try:
            new = config.from_env({**self.env, **trial})
        except ConfigError as error:
            return False, f"没保存：{error}"
        old = self.engine.s
        before = config.current_values(old)
        after = config.current_values(new)
        diff = [f"{k} {before[k] or '（空）'} → {after[k] or '（空）'}" for k in after if before.get(k) != after.get(k)]
        config.save_overrides(self.path, trial)
        self.overrides = trial
        if new.live != old.live:
            return True, await self.switch_live(new)
        withdrawn = self.engine.apply_settings(new)
        text = f"已保存：{what}" + (f"（{'；'.join(diff)}）" if diff else "（数值没变）")
        if withdrawn:
            text += f"；{withdrawn} 场和新设置不符，撤掉后按新设置重挂"
        self.notify("🛠 网页改了设置：" + text)
        return True, text

    async def switch_live(self, new: config.Settings) -> str:
        """LIVE switched on the page: saved; leaving live trading, the bot's orders are cancelled (by id, an unknown one
        by its hash; with PREOPEN_KEY_EXCLUSIVE=on every open order of the API key at once) and the records withdrawn
        first; then the process starts again in the new mode."""
        e = self.engine
        if e.s.live and not new.live:
            if e.s.key_exclusive:
                try:
                    r = await e.x.cancel_all()
                    e.mark_cancelled(r)
                except Exception as error:
                    LOG.warning("切换到模拟盘：一次撤光失败：%s", error)
            left = await e.withdraw_all("切换到模拟盘", deadline_s=30)
            text = "已切到模拟盘：真实挂单已撤" + (f"（还可能挂着 {left} 张，到期自动失效）" if left else "") + "，程序正在重启"
        else:
            text = "已切到真实交易，程序正在重启（约半分钟后开始挂单）"
        self.notify("🛠 网页切换了交易模式：" + text)
        e.restart = True
        e.stop.set()
        return text

    async def action(self, body: dict[str, Any]) -> str:
        e = self.engine
        name = str(body.get("action") or "")
        if name == "pause":
            return f"已暂停：{e.pause()} 场在撤单，不再新挂"
        if name == "resume":
            e.resume()
            return "已恢复挂单"
        if name == "emergency":
            return await e.emergency()
        if name == "withdraw":
            return e.withdraw(str(body.get("key") or ""))
        if name == "rebind":
            return e.rebind()
        raise ValueError(f"unknown action {name}")

    ACTIONS = ("pause", "resume", "emergency", "withdraw", "rebind")
