"""The web console's back end: the settings saved on the page (控制台在线改参数), the operator's actions, the order list
and the review — what preopen/web.py serves.

A saved setting beats the environment variable of the same name, takes effect at once (Engine.apply_settings: a resting
quote that no longer fits is withdrawn and quoted again, one that fits keeps its queue place) and is kept in
<PREOPEN_DB stem>.control.json, so a restart runs on it too. LIVE is the one that needs a restart: switching it saves the
value and starts the process again (leaving live trading, every order the bot signed is cancelled first; with
PREOPEN_KEY_EXCLUSIVE=on every open order of the API key).
"""
from __future__ import annotations

import asyncio
import logging
import time
from pathlib import Path
from typing import Any, Callable

from . import config, export, journal
from . import markets as mk
from .config import ConfigError

LOG = logging.getLogger(__name__)
REVIEW_CACHE_S = 60       # one review (one filter) is worked out at most this often: the page refreshes it every minute
CARDS_CACHE_S = 600       # the cards' days before today: worked out again after this, a new day or a grown archive


class Console:
    def __init__(self, engine: Any, env: dict[str, str], path: Path, overrides: dict[str, str],
                 notify: Callable[[str], None] | None = None):
        self.engine = engine
        self.env = dict(env)
        self.path = path
        self.overrides = dict(overrides)
        self.notify_cb = notify
        self._reviews: dict[tuple, tuple[float, Any]] = {}
        self._older: tuple[tuple, float, dict] | None = None   # (today, archive size), when, the raw cards before today

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

    async def orders(self, tab: str, coin: str, limit: int = 300) -> dict[str, Any]:
        """The order list: the open tab from the working set; the others with the archive, read in a thread."""
        now, limit = self.engine.now(), max(1, min(limit, 2000))
        if tab not in ("all", "filled", "closed"):
            return journal.orders(list(self.engine.ledger.rows.values()), tab, coin.lower().strip(), now, limit=limit)
        read = self.engine.review_reader()

        def work() -> dict[str, Any]:
            with journal.quiet_gc():
                return journal.orders(read(), tab, coin.lower().strip(), now, limit)
        return await asyncio.to_thread(work)

    def span(self, query: dict[str, str] | None) -> tuple[tuple[float, float] | None, dict[str, str]]:
        """The review's time filter from the page: range=today|yesterday|7d|30d|all, or custom with from/to (Beijing
        time, by the windows' start). ValueError for custom bounds that are not times."""
        q = query or {}
        return journal.span_from(str(q.get("range") or "all"), str(q.get("from") or ""), str(q.get("to") or ""), self.engine.now())

    @staticmethod
    def coins(query: dict[str, str] | None) -> list[str]:
        """The coin filter: coins=btc,eth (none or unknown ones: every coin)."""
        return sorted({c for c in str((query or {}).get("coins") or "").lower().replace("，", ",").split(",") if c in config.COINS})

    async def cards(self, read: Callable[..., Any]) -> dict[str, dict[str, Any]]:
        """The cards' raw totals: the days before today kept (CARDS_CACHE_S, a new day, a grown archive), today's added
        afresh (today's windows are all in the working set: they are archived two days after)."""
        now = self.engine.now()
        today = journal.day_start(now)
        key = (today, self.engine.ledger.archive_count())

        def older() -> dict:
            with journal.quiet_gc():
                return journal.card_totals(read(float("-inf"), today), now)
        if self._older is None or self._older[0] != key or time.monotonic() - self._older[1] > CARDS_CACHE_S:
            self._older = (key, time.monotonic(), await asyncio.to_thread(older))
        fresh = await asyncio.to_thread(journal.card_totals, read(today, float("inf")), now)
        return journal.add_cards(self._older[2], fresh)

    async def computed(self, key: tuple, work: Callable[[Callable[..., Any]], Any]) -> Any:
        """`work(read)` in a thread on the engine's reader (made here, on the event loop: the working records copied),
        the collector quiet meanwhile; its result kept REVIEW_CACHE_S for the same key. The trading loops never wait
        for a review, nor does the process hold the archive in memory."""
        hit = self._reviews.get(key)
        if hit and time.monotonic() - hit[0] < REVIEW_CACHE_S:
            return hit[1]
        read = self.engine.review_reader()

        def run() -> Any:
            with journal.quiet_gc():
                return work(read)
        result = await asyncio.to_thread(run)
        if len(self._reviews) > 32:
            self._reviews.clear()
        self._reviews[key] = (time.monotonic(), result)
        return result

    async def review(self, query: dict[str, str] | None = None) -> dict[str, Any]:
        """The review for the page: the whole ledger's today / 7 days / all, the rest for the windows in the filter
        (time and coins); the markets sorted over all of them (sort=time|loss|gain) before the first 500 are sent; the
        settings changes with the windows before and after each."""
        try:
            span, info = self.span(query)
        except ValueError as error:
            return {"error": str(error)}
        coins = self.coins(query)
        how = str((query or {}).get("sort") or "time")
        how = how if how in ("time", "loss", "gain") else "time"
        key = ("review", info["range"], info["from"], info["to"], tuple(coins), how)
        hit = self._reviews.get(key)
        if hit and time.monotonic() - hit[0] < REVIEW_CACHE_S:
            return hit[1]
        now, cancel_s = self.engine.now(), self.engine.s.cancel_s
        log = list(self.engine.settings_log)
        lo, hi = span or (float("-inf"), float("inf"))
        cards = await self.cards(self.engine.review_reader())

        def work(read: Callable[..., Any]) -> dict[str, Any]:
            out = journal.journal(read(lo, hi), now, cancel_s, span, coins or None, cards=cards,
                                  market_limit=journal.MARKETS_SHOWN, market_sort=how)
            out.update(span=info, sort=how, market_more=max(0, out["market_count"] - journal.MARKETS_SHOWN),
                       changes=journal.change_effects(read, now, log, coins or None))
            return out
        return await self.computed(key, work)

    async def price_detail(self, query: dict[str, str] | None = None) -> dict[str, Any]:
        """One price opened up (the page's 按挂价 row): its quotes that filled, in the filter, the biggest loss first."""
        try:
            span, info = self.span(query)
            price = float(str((query or {}).get("price") or ""))
        except ValueError as error:
            return {"error": str(error) or "价格不对"}
        coins = self.coins(query)
        now = self.engine.now()
        lo, hi = span or (float("-inf"), float("inf"))
        return await self.computed(("price", info["range"], info["from"], info["to"], tuple(coins), price),
                                   lambda read: journal.price_markets(read(lo, hi), now, price, span, coins or None))

    def label(self, query: dict[str, str] | None) -> tuple[tuple[float, float] | None, str, list[str]]:
        """The filter for an export: its span, a label for the file name, the coins. ValueError on a bad custom filter
        (11-21 review: it fell back to everything without a word)."""
        span, info = self.span(query)
        coins = self.coins(query)
        return span, (info.get("label") or "全部") + ("_" + "".join(mk.name_of(c) for c in coins) if coins else ""), coins

    async def xlsx(self, query: dict[str, str] | None = None) -> tuple[str, bytes]:
        """The review as one Excel file, for the windows in the filter."""
        span, label, coins = self.label(query)

        def work(read: Callable[..., Any]) -> tuple[str, bytes]:
            name, data, _ = export.from_engine(self.engine, span, label, read, coins or None)
            return name, data
        return await self.computed(("xlsx", label, span, tuple(coins)), work)

    async def csv(self, kind: str, query: dict[str, str] | None = None) -> tuple[str, str]:
        """A CSV of the windows in the filter, named after it (the quotes and the fills: all of them, the latest first)."""
        span, label, coins = self.label(query)
        now, cancel_s = self.engine.now(), self.engine.s.cancel_s
        tag = "".join(ch for ch in label if ch.isalnum() or ch in "-_")
        lo, hi = span or (float("-inf"), float("inf"))

        def work(read: Callable[..., Any]) -> tuple[str, str]:
            def stream(newest: bool = False) -> Any:
                return (q for q in read(lo, hi, newest) if not coins or q["coin"] in coins)
            if kind == "quotes":
                return f"preopen-quotes-{tag}.csv", journal.quotes_csv(stream(True))
            if kind == "detail":
                return f"preopen-fills-{tag}.csv", journal.detail_csv(stream(True))
            review = journal.journal(stream(), now, cancel_s, span)
            if kind == "daily":
                return f"preopen-daily-{tag}.csv", journal.daily_csv(review)
            if kind == "markets":
                return f"preopen-markets-{tag}.csv", journal.markets_csv(review)
            return f"preopen-cancel-review-{tag}.csv", journal.cancel_csv(review["cancel"])
        return await self.computed(("csv", kind, label, span, tuple(coins)), work)

    async def set_cancel(self, seconds: int, via: str = "网页") -> tuple[bool, str]:
        """The review's recommendation applied (the page's button, Telegram's): the cancel time with the times tied to
        it, checked and saved together: no new order within 60 s more than it, and the orders' own expiry close behind
        it (5 s after it where the rules allow), so an order whose cancel fails still dies soon after. A changed expiry
        re-places the resting orders (about a minute)."""
        s = self.engine.s
        entry = max(s.entry_s, seconds + 60)
        expiry = max(0, min(seconds - 5, entry - 150))
        if (s.cancel_s, s.entry_s, s.expiry_s) == (seconds, entry, expiry):
            return True, f"已经是开场前 {journal.secs_label(seconds)}撤单了"
        ok, text = await self.change_many({"PREOPEN_CANCEL_SECONDS": str(seconds), "PREOPEN_ENTRY_SECONDS": str(entry),
                                           "PREOPEN_EXPIRY_SECONDS": str(expiry)}, f"开场前 {journal.secs_label(seconds)}撤单", via)
        if not ok:
            return False, text
        notes = [f"距开场不足 {journal.secs_label(entry)}不再新挂"] if entry != s.entry_s else []
        if expiry != s.expiry_s:
            notes.append(f"订单自己在开场前 {journal.secs_label(expiry)}到期（撤单万一失败也会失效），挂着的单按新到期时间重挂一遍，约 1 分钟")
        self._reviews.clear()
        return True, f"✅ 已改成开场前 {journal.secs_label(seconds)}撤单" + (f"；{'；'.join(notes)}" if notes else "") + "。立即生效"

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

    async def change_many(self, values: dict[str, str], what: str, via: str = "Telegram") -> tuple[bool, str]:
        """Several settings at once, checked together (the rules tie the cancel, expiry and no-new-order times: one at
        a time could pass through a combination they refuse), saved and applied in one step."""
        if not values or any(name not in config.CONTROL_KEY_SET or name == "LIVE" for name in values):
            return False, "这些设置不能这样改"
        trial = {**self.overrides, **{k: str(v) for k, v in values.items()}}
        try:
            new = config.from_env({**self.env, **trial})
        except ConfigError as error:
            return False, f"没保存：{error}"
        before, after = config.current_values(self.engine.s), config.current_values(new)
        diff = [f"{k} {before[k] or '（空）'} → {after[k] or '（空）'}" for k in after if before.get(k) != after.get(k)]
        config.save_overrides(self.path, trial)
        self.overrides = trial
        withdrawn = self.engine.apply_settings(new, via=f"{via}：{what}")
        text = f"已保存：{what}" + (f"（{'；'.join(diff)}）" if diff else "（数值没变）")
        if withdrawn:
            text += f"；{withdrawn} 场的挂单按新的到期时间撤掉重挂"
        self.notify("🛠 改了设置：" + text)
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
            return f"已暂停：{e.pause('网页上点了 ⏸ 暂停')} 场在撤单，不再新挂"
        if name == "resume":
            e.resume()
            return "已恢复挂单"
        if name == "emergency":
            return await e.emergency()
        if name == "withdraw":
            return e.withdraw(str(body.get("key") or ""))
        if name == "rebind":
            return e.rebind()
        if name == "set_cancel":  # the review's recommendation, applied from the page
            try:
                seconds = int(body.get("seconds"))
            except (TypeError, ValueError):
                raise ValueError("撤单秒数不对") from None
            ok, text = await self.set_cancel(seconds, "网页复盘")
            if not ok:
                raise ValueError(text)
            return text
        raise ValueError(f"unknown action {name}")

    ACTIONS = ("pause", "resume", "emergency", "withdraw", "rebind", "set_cancel")
