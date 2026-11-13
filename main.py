#!/usr/bin/env python3
"""Polymarket 5-minute up/down pre-listing quoter (预挂机器人).

    python -u main.py            run (LIVE=off by default: a paper exchange, nothing real is sent)
    python main.py probe [hours] read-only: the coming windows, the listings, the books (no key needed)
    python main.py status        the records in the local database (no network)

Settings come from the environment (or a local .env); see .env.example and README.md.
"""
from __future__ import annotations

import asyncio
import logging
import os
import signal
import socket
import sys
import time
from pathlib import Path

from preopen import config, region
from preopen.engine import Engine
from preopen.exchange import LiveExchange, PaperExchange
from preopen.gamma import PublicApi
from preopen.ledger import Ledger
from preopen.probe import probe
from preopen.telegram import CONFIRM_PAUSE, HELP, Reply, Telegram, confirm_markup, web_markup
from preopen.console import Console
from preopen.web import WebServer

LOG = logging.getLogger("preopen")


def setup_logging(level: str) -> None:
    logging.basicConfig(level=getattr(logging, level, logging.INFO), format="%(asctime)s %(levelname)s %(name)s: %(message)s",
                        stream=sys.stdout)
    for noisy in ("httpx", "httpcore", "hpack", "polymarket"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


WEB_HOWTO = ("网页控制台还没开：在 Railway 打开这个服务 → Settings → Networking → Generate Domain 生成一个域名，"
             "再在 Variables 里加 WEB_KEY（至少 12 位的密码）后重新部署。之后左下角的菜单按钮就能直接打开控制台。")


class InstanceLocked(RuntimeError):
    pass


def acquire_lock(ledger_path: str):
    """One process per ledger: a second one (a second replica, a deploy overlapping the old one, a local run on the same
    file) would quote every window twice. An exclusive lock on <ledger>.lock, held for the life of the process (the OS
    lets it go when the process ends, however it ends). None where there is nothing to lock."""
    if ledger_path == ":memory:":
        return None
    try:
        import fcntl
    except ImportError:  # pragma: no cover - Windows
        return None
    path = Path(ledger_path).with_name(Path(ledger_path).name + ".lock")
    path.parent.mkdir(parents=True, exist_ok=True)
    f = open(path, "a+", encoding="utf-8")
    try:
        fcntl.flock(f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        f.seek(0)
        holder = f.read().strip()
        f.close()
        raise InstanceLocked(f"账本 {ledger_path} 正被另一个进程使用（{holder or '未知'}）：同一个账本只能跑一个实例，这个实例退出")
    f.seek(0)
    f.truncate()
    f.write(f"pid {os.getpid()} @ {socket.gethostname()} since {time.strftime('%Y-%m-%d %H:%M:%S')}")
    f.flush()
    return f


def commander(settings: config.Settings, engine: Engine, pub: PublicApi):
    """The Telegram commands and buttons: one reply each (the pause asks first)."""
    async def handle(cmd: str, args: list[str]) -> Reply | str:
        if cmd in ("/status", "/preopen"):
            return engine.status_text()
        if cmd in ("/start", "/help", "/menu"):
            return Reply(HELP)
        if cmd == "/check":
            return engine.check_text()
        if cmd == "/probe":
            try:
                hours = float(args[0]) if args else 1.0
            except ValueError:
                hours = 1.0
            return await probe(settings, pub, hours=max(0.1, min(hours, 24.0)))
        if cmd == "/web":
            if settings.web_url.startswith("https://") and settings.web_port:
                return Reply("点下面的按钮在 Telegram 里打开控制台（也可以用左下角的菜单按钮）。"
                             f"\n浏览器打开：{settings.web_url}（输入 WEB_KEY 登录）", web_markup(settings.web_url))
            return WEB_HOWTO
        if cmd == "/pause":
            c = engine.counts()
            return Reply(f"确定暂停吗？会撤掉本程序挂着的 {c['resting']} 张单（{c['windows']} 场），之后不再新挂，直到恢复。",
                         confirm_markup(CONFIRM_PAUSE, "确认暂停并撤单"))
        if cmd == "/" + CONFIRM_PAUSE:
            n = engine.pause()
            return f"⏸ 已暂停：{n} 场在撤单，不再新挂（▶️ 恢复 / /resume）"
        if cmd == "/resume":
            engine.resume()
            return "▶️ 已恢复挂单"
        return Reply(HELP)
    return handle


async def run(settings: config.Settings, env: dict[str, str] | None = None, overrides: dict[str, str] | None = None,
              dropped: list[str] | None = None) -> bool:
    """Run until stopped. True when the console asked for a restart (a LIVE switch)."""
    env = dict(os.environ) if env is None else env
    ledger = Ledger(settings.ledger_path)
    pub = PublicApi()
    exchange = LiveExchange(settings) if settings.live else PaperExchange(settings.paper_balance)
    tg = (Telegram(settings.telegram_token, settings.telegram_chat, web_url=settings.web_url if settings.web_port else "",
                   admins=settings.telegram_admins)
          if settings.telegram_token and settings.telegram_chat else None)
    engine = Engine(settings, exchange, pub, ledger, notify=tg.send if tg else None)
    for note in [*settings.notes, *(dropped or [])]:
        LOG.warning(note)
    for note in dropped or []:
        engine.notify(f"⚠️ {note}")
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, engine.stop.set)
        except (NotImplementedError, RuntimeError):  # pragma: no cover - Windows
            pass
    console = Console(engine, env, config.control_path(settings.db_path), overrides or {},
                      notify=(lambda text: tg.send(text)) if tg else None)
    web = None
    if settings.web_port:
        web = WebServer(settings.web_port, console, web_key=settings.web_key, bot_token=settings.telegram_token,
                        admin_id=settings.telegram_admins)
        try:
            await web.start()
        except OSError as error:
            LOG.warning("网页控制台没能启动（端口 %d）：%s", settings.web_port, error)
            web = None
    side_tasks = []
    if tg:
        side_tasks.append(asyncio.create_task(tg.run(commander(settings, engine, pub)), name="telegram"))
    LOG.info("启动：%s，币种 %s，每边 %s，数据库 %s%s", settings.mode(), ",".join(settings.coins), settings.levels_text(),
             settings.ledger_path, f"，网页 {settings.web_url or '端口 ' + str(settings.web_port)}" if web else "")
    if settings.live:
        allowed, line = await region.check()
        (LOG.info if allowed else LOG.warning)(line)
        if tg and allowed is not True:
            tg.send(line)
    try:
        await engine.run()
    except Exception as error:
        LOG.exception("机器人异常退出")
        if tg:
            tg.send(f"❌ 预挂机器人异常退出：{error}")
        raise
    finally:
        if tg:
            await asyncio.sleep(2.0)  # let the last messages out before the sender stops
        for t in side_tasks:
            t.cancel()
        await asyncio.gather(*side_tasks, return_exceptions=True)
        if web:
            await web.close()
        if tg:
            await tg.close()
        await exchange.close()
        await pub.close()
        ledger.close()
    return engine.restart


def offline_status(settings: config.Settings) -> str:
    ledger = Ledger(settings.ledger_path)
    try:
        phases: dict[str, int] = {}
        pnl = 0.0
        for q in ledger.rows.values():
            phases[q["phase"]] = phases.get(q["phase"], 0) + 1
            pnl += float(q.get("pnl") or 0)
        return (f"数据库 {settings.ledger_path}：{len(ledger.rows)} 条记录 {phases}，已归档 {ledger.archive_count()} 条；"
                f"已结算盈亏合计 {pnl:+.2f} 美元；暂停={ledger.get('paused') == '1'}")
    finally:
        ledger.close()


def main(argv: list[str]) -> int:
    config.load_dotenv()
    env = dict(os.environ)
    try:
        config.from_env(env)  # the environment alone must hold
        path = config.control_path((env.get("PREOPEN_DB") or "data/preopen.sqlite3").strip())
        saved = config.load_overrides(path)
        settings, kept, dropped = config.effective(env, saved)
    except config.ConfigError as error:
        print(f"设置有误：{error}", file=sys.stderr)
        return 2
    if kept != saved:
        config.save_overrides(path, kept)  # a saved value that no longer holds is dropped, once
    setup_logging(settings.log_level)
    cmd = argv[0] if argv else "run"
    if cmd == "probe":
        hours = float(argv[1]) if len(argv) > 1 else 1.0

        async def go() -> str:
            pub = PublicApi()
            try:
                return await probe(settings, pub, hours=hours)
            finally:
                await pub.close()
        print(asyncio.run(go()))
        return 0
    if cmd == "status":
        print(offline_status(settings))
        return 0
    if cmd != "run":
        print(__doc__)
        return 2
    try:
        lock = acquire_lock(settings.ledger_path)
    except InstanceLocked as error:
        LOG.error("%s", error)
        time.sleep(30)  # the platform starts it again: not in a tight loop
        return 3
    try:
        restart = asyncio.run(run(settings, env, kept, dropped))
    finally:
        if lock is not None:
            lock.close()
    if restart:
        LOG.info("按网页的设置重启")
        sys.stdout.flush()
        os.execv(sys.executable, [sys.executable, *sys.orig_argv[1:]])
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
