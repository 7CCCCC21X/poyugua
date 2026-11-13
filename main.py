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
import signal
import sys

from preopen import config
from preopen.engine import Engine
from preopen.exchange import LiveExchange, PaperExchange
from preopen.gamma import PublicApi
from preopen.ledger import Ledger
from preopen.probe import probe
from preopen.telegram import HELP, Telegram

LOG = logging.getLogger("preopen")


def setup_logging(level: str) -> None:
    logging.basicConfig(level=getattr(logging, level, logging.INFO), format="%(asctime)s %(levelname)s %(name)s: %(message)s",
                        stream=sys.stdout)
    for noisy in ("httpx", "httpcore", "hpack", "polymarket"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


async def run(settings: config.Settings) -> None:
    ledger = Ledger(settings.ledger_path)
    pub = PublicApi()
    exchange = LiveExchange(settings) if settings.live else PaperExchange(settings.paper_balance)
    tg = Telegram(settings.telegram_token, settings.telegram_chat) if settings.telegram_token and settings.telegram_chat else None
    engine = Engine(settings, exchange, pub, ledger, notify=tg.send if tg else None)
    for note in settings.notes:
        LOG.warning(note)
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, engine.stop.set)
        except (NotImplementedError, RuntimeError):  # pragma: no cover - Windows
            pass

    async def handle(cmd: str, args: list[str]) -> str:
        if cmd in ("/status", "/preopen", "/start"):
            return engine.status_text()
        if cmd == "/check":
            return engine.check_text()
        if cmd == "/probe":
            hours = float(args[0]) if args else 1.0
            return await probe(settings, pub, hours=max(0.1, min(hours, 24.0)))
        if cmd == "/pause":
            n = engine.pause()
            return f"⏸ 已暂停：{n} 场在撤单，不再新挂（/resume 恢复）"
        if cmd == "/resume":
            engine.resume()
            return "▶️ 已恢复挂单"
        return HELP

    side_tasks = []
    if tg:
        side_tasks.append(asyncio.create_task(tg.run(handle), name="telegram"))
    LOG.info("启动：%s，币种 %s，每边 %s，数据库 %s", settings.mode(), ",".join(settings.coins), settings.levels_text(), settings.ledger_path)
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
        if tg:
            await tg.close()
        await exchange.close()
        await pub.close()
        ledger.close()


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
    try:
        settings = config.from_env()
    except config.ConfigError as error:
        print(f"设置有误：{error}", file=sys.stderr)
        return 2
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
    asyncio.run(run(settings))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
