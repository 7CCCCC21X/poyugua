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
from typing import Any


from preopen import config, export as xls, journal, refusals, region
from preopen import markets as mk
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


REVIEW_USAGE = ("用法：/review（全部）、/review 今天、/review 昨天、/review 7（最近 7 天）、/review 11-20（那一天）、"
                "/review 11-18 11-20（这几天）、/review btc、/review btc eth 3")
EXPORT_USAGE = ("用法：/export（点按钮选时间）、/export 今天、/export 昨天、/export 7（最近 7 天）、/export 11-20、/export 11-18 11-20、"
                "/export 全部；/export csv 7 = 旧的 CSV 文件")
RANGE_BUTTONS = (("今天", "today"), ("昨天", "yesterday"), ("近 7 天", "7"), ("近 30 天", "30"), ("全部", "all"))
RANGE_WORDS = {"today": "今天", "yesterday": "昨天", "all": "全部"}   # a button's callback -> the time word it stands for


def range_buttons(prefix: str) -> list[list[dict]]:
    """今天 / 昨天 / 近 7 天 / 近 30 天 / 全部 as inline buttons whose callback is prefix + the range."""
    keys = [{"text": text, "callback_data": f"{prefix}{key}"} for text, key in RANGE_BUTTONS]
    return [keys[:3], keys[3:]]


def review(engine: Engine, args: list[str], read: Any = None) -> Reply | str:
    """/review [time] [coin ...]: the analysis as a message, for the windows starting in the time named (今天, 昨天, a
    number of days back, a date or two) and the coins named (default: all). `read`: the engine's reader, made on the
    event loop (Engine.review_reader), so that this runs in a thread of its own."""
    now = engine.now()
    try:
        span, label, rest = journal.span_args(args, now)
    except ValueError as error:
        return f"{error}。{REVIEW_USAGE}"
    coins, bad = set(), []
    for a in rest:
        key = a.lower().strip()
        if key in config.COINS:
            coins.add(key)
        else:
            bad.append(a)
    if bad:
        return f"看不懂：{' '.join(bad)}。{REVIEW_USAGE}"
    read = read or engine.review_reader()
    lo, hi = span or (float("-inf"), float("inf"))
    scope = "、".join(x for x in (" ".join(mk.name_of(c) for c in sorted(coins)), label) if x)
    with journal.quiet_gc():
        j = journal.journal((q for q in read(lo, hi) if not coins or q["coin"] in coins), now, engine.s.cancel_s, market_limit=0)
    text = journal.review_text([], now, engine.s.cancel_s, scope, float(engine.s.max_daily_loss_usd), j)
    rec = j["cancel"]["recommend"]   # the same recommendation as the page's (none on a tie, a thin sample, a cent)
    rows = range_buttons("rev_")   # the same review for another time, a tap away
    if rec and not coins:  # one coin's sample is no reason to change them all
        rows = [[{"text": f"改成开场前 {journal.secs_label(rec)}撤单", "callback_data": f"set_cancel_{rec}"}]] + rows
    return Reply(text, {"inline_keyboard": rows})


async def set_cancel(engine: Engine, console: Console | None, seconds: int) -> str:
    """The review's button: the cancel time changed with the times tied to it (Console.set_cancel, as the page's)."""
    if console is None:
        return "这里改不了设置：控制台没启动"
    return (await console.set_cancel(seconds, "Telegram"))[1]


def export(engine: Engine, args: list[str], read: Any = None) -> Reply:
    """/export: buttons to choose the time; /export <time>: the review as one Excel file (every table of the review,
    the refusals, the settings and their changes, a first sheet saying what each is for); /export csv [time]: the
    tables as CSV files, as before. The time as /review takes it."""
    if not args:
        return Reply("📤 导出哪段时间的复盘数据？一个 Excel 文件，里面 12 张表：总览、每日、市场盈亏、档位、成交时刻、撤单时间、"
                     "币种、时段、成交明细、全部报价、拒单、参数（第一张表说明每张表看什么）。",
                     {"inline_keyboard": range_buttons("exp_")})
    now = engine.now()
    csv_files = args[0].lower() == "csv"
    try:
        span, label, rest = journal.span_args(args[1:] if csv_files else args, now)
    except ValueError as error:
        return Reply(f"{error}。{EXPORT_USAGE}")
    if rest:
        return Reply(f"看不懂：{' '.join(rest)}。{EXPORT_USAGE}")
    read = read or engine.review_reader()
    lo, hi = span or (float("-inf"), float("inf"))
    if not csv_files:
        name, data, t = xls.from_engine(engine, span, label or "全部", read)
        return Reply(f"📤 {label or '全部'}的复盘数据：{t['filled']} 场有成交，成交额 ${t['volume']:.2f}，已结算盈亏 {t['pnl']:+.2f} 美元。"
                     "Excel 文件马上发过来，第一张「总览」写着每张表看什么。", files=[(name, data)])
    stamp = time.strftime("%Y%m%d-%H%M", time.gmtime(now + 8 * 3600))
    with journal.quiet_gc():
        review = journal.journal(read(lo, hi), now, engine.s.cancel_s)
        files = [(f"市场盈亏_{stamp}.csv", journal.markets_csv(review)), (f"每日汇总_{stamp}.csv", journal.daily_csv(review)),
                 (f"成交明细_{stamp}.csv", journal.detail_csv(read(lo, hi, True))), (f"全部报价_{stamp}.csv", journal.quotes_csv(read(lo, hi, True))),
                 (f"撤单时间复盘_{stamp}.csv", journal.cancel_csv(review["cancel"]))]
    t = review["totals"]["all"]
    quoted = t["quoted"] + t["upcoming"]
    return Reply(f"📤 导出{label or '全部'}的记录：{quoted} 条报价，{t['filled']} 场有成交，成交额 ${t['volume']:.2f}，"
                 f"已结算盈亏 {t['pnl']:+.2f} 美元。{len(files)} 个 CSV 文件马上发过来"
                 "（Excel 可以直接打开；成交时刻一栏是「开场前几秒:份数」）。", files=[(name, text.encode("utf-8")) for name, text in files])


def commander(settings: config.Settings, engine: Engine, pub: PublicApi, console: Console | None = None):
    """The Telegram commands and buttons: one reply each (the pause asks first)."""
    async def handle(cmd: str, args: list[str]) -> Reply | str:
        if cmd in ("/status", "/preopen"):
            return engine.status_text()
        if cmd in ("/start", "/help", "/menu"):
            return Reply(HELP)
        if cmd == "/check":
            return engine.check_text()
        if cmd == "/review":  # the work in a thread through a reader: the trading loops never wait for a review
            return await asyncio.to_thread(review, engine, args, engine.review_reader())
        if cmd == "/rejects":
            try:
                hours = float(args[0]) if args else 24.0
            except ValueError:
                return "用法：/rejects（最近 24 小时）、/rejects 1（最近 1 小时）"
            hours = max(0.1, min(hours, 24 * 7))
            rate = engine.x.rate_status() if hasattr(engine.x, "rate_status") else None
            text = refusals.text(engine.refusal_log, engine.now(), hours, rate)
            if refusals.recent(engine.refusal_log, engine.now(), hours):
                return Reply(text, {"inline_keyboard": [[{"text": "🔬 现场检测", "callback_data": "chk_rejects"}]]})
            return text
        if cmd == "/chk_rejects":
            return await refusals.live_check(engine)
        if cmd.startswith("/set_cancel_"):
            try:
                seconds = int(cmd.removeprefix("/set_cancel_"))
            except ValueError:
                return "看不懂这个撤单时间"
            return await set_cancel(engine, console, seconds)
        if cmd == "/export":
            if not args:
                return export(engine, args)   # the time buttons
            return await asyncio.to_thread(export, engine, args, engine.review_reader())
        if cmd.startswith(("/exp_", "/rev_")):  # a time button under 📤 导出 or 📈 复盘
            key = cmd[5:]
            words = [RANGE_WORDS.get(key, key)]
            return await asyncio.to_thread(export if cmd.startswith("/exp_") else review, engine, words, engine.review_reader())
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
            n = engine.pause("Telegram 上点了 ⏸ 暂停")
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
        side_tasks.append(asyncio.create_task(tg.run(commander(settings, engine, pub, console)), name="telegram"))
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
