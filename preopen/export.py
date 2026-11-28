"""The review as one Excel file (Telegram's 📤 导出, the page's 导出 Excel): every table of the review for the windows
starting in a time span, the refusals, the settings and their changes, and a first sheet that says what each sheet is
for when tuning the settings."""
from __future__ import annotations

import gzip
import io
import json
import re
from typing import Any

from . import config, journal, refusals
from . import markets as mk
from .xlsx import Sheet, workbook

QUOTES_KEPT = 20_000      # the 全部报价 sheet: the latest this many quotes (all of them: /export csv)
RAW_PART_BYTES = 40 * 2**20   # a raw export's file at most this (Telegram takes 50 MB a file)
RAW_PARTS_MAX = 3             # files of it at most: a longer span is cut short, and said
RAW_FLUSH_EVERY = 1000        # rows between flushes of the compressor: its output measured that often
FILLS_KEPT = 50_000       # the 成交明细 sheet: the latest this many filled levels

GUIDE: tuple[tuple[str, str], ...] = (
    ("每日", "每天的成交量（份数、成交额）和盈亏：变好还是变坏；改参数前后对比（改的时间在「参数」表）"),
    ("余额", "每天北京时间 0 点自动记的账户：现金、挂单占用、未结算持仓（按成本）、待领取、合计。「比上一次」和前一天的已结算盈亏放在一起，"
             "差额是机器人账本以外的钱（充值提现、奖励返佣、手续费、跨零点结算的场次）：对账时的期初、期末"),
    ("模型EV", "实际盈亏和模型预期（每边按 50% 胜率）的差，够不够得上「不是运气」：z 值低于 −2 说明买到的那一边赢得比五五开少；"
               "下面是曲线的每个点（累计实际、累计模型、运气范围）"),
    ("周末", "周末（北京时间周六 0 点～周一 8 点）和工作日并排比；周末再拆成四段、按每个周末列：周末是不是在亏、亏在哪一段、"
             "是不是每个周末都这样。z 值低于 −2 的可以考虑周末不挂"),
    ("市场盈亏", "每一场（一个币的一个 5 分钟）几档合起来的盈亏：亏得多的场次有什么共同点（币种、时段、成交时刻）"),
    ("档位", "每个价格报价多少次、成交率、买到的那一边赢的比例。赢的比例低于保本比例（=价格）的价格在亏钱：降价或去掉这一档"),
    ("成交时刻", "成交离开场多久：越靠近开场越可能是知道方向的人在吃单。哪一行赢的比例低于保本，就把撤单提前到它之前"),
    ("撤单时间", "如果开场前 X 秒撤单会怎样（同一批场次重算），每一行和现在的设置比：★ 是最好的；还有可信度"),
    ("方向", "配成对的份数（锁定价差）和单边多出来的份数分开算：单边那部分赢的比例低于保本，就是被知道方向的人吃了"),
    ("速度", "第一档开放后几秒挂上：挂得慢的那几档双边率低、亏钱，说明排队靠后只吃到单边"),
    ("改参对比", "每次改参数，改之前和改之后一样长的时间各自的成交率、双边率、成交额、盈亏"),
    ("币种", "每个币的成交和盈亏：一直亏的币可以不挂"),
    ("时段", "按开场的小时（北京时间）：哪些时段亏，可以考虑那段时间不挂"),
    ("成交明细", "每一档的成交，含每笔成交的时刻（开场前几秒:份数），可以自己再筛选、透视"),
    ("全部报价", "每一次报价，成交没成交都有：上架后几秒、开放下单后几秒挂上（越快排队越靠前），怎么结束的"),
    ("拒单", "交易所拒单的原话和当时的情况（最近几百条）：429 多是下单太快，invalid token id 是市场刚开放"),
    ("参数", "现在的参数，每次改参数的时间、改了什么，以及交易逻辑每次更新的时间：对照「每日」看改完有没有变好"),
)


def file_name(label: str, now: float, head: str = "复盘", ext: str = "xlsx") -> str:
    """复盘_近7天_20261121-0930.xlsx: a name every system accepts."""
    tag = re.sub(r"[\\/:*?\"<>|\s]+", "", label.replace("～", "至").replace(":", "")) or "全部"
    return f"{head}_{tag}_{mk.dt.datetime.fromtimestamp(now, mk.BEIJING).strftime('%Y%m%d-%H%M')}.{ext}"


def jsonl_gz(rows: Any) -> tuple[list[bytes], int, bool]:
    """Rows as JSON Lines, gzipped, in parts of RAW_PART_BYTES at most (RAW_PARTS_MAX of them): (parts, rows written,
    cut short)."""
    parts: list[bytes] = []
    n, cut = 0, False
    buf = io.BytesIO()
    gz = gzip.GzipFile(fileobj=buf, mode="wb", mtime=0)
    for row in rows:
        gz.write((json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n").encode("utf-8"))
        n += 1
        if n % RAW_FLUSH_EVERY == 0:  # the compressor keeps its output until it is flushed: the size is known only then
            gz.flush()
        if buf.tell() >= RAW_PART_BYTES:
            gz.close()
            parts.append(buf.getvalue())
            if len(parts) >= RAW_PARTS_MAX:
                cut = True
                break
            buf = io.BytesIO()
            gz = gzip.GzipFile(fileobj=buf, mode="wb", mtime=0)
    else:
        gz.close()
        parts.append(buf.getvalue())
    return parts, n, cut


def named(parts: list[bytes], name: str) -> list[tuple[str, bytes]]:
    """账本原始记录_全部_20261128-2200.jsonl.gz, or _1, _2, … before the extension when in parts."""
    if len(parts) == 1:
        return [(name, parts[0])]
    head, dot, ext = name.partition(".")
    return [(f"{head}_{i}{dot}{ext}", part) for i, part in enumerate(parts, 1)]


def raw_ledger(read: Any, span: tuple[float, float] | None, label: str, now: float,
               notes: dict[str, Any]) -> tuple[list[tuple[str, bytes]], int, bool]:
    """The ledger's records of the windows starting within `span`, as they are stored: every field (the orders' ids and
    hashes, each fill with its time, how each order ended and why, the result, the claim, the notes), one JSON a line,
    gzipped (.jsonl.gz, in parts when long); and `notes` (the settings in force and their changes, the trading versions,
    the refusals) as one JSON. No key, token or wallet is in either. For an audit: the exchange's trades (/export 成交)
    match these records by order id. Returns (files, records, cut short)."""
    lo, hi = span or (float("-inf"), float("inf"))
    parts, n, cut = jsonl_gz(read(lo, hi))
    files = named(parts, file_name(label, now, "账本原始记录", "jsonl.gz"))
    files.append((file_name(label, now, "账本附加", "json"), json.dumps(notes, ensure_ascii=False, indent=1).encode("utf-8")))
    return files, n, cut


def overview(review: dict[str, Any], label: str, now: float, mode: str, quotes: int, quotes_all: int, fills: int) -> Sheet:
    """The first sheet: the span's totals, the cancel-time conclusion, and what each sheet is for."""
    r, c = review["totals"]["range"], review["cancel"]
    rows: list[list[Any]] = [
        ["预挂复盘", ""], ["时间范围（按开场，北京时间）", label], ["导出时间（北京时间）", mk.hm(now)], ["模式", mode], [],
        ["所选时段合计", ""], ["报价场次", r["quoted"]], ["成交场次", r["filled"]], ["成交率%", r["fill_rate"]], ["双边成交场次", r["both"]],
        ["成交份数", r["shares"]], ["成交额$（份数×买价，没结算的也算）", r["volume"]], ["赢的场次", r["wins"]], ["输的场次", r["losses"]],
        ["待结算场次", r["open"]], ["已结算成本$", r["cost"]], ["回款$", r["payout"]], ["盈亏$（已结算）", r["pnl"]], ["收益率%", r["roi"]], [],
        ["撤单时间", c["advice"]],
        *([["成交未核实", line]] if (line := journal.unverified_text(review)) else []),
        ["可信度", f"{(c.get('confidence') or {}).get('level', '低')}：{'；'.join((c.get('confidence') or {}).get('notes') or [])}"],
    ]
    for r in (review.get("weekend") or {}).get("split", []):
        if r["filled"]:
            z = f"{r['z']:+.1f}σ" if r["z"] is not None else "—"
            rows.append([f"{r['name']}表现", f"盈亏 {r['pnl']:+.2f}（单场均 {r['avg_pnl'] or 0:+.2f}），模型 {r['ev']:+.2f}（{z}），"
                                            f"成交 {r['filled']}/{r['quoted']} 场，双边率 {r['both_rate']}%（见「周末」表）"])
    model = review.get("model") or {}
    if (model.get("total") or {}).get("windows"):
        t = model["total"]
        rows.append(["模型对实际", f"模型 EV {t['ev']:+.2f}（每边按 50% 胜率），实际 {t['pnl']:+.2f}。{model['verdict']}（见「模型EV」表）"])
    if c.get("losses"):
        rows.append(["亏在哪（成交时刻）", "；".join(f"{t['bucket']} {t['pnl']:+.2f}（买到的一边赢 {t['win_rate'] * 100:.0f}%，"
                                                  f"保本 {t['breakeven'] * 100:.0f}%）" for t in c["losses"])])
    losing = [p for p in review["by_price"] if p["win_rate"] is not None and p["win_rate"] < p["breakeven"]]
    if losing:
        rows.append(["亏钱的价格", "、".join(f"{p['price']:g}¢（赢 {p['win_rate']:g}% < 保本 {p['breakeven']:g}%）" for p in losing)])
    if quotes < quotes_all:
        rows.append(["注意", f"「全部报价」只放了最近 {quotes} 条（共 {quotes_all} 条）；全部的发 /export csv"])
    if fills >= FILLS_KEPT:
        rows.append(["注意", f"「成交明细」只放了最近 {FILLS_KEPT} 条；全部的发 /export csv"])
    rows += [[], ["每张表看什么", ""]]
    title = len(rows) - 1
    rows += [[name, text] for name, text in GUIDE]
    return Sheet("总览", rows, header=False, bold={0, 5, 20, title})


def changes_for(records: Any, now: float, log: list[dict], coins: list[str] | None, day_type: str = "all") -> list[dict]:
    """The settings changes with the time before and after each, through the reader (or a list)."""
    def read(lo: float, hi: float, newest: bool = False) -> Any:
        if callable(records):
            return records(lo, hi, newest)
        return [q for q in records if lo <= q["start"] < hi]
    return journal.change_effects(read, now, log, coins, day_type=day_type)


def settings_sheet(values: dict[str, str], log: list[dict], versions: list[dict] | None = None) -> Sheet:
    """The settings now, then every change kept, the latest first, then the versions of the trading code."""
    labels = {k: label for k, label, _ in config.CONTROL_KEYS}
    rows: list[list[Any]] = [["参数", "现在的值", "变量名"]] + [[labels.get(k, k), v or "（空）", k] for k, v in values.items()]
    rows.append([])
    title = len(rows)
    rows.append(["什么时候改的（北京时间）", "怎么改的", "参数", "改之前", "改之后"])
    for entry in sorted(log, key=lambda e: -float(e.get("at") or 0)):
        for name, old, new in entry.get("changes") or []:
            rows.append([mk.hm(float(entry["at"])), entry.get("via") or "", labels.get(name, name), old or "（空）", new or "（空）"])
    if not log:
        rows.append(["（还没有改过：从这个版本起每次改参数都会记下来）"])
    bold = {title}
    if versions:
        rows.append([])
        bold.add(len(rows))
        rows.append(["交易逻辑什么时候更新的（北京时间）", "改了什么"])
        rows += [[mk.hm(float(v["at"])), v.get("note") or ""] for v in sorted(versions, key=lambda v: -float(v.get("at") or 0))]
    return Sheet("参数", rows, bold=bold)


BALANCE_HEAD = ["日期", "记下的时刻（北京时间）", "模式", "现金 pUSD$", "挂单占用$（含在现金里）", "挂单张数", "未结算持仓$（按成本）", "待领取$",
                "合计$（现金+未结算+待领取）", "比上一次$", "前一天已结算盈亏$", "差额$（充值提现、奖励、费用、跨零点结算）"]


def balance_table(balances: list[dict], review: dict[str, Any], span: tuple[float, float] | None) -> list[list[Any]]:
    """The daily records (Engine.note_balance) in the span and the one just after it (the span's end): each beside the
    one before it, and the day before's settled P&L (the review's, else as known at the time): the difference is money
    the ledger does not hold."""
    lo, hi = span or (float("-inf"), float("inf"))
    by_day = {d["day"]: d["pnl"] for d in review.get("by_day") or []}
    rows: list[list[Any]] = [BALANCE_HEAD]
    prev = None
    for b in balances:
        at = float(b.get("at") or 0)
        if lo <= at < hi + 86400:
            change = round(float(b["total"]) - float(prev["total"]), 4) if prev else None
            before = (mk.dt.date.fromisoformat(b["day"]) - mk.dt.timedelta(days=1)).isoformat()
            pnl = by_day.get(before, b.get("pnl_day_before"))
            rows.append([b["day"], mk.hm(at), b.get("mode", ""), b.get("cash"), b.get("reserved"), b.get("open_orders"),
                         b.get("unsettled"), b.get("unclaimed"), b.get("total"), change, pnl,
                         round(change - float(pnl), 4) if change is not None and pnl is not None else None])
        prev = b
    if len(rows) == 1:
        rows.append(["这段时间还没有记录：每天北京时间 0 点自动记一次（11-28 加上的，从那以后才有）"])
    return rows


def build(records: Any, now: float, current: int | None, span: tuple[float, float] | None, label: str, *,
          refusal_log: list[dict], settings: dict[str, str], settings_log: list[dict], mode: str,
          coins: list[str] | None = None, versions: list[dict] | None = None, day_type: str = "all",
          balances: list[dict] | None = None) -> tuple[bytes, dict[str, Any]]:
    """The workbook of the windows starting within `span` (all of them for None) of the coins named, and their totals.
    `records`: a list, or the engine's reader read(lo, hi, newest_first) (each part reads what it needs: the review in
    start order, the latest quotes and fills the newest first; the ledger never sits in memory whole)."""
    lo, hi = span or (float("-inf"), float("inf"))
    wanted = set(coins or [])

    def stream(newest: bool = False, days: bool = False) -> Any:
        """The records of the span and coins; with `days`, of the day filter too (the review applies it itself:
        its weekend sheet sets the weekend beside the weekdays whatever the filter)."""
        source = journal.within(records, span) if isinstance(records, list) else records(lo, hi, newest)
        keep = (lambda q: (not wanted or q["coin"] in wanted) and (not days or journal.day_ok(q["start"], day_type)))
        return [q for q in source if keep(q)] if isinstance(source, list) else (q for q in source if keep(q))
    with journal.quiet_gc():
        review = journal.journal(stream(), now, current, span, day_type=day_type)
        cancel = review["cancel"]
        changes = changes_for(records, now, settings_log, coins, day_type)
        quotes, quotes_all = journal.quotes_rows(stream(True, days=True), QUOTES_KEPT)
        fills = journal.detail_table(stream(True, days=True), FILLS_KEPT)
        model_rows, model_bold = journal.model_table(review)
        weekend_rows, weekend_bold = journal.weekend_table(review)
    return workbook([
        overview(review, label, now, mode, len(quotes) - 1, quotes_all, len(fills) - 1),
        Sheet("每日", journal.daily_table(review)),
        Sheet("余额", balance_table(list(balances or []), review, span)),
        Sheet("模型EV", model_rows, header=False, bold=model_bold),
        Sheet("周末", weekend_rows, header=False, bold=weekend_bold),
        Sheet("市场盈亏", journal.markets_table(review)),
        Sheet("档位", journal.price_table(review)),
        Sheet("成交时刻", journal.fill_time_table(cancel)),
        Sheet("撤单时间", journal.replay_table(cancel) + [[]] + journal.cancel_notes(cancel)),
        Sheet("方向", journal.side_table(review)),
        Sheet("速度", journal.speed_table(review)),
        Sheet("改参对比", journal.changes_table(changes)),
        Sheet("币种", journal.coin_table(review)),
        Sheet("时段", journal.hour_table(review)),
        Sheet("成交明细", fills),
        Sheet("全部报价", quotes),
        Sheet("拒单", refusals.table(refusal_log, span)),
        settings_sheet(settings, settings_log, versions),
    ]), review["totals"]["range"]


def from_engine(engine: Any, span: tuple[float, float] | None, label: str, read: Any,
                coins: list[str] | None = None, day_type: str = "all") -> tuple[str, bytes, dict[str, Any]]:
    """The file name, the workbook and the span's totals, from the engine's reader (engine.review_reader(), made on the
    event loop), refusal log and settings. Meant for a thread of its own."""
    now = engine.now()
    data, totals = build(read, now, engine.s.cancel_s, span, label, refusal_log=list(engine.refusal_log),
                         settings=config.current_values(engine.s), settings_log=list(engine.settings_log), mode=engine.s.mode(),
                         coins=coins, versions=list(getattr(engine, "version_log", [])), day_type=day_type,
                         balances=list(getattr(engine, "balance_log", [])))
    return file_name(label, now), data, totals
