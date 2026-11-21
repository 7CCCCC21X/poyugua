"""The review as one Excel file (Telegram's 📤 导出, the page's 导出 Excel): every table of the review for the windows
starting in a time span, the refusals, the settings and their changes, and a first sheet that says what each sheet is
for when tuning the settings."""
from __future__ import annotations

import re
from typing import Any

from . import config, journal, refusals
from . import markets as mk
from .xlsx import Sheet, workbook

QUOTES_KEPT = 20_000      # the 全部报价 sheet: the latest this many quotes (all of them: /export csv)

GUIDE: tuple[tuple[str, str], ...] = (
    ("每日", "每天的成交量（份数、成交额）和盈亏：变好还是变坏；改参数前后对比（改的时间在「参数」表）"),
    ("市场盈亏", "每一场（一个币的一个 5 分钟）几档合起来的盈亏：亏得多的场次有什么共同点（币种、时段、成交时刻）"),
    ("档位", "每个价格报价多少次、成交率、买到的那一边赢的比例。赢的比例低于保本比例（=价格）的价格在亏钱：降价或去掉这一档"),
    ("成交时刻", "成交离开场多久：越靠近开场越可能是知道方向的人在吃单。哪一行赢的比例低于保本，就把撤单提前到它之前"),
    ("撤单时间", "如果开场前 X 秒撤单会怎样（同一批场次重算）：★ 是这些记录里最好的撤单时间"),
    ("币种", "每个币的成交和盈亏：一直亏的币可以不挂"),
    ("时段", "按开场的小时（北京时间）：哪些时段亏，可以考虑那段时间不挂"),
    ("成交明细", "每一档的成交，含每笔成交的时刻（开场前几秒:份数），可以自己再筛选、透视"),
    ("全部报价", "每一次报价，成交没成交都有：上架后几秒、开放下单后几秒挂上（越快排队越靠前），怎么结束的"),
    ("拒单", "交易所拒单的原话和当时的情况（最近几百条）：429 多是下单太快，invalid token id 是市场刚开放"),
    ("参数", "现在的参数，和每次改参数的时间、改了什么：对照「每日」看改完有没有变好"),
)


def file_name(label: str, now: float) -> str:
    """复盘_近7天_20261121-0930.xlsx: a name every system accepts."""
    tag = re.sub(r"[\\/:*?\"<>|\s]+", "", label.replace("～", "至").replace(":", "")) or "全部"
    return f"复盘_{tag}_{mk.dt.datetime.fromtimestamp(now, mk.BEIJING).strftime('%Y%m%d-%H%M')}.xlsx"


def overview(review: dict[str, Any], label: str, now: float, mode: str, quotes: int, quotes_all: int) -> Sheet:
    """The first sheet: the span's totals, the cancel-time conclusion, and what each sheet is for."""
    r, c = review["totals"]["range"], review["cancel"]
    rows: list[list[Any]] = [
        ["预挂复盘", ""], ["时间范围（按开场，北京时间）", label], ["导出时间（北京时间）", mk.hm(now)], ["模式", mode], [],
        ["所选时段合计", ""], ["报价场次", r["quoted"]], ["成交场次", r["filled"]], ["成交率%", r["fill_rate"]], ["双边成交场次", r["both"]],
        ["成交份数", r["shares"]], ["成交额$（份数×买价，没结算的也算）", r["volume"]], ["赢的场次", r["wins"]], ["输的场次", r["losses"]],
        ["待结算场次", r["open"]], ["已结算成本$", r["cost"]], ["回款$", r["payout"]], ["盈亏$（已结算）", r["pnl"]], ["收益率%", r["roi"]], [],
        ["撤单时间", c["advice"]],
    ]
    if c.get("losses"):
        rows.append(["亏在哪（成交时刻）", "；".join(f"{t['bucket']} {t['pnl']:+.2f}（买到的一边赢 {t['win_rate'] * 100:.0f}%，"
                                                  f"保本 {t['breakeven'] * 100:.0f}%）" for t in c["losses"])])
    losing = [p for p in review["by_price"] if p["win_rate"] is not None and p["win_rate"] < p["breakeven"]]
    if losing:
        rows.append(["亏钱的价格", "、".join(f"{p['price']:g}¢（赢 {p['win_rate']:g}% < 保本 {p['breakeven']:g}%）" for p in losing)])
    if quotes < quotes_all:
        rows.append(["注意", f"「全部报价」只放了最近 {quotes} 条（共 {quotes_all} 条）；全部的发 /export csv"])
    rows += [[], ["每张表看什么", ""]]
    title = len(rows) - 1
    rows += [[name, text] for name, text in GUIDE]
    return Sheet("总览", rows, header=False, bold={0, 5, 20, title})


def settings_sheet(values: dict[str, str], log: list[dict]) -> Sheet:
    """The settings now, then every change kept, the latest first."""
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
    return Sheet("参数", rows, bold={title})


def build(records: list[dict], now: float, current: int | None, span: tuple[float, float] | None, label: str, *,
          refusal_log: list[dict], settings: dict[str, str], settings_log: list[dict], mode: str) -> tuple[bytes, dict[str, Any]]:
    """The workbook of the windows starting within `span` (all of them for None), and the span's totals."""
    chosen = journal.within(records, span)
    review = journal.journal(records, now, current, span)
    cancel = review["cancel"]
    quotes = journal.quotes_table(chosen, QUOTES_KEPT)
    quotes_all = sum(1 for q in chosen if q.get("placed") or any(journal.filled(q).values()))
    return workbook([
        overview(review, label, now, mode, len(quotes) - 1, quotes_all),
        Sheet("每日", journal.daily_table(review)),
        Sheet("市场盈亏", journal.markets_table(review)),
        Sheet("档位", journal.price_table(review)),
        Sheet("成交时刻", journal.fill_time_table(cancel)),
        Sheet("撤单时间", journal.replay_table(cancel) + [[]] + journal.cancel_notes(cancel)),
        Sheet("币种", journal.coin_table(review)),
        Sheet("时段", journal.hour_table(review)),
        Sheet("成交明细", journal.detail_table(chosen)),
        Sheet("全部报价", quotes),
        Sheet("拒单", refusals.table(refusal_log, span)),
        settings_sheet(settings, settings_log),
    ]), review["totals"]["range"]


def from_engine(engine: Any, span: tuple[float, float] | None, label: str) -> tuple[str, bytes, dict[str, Any]]:
    """The file name, the workbook and the span's totals, from the engine's ledger, refusal log and settings."""
    now = engine.now()
    data, totals = build(engine.all_records(), now, engine.s.cancel_s, span, label, refusal_log=list(engine.refusal_log),
                         settings=config.current_values(engine.s), settings_log=list(engine.settings_log), mode=engine.s.mode())
    return file_name(label, now), data, totals
