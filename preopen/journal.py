"""The console's order list and review (复盘): plain functions over the ledger's records (the working set and the archive).

A record is one window level and attempt (ledger.py). Its money: the filled shares of each side at its price are the
cost; the winning side's shares pay $1 each. P&L is settled records only (the official result known).
"""
from __future__ import annotations

import collections
import contextlib
import csv
import datetime as dt
import gc
import io
import math
import re
import threading
from bisect import bisect_right
from collections import defaultdict
from decimal import Decimal as D
from typing import Any, Callable, Iterable, Iterator

from . import markets as mk

PHASES = {"active": "挂着", "closing": "撤单中", "closed": "已撤，待结算", "settled": "已结束"}
STATES = {"open": "挂着", "placing": "发送中", "unknown": "状态不明", "gone": "已撤/核对中", "final": "已结束", "rejected": "被拒",
          "not_sent": "没发出", "not_found": "没挂上", "prepared": "准备中"}
LEAD_BUCKETS = ((60, "开场前 1 分钟内"), (300, "1～5 分钟"), (1800, "5～30 分钟"), (7200, "30 分钟～2 小时"), (21600, "2～6 小时"),
                (float("inf"), "6 小时以上"))
TABS = ("open", "filled", "closed", "all")
CANCEL_CHOICES = (10, 15, 20, 30, 45, 60, 90, 120, 180, 240, 300, 420, 600, 900, 1800)   # seconds before the start
FILL_TIMES = ((0, "开场之后（不该有）"), (15, "开场前 0～15 秒"), (30, "15～30 秒"), (60, "30～60 秒"), (120, "1～2 分钟"),
              (300, "2～5 分钟"), (900, "5～15 分钟"), (float("inf"), "15 分钟以上"))
RANGES = {"all": "全部", "today": "今天", "yesterday": "昨天", "7d": "近 7 天", "30d": "近 30 天", "update": "上次更新起"}   # the time filter
UPDATE_WORDS = ("更新", "上次更新", "上次更新起", "更新起", "update")   # Telegram's word for the span since the last update
COIN_RHO = 0.62           # the coins' results of one window go together: two of them agreed in 81% of 36,288 pairs
                          # (11-20 to 11-25, seven coins), a correlation of 0.62. The luck of a start's fills counts it.
RESULTS = (*mk.SIDES, mk.SPLIT)   # a settled record's result: a side won, or the market paid 50/50
DAY_TYPES = {"all": "全部", "weekend": "仅周末", "weekday": "仅工作日"}   # the review's day filter
DAY_WORDS = {"周末": "weekend", "weekend": "weekend", "工作日": "weekday", "平日": "weekday", "weekday": "weekday"}
WEEKDAY_CN = ("周一", "周二", "周三", "周四", "周五", "周六", "周日")
WEEKEND_PARTS = (("sat_early", "周六 00–05 点（美股周五下午还开着）"), ("sat_main", "周六 05–24 点（美股收盘后）"),
                 ("sun_all", "周日全天"), ("mon_early", "周一 00–08 点（期货周日晚开盘前后）"))
WEEKEND_RULE = "北京时间周六 00:00 ～ 周一 08:00，按开场时间（和参数「周末也挂」用的同一条线）"
EACH_WEEKEND_IN_TEXT = 4  # Telegram's weekend review: this many weekends, the latest first
MAX_DAYS = 3650           # /review 7, /export 30: a number of days back, at most this many
MODEL_MIN = 30            # settled windows with a fill before the model's comparison is called anything but thin
MODEL_STEPS = ((12 * 3600, 300), (7 * 86400, 3600))   # the curve's points: a window each up to 12 hours, an hour each up
                          # to 7 days, a (Beijing) day each beyond
MARKETS_SHOWN = 500       # windows listed on the page (all of them in the CSV)
DETAIL_SHOWN = 200        # the fills listed one by one on the page (all of them in the CSV)
DAYS_IN_TEXT = 7          # Telegram's review: this many days, and the markets that lost and won most, this many each
MARKETS_IN_TEXT = 3
CANCEL_SAMPLE = 20        # filled windows before the review names a best cancel time
END_SLACK_S = 2.0         # a cancel confirmed this much earlier than a candidate still counts as resting until it
APPROX_MAX = 0.3          # fills timed by a snapshot above this share of the replay's: low confidence, no recommendation
LAG_MAX_S = 30.0          # a window's cancel lag (decided to confirmed) above this is taken as this in the replay


def num(value: Any) -> D:
    return D(str(value or 0))


def filled(q: dict) -> dict[str, D]:
    return {side: num(o.get("filled")) for side, o in (q.get("orders") or {}).items()}


def day_of(ts: float) -> str:
    return dt.datetime.fromtimestamp(ts, mk.BEIJING).strftime("%Y-%m-%d")


def hour_of(ts: float) -> int:
    return dt.datetime.fromtimestamp(ts, mk.BEIJING).hour


def day_start(ts: float) -> float:
    """00:00 Beijing time of the day `ts` falls in."""
    d = dt.datetime.fromtimestamp(ts, mk.BEIJING)
    return dt.datetime(d.year, d.month, d.day, tzinfo=mk.BEIJING).timestamp()


WHEN = re.compile(r"^(?:(\d{4})-)?(\d{1,2})-(\d{1,2})(?:[ T](\d{1,2}):(\d{2}))?$")


def parse_when(text: str, now: float) -> tuple[float, bool]:
    """A moment the operator typed, Beijing time: 2026-11-20, 11-20, 11/20, 2026-11-20T08:30 (the page's picker),
    11-20 08:30. Returns (the moment, whether a time of day was given). A month and day without a year is the latest
    such date not after tomorrow (typed in January, 12-30 is last December's). ValueError when it is not one."""
    m = WHEN.match(text.strip().replace("/", "-").replace("：", ":"))
    if not m:
        raise ValueError(f"看不懂的时间：{text}（写成 11-20、2026-11-20 或 11-20 08:30）")
    year, month, day, hour, minute = m.groups()
    this_year = dt.datetime.fromtimestamp(now, mk.BEIJING).year
    try:
        moment = dt.datetime(int(year or this_year), int(month), int(day), int(hour or 0), int(minute or 0), tzinfo=mk.BEIJING)
        if not year and moment.timestamp() > day_start(now) + 2 * 86400:
            moment = moment.replace(year=this_year - 1)
    except ValueError:
        raise ValueError(f"没有这个时间：{text}") from None
    return moment.timestamp(), hour is not None


def preset_span(name: str, now: float) -> tuple[float, float] | None:
    """The filter's presets, by the windows' start (Beijing days): today and the days before it; None is all."""
    today = day_start(now)
    return {"today": (today, today + 86400), "yesterday": (today - 86400, today), "7d": (today - 6 * 86400, today + 86400),
            "30d": (today - 29 * 86400, today + 86400)}.get(name)


def custom_span(frm: str, to: str, now: float) -> tuple[float, float]:
    """From one moment to another, the windows starting from `frm` (a date: its 00:00) up to `to` (a date: the end of
    that day; a time: until then, the window starting then not included)."""
    lo, _ = parse_when(frm, now)
    hi, timed = parse_when(to, now)
    if not timed:
        hi += 86400
    if hi <= lo:
        raise ValueError("结束时间要晚于开始时间")
    return lo, hi


def span_label(span: tuple[float, float] | None) -> str:
    """11-20, 11-18 ～ 11-20 (whole days), or 11-20 08:00 ～ 11-20 12:00."""
    if span is None:
        return "全部"
    lo, hi = span
    if hi == float("inf"):
        return f"{mk.hm(lo)} 以来"
    if lo == day_start(lo) and hi == day_start(hi):
        first, last = day_of(lo)[5:], day_of(hi - 86400)[5:]
        return first if first == last else f"{first} ～ {last}"
    return f"{mk.hm(lo)} ～ {mk.hm(hi)}"


def update_label(point: dict[str, Any]) -> str:
    """上次更新（11-25 17:00 版本更新）以来: which update a span starts from."""
    return f"上次更新（{mk.hm(float(point['at']))} {point.get('kind') or ''}）以来".replace(" ）", "）")


def span_from(name: str, frm: str, to: str, now: float,
              updates: list[dict[str, Any]] | None = None) -> tuple[tuple[float, float] | None, dict[str, Any]]:
    """The page's filter (range=today|yesterday|7d|30d|all, custom with from/to, update: since the latest update, or
    since with from: since that moment): the span and how to show it, the bounds as the picker writes them, and the
    update points to choose from (`updates`: Engine.update_points(), the latest first). ValueError when the bounds are
    not times. update with no update kept is everything."""
    name = name if name in RANGES or name in ("custom", "since") else "all"
    updates = updates or []
    label = ""
    if name == "custom":
        span: tuple[float, float] | None = custom_span(frm, to, now)
    elif name == "since":
        span = (parse_when(frm, now)[0], float("inf"))
    elif name == "update":
        span = (float(updates[0]["at"]), float("inf")) if updates else None
        label = update_label(updates[0]) if updates else "全部（还没有更新记录）"
    else:
        span = preset_span(name, now)

    def picker(ts: float) -> str:
        return dt.datetime.fromtimestamp(ts, mk.BEIJING).strftime("%Y-%m-%dT%H:%M")
    presets = {k: dict(zip(("from", "to"), map(picker, preset_span(k, now) or (now, now)))) for k in RANGES if k not in ("all", "update")}
    label = label or (RANGES.get(name) if name not in ("custom", "since", "all") and span else span_label(span))
    return span, {"range": name, "label": label, "from": picker(span[0]) if span else "",
                  "to": picker(span[1]) if span and span[1] != float("inf") else "", "presets": presets,
                  "updates": [{**u, "from": picker(float(u["at"]))} for u in updates]}


def within(records: Iterable[dict], span: tuple[float, float] | None) -> list[dict]:
    """The records of the windows starting within the span (all of them for None)."""
    if span is None:
        return list(records)
    lo, hi = span
    return [q for q in records if lo <= q["start"] < hi]


def day_ok(start: float, day_type: str) -> bool:
    """Whether a window starting then is in the day filter: all, the weekend (markets.on_weekend, the rule
    PREOPEN_WEEKEND=off skips by), or the rest of the week."""
    if day_type == "weekend":
        return mk.on_weekend(int(start))
    if day_type == "weekday":
        return not mk.on_weekend(int(start))
    return True


def weekend_part(start: float) -> str | None:
    """Which stretch of the weekend a window starting then is in (WEEKEND_PARTS), None on a weekday."""
    t = dt.datetime.fromtimestamp(start, mk.BEIJING)
    if t.weekday() == 5:
        return "sat_early" if t.hour < 5 else "sat_main"
    if t.weekday() == 6:
        return "sun_all"
    if t.weekday() == 0 and t.hour < 8:
        return "mon_early"
    return None


def weekend_of(start: float) -> str:
    """The weekend a window starting then belongs to: its Saturday's date (Monday before 08:00 is the Saturday before)."""
    t = dt.datetime.fromtimestamp(start, mk.BEIJING)
    return (t.date() - dt.timedelta(days=(t.weekday() - 5) % 7)).isoformat()


def day_args(args: list[str]) -> tuple[str, list[str]]:
    """Telegram's words for the day filter (周末, 工作日): the filter and the words left."""
    day_type, rest = "all", []
    for a in args:
        word = DAY_WORDS.get(a.strip().lower())
        if word:
            day_type = word
        else:
            rest.append(a)
    return day_type, rest


def weekday_label(day: str, day_type: str = "all") -> str:
    """周六; a Monday under a day filter says which part of it is in: 周一 00–08 点 (weekend) or 周一 08 点后 (weekdays)."""
    name = WEEKDAY_CN[dt.date.fromisoformat(day).weekday()]
    if name == "周一" and day_type == "weekend":
        return "周一 00–08 点"
    if name == "周一" and day_type == "weekday":
        return "周一 08 点后"
    return name


def span_args(args: list[str], now: float,
              updates: list[dict[str, Any]] | None = None) -> tuple[tuple[float, float] | None, str, list[str]]:
    """Telegram's time words: 今天, 昨天, a number of days back (rolling), a date (11-20) or two (11-18 11-20, or
    11-18~11-20), 更新 (since the latest update: a settings change or a new version, `updates` the latest first).
    Returns the span, its label ("" for all) and the words left (the coins). ValueError on a bad date, or on 更新
    with no update kept."""
    span: tuple[float, float] | None = None
    label, dates, rest = "", [], []
    for a in args:
        key = a.strip().lower()
        if key in ("全部", "all"):
            span, label = None, RANGES["all"]
            continue
        if key in UPDATE_WORDS:
            if not updates:
                raise ValueError("还没有更新记录（改过参数或更新过版本以后才有）")
            span, label = (float(updates[0]["at"]), float("inf")), update_label(updates[0])
            continue
        if key in ("今天", "today", "昨天", "yesterday"):
            name = "today" if key in ("今天", "today") else "yesterday"
            span, label = preset_span(name, now), RANGES[name]
            continue
        try:
            days = float(key)
        except ValueError:
            pass
        else:  # audit 11-27: 0 or less was ignored without a word, a huge number crashed the time's formatting
            if not (math.isfinite(days) and 0 < days <= MAX_DAYS):
                raise ValueError(f"天数要在 0～{MAX_DAYS} 之间（不含 0）：{a}")
            span, label = (now - days * 86400, float("inf")), f"最近 {mk.fmt(D(str(days)))} 天"
            continue
        parts = re.split(r"[~～至到]", key)
        if all(WHEN.match(x.strip().replace("/", "-")) for x in parts if x.strip()) and any(x.strip() for x in parts):
            dates += [x.strip() for x in parts if x.strip()]
            continue
        rest.append(a)
    if dates:
        span = custom_span(dates[0], dates[-1], now)
        label = span_label(span)
    return span, label, rest


def lead_bucket(seconds: float) -> str:
    for limit, name in LEAD_BUCKETS:
        if seconds < limit:
            return name
    return LEAD_BUCKETS[-1][1]


def row_of(q: dict, now: float) -> dict[str, Any]:
    f = filled(q)
    placed = q.get("placed")
    return {
        "key": q["key"], "coin": mk.name_of(q["coin"]), "when": mk.hm(q["start"]), "start": q["start"],
        "in": mk.span(q["start"] - now) if q["start"] > now else "已开场", "level": int(q["level"]) + 1,
        "price": float(num(q["price"]) * 100), "shares": float(num(q["shares"])), "attempt": int(q.get("attempt") or 0) + 1,
        "phase": q["phase"], "phase_text": PHASES.get(q["phase"], q["phase"]), "reason": q.get("reason") or "",
        "up": STATES.get(q["orders"]["up"]["state"], q["orders"]["up"]["state"]), "down": STATES.get(q["orders"]["down"]["state"], q["orders"]["down"]["state"]),
        "up_filled": float(f.get("up", 0)), "down_filled": float(f.get("down", 0)),
        "placed_lead": mk.span(q["start"] - float(placed)) if placed else "",
        "result": mk.RESULT_CN.get(str(q.get("result") or ""), ""), "pnl": q.get("pnl"), "redeem": q.get("redeem") or "",
        "can_withdraw": q["phase"] == "active",
    }


def by_start(records: Iterable[dict]) -> Iterator[dict]:
    """Records in the order of their window's start: a list is sorted; anything else is taken as it comes (the ledger's
    reader yields them in that order). The review goes through them once and closes each window as the starts move
    past it, so months of records never sit in memory at once."""
    if isinstance(records, (list, tuple)):
        return iter(sorted(records, key=lambda q: q["start"]))
    return iter(records)


_quiet_lock = threading.Lock()
_quiet_users = 0
_quiet_was = True


@contextlib.contextmanager
def quiet_gc():
    """No cyclic garbage collection while a review is worked out (in its thread): a full collection walks every object
    of the process and holds every thread meanwhile (11-21: 0.8 s on a 60-day ledger, the trading loops with it).
    Reference counting frees the review's objects as usual; the collector runs again once the last review is done."""
    global _quiet_users, _quiet_was
    with _quiet_lock:
        if _quiet_users == 0:
            _quiet_was = gc.isenabled()
            gc.disable()
        _quiet_users += 1
    try:
        yield
    finally:
        with _quiet_lock:
            _quiet_users -= 1
            if _quiet_users == 0 and _quiet_was:
                gc.enable()


def orders(records: Iterable[dict], tab: str, coin: str, now: float, limit: int = 300) -> dict[str, Any]:
    """The console's order table: open (active and closing, the soonest first), filled, closed without fills, or all
    (the latest first), optionally one coin; with the count of every tab. Records in start order (by_start): only the
    rows shown are kept."""
    tab = tab if tab in TABS else "open"
    counts = {t: 0 for t in TABS}
    kept: collections.deque = collections.deque(maxlen=limit)
    soonest: list[dict] = []
    for q in by_start(records):
        if (coin and q["coin"] != coin) or q.get("foreign"):
            continue
        is_open, has_fill = q["phase"] in ("active", "closing"), any(filled(q).values())
        tabs = ["all", "open" if is_open else "filled" if has_fill else "closed"] + (["filled"] if is_open and has_fill else [])
        for t in tabs:
            counts[t] += 1
        if tab in tabs:
            if tab == "open":
                soonest.append(q)
            else:
                kept.append(q)
    chosen = sorted(soonest, key=lambda q: (q["start"], q["coin"], q["level"])) if tab == "open" else sorted(kept, key=lambda q: (q["start"], q["coin"], q["level"]), reverse=True)
    return {"tab": tab, "coin": coin, "counts": counts, "rows": [row_of(q, now) for q in chosen[:limit]],
            "more": max(0, counts[tab] - limit)}


def _blank() -> dict[str, Any]:
    return {"quoted": 0, "upcoming": 0, "filled": 0, "filled_started": 0, "both": 0, "wins": 0, "losses": 0, "evens": 0,
            "open": 0, "cost": D(0), "payout": D(0), "pnl": D(0), "shares": D(0), "volume": D(0)}


def _finish(t: dict[str, Any]) -> dict[str, Any]:
    """quoted: the windows quoted that have started (their quoting over: the fill rate's base); upcoming: quoted, not
    started yet (11-21 review: a day of pre-listed windows resting ahead diluted the fill rate to a tenth); filled: the
    windows with a fill (an upcoming one too: orders rest for hours); cost, payout, pnl: the settled windows' (the roi on
    them); shares, volume: everything bought, settled or not; both_rate: of the windows filled, those filled on both
    sides; avg_pnl: the P&L of a settled filled window, on average."""
    out = dict(t)
    out["fill_rate"] = round(t["filled_started"] / t["quoted"] * 100, 1) if t["quoted"] else None
    out["both_rate"] = round(t["both"] / t["filled"] * 100, 1) if t["filled"] else None
    settled = t["wins"] + t["losses"] + t["evens"]
    out["avg_pnl"] = float(round(t["pnl"] / settled, 4)) if settled else None
    out["roi"] = round(float(t["pnl"] / t["cost"] * 100), 2) if t["cost"] else None
    for k in ("cost", "payout", "pnl", "volume"):
        out[k] = float(round(t[k], 4))
    out["shares"] = float(t["shares"])
    return out


SPEED = ((0.5, "开放后 0.5 秒内"), (1.0, "0.5～1 秒"), (2.0, "1～2 秒"), (5.0, "2～5 秒"), (60.0, "5～60 秒"))
SPEED_LATE = "开放 60 秒后（补挂、接管、重启后）"


def _speed_blank(name: str) -> dict[str, Any]:
    return {"bucket": name, "quotes": 0, "filled": 0, "both": 0, "shares": D(0), "cost": D(0), "payout": D(0)}


def _window(q: dict, started: bool, inside: bool, buckets: list[dict]) -> dict[str, Any]:
    return {"buckets": buckets, "sides": set(), "cost": D(0), "payout": D(0), "settled": True, "started": started,
            "inside": inside, "q": q, "levels": set(), "result": "", "volume": D(0), "recs": [],
            "shares": {side: D(0) for side in mk.SIDES}, "paid": {side: D(0) for side in mk.SIDES}}


def _count(b: dict[str, Any], w: dict[str, Any]) -> None:
    """A window into a bucket's counts, once (its levels and attempts together)."""
    if w["started"]:
        b["quoted"] += 1
    else:
        b["upcoming"] += 1
    if not w["sides"]:
        return
    b["filled"] += 1
    b["filled_started"] += 1 if w["started"] else 0
    b["both"] += 1 if len(w["sides"]) == len(mk.SIDES) else 0
    if w["settled"]:
        b["wins"] += 1 if w["payout"] > w["cost"] else 0
        b["losses"] += 1 if w["payout"] < w["cost"] else 0
        b["evens"] += 1 if w["payout"] == w["cost"] else 0
    else:
        b["open"] += 1


UNVERIFIED_SHOWN = 20   # the latest records whose fills could not be verified, listed (all of them counted)


def unverified_row(q: dict) -> dict[str, Any]:
    """A record with an order whose fills could not be verified: which, and what was recorded for it."""
    sides = [side for side, o in (q.get("orders") or {}).items() if o.get("unverified")]
    return {"coin": mk.name_of(q["coin"]), "when": mk.hm(q["start"]), "start": q["start"], "slug": q["slug"],
            "level": int(q.get("level") or 0) + 1, "sides": "、".join(mk.SIDES_CN.get(s, s) for s in sides),
            "filled": "、".join(f"{mk.SIDES_CN.get(s, s)} {mk.fmt(num(q['orders'][s].get('filled')))} 份" for s in sides),
            "note": next((str(q["orders"][s].get("note") or "")[-120:] for s in sides), "")}


def unverified_text(j: dict[str, Any]) -> str:
    """The orders whose fills could not be verified, said wherever the review is (audit 11-28: they were in no review,
    export or check, and counted as what was known of them)."""
    u = j.get("unverified") or {}
    if not u.get("count"):
        return ""
    which = "；".join(f"{x['coin']} {x['when']} 第{x['level']}档{x['sides']}方（记的 {x['filled']}）" for x in u["latest"][:3])
    return (f"⚠️ 有 {u['count']} 场的成交没能核实，按已知的记（可能少记或多记）：{which}"
            + ("…" if u["count"] > 3 else "") + "。请到 Polymarket 的成交记录核对；结算时能读到成交记录的会自动核实")


def settled_record(q: dict) -> bool:
    return q.get("phase") == "settled" and q.get("result") in RESULTS


def _add(w: dict[str, Any], q: dict, f: dict[str, D], price: D, whole: bool = True) -> tuple[D, D, bool, str]:
    """A record's fills into its window; returns (its cost, its payout, settled, the result). `whole`: every filled
    record of the window is settled (one that is not leaves the whole window waiting)."""
    result = q.get("result")
    settled = whole and settled_record(q)
    cost = sum((v * price for v in f.values()), D(0))
    payout = sum((v * mk.paid(result, side) for side, v in f.items()), D(0)) if settled else D(0)
    w["sides"] |= {side for side, v in f.items() if v > 0}
    w["levels"].add(int(q["level"]))
    w["volume"] += cost
    for side, v in f.items():
        w["shares"][side] = w["shares"].get(side, D(0)) + v
        w["paid"][side] = w["paid"].get(side, D(0)) + v * price
    for b in w["buckets"]:
        b["shares"] += sum(f.values(), D(0))
        b["volume"] += cost
    if settled:
        w["cost"] += cost
        w["payout"] += payout
        w["result"] = result
        for b in w["buckets"]:
            b["cost"] += cost
            b["payout"] += payout
            b["pnl"] += payout - cost
    else:
        w["settled"] = False
    return cost, payout, settled, str(result or "")


class Sides:
    """The settled windows' shares split by what they were: pairs (one up and one down share: $1 back whatever wins, the
    margin locked) and the one-sided rest (up or down alone: won or lost on the result), at the window's average price
    per side; and what is not settled yet. The three settled rows add up to the settled P&L exactly. 11-21 review: up
    and down were added up as they came, pairs and one-sided shares together, the unsettled shares with them."""

    def __init__(self) -> None:
        self.pair = {"name": "双边配对（每对一涨一跌，锁定价差）", "shares": D(0), "cost": D(0), "payout": D(0)}
        self.one = {side: {"name": f"单边：只多出{mk.SIDES_CN[side]}", "shares": D(0), "cost": D(0), "payout": D(0)} for side in mk.SIDES}
        self.waiting = {"name": "还没结算", "shares": D(0), "cost": D(0), "payout": D(0)}

    def add(self, w: dict[str, Any]) -> None:
        if not w["sides"]:
            return
        if not w["settled"]:
            self.waiting["shares"] += sum(w["shares"].values(), D(0))
            self.waiting["cost"] += w["volume"]
            return
        up, down = w["shares"].get("up", D(0)), w["shares"].get("down", D(0))
        paired = min(up, down)
        avg = {side: (w["paid"][side] / w["shares"][side]) if w["shares"].get(side) else D(0) for side in mk.SIDES}
        if paired > 0:
            self.pair["shares"] += paired
            self.pair["cost"] += paired * (avg["up"] + avg["down"])
            self.pair["payout"] += paired
        for side in mk.SIDES:
            net = w["shares"].get(side, D(0)) - paired
            if net > 0:
                self.one[side]["shares"] += net
                self.one[side]["cost"] += net * avg[side]
                self.one[side]["payout"] += net * mk.paid(w["result"], side)

    def rows(self) -> list[dict[str, Any]]:
        out = []
        for row in (self.pair, self.one["up"], self.one["down"], self.waiting):
            settled, alone = row is not self.waiting, row is not self.pair
            out.append({"name": row["name"], "shares": float(row["shares"]), "cost": float(round(row["cost"], 4)),
                        "payout": float(round(row["payout"], 4)) if settled else None,
                        "pnl": float(round(row["payout"] - row["cost"], 4)) if settled else None,
                        "win_rate": round(float(row["payout"] / row["shares"] * 100), 1) if settled and alone and row["shares"] else None,
                        "breakeven": round(float(row["cost"] / row["shares"] * 100), 1) if alone and row["shares"] else None})
        return out


class Model:
    """The settled fills against what the plan expects of them (模型 EV 对实际盈亏). The plan: before a window starts
    its price to beat is not set, so either side is a coin's toss, and a share bought at 47¢ is worth 50¢: a window that
    bought U up and D down shares for C dollars expects 0.5 x (U + D) - C, whatever happens. What it gets is U or D,
    so its luck (one standard deviation) is half of |U - D|: nothing for a pair, the whole stake for one side alone.
    The coins of one start go the same way more often than not (COIN_RHO), and so do their fills: a start's variance is
    0.25 x ((1 - rho) x the sum of the squares of its windows' U - D + rho x the square of their sum).
    Actual far under the plan (z below -2) is not bad luck: the side bought wins less than half the time, which is
    what being picked off by takers who know more looks like."""

    def __init__(self) -> None:
        self.starts: list[tuple[float, float, float, float]] = []   # (start, actual, expected, variance), in start order
        self.coins: dict[str, list[float]] = {}                     # coin -> windows, shares, cost, expected, actual, variance
        self.waiting = 0

    def add(self, start: float, windows: Iterable[dict[str, Any]], key: str = "inside") -> None:
        """The windows of one start (every coin's), complete; those with `key` set (inside the review's filter, or
        "scoped": in its time and coins whatever the day, for the weekend's comparison)."""
        nets, expected, actual = [], 0.0, 0.0
        for w in windows:
            if not w.get(key) or not w["sides"]:
                continue
            if not w["settled"]:
                self.waiting += 1
                continue
            up, down, cost = float(w["shares"].get("up", 0)), float(w["shares"].get("down", 0)), float(w["cost"])
            ev, pnl, net = 0.5 * (up + down) - cost, float(w["payout"]) - cost, up - down
            if w.get("result") == mk.SPLIT:  # resolved 50/50: paid exactly what the plan expects, no luck in it
                net = 0.0
            c = self.coins.setdefault(mk.name_of(w["q"]["coin"]), [0, 0.0, 0.0, 0.0, 0.0, 0.0])
            c[0] += 1
            for i, v in enumerate((up + down, cost, ev, pnl, 0.25 * net * net), 1):
                c[i] += v
            nets.append(net)
            expected += ev
            actual += pnl
        if nets:
            variance = 0.25 * ((1 - COIN_RHO) * sum(n * n for n in nets) + COIN_RHO * sum(nets) ** 2)
            self.starts.append((start, actual, expected, variance))

    @staticmethod
    def row(name: str, n: float, shares: float, cost: float, ev: float, pnl: float, variance: float) -> dict[str, Any]:
        sd = variance ** 0.5
        return {"name": name, "windows": int(n), "shares": round(shares, 2), "cost": round(cost, 4), "ev": round(ev, 4),
                "pnl": round(pnl, 4), "diff": round(pnl - ev, 4), "sd": round(sd, 4),
                "z": round((pnl - ev) / sd, 2) if sd > 0 else None}

    def points(self) -> tuple[list[dict[str, Any]], int]:
        """The curve: the running totals at the end of each step (a window, an hour or a day, by how long it spans)."""
        if not self.starts:
            return [], 0
        length = self.starts[-1][0] - self.starts[0][0]
        step = next((s for limit, s in MODEL_STEPS if length <= limit), 86400)

        def key(t: float) -> float:
            return day_start(t) if step == 86400 else t // step * step
        out: list[dict[str, Any]] = []
        actual = expected = variance = 0.0
        for t, a, e, v in self.starts:
            actual, expected, variance = actual + a, expected + e, variance + v
            k = key(t)
            point = {"t": k, "label": day_of(k)[5:] if step == 86400 else mk.hm(k), "pnl": round(actual, 2), "ev": round(expected, 2),
                     "sd": round(variance ** 0.5, 2)}
            if out and out[-1]["t"] == k:
                out[-1] = point
            else:
                out.append(point)
        return out, step

    def total(self) -> dict[str, Any]:
        n = sum(int(c[0]) for c in self.coins.values())
        return self.row("合计", n, sum(c[1] for c in self.coins.values()), sum(c[2] for c in self.coins.values()),
                        sum(e for _, _, e, _ in self.starts), sum(a for _, a, _, _ in self.starts), sum(v for *_, v in self.starts))

    def finish(self) -> dict[str, Any]:
        rows = [self.row(name, *c) for name, c in sorted(self.coins.items())]
        total = self.total()
        points, step = self.points()
        return {"total": total, "by_coin": rows, "points": points, "step": step, "waiting": self.waiting,
                "verdict": model_verdict(total), "rho": COIN_RHO}


class Weekend:
    """周末专项: the windows in the time and coins asked, whatever the day filter, split into the weekend (WEEKEND_RULE)
    and the rest of the week; the weekend also into its four stretches (WEEKEND_PARTS) and into each weekend. Each
    row: the counts and money as journal() counts them, the plan's comparison (Model: EV, luck, z) and the one-sided
    shares' win rate against their breakeven (Sides). Fed a start at a time, its windows complete. A weekend not over
    at `now` is named so (进行中): its numbers are part of one."""

    def __init__(self, now: float = float("inf")) -> None:
        self.now = now
        self.rows: dict[str, dict[str, Any]] = {}
        self.models: dict[str, Model] = {}
        self.sides: dict[str, Sides] = {}
        self.at: float | None = None                 # the start whose keys and buckets are at hand (a start's windows
        self.at_keys: list[str] = []                 # come together: worked out once a start, not once a window)
        self.at_buckets: list[dict[str, Any]] = []

    @staticmethod
    def keys(start: float) -> list[str]:
        part = weekend_part(start)
        return ["weekday"] if part is None else ["weekend", part, "each:" + weekend_of(start)]

    def _at(self, start: float) -> None:
        if start != self.at:
            self.at, self.at_keys = start, self.keys(start)
            self.at_buckets = [self.rows[k] if k in self.rows else self.rows.setdefault(k, _blank()) for k in self.at_keys]

    def buckets(self, start: float) -> list[dict[str, Any]]:
        self._at(start)
        return self.at_buckets

    def add(self, start: float, windows: Iterable[dict[str, Any]]) -> None:
        scoped = [w for w in windows if w.get("scoped")]
        if not scoped:
            return
        self._at(start)
        for k in self.at_keys:
            self.models.setdefault(k, Model()).add(start, scoped, key="scoped")
            sides = self.sides.setdefault(k, Sides())
            for w in scoped:
                sides.add(w)

    def row(self, key: str, name: str, **extra: Any) -> dict[str, Any]:
        out = {"key": key, "name": name, **_finish(self.rows.get(key) or _blank()), **extra}
        t = (self.models.get(key) or Model()).total()
        out.update(ev=t["ev"], diff=t["diff"], sd=t["sd"], z=t["z"], settled_filled=t["windows"])
        one = [r for r in (self.sides.get(key) or Sides()).rows()[1:3] if r["shares"]]
        sh = sum(r["shares"] for r in one)
        out["one_win_rate"] = round(sum(r["payout"] or 0 for r in one) / sh * 100, 1) if sh else None
        out["one_breakeven"] = round(sum(r["cost"] for r in one) / sh * 100, 1) if sh else None
        return out

    def finish(self) -> dict[str, Any]:
        split = [self.row("weekday", "工作日"), self.row("weekend", "周末")]
        parts = [self.row(k, name) for k, name in WEEKEND_PARTS]
        each = []
        for key in sorted((k for k in self.rows if k.startswith("each:")), reverse=True):
            sat = dt.date.fromisoformat(key[5:])
            mon = sat + dt.timedelta(days=2)
            ongoing = dt.datetime.combine(mon, dt.time(8), tzinfo=mk.BEIJING).timestamp() > self.now
            each.append(self.row(key, f"{sat:%m-%d}～{mon:%m-%d}" + ("（进行中）" if ongoing else ""), sat=sat.isoformat(),
                                 ongoing=ongoing, **{"from": f"{sat.isoformat()}T00:00", "to": f"{mon.isoformat()}T08:00"}))
        return {"definition": WEEKEND_RULE, "verdict": weekend_verdict(split[0], split[1]), "split": split, "parts": parts,
                "each": each}


def weekend_verdict(weekday: dict[str, Any], weekend: dict[str, Any]) -> str:
    """One sentence: the weekend against the plan, beside the weekdays; whether its gap is more than luck."""
    if not weekend["filled"]:
        return "这段时间没有周末的成交"
    n = weekend["settled_filled"]
    if n < MODEL_MIN:
        return f"周末已结算的成交只有 {n} 场，样本太少，先别下结论（攒够 {MODEL_MIN} 场、两三个周末再看）"

    def said(r: dict[str, Any]) -> str:
        z = f"{r['z']:+.1f}σ" if r["z"] is not None else "没有运气成分"
        return f"实际 {r['pnl']:+.2f}，模型 {r['ev']:+.2f}（{z}），单场均 {r['avg_pnl'] or 0:+.2f}"
    text = f"周末 {n} 场：{said(weekend)}" + (f"；工作日 {weekday['settled_filled']} 场：{said(weekday)}" if weekday["settled_filled"] else "")
    z = weekend["z"]
    if z is not None and z <= -2:
        return text + "。周末买到的那一边赢得明显比五五开少（被知道方向的人吃单）：可以考虑周末不挂（参数「周末也挂」关掉）"
    if z is not None and z >= 2:
        return text + "。周末比五五开好，超出了运气能解释的范围"
    return text + "。周末的差距还在运气能解释的范围内（±2σ）"


def model_verdict(total: dict[str, Any]) -> str:
    """One sentence: is the gap between the plan and what happened more than luck explains?"""
    n, z, diff = total["windows"], total["z"], total["diff"]
    if not n:
        return "还没有已结算的成交"
    if z is None:
        return f"实际 {total['pnl']:+.2f}，模型 {total['ev']:+.2f}：成交都配成了对，没有运气成分"
    said = f"实际比模型{'多' if diff >= 0 else '少'} {abs(diff):.2f} 美元（{z:+.1f}σ）"
    if n < MODEL_MIN:
        return f"{said}；只有 {n} 场有成交并已结算，样本太少，先别下结论"
    if z <= -2:
        return f"{said}，超出了运气能解释的范围：买到的那一边赢得明显比五五开少，说明吃单的人比我们知道得多"
    if z >= 2:
        return f"{said}，超出了运气能解释的范围：买到的那一边赢得比五五开多"
    return f"{said}，在运气能解释的范围内（±2σ）：还看不出比五五开好或差"


def card_totals(records: Iterable[dict], now: float) -> dict[str, dict[str, Any]]:
    """The cards' raw totals (today, the 7 days to today, all; Beijing days by the windows' start), counted as journal()
    counts them, before _finish: additive, so the days before today can be cached and today's added afresh (a window's
    records share its start: a window is never split between the two). Records in start order (by_start)."""
    today = day_of(now)
    week = {day_of(now - 86400 * i) for i in range(7)}
    out = {"today": _blank(), "7d": _blank(), "all": _blank()}
    windows: dict[str, dict[str, Any]] = {}
    at: float | None = None
    prices: dict[str, D] = {}

    def close() -> None:
        for w in windows.values():
            for b in w["buckets"]:
                _count(b, w)
        windows.clear()
    for q in by_start(records):
        if q.get("foreign"):
            continue
        f = filled(q)
        if not q.get("placed") and not any(f.values()):
            continue
        if q["start"] != at:  # the windows of the start before are complete
            close()
            at = q["start"]
            day = day_of(at)
            buckets = [out["all"]] + ([out["today"]] if day == today else []) + ([out["7d"]] if day in week else [])
        w = windows.get(q["slug"])
        if w is None:
            w = windows[q["slug"]] = _window(q, q["start"] <= now, False, buckets)
        if any(f.values()):
            raw = str(q["price"])
            price = prices.get(raw)
            if price is None:
                price = prices[raw] = num(raw)
            _add(w, q, f, price)
    close()
    return out


def add_cards(*parts: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Raw card totals of disjoint sets of windows, added up."""
    out = {k: _blank() for k in ("today", "7d", "all")}
    for part in parts:
        for k, t in part.items():
            for x, v in t.items():
                out[k][x] += v
    return out


def journal(records: Iterable[dict], now: float, current: int | None = None, span: tuple[float, float] | None = None,
            coins: Iterable[str] | None = None, cards: dict[str, dict[str, Any]] | None = None,
            market_limit: int | None = None, market_sort: str = "time", day_type: str = "all") -> dict[str, Any]:
    """The review: totals (today, 7 days, all: always the whole of `records`, unless `cards` brings them made
    elsewhere), and for the windows starting within `span` (all of them for None) of the coins named (all for None):
    their totals ("range"), by day, coin, starting hour, price, side (pairs and one-sided), how fast the first level
    rested after the opening, how long before the start the fills came, every window traded with its P&L (by_market),
    the latest fills one by one and the cancel-time replay. Windows are counted as windows (its levels and attempts
    together); the money adds up record by record; each fill goes to the time it came. Another account's records (an
    old one, after the ledger was bound anew) are left out. Records in start order (by_start): each window is closed
    as the starts move past it, so the work's memory is the summaries', not the ledger's. The markets are sorted
    (market_sort: time, loss, gain) over all of them and the first `market_limit` listed (market_count: how many).
    `day_type` (DAY_TYPES) narrows everything to the weekend or the weekdays, except the weekend's comparison
    ("weekend"), which always sets the two side by side over the time and coins asked."""
    lo, hi = span or (float("-inf"), float("inf"))
    day_type = day_type if day_type in DAY_TYPES else "all"
    wanted = {c.lower() for c in coins} if coins else None
    stream = by_start(records)
    if cards is not None:  # the cards come ready: the filter's records alone
        stream = (q for q in stream if lo <= q["start"] < hi and (wanted is None or q["coin"] in wanted))
    today = day_of(now)
    week = {day_of(now - 86400 * i) for i in range(7)}
    totals = {"today": _blank(), "7d": _blank(), "all": _blank(), "range": _blank()}
    by_day: dict[str, dict] = defaultdict(_blank)
    by_coin: dict[str, dict] = defaultdict(_blank)
    by_hour: dict[int, dict] = defaultdict(_blank)
    by_lead: dict[str, dict] = {name: {"bucket": name, "fills": 0, "shares": D(0), "cost": D(0), "won": D(0)} for _, name in LEAD_BUCKETS}
    by_speed = {name: _speed_blank(name) for name in [n for _, n in SPEED] + [SPEED_LATE]}
    by_price: dict[D, dict[str, Any]] = {}
    sides, replay, model, weekend = Sides(), Replay(now, current), Model(), Weekend(now)
    markets: list[tuple] = []   # compact (market_tuple): months of windows are many
    detail: collections.deque = collections.deque(maxlen=DETAIL_SHOWN)
    detail_n = 0
    unverified: collections.deque = collections.deque(maxlen=UNVERIFIED_SHOWN)   # fills that could not be verified
    unverified_n = 0
    windows: dict[str, dict[str, Any]] = {}
    at: float | None = None
    day = ""
    hour = 0
    prices: dict[str, D] = {}

    def close() -> None:
        """The windows of a start, complete: counted, listed, split by side, replayed; then let go."""
        if at is not None:
            model.add(at, windows.values())
            weekend.add(at, windows.values())
        for w in windows.values():
            for b in w["buckets"]:
                _count(b, w)
            if w["inside"]:
                sides.add(w)
                if w["sides"]:
                    markets.append(market_tuple(w))
                if w["recs"]:
                    replay.add(w["recs"])
        windows.clear()
    pending: list[tuple[dict, dict[str, D]]] = []   # the records of the start at hand

    def take(q: dict, f: dict[str, D], whole: bool) -> None:
        """One record into its window and the tables. `whole`: every filled record of its window is settled, so its
        money counts (a window settled level by level counts once, whole: audit 11-28)."""
        nonlocal detail_n
        start = q["start"]
        scoped = lo <= start < hi and (wanted is None or q["coin"] in wanted)   # the time and coins asked
        inside = scoped and day_ok(start, day_type)                             # ...and the days
        started = start <= now
        w = windows.get(q["slug"])
        if w is None:
            buckets = []
            if cards is None:
                buckets.append(totals["all"])
                if day == today:
                    buckets.append(totals["today"])
                if day in week:
                    buckets.append(totals["7d"])
            if scoped:
                buckets += weekend.buckets(start)
            if inside:
                buckets += [totals["range"], by_day[day], by_coin[mk.name_of(q["coin"])], by_hour[hour]]
            w = windows[q["slug"]] = _window(q, started, inside, buckets)
            w["scoped"] = scoped
        raw = str(q["price"])
        price = prices.get(raw)
        if price is None:
            price = prices[raw] = num(raw)
        level = int(q["level"])
        pb = sb = None
        timed = None
        if inside:
            pb = by_price.setdefault(price, {"quotes": 0, "upcoming": 0, "filled": 0, "filled_started": 0, "both": 0,
                                             "shares": D(0), "volume": D(0), "cost": D(0), "payout": D(0), "won": D(0),
                                             "settled": D(0), "levels": set()})
            pb["quotes" if started else "upcoming"] += 1
            pb["levels"].add(level + 1)
            if level == 0 and started and q.get("placed"):  # the race for the queue is the first level's
                lag = float(q["placed"]) - float(q["opened"]) if q.get("opened") else None
                sb = by_speed[next((name for limit, name in SPEED if lag is not None and lag < limit), SPEED_LATE)]
                sb["quotes"] += 1
            if start + 300 <= now:  # its story is over: the replay takes it, with its fill times
                timed = fill_events(q) if any(f.values()) else ({}, EXACT)
                w["recs"].append((q, f, timed))
        if not any(f.values()):
            return
        cost, payout, settled, result = _add(w, q, f, price, whole)
        if not inside:
            return
        shares = sum(f.values(), D(0))
        pb["filled"] += 1
        pb["filled_started"] += 1 if started else 0
        pb["both"] += 1 if all(f.values()) else 0
        pb["shares"] += shares
        pb["volume"] += cost
        if settled:
            pb["cost"] += cost
            pb["payout"] += payout
            pb["won"] += sum((v * mk.paid(result, s) for s, v in f.items()), D(0))
            pb["settled"] += shares
        if sb is not None:
            sb["filled"] += 1
            sb["both"] += 1 if all(f.values()) else 0
            sb["shares"] += shares
            if settled:
                sb["cost"] += cost
                sb["payout"] += payout
        for side, events in (timed or fill_events(q))[0].items():
            o = q["orders"][side]
            if not (o.get("fills_at") or o.get("first_fill") or o.get("gone")):
                continue  # no time known for its fills
            for ahead, sh in events:
                lead = by_lead[lead_bucket(ahead)]
                lead["fills"] += 1
                lead["shares"] += sh
                if settled:
                    lead["cost"] += sh * price
                    lead["won"] += sh * mk.paid(result, side)
        detail.append(detail_row(q))
        detail_n += 1

    def flush() -> None:
        """The records of one start, each window taken whole (audit 11-28: a window settled level by level counted the
        settled level's money in the totals while the side split, the model and the market list waited for the
        window, so the tables disagreed), then closed."""
        open_slugs = {q["slug"] for q, f in pending if any(f.values()) and not settled_record(q)}
        for q, f in pending:
            take(q, f, q["slug"] not in open_slugs)
        pending.clear()
        close()
    for q in stream:
        if q.get("foreign"):
            continue
        f = filled(q)
        if any(o.get("unverified") for o in (q.get("orders") or {}).values()) and lo <= q["start"] < hi \
                and (wanted is None or q["coin"] in wanted) and day_ok(q["start"], day_type):
            unverified_n += 1
            unverified.append(q)
        if not q.get("placed") and not any(f.values()):
            continue  # never rested: not a quote
        if q["start"] != at:
            flush()
            at = q["start"]
            day, hour = day_of(at), hour_of(at)
        pending.append((q, f))
    flush()

    def money(x: dict) -> dict:
        return {**x, **{k: float(round(v, 4)) for k, v in x.items() if isinstance(v, D)},
                "pnl": float(round(x["won"] - x["cost"], 4))}

    def speed(x: dict) -> dict:
        pnl = x["payout"] - x["cost"]
        return {"bucket": x["bucket"], "quotes": x["quotes"], "filled": x["filled"], "both": x["both"],
                "fill_rate": round(x["filled"] / x["quotes"] * 100, 1) if x["quotes"] else None,
                "both_rate": round(x["both"] / x["filled"] * 100, 1) if x["filled"] else None,
                "shares": float(x["shares"]), "cost": float(round(x["cost"], 4)), "pnl": float(round(pnl, 4))}
    if cards is not None:
        totals.update({k: cards[k] for k in ("today", "7d", "all")})
    return {
        "totals": {k: _finish(v) for k, v in totals.items()},
        "span": span_label(span),
        "coins": sorted(mk.name_of(c) for c in wanted) if wanted else [],
        "by_day": [{"day": d, "weekday": weekday_label(d, day_type), **_finish(v)} for d, v in sorted(by_day.items(), reverse=True)],
        "by_coin": [{"coin": c, **_finish(v)} for c, v in sorted(by_coin.items())],
        "by_hour": [{"hour": h, **_finish(by_hour[h])} for h in range(24) if h in by_hour],
        "by_side": sides.rows(),
        "by_speed": [speed(v) for v in by_speed.values() if v["quotes"]],
        "by_lead": [money(v) for v in by_lead.values() if v["fills"]],
        "by_market": [market_dict(t) for t in sort_markets(markets, market_sort)[:market_limit]],
        "market_count": len(markets),
        "market_waiting": sum(1 for t in markets if t[-1] is None),
        "market_worst": [market_dict(t) for t in sort_markets([t for t in markets if t[-1] is not None and t[-1] < 0], "loss")[:MARKETS_IN_TEXT]],
        "market_best": [market_dict(t) for t in sort_markets([t for t in markets if t[-1] is not None and t[-1] > 0], "gain")[:MARKETS_IN_TEXT]],
        "by_price": [price_row(p, b) for p, b in sorted(by_price.items(), reverse=True)],
        "detail": list(reversed(detail)),
        "cancel": replay.finish(),
        "model": model.finish(),
        "day_type": day_type,
        "weekend": weekend.finish(),
        "unverified": {"count": unverified_n, "latest": [unverified_row(q) for q in reversed(unverified)]},
        "detail_more": max(0, detail_n - DETAIL_SHOWN),
    }


def price_row(price: D, b: dict[str, Any]) -> dict[str, Any]:
    """One price quoted (a level; its price may have moved between settings): how often a quote there filled (of the
    quotes whose window started), and whether the side bought there won more often than the price (its breakeven: a
    share bought at 47¢ pays $1 when its side wins, so it needs to win more than 47% of the time)."""
    pnl = b["payout"] - b["cost"]
    return {"price": float(price * 100), "levels": "、".join(str(x) for x in sorted(b["levels"])), "quotes": b["quotes"],
            "upcoming": b["upcoming"], "filled": b["filled"],
            "fill_rate": round(b["filled_started"] / b["quotes"] * 100, 1) if b["quotes"] else None,
            "both": b["both"], "shares": float(b["shares"]), "volume": float(round(b["volume"], 4)),
            "cost": float(round(b["cost"], 4)), "payout": float(round(b["payout"], 4)), "pnl": float(round(pnl, 4)),
            "roi": round(float(pnl / b["cost"] * 100), 2) if b["cost"] else None,
            "win_rate": round(float(b["won"] / b["settled"] * 100), 1) if b["settled"] else None, "breakeven": float(price * 100)}


MARKET_KEYS = ("start", "coin", "slug", "levels", "up", "down", "both", "volume", "result", "pnl")


def market_tuple(w: dict[str, Any]) -> tuple:
    """One window traded (one Polymarket market: a coin's five minutes), its levels together, compact: the start, the
    coin, the levels that filled, the shares each side bought, both sides or not, the dollars, the result and the P&L
    (None until every filled level is settled)."""
    q = w["q"]
    return (q["start"], mk.name_of(q["coin"]), q["slug"], "、".join(str(i + 1) for i in sorted(w["levels"])),
            float(w["shares"].get("up", D(0))), float(w["shares"].get("down", D(0))), len(w["sides"]) == len(mk.SIDES),
            float(round(w["volume"], 4)), mk.RESULT_CN.get(w["result"], "") if w["settled"] else "",
            float(round(w["payout"] - w["cost"], 4)) if w["settled"] else None)


def market_dict(t: tuple | dict) -> dict[str, Any]:
    """A market as the page and the tables take it (with its day and Beijing time)."""
    m = t if isinstance(t, dict) else dict(zip(MARKET_KEYS, t))
    return {**m, "day": day_of(m["start"]), "when": mk.hm(m["start"])}


def sort_markets(markets: list, how: str) -> list:
    """The latest first (time; a start's coins in order), or the biggest loss / gain first over the whole list (11-21
    review: the page sorted only the 500 it was sent); windows not settled yet last. Compact markets or dicts."""
    if not markets:
        return markets
    get = (lambda m, k: m[k]) if isinstance(markets[0], dict) else (lambda m, k: m[MARKET_KEYS.index(k)])
    if how == "loss":
        return sorted(markets, key=lambda m: (get(m, "pnl") is None, get(m, "pnl") or 0, -get(m, "start")))
    if how == "gain":
        return sorted(markets, key=lambda m: (get(m, "pnl") is None, -(get(m, "pnl") or 0), -get(m, "start")))
    return sorted(markets, key=lambda m: (-get(m, "start"), get(m, "coin")))


def price_markets(records: Iterable[dict], now: float, price_cents: float, span: tuple[float, float] | None = None,
                  coins: Iterable[str] | None = None, limit: int = 100, day_type: str = "all") -> dict[str, Any]:
    """One price opened up: every quote there that filled, in the filter, the biggest loss first: where the money at
    that price went (which coins, which hours, which side)."""
    lo, hi = span or (float("-inf"), float("inf"))
    wanted = {c.lower() for c in coins} if coins else None
    target = D(str(price_cents)) / 100
    rows = []
    for q in records:
        if q.get("foreign") or not (lo <= q["start"] < hi) or (wanted is not None and q["coin"] not in wanted):
            continue
        if num(q["price"]) != target or not day_ok(q["start"], day_type):
            continue
        f = filled(q)
        if not any(f.values()):
            continue
        result = q.get("result")
        settled = q.get("phase") == "settled" and result in RESULTS
        cost = sum((v * target for v in f.values()), D(0))
        rows.append({"slug": q["slug"], "coin": mk.name_of(q["coin"]), "when": mk.hm(q["start"]), "start": q["start"],
                     "level": int(q["level"]) + 1, "up": float(f.get("up", 0)), "down": float(f.get("down", 0)),
                     "cost": float(round(cost, 4)), "result": mk.RESULT_CN.get(str(result or ""), "") if settled else "",
                     "pnl": float(round(sum((v * mk.paid(result, side) for side, v in f.items()), D(0)) - cost, 4)) if settled else None,
                     "first_fill_lead_s": detail_row(q)["first_fill_lead_s"]})
    rows = sort_markets(rows, "loss")
    settled_rows = [r for r in rows if r["pnl"] is not None]
    return {"price": float(target * 100), "count": len(rows), "pnl": float(round(sum((D(str(r["pnl"])) for r in settled_rows), D(0)), 4)),
            "losing": sum(1 for r in settled_rows if r["pnl"] < 0), "rows": rows[:limit], "more": max(0, len(rows) - limit)}


def span_stats(records: Iterable[dict], now: float, lo: float, hi: float, coins: Iterable[str] | None = None,
               day_type: str = "all") -> dict[str, Any]:
    """The totals of the windows starting from `lo` to `hi`, counted as journal() counts them."""
    wanted = {c.lower() for c in coins} if coins else None
    total = _blank()
    windows: dict[str, dict[str, Any]] = {}
    for q in records:
        if q.get("foreign") or not (lo <= q["start"] < hi) or (wanted is not None and q["coin"] not in wanted) \
                or not day_ok(q["start"], day_type):
            continue
        f = filled(q)
        if not q.get("placed") and not any(f.values()):
            continue
        w = windows.get(q["slug"])
        if w is None:
            w = windows[q["slug"]] = _window(q, q["start"] <= now, True, [total])
        if any(f.values()):
            _add(w, q, f, num(q["price"]))
    for w in windows.values():
        _count(total, w)
    return _finish(total)


def change_effects(read: Callable[[float, float], Iterable[dict]], now: float, log: Iterable[dict],
                   coins: Iterable[str] | None = None, limit: int = 8, day_type: str = "all") -> list[dict[str, Any]]:
    """Each settings change (the latest first) with the windows after it (until the next change, or now) beside as long
    a time before it (from the change before, at most): what the change did to the fill rate, both sides, the volume
    and the P&L. Windows are placed by their start; a window quoted before a change and starting after it counts after.
    `read(lo, hi)`: the records of the windows starting from lo to hi (each span is read on its own)."""
    changes = sorted((e for e in log if e.get("changes")), key=lambda e: float(e["at"]))
    out = []
    for i in range(len(changes) - 1, max(-1, len(changes) - 1 - limit), -1):
        at = float(changes[i]["at"])
        until = float(changes[i + 1]["at"]) if i + 1 < len(changes) else now
        length = max(0.0, min(until, now) - at)
        since = max(at - length, float(changes[i - 1]["at"]) if i > 0 else float("-inf"))
        out.append({"at": at, "when": mk.hm(at), "via": changes[i].get("via") or "", "changes": changes[i]["changes"],
                    "after_hours": round(length / 3600, 1), "before_hours": round((at - since) / 3600, 1),
                    "after": span_stats(read(at, at + length), now, at, at + length, coins, day_type),
                    "before": span_stats(read(since, at), now, since, at, coins, day_type)})
    return out


EXACT, SEEN, LUMPED = "exact", "seen", "lumped"   # how a record's fill times are known (fill_events)
QUALITY_CN = {EXACT: "是", SEEN: "否（快照看到的时间）", LUMPED: "否（只知道第一次看到成交的时间）"}


def fill_events(q: dict) -> tuple[dict[str, list[tuple[float, D]]], str]:
    """Each filled side's fills as (seconds before the start, shares), and how their times are known: EXACT (the
    trades' match times), SEEN (when a snapshot saw each fill: up to ~10 s late) or LUMPED (shares whose time is not
    known, put at the first fill seen: the replay cannot tell when they came, and leaves the window out — audit 11-28:
    such windows turned the recommended cancel time around). The shares always add up to what the record holds
    (filled): the review's totals and its replay are one basis (audit 11-28: trade times listing more than that — a
    trade listed twice by an older version — counted it twice)."""
    out: dict[str, list[tuple[float, D]]] = {}
    quality = EXACT

    def worse(a: str, b: str) -> str:
        return a if (EXACT, SEEN, LUMPED).index(a) >= (EXACT, SEEN, LUMPED).index(b) else b
    for side, o in (q.get("orders") or {}).items():
        total = num(o.get("filled"))
        if total <= 0:
            continue
        log = [(q["start"] - float(t), num(sh)) for t, sh in (o.get("fills_at") or [])]
        if not log:  # the trades unread: when the snapshots saw each fill, else when the first fill was seen
            log = [(q["start"] - float(t), num(sh)) for t, sh in (o.get("seen") or []) if num(sh) > 0]
            quality = worse(quality, SEEN)
            if not log:
                first = o.get("first_fill") or o.get("gone")
                log = [(q["start"] - float(first) if first else 0.0, total)]
                quality = LUMPED
        got = sum((sh for _, sh in log), D(0))
        if got < total:  # the times show less than the book did: the rest, its time not known, at the latest seen
            log.append((min(lead for lead, _ in log), total - got))
            quality = LUMPED
        elif got > total:  # more: each scaled down to what was bought
            log = [(lead, sh * total / got) for lead, sh in log]
            quality = worse(quality, SEEN)
        out[side] = log
    return out, quality


def secs_label(seconds: int) -> str:
    """60 秒, 90 秒, 5 分: seconds below two minutes, whole minutes above."""
    return f"{seconds} 秒" if seconds < 120 or seconds % 60 else f"{seconds // 60} 分"


def ended_by_cancel(q: dict) -> float | None:
    """Seconds before the start at which the cancel rule ended this record's orders, or None when something else ended
    them (a settings change, a pause, a restart, a refusal, the exchange, a full fill, the expiry): that would have
    happened under any cancel time, so every cancel time replays such a record just as it was."""
    if q.get("cancel_lead") is not None:
        return max(0.0, float(q["cancel_lead"]))
    if q.get("reason") == "到点撤单" and q.get("closing_at"):
        return max(0.0, q["start"] - float(q["closing_at"]))
    return None




TIE_USD = D("0.01")       # candidates within this of each other tie: no change is recommended for less


class Replay:
    """撤单提前多少秒: every finished window replayed as if the cancel rule had been X seconds before the start (the fills
    after that dropped), one set of windows for every X so the rows compare (11-21 review: windows are taken or left
    whole; a window of several levels was cut level by level before). A window the cancel rule ended can only be
    replayed for an X no later than its earliest cancel (it rested no longer); one something else ended (a restart, a
    settings change, a refusal) would have ended the same way and replays for every X. The current setting's row is the
    baseline every row is compared to (Δ); a change is recommended only for more than a cent over it, with enough
    windows, and never between candidates that tie (the current one wins a tie): web, Telegram, CSV and Excel all take
    `recommend` from here. Also: how far the data can be trusted (how many windows, fills timed by a snapshot rather
    than the trade), when the fills came against their breakeven, fills after the start or after a confirmed cancel.

    Fed one window at a time (add: the window's records, their fills and fill times) and finished once; a window keeps
    only its earliest cancel and its fills (lead, side, shares, price, won), so a long ledger replays in little memory."""

    def __init__(self, now: float, current: int | None) -> None:
        self.now, self.current = now, current
        # (earliest cancel, fills, records timed by a snapshot, fills so timed, coin, cancel lag, fills of unknown time)
        self.windows: list[tuple[float | None, list[tuple[float, str, D, D, bool]], int, int, str, float | None, bool]] = []
        self.waiting = 0
        self.after_start = self.after_cancel = 0

    def add(self, recs: list[tuple[dict, dict[str, D], tuple[dict, bool]]]) -> None:
        """One window whose story is over: its records (rested or filled), their fills and fill_events."""
        if any(any(f.values()) and not (q.get("phase") == "settled" and q.get("result") in RESULTS) for q, f, _ in recs):
            self.waiting += 1  # a fill without its result yet: the window waits, whole
            return
        ends = [ended_by_cancel(q) for q, _, _ in recs]
        known = [e for e in ends if e is not None]
        events: list[tuple[float, str, D, D, bool]] = []
        approx_records = approx_events = 0
        lumped = False
        for (q, _, (timed, quality)), end in zip(recs, ends):
            exact = quality == EXACT
            lumped = lumped or (bool(timed) and quality == LUMPED)
            if timed and not exact:
                approx_records += 1
            price, result = num(q["price"]), q.get("result")
            for side, ev in timed.items():
                for lead, sh in ev:
                    won = mk.paid(result, side)   # what a share paid: 1, 0, or 0.5 on a 50/50 market
                    events.append((lead, side, sh, price, won))
                    approx_events += 0 if exact else 1
                    self.after_start += 1 if lead < 0 else 0
                    self.after_cancel += 1 if end is not None and lead < end - END_SLACK_S else 0
        # how long the cancel rule's cancels took to be confirmed in this window (decided at start - closing_at,
        # confirmed at cancel_lead): a candidate's cancel takes as long (audit 11-28: replayed as instant, the current
        # setting's row left out the fills that came while its cancel was on the way)
        lags = [q["start"] - float(q["closing_at"]) - float(q["cancel_lead"]) for q, _, _ in recs
                if q.get("reason") == "到点撤单" and q.get("closing_at") and q.get("cancel_lead") is not None]
        lag = min(LAG_MAX_S, max(0.0, max(lags))) if lags else None
        self.windows.append((max(known) if known else None, events, approx_records, approx_events,
                             mk.name_of(recs[0][0]["coin"]), lag, lumped))

    def finish(self) -> dict[str, Any]:
        current = self.current
        out: dict[str, Any] = {"windows": 0, "rows": [], "groups": [], "actual": None, "baseline": None, "by_fill_time": [],
                               "by_coin": [], "current": current, "best": None, "recommend": None, "advice": "", "base": None,
                               "excluded": 0, "waiting": self.waiting, "approx": 0, "approx_share": 0.0, "filled": 0,
                               "after_start": 0, "after_cancel": 0, "losses": [],
                               "confidence": {"level": "低", "notes": ["还没有结束的场次"]}}
        if not self.windows:
            out["advice"] = "还没有结束的场次可以复盘"
            return out
        ends = [w[0] for w in self.windows if w[0] is not None]
        base = next((c for c in CANCEL_CHOICES if sum(1 for e in ends if e <= c + END_SLACK_S) >= 0.8 * len(ends)),
                    CANCEL_CHOICES[-1]) if ends else CANCEL_CHOICES[0]
        early = sum(1 for w in self.windows if w[0] is not None and w[0] > base + END_SLACK_S)
        lumped = sum(1 for w in self.windows if w[6] and not (w[0] is not None and w[0] > base + END_SLACK_S))
        common = [w for w in self.windows if (w[0] is None or w[0] <= base + END_SLACK_S) and not w[6]]
        lags = sorted(w[5] for w in common if w[5] is not None)
        typical = lags[len(lags) // 2] if lags else 0.0   # for a window whose cancel was not the rule's
        cands = sorted({c for c in CANCEL_CHOICES if c >= base} | ({int(current)} if current is not None and current >= base else set()))
        k = len(cands)
        # one pass over the fills: a fill `lead` seconds before the start stays for every candidate up to it (the first
        # bisect_right(cands, lead) of them): difference arrays over the candidates, summed up afterwards
        d_sh, d_cost, d_pay = [D(0)] * (k + 1), [D(0)] * (k + 1), [D(0)] * (k + 1)
        d_filled, d_both = [0] * (k + 1), [0] * (k + 1)
        actual = {"windows": len(common), "filled": 0, "both": 0, "shares": D(0), "cost": D(0), "payout": D(0)}
        approx = events_all = events_approx = 0
        times = {name: {"bucket": name, "shares": D(0), "cost": D(0), "won": D(0)} for _, name in FILL_TIMES}
        coins: dict[str, dict] = defaultdict(lambda: {"coin": "", "filled": 0, "cost": D(0), "won": D(0)})
        for _, events, approx_records, approx_events, name, lag, _ in common:  # the tables below: the same windows as actual
            approx += approx_records
            events_approx += approx_events
            lag = typical if lag is None else lag
            coin = coins[name]
            coin["coin"] = name
            coin["filled"] += 1 if events else 0
            latest: dict[str, float] = {}
            for lead, side, sh, price, won in events:
                events_all += 1
                cost, pay = sh * price, sh * won
                actual["shares"] += sh
                actual["cost"] += cost
                actual["payout"] += pay
                t = times[next(n for limit, n in FILL_TIMES if lead < limit)]
                t["shares"] += sh
                t["cost"] += cost
                t["won"] += pay
                coin["cost"] += cost
                coin["won"] += pay
                n = bisect_right(cands, lead + lag)   # a candidate's cancel confirmed `lag` seconds after it was due
                if n:
                    d_sh[0] += sh
                    d_sh[n] -= sh
                    d_cost[0] += cost
                    d_cost[n] -= cost
                    d_pay[0] += pay
                    d_pay[n] -= pay
                latest[side] = max(latest.get(side, float("-inf")), lead)
            if latest:
                actual["filled"] += 1
                actual["both"] += 1 if len(latest) == len(mk.SIDES) else 0
                reach = [bisect_right(cands, x + lag) for x in latest.values()]
                top, both = max(reach), (min(reach) if len(latest) == len(mk.SIDES) else 0)
                if top:
                    d_filled[0] += 1
                    d_filled[top] -= 1
                if both:
                    d_both[0] += 1
                    d_both[both] -= 1
        sh = cost = pay = D(0)
        nf = nb = 0
        rows = []
        for j, c in enumerate(cands):
            sh, cost, pay, nf, nb = sh + d_sh[j], cost + d_cost[j], pay + d_pay[j], nf + d_filled[j], nb + d_both[j]
            rows.append({"cancel_s": c, "windows": len(common), "filled": nf, "both": nb, "shares": sh, "cost": cost,
                         "payout": pay, "pnl": pay - cost, "current": c == current})
        actual["pnl"] = actual["payout"] - actual["cost"]
        by_c = {r["cancel_s"]: r for r in rows}
        base_row = by_c.get(current) if current is not None else None
        baseline = {"name": f"现在的开场前 {secs_label(current)}" if base_row else "实际",
                    "cancel_s": current if base_row else None, "pnl": (base_row or actual)["pnl"]}
        top = max(r["pnl"] for r in rows)
        tied = [r for r in rows if top - r["pnl"] <= TIE_USD]
        best = current if base_row is not None and base_row in tied else min(
            tied, key=lambda r: (abs(r["cancel_s"] - (current if current is not None else r["cancel_s"])), r["cancel_s"]))["cancel_s"]
        gain = by_c[best]["pnl"] - baseline["pnl"]
        filled_n = actual["filled"]
        share = events_approx / events_all if events_all else 0.0
        # never on times mostly guessed: a change is recommended only from fills timed by the trades for the most part
        recommend = best if filled_n >= CANCEL_SAMPLE and share <= APPROX_MAX and best != current and gain > TIE_USD else None
        for r in rows:
            r["delta"] = r["pnl"] - baseline["pnl"]
            r["best"] = r["cancel_s"] == best
        merged: list[dict[str, Any]] = []  # rows with the same outcome side by side: one line for the range
        for r in rows:
            last = merged[-1] if merged else None
            if last and (last["filled"], last["both"], last["pnl"]) == (r["filled"], r["both"], r["pnl"]):
                last.update(to=r["cancel_s"], current=last["current"] or r["current"], best=last["best"] or r["best"])
            else:
                merged.append({"from": r["cancel_s"], "to": r["cancel_s"],
                               **{x: r[x] for x in ("filled", "both", "shares", "cost", "pnl", "delta", "current", "best")}})
        by_time = [{**t, "win_rate": float(t["won"] / t["shares"]), "breakeven": float(t["cost"] / t["shares"]), "pnl": t["won"] - t["cost"]}
                   for t in times.values() if t["shares"] > 0]
        notes = [f"可比 {len(common)} 场，{filled_n} 场有成交"]
        if early:
            notes.append(f"{early} 场当时撤得更早，整场没算")
        if lumped:
            notes.append(f"{lumped} 场的成交时刻读不到（只知道第一次看到成交的时间），整场没算")
        if self.waiting:
            notes.append(f"{self.waiting} 场有成交还没结算，整场没算")
        if events_approx:
            notes.append(f"{share:.0%} 的成交时刻是快照推算的（偏晚几秒）")
        if typical > 0:
            notes.append(f"按实测的撤单确认延迟回放（中位 {typical:.1f} 秒）")
        if self.after_cancel or self.after_start:
            notes.append(f"{self.after_start} 笔开场后成交、{self.after_cancel} 笔撤单确认后成交")
        level = ("低" if filled_n < CANCEL_SAMPLE or share > APPROX_MAX else
                 "高" if filled_n >= 100 and share <= 0.1 and not self.after_cancel and not self.after_start else "中")
        if filled_n < CANCEL_SAMPLE:
            advice = f"样本还少（{len(common)} 场里只有 {filled_n} 场有成交），先别改，攒到 {CANCEL_SAMPLE} 场以上再看"
        elif share > APPROX_MAX and best != current:
            advice = (f"成交时刻有 {share:.0%} 是快照推算的，回放不够准，先不改撤单时间"
                      f"（按推算的看，开场前 {secs_label(best)}撤单会多 {gain:.2f}）")
        elif recommend is not None:
            advice = (f"改成开场前 {secs_label(recommend)}撤单最好：这 {len(common)} 场会是 {by_c[recommend]['pnl']:+.2f} 美元，"
                      f"比{baseline['name']}（{baseline['pnl']:+.2f}）多 {gain:.2f}")
        elif base_row is not None:
            same = [secs_label(r["cancel_s"]) for r in tied if r["cancel_s"] != current]
            advice = (f"现在的撤单时间（开场前 {secs_label(current)}）已经是这些记录里最好的（{len(common)} 场 {base_row['pnl']:+.2f} 美元）"
                      + (f"；开场前 {'、'.join(same)}一样好，不用改" if same else ""))
        else:
            advice = f"没有比实际更好的撤单时间（实际 {actual['pnl']:+.2f} 美元）"

        def floats(r: dict) -> dict:
            return {x: float(round(v, 4)) if isinstance(v, D) else v for x, v in r.items()}
        by_time_f = [floats(t) for t in by_time]
        out.update(windows=len(common), base=base, excluded=len(self.windows) - len(common), approx=approx,
                   approx_share=round(share, 3), filled=filled_n, best=best, recommend=recommend, advice=advice,
                   after_start=self.after_start, after_cancel=self.after_cancel, by_fill_time=by_time_f,
                   losses=sorted((t for t in by_time_f if t["pnl"] < 0), key=lambda t: t["pnl"])[:3],
                   by_coin=[floats(c) for c in sorted(({**c, "pnl": c["won"] - c["cost"]} for c in coins.values() if c["filled"]),
                                                      key=lambda c: c["pnl"])],
                   confidence={"level": level, "notes": notes}, rows=[floats(r) for r in rows],
                   groups=[floats(g) for g in merged], actual=floats(actual), baseline=floats(baseline))
        return out


def cancel_review(records: Iterable[dict], now: float, current: int | None = None) -> dict[str, Any]:
    """The cancel-time replay (Replay) of the windows among `records` whose story is over."""
    groups: dict[str, list] = defaultdict(list)
    for q in records:
        if q.get("foreign"):
            continue
        f = filled(q)
        if not q.get("placed") and not any(f.values()):
            continue  # never rested
        if q["start"] + 300 > now:
            continue  # its story is not over
        groups[q["slug"]].append((q, f, fill_events(q) if any(f.values()) else ({}, EXACT)))
    replay = Replay(now, current)
    for recs in groups.values():
        replay.add(recs)
    return replay.finish()


def fill_times_text(q: dict, side: str) -> str:
    """A side's fills as "seconds before the start:shares" for the CSVs (exact times when known)."""
    events, _ = fill_events(q)
    return ";".join(f"{lead:.1f}:{mk.fmt(sh)}" for lead, sh in sorted(events.get(side, []), reverse=True))


def detail_row(q: dict) -> dict[str, Any]:
    f = filled(q)
    price = num(q["price"])
    firsts = [float(o["first_fill"]) for o in q["orders"].values() if o.get("first_fill")]
    return {
        "when": mk.hm(q["start"]), "day": day_of(q["start"]), "coin": mk.name_of(q["coin"]), "level": int(q["level"]) + 1,
        "price": float(price * 100), "shares": float(num(q["shares"])), "attempt": int(q.get("attempt") or 0) + 1,
        "up": float(f.get("up", 0)), "down": float(f.get("down", 0)), "cost": float(sum((v * price for v in f.values()), D(0))),
        "result": mk.RESULT_CN.get(str(q.get("result") or ""), "待结算"), "pnl": q.get("pnl"),
        "placed_lead_s": round(q["start"] - float(q["placed"])) if q.get("placed") else None,
        "cancel_lead_s": q.get("cancel_lead"),
        "first_fill_lead_s": round(q["start"] - min(firsts)) if firsts else None,
        "redeem": q.get("redeem") or "",
        "up_times": fill_times_text(q, "up"), "down_times": fill_times_text(q, "down"),
        "exact": fill_events(q)[1], "check": check_text(q),
    }


def check_text(q: dict) -> str:
    """Whether a record's fills were verified: 未核实 (said, and listed), how the trades corrected them, or nothing."""
    unverified = [mk.SIDES_CN.get(s, s) for s, o in (q.get("orders") or {}).items() if o.get("unverified")]
    if unverified:
        return f"未核实（{'、'.join(unverified)}方）：请到 Polymarket 核对"
    return "；".join(f"{mk.SIDES_CN.get(s, s)}方{o['verified']}" for s, o in (q.get("orders") or {}).items() if o.get("verified"))


def to_csv(rows: list[list[Any]]) -> str:
    """A table as CSV for Excel (UTF-8 with the BOM); a whole float written without its .0."""
    out = io.StringIO()
    w = csv.writer(out)
    for row in rows:
        w.writerow(["" if v is None else int(v) if isinstance(v, float) and v.is_integer() else v for v in row])
    return "\ufeff" + out.getvalue()


def newest_first(records: Iterable[dict]) -> Iterator[dict]:
    """Records the latest window first, a start's records by coin and level: a list is sorted; anything else is taken
    to come the newest first already (the ledger's reader read(..., newest_first=True)) and only each start's few
    records are put in order."""
    if isinstance(records, (list, tuple)):
        yield from sorted(records, key=lambda q: (-q["start"], q["coin"], q["level"]))
        return
    group: list[dict] = []
    for q in records:
        if group and q["start"] != group[0]["start"]:
            yield from sorted(group, key=lambda x: (x["coin"], x["level"]))
            group = []
        group.append(q)
    yield from sorted(group, key=lambda x: (x["coin"], x["level"]))


def detail_table(records: Iterable[dict], limit: int | None = None) -> list[list[Any]]:
    """Every level of a window that filled, the latest first (the latest `limit` of them)."""
    rows = [["场次（北京时间）", "日期", "币种", "档", "价格¢", "份数", "第几次报价", "涨成交", "跌成交", "成本$", "结果", "盈亏$",
             "挂上时距开场(秒)", "撤单确认距开场(秒)", "首次成交距开场(秒)", "涨成交时刻(开场前秒:份)", "跌成交时刻(开场前秒:份)",
             "成交时刻精确", "领取", "成交核实"]]
    for q in newest_first(records):
        if q.get("foreign") or not any(filled(q).values()):  # another account's (改绑): in no total, so in no list
            continue
        if limit is not None and len(rows) > limit:
            break
        d = detail_row(q)
        rows.append([d["when"], d["day"], d["coin"], d["level"], d["price"], d["shares"], d["attempt"], d["up"], d["down"],
                     round(d["cost"], 4), d["result"], None if d["pnl"] is None else round(float(d["pnl"]), 4), d["placed_lead_s"],
                     d["cancel_lead_s"], d["first_fill_lead_s"], d["up_times"], d["down_times"],
                     QUALITY_CN.get(d["exact"], d["exact"]), d["redeem"], d["check"]])
    return rows


GROUP_HEAD = ["报价场次（已开场）", "还没开场", "成交场次", "成交率%", "双边成交", "双边率%", "成交份数", "成交额$", "赢", "输", "平", "待结算",
              "已结算成本$", "回款$", "盈亏$", "单场均盈亏$", "收益率%"]


def group_table(first: str, groups: list[dict], key: str, label=lambda v: v) -> list[list[Any]]:
    """Windows grouped (by day, coin, hour): counted as windows (the fill rate over those started), the money record
    by record."""
    return [[first, *GROUP_HEAD]] + [[label(d[key]), d["quoted"], d["upcoming"], d["filled"], d["fill_rate"], d["both"], d["both_rate"],
                                      d["shares"], d["volume"], d["wins"], d["losses"], d["evens"], d["open"], d["cost"], d["payout"], d["pnl"],
                                      d["avg_pnl"], d["roi"]] for d in groups]


def daily_table(review: dict) -> list[list[Any]]:
    return group_table("日期", review["by_day"], "day")


def coin_table(review: dict) -> list[list[Any]]:
    return group_table("币种", review["by_coin"], "coin")


def hour_table(review: dict) -> list[list[Any]]:
    return group_table("开场（北京时间）", review["by_hour"], "hour", lambda h: f"{int(h):02d}:00")


def markets_table(review: dict) -> list[list[Any]]:
    """Every window traded, its levels together, the latest first."""
    return [["市场（开场，北京时间）", "日期", "币种", "成交的档", "涨成交份数", "跌成交份数", "双边", "成交额$", "结果", "盈亏$"]] + [
        [m["when"], m["day"], m["coin"], m["levels"], m["up"], m["down"], "是" if m["both"] else "", m["volume"],
         m["result"] or "待结算", m["pnl"]] for m in review["by_market"]]


def price_table(review: dict) -> list[list[Any]]:
    """Each price quoted: the fill rate, and the side bought there won more often than the price or not."""
    return [["价格¢", "第几档", "报价（已开场）", "还没开场", "有成交", "成交率%", "两边都成交", "成交份数", "成交额$", "已结算成本$",
             "回款$", "盈亏$", "收益率%", "买到的那一边赢的比例%", "保本比例%（=价格）"]] + [
        [p["price"], p["levels"], p["quotes"], p["upcoming"], p["filled"], p["fill_rate"], p["both"], p["shares"], p["volume"],
         p["cost"], p["payout"], p["pnl"], p["roi"], p["win_rate"], p["breakeven"]] for p in review["by_price"]]


def side_table(review: dict) -> list[list[Any]]:
    """Pairs and one-sided shares (settled), and what is not settled yet."""
    return [["", "份数", "成本$", "回款$", "盈亏$", "这一边赢的比例%", "保本比例%（平均买价）"]] + [
        [r["name"], r["shares"], r["cost"], r["payout"], r["pnl"], r["win_rate"], r["breakeven"]] for r in review["by_side"]]


def speed_table(review: dict) -> list[list[Any]]:
    """The first level by how long after the opening it rested."""
    return [["开放后几秒挂上（第一档）", "报价", "有成交", "成交率%", "两边都成交", "双边率%", "成交份数", "已结算成本$", "已结算盈亏$"]] + [
        [r["bucket"], r["quotes"], r["filled"], r["fill_rate"], r["both"], r["both_rate"], r["shares"], r["cost"], r["pnl"]]
        for r in review["by_speed"]]


WEEKEND_HEAD = ["已结算盈亏$", "模型 EV$", "实际 - 模型$", "z 值（σ）", "单边：买到的一边赢%", "单边保本%", "成交率%", "双边率%",
                "成交场次", "报价场次（已开场）", "成交额$", "单场均盈亏$", "收益率%", "已结算的成交场次"]


def weekend_rows(rows: list[dict[str, Any]]) -> list[list[Any]]:
    def cell(v: Any) -> Any:
        return "" if v is None else v
    return [[r["name"]] + [cell(r[k]) for k in ("pnl", "ev", "diff", "z", "one_win_rate", "one_breakeven", "fill_rate",
                                                 "both_rate", "filled", "quoted", "volume", "avg_pnl", "roi", "settled_filled")]
            for r in rows]


def weekend_table(review: dict) -> tuple[list[list[Any]], set[int]]:
    """The weekend's comparison for the workbook: how to read it, the weekdays beside the weekend, the weekend's four
    stretches, and each weekend (the latest first). Returns the rows and those that title a part."""
    w = review.get("weekend") or {"verdict": "", "definition": WEEKEND_RULE, "split": [], "parts": [], "each": []}
    rows: list[list[Any]] = [["结论", w["verdict"]], ["周末是", w["definition"]],
                             ["怎么读", "z 值低于 −2：那一段买到的一边赢得明显比五五开少（被知道方向的人吃单）。周末一轮 56 小时、工作日 112 小时，"
                                       "总盈亏不能直接比，看「单场均盈亏」和「收益率」；四段的长短也不一样（5、19、24、8 小时）"], []]
    bold = set()
    for title, part in (("工作日 vs 周末", w["split"]), ("周末的四段", w["parts"]), ("逐个周末（周六 0 点～周一 8 点）", w["each"])):
        bold.add(len(rows))
        rows.append([title] + WEEKEND_HEAD)
        rows += weekend_rows(part) or [["（这段时间没有）"]]
        rows.append([])
    return rows, bold


MODEL_HEAD = ["币种", "已结算的成交场次", "份数", "成本$", "模型 EV$（每边按 50% 胜率）", "实际盈亏$", "实际 - 模型$", "运气的一个标准差$", "z 值（σ）"]


def model_table(review: dict) -> tuple[list[list[Any]], set[int]]:
    """The model's comparison for the workbook: how to read it, by coin with the total, then the curve's points (the
    running totals and the range luck explains around the plan). Returns the rows and those that title a part."""
    m = review.get("model") or {"total": {}, "by_coin": [], "points": [], "verdict": "", "waiting": 0, "rho": COIN_RHO}
    rows: list[list[Any]] = [["结论", m.get("verdict") or ""],
                             ["怎么算的", "模型：开场前价格基准还没定，每一边都按 50% 胜率，一份 47¢ 买到的预期值 50¢，所以 "
                                         "模型 EV = 0.5 ×（涨的份数 + 跌的份数）− 成本。实际 = 赢的那一边的份数 − 成本。"],
                             ["z 值", "（实际 − 模型）÷ 运气的一个标准差。在 ±2 以内：差距是运气能解释的；低于 −2："
                                      "买到的那一边赢得明显比五五开少（被知道方向的人吃单）；高于 +2：比五五开好。"],
                             ["运气怎么算", f"一场只买到一边时，要么全赢要么全输，标准差 = 份数的一半；配成对的没有运气。同一场各个币的结果"
                                           f"往往同方向（相关系数 {m.get('rho', COIN_RHO)}），合计时算进去了，所以合计的标准差比各币平方和开根号大。"],
                             []]
    bold = {len(rows)}

    def line(r: dict) -> list[Any]:
        return [r["name"], r["windows"], r["shares"], r["cost"], r["ev"], r["pnl"], r["diff"], r["sd"], r["z"] if r["z"] is not None else ""]
    rows.append(MODEL_HEAD)
    rows += [line(r) for r in m["by_coin"]]
    if m.get("total", {}).get("windows"):
        rows.append(line(m["total"]))
    if m.get("waiting"):
        rows.append([f"另有 {m['waiting']} 场有成交还没结算，没算在内"])
    rows.append([])
    bold.add(len(rows))
    rows.append(["曲线（按开场时间累计）", "累计实际盈亏$", "累计模型 EV$", "运气范围下沿$（模型 − 2σ）", "运气范围上沿$（模型 + 2σ）"])
    rows += [[p["label"], p["pnl"], p["ev"], round(p["ev"] - 2 * p["sd"], 2), round(p["ev"] + 2 * p["sd"], 2)] for p in m["points"]]
    return rows, bold


def changes_table(changes: list[dict]) -> list[list[Any]]:
    """Each settings change with as long a time before and after it."""
    rows: list[list[Any]] = [["改参时间（北京时间）", "怎么改的", "改了什么", "前/后", "时长（小时）", "报价场次", "成交率%", "双边率%",
                              "成交额$", "已结算盈亏$", "单场均盈亏$"]]
    for c in changes:
        what = "；".join(f"{x[0]}：{x[1] or '（空）'} → {x[2] or '（空）'}" for x in c["changes"])
        for name, hours, st in (("改之前", c["before_hours"], c["before"]), ("改之后", c["after_hours"], c["after"])):
            rows.append([c["when"], c["via"], what, name, hours, st["quoted"], st["fill_rate"], st["both_rate"], st["volume"],
                         st["pnl"], st["avg_pnl"]])
    return rows


def quotes_table(records: Iterable[dict], limit: int | None = None) -> list[list[Any]]:
    """Every quote (each window level and attempt that rested or filled), filled or not, the latest first (the latest
    `limit` of them): fill rates, cancel times, the end of each, and how fast it rested after the listing and after the
    market opened."""
    return quotes_rows(records, limit)[0]


def quotes_rows(records: Iterable[dict], limit: int | None = None) -> tuple[list[list[Any]], int]:
    """quotes_table, and how many quotes there were in all (past the limit too)."""
    total = 0
    rows = [["场次（北京时间）", "日期", "币种", "档", "第几次报价", "价格¢", "份数", "状态", "结束原因", "挂上时距开场(秒)",
             "上架后几秒挂上", "开放下单后几秒挂上", "撤单确认距开场(秒)", "涨成交", "跌成交", "涨成交时刻(开场前秒:份)",
             "跌成交时刻(开场前秒:份)", "结果", "盈亏$", "成交核实"]]
    for q in newest_first(records):
        f = filled(q)
        if q.get("foreign") or (not q.get("placed") and not any(f.values())
                                and not any(o.get("unverified") for o in q["orders"].values())):
            continue  # never rested (one that may have, unverified, is listed)
        total += 1
        if limit is not None and len(rows) > limit:
            continue  # counted, not listed
        placed = float(q["placed"]) if q.get("placed") else None
        rows.append([mk.hm(q["start"]), day_of(q["start"]), mk.name_of(q["coin"]), int(q["level"]) + 1, int(q.get("attempt") or 0) + 1,
                     float(num(q["price"]) * 100), float(num(q["shares"])), PHASES.get(q["phase"], q["phase"]), q.get("reason") or "",
                     round(q["start"] - placed) if placed else None,
                     round(placed - float(q["listed"]), 1) if placed and q.get("listed") else None,
                     round(placed - float(q["opened"]), 2) if placed and q.get("opened") else None,
                     q.get("cancel_lead"), float(f.get("up", 0)), float(f.get("down", 0)),
                     fill_times_text(q, "up"), fill_times_text(q, "down"), mk.RESULT_CN.get(str(q.get("result") or ""), ""),
                     None if q.get("pnl") is None else round(float(q["pnl"]), 4), check_text(q)])
    return rows, total


def replay_table(review: dict) -> list[list[Any]]:
    """The cancel-time replay: the same windows as if cancelled X seconds before the start, each against the baseline
    (the current setting replayed on them, or what happened when it cannot be)."""
    base = review.get("baseline") or {}
    rows: list[list[Any]] = [["开场前几秒撤单", "场次", "有成交的场次", "双边成交", "成交份数", "成本$", "回款$", "盈亏$",
                              f"比{base.get('name') or '基准'}$", "说明"]]
    if review.get("actual"):
        a = review["actual"]
        rows.append(["实际发生的", a["windows"], a["filled"], a["both"], a["shares"], a["cost"], a["payout"], a["pnl"],
                     round(a["pnl"] - base["pnl"], 4) if base else None, "当时的撤单"])
    for r in review["rows"]:
        note = "、".join(x for x in ("现在的设置（基准）" if r.get("current") else "", "★最好" if r.get("best") else "",
                                    "建议改成这个" if r["cancel_s"] == review.get("recommend") else "") if x)
        rows.append([r["cancel_s"], r["windows"], r["filled"], r["both"], r["shares"], r["cost"], r["payout"], r["pnl"],
                     r.get("delta"), note])
    return rows


def fill_time_table(review: dict) -> list[list[Any]]:
    """When the fills came, and whether the side bought then won more often than its price."""
    return [["成交时距开场", "份数", "成本$", "赢回$", "盈亏$", "这一边赢的比例%", "保本比例%（=买价）"]] + [
        [t["bucket"], t["shares"], t["cost"], t["won"], t["pnl"], round(t["win_rate"] * 100, 1), round(t["breakeven"] * 100, 1)]
        for t in review["by_fill_time"]]


def cancel_notes(review: dict) -> list[list[Any]]:
    conf = review.get("confidence") or {}
    rows: list[list[Any]] = [["结论", review["advice"]],
                             ["可信度", f"{conf.get('level', '低')}：{'；'.join(conf.get('notes') or [])}"]]
    if review.get("after_start") or review.get("after_cancel"):
        rows.append(["注意", f"{review['after_start']} 笔开场后成交、{review['after_cancel']} 笔撤单确认后成交：撤单或到期没及时生效"])
    if review["rows"]:
        rows.append(["说明", f"只能复盘「比当时撤得更早」：开场前 {review['base']} 秒以后单子已经撤了，那段时间会不会成交没有记录。"
                     f"另有 {review['excluded']} 场当时撤得更早，没算在内；{review['approx']} 场的成交时刻是快照看到的时间（偏晚几秒）"])
    return rows


def cancel_table(review: dict) -> tuple[list[list[Any]], set[int]]:
    """The cancel-time replay, the fill times and the coins, one under the other; the rows that title a part."""
    rows = replay_table(review)
    bold = set()
    rows.append([])
    bold.add(len(rows))
    rows += fill_time_table(review)
    rows.append([])
    bold.add(len(rows))
    rows.append(["币种", "有成交的场次", "成本$", "赢回$", "盈亏$"])
    for c in review.get("by_coin", []):
        rows.append([c["coin"], c["filled"], c["cost"], c["won"], c["pnl"]])
    rows.append([])
    rows += cancel_notes(review)
    return rows, bold


def detail_csv(records: Iterable[dict]) -> str:
    return to_csv(detail_table(records))


def daily_csv(review: dict) -> str:
    return to_csv(daily_table(review))


def markets_csv(review: dict) -> str:
    return to_csv(markets_table(review))


def quotes_csv(records: Iterable[dict]) -> str:
    return to_csv(quotes_table(records))


def cancel_csv(review: dict) -> str:
    return to_csv(cancel_table(review)[0])


def weekend_text(j: dict[str, Any]) -> list[str]:
    """Telegram's weekend part: the weekend beside the weekdays (when both traded and no day filter is set); under
    the weekend filter, its four stretches and the latest weekends."""
    w = j.get("weekend") or {}
    if not w:
        return []

    def line(r: dict[str, Any]) -> str:
        z = f"{r['z']:+.1f}σ" if r["z"] is not None else "—"
        return (f"  {r['name']}：成交 {r['filled']}/{r['quoted']} 场，双边率 {r['both_rate'] if r['both_rate'] is not None else '—'}%，"
                f"盈亏 {r['pnl']:+.2f}（单场均 {r['avg_pnl'] or 0:+.2f}），模型 {r['ev']:+.2f}（{z}）")
    day_type = j.get("day_type") or "all"
    out: list[str] = []
    if day_type == "all" and all(r["filled"] for r in w["split"]):
        out += ["", "【周末 vs 工作日】" + w["verdict"]] + [line(r) for r in w["split"]]
    if day_type == "weekend":
        out += ["", "【周末】" + w["verdict"], "【周末的四段】"] + [line(r) for r in w["parts"] if r["quoted"] or r["filled"]]
        if w["each"]:
            out += ["【逐个周末】最近的在前"] + [line(r) for r in w["each"][:EACH_WEEKEND_IN_TEXT]]
    return out


def review_text(records: list[dict], now: float, current: int | None = None, scope: str = "",
                daily_limit: float = 0.0, j: dict[str, Any] | None = None) -> str:
    """The review for Telegram: the conclusion and where the money went first, then the replay, the fill times, the
    coins and the totals. `j`: journal(records, now, current) when the caller has it already."""
    j = j or journal(records, now, current)
    c = j["cancel"]
    lines = [f"📈 复盘{f'（{scope}）' if scope else ''} · 北京时间 {mk.hms(now)}", f"结论：{c['advice']}"]
    if c["losses"]:
        worst = "；".join(f"{t['bucket']} {t['pnl']:+.2f}（买到的一边赢 {t['win_rate'] * 100:.0f}%，保本 {t['breakeven'] * 100:.0f}%）"
                         for t in c["losses"])
        lines.append(f"亏在哪：{worst}")
    m = j.get("model") or {}
    if (m.get("total") or {}).get("windows"):
        t = m["total"]
        lines.append(f"模型对实际：模型 EV {t['ev']:+.2f}（每边按 50% 胜率），实际 {t['pnl']:+.2f}。{m['verdict']}")
    late = "、".join(x for x in (f"{c['after_start']} 笔在开场后成交" if c["after_start"] else "",
                                 f"{c['after_cancel']} 笔在撤单确认后成交" if c["after_cancel"] else "") if x)
    if late:
        lines.append(f"⚠️ 有 {late}：撤单或到期没及时生效（比如那段时间撤单被交易所拒了），看「最近的问题」")
    if line := unverified_text(j):
        lines.append(line)
    today, alltime = j["totals"]["today"], j["totals"]["all"]
    if daily_limit <= 0 and today["pnl"] < 0:
        lines.append(f"⚠️ 今天已亏 {-today['pnl']:.2f} 美元，没设日亏损上限（网页「参数」→ 资金 → 日亏损上限）")
    lines.append("")
    if c["groups"]:
        a, b = c["actual"], c["baseline"]
        lines.append(f"【如果提前撤单】同一批 {c['windows']} 场按不同撤单时间重算，Δ 是比{b['name']}多（少）的钱")
        lines.append(f"  实际发生的：成交 {a['filled']} 场（双边 {a['both']}），盈亏 {a['pnl']:+.2f}")
        for g in c["groups"]:
            span = secs_label(g["from"]) + (f"～{secs_label(g['to'])}" if g["to"] != g["from"] else "")
            mark = "（现在）" if g["current"] else ""
            lines.append(f"  开场前 {span}{mark}：成交 {g['filled']} 场（双边 {g['both']}），盈亏 {g['pnl']:+.2f}，Δ {g['delta']:+.2f}"
                         + ("  ★" if g["best"] else ""))
        lines.append(f"  可信度：{c['confidence']['level']}（{'；'.join(c['confidence']['notes'])}；"
                     f"比开场前 {secs_label(c['base'])}更晚撤算不出来，那时单子已经撤了）")
    if c["by_fill_time"]:
        lines += ["", "【什么时候成交的】买到的那一边最后赢的比例，低于保本（=买价）就亏"]
        for t in c["by_fill_time"]:
            mark = "✅" if t["win_rate"] >= t["breakeven"] else "⚠️"
            lines.append(f"  {mark} {t['bucket']}：{mk.fmt(D(str(round(t['shares']))))} 份，赢 {t['win_rate'] * 100:.0f}%"
                         f"（保本 {t['breakeven'] * 100:.0f}%），盈亏 {t['pnl']:+.2f}")
    if any(x["filled"] for x in j["by_coin"]):
        lines += ["", "【按币种】" + "，".join(f"{x['coin']} {x['pnl']:+.2f}（{x['filled']} 场成交）"
                                            for x in sorted((x for x in j["by_coin"] if x["filled"]), key=lambda x: x["pnl"]))]
    lines += weekend_text(j)
    if j["by_day"]:
        lines += ["", "【每日】成交额 = 买到的份数 × 买价；盈亏只算已结算的"]
        for d in j["by_day"][:DAYS_IN_TEXT]:
            lines.append(f"  {d['day'][5:]}：成交 {d['filled']}/{d['quoted']} 场，{mk.fmt(D(str(d['shares'])))} 份 ${d['volume']:.2f}，"
                         f"盈亏 {d['pnl']:+.2f}" + (f"（{d['open']} 场待结算）" if d["open"] else ""))
        if len(j["by_day"]) > DAYS_IN_TEXT:
            lines.append(f"  …更早的 {len(j['by_day']) - DAYS_IN_TEXT} 天：网页「复盘」或 /export 的每日汇总")
    if j["by_price"]:
        lines += ["", "【按挂价】买到的那一边赢的比例对比保本（=价格），低于保本就在亏"]
        for x in j["by_price"]:
            mark = "" if x["win_rate"] is None else "✅" if x["win_rate"] >= x["breakeven"] else "⚠️"
            lines.append(f"  {mark}{mk.fmt(D(str(x['price'])))}¢：成交率 {x['fill_rate'] if x['fill_rate'] is not None else '—'}%，"
                         + (f"赢 {x['win_rate']}%（保本 {x['breakeven']:g}%）" if x["win_rate"] is not None else "还没结算")
                         + f"，盈亏 {x['pnl']:+.2f}")
    if j["market_count"]:
        waiting = j["market_waiting"]
        lines += ["", f"【按市场】{j['market_count']} 场有成交" + (f"（{waiting} 场待结算）" if waiting else "")]
        for name, chosen in (("亏最多", j["market_worst"]), ("赚最多", j["market_best"])):
            if chosen:
                lines.append(f"  {name}：" + "；".join(f"{m['coin']} {m['when']} {m['pnl']:+.2f}" for m in chosen))
        lines.append("  每一场的盈亏：网页「复盘」→ 按市场，或 /export 的市场盈亏 CSV")
    lines.append("")
    if scope:  # the records were chosen by the caller: their whole is the selection, narrowed to the days asked
        t = j["totals"]["range"]
        lines.append(f"合计（{scope}）：报价 {t['quoted']} 场，成交 {t['filled']} 场（双边 {t['both']}），成交额 ${t['volume']:.2f}，"
                     f"赢 {t['wins']} 输 {t['losses']}" + (f" 平 {t['evens']}" if t["evens"] else "") + (f"，待结算 {t['open']}" if t["open"] else "") + f"，盈亏 {t['pnl']:+.2f} 美元")
        return "\n".join(lines)
    for k, name in (("today", "今天"), ("7d", "近 7 天"), ("all", "累计")):
        t = j["totals"][k]
        if k != "today" and (t["quoted"], t["pnl"]) == (today["quoted"], today["pnl"]):
            continue  # nothing before today
        lines.append(f"{name}：报价 {t['quoted']} 场，成交 {t['filled']} 场（双边 {t['both']}），成交额 ${t['volume']:.2f}，"
                     f"赢 {t['wins']} 输 {t['losses']}" + (f" 平 {t['evens']}" if t["evens"] else "") + (f"，待结算 {t['open']}" if t["open"] else "") + f"，盈亏 {t['pnl']:+.2f} 美元")
    if (alltime["quoted"], alltime["pnl"]) == (today["quoted"], today["pnl"]):
        lines[-1] += "（全部记录都在今天）"
    return "\n".join(lines)
