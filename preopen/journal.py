"""The console's order list and review (复盘): plain functions over the ledger's records (the working set and the archive).

A record is one window level and attempt (ledger.py). Its money: the filled shares of each side at its price are the
cost; the winning side's shares pay $1 each. P&L is settled records only (the official result known).
"""
from __future__ import annotations

import csv
import datetime as dt
import io
from collections import defaultdict
from decimal import Decimal as D
from typing import Any, Iterable

from . import markets as mk

PHASES = {"active": "挂着", "closing": "撤单中", "closed": "已撤，待结算", "settled": "已结束"}
STATES = {"open": "挂着", "placing": "发送中", "unknown": "状态不明", "gone": "已撤/核对中", "final": "已结束", "rejected": "被拒",
          "not_sent": "没发出", "not_found": "没挂上", "prepared": "准备中"}
LEAD_BUCKETS = ((60, "开场前 1 分钟内"), (300, "1～5 分钟"), (1800, "5～30 分钟"), (7200, "30 分钟～2 小时"), (21600, "2～6 小时"),
                (float("inf"), "6 小时以上"))
TABS = ("open", "filled", "closed", "all")


def num(value: Any) -> D:
    return D(str(value or 0))


def filled(q: dict) -> dict[str, D]:
    return {side: num(o.get("filled")) for side, o in (q.get("orders") or {}).items()}


def day_of(ts: float) -> str:
    return dt.datetime.fromtimestamp(ts, mk.BEIJING).strftime("%Y-%m-%d")


def hour_of(ts: float) -> int:
    return dt.datetime.fromtimestamp(ts, mk.BEIJING).hour


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
        "result": mk.SIDES_CN.get(str(q.get("result") or ""), ""), "pnl": q.get("pnl"), "redeem": q.get("redeem") or "",
        "can_withdraw": q["phase"] == "active",
    }


def orders(records: Iterable[dict], tab: str, coin: str, now: float, limit: int = 300) -> dict[str, Any]:
    """The console's order table: open (active and closing, the soonest first), filled, closed without fills, or all
    (the latest first), optionally one coin; with the count of every tab."""
    rows = [q for q in records if not coin or q["coin"] == coin]
    groups: dict[str, list[dict]] = {t: [] for t in TABS}
    for q in rows:
        groups["all"].append(q)
        if q["phase"] in ("active", "closing"):
            groups["open"].append(q)
        elif any(filled(q).values()):
            groups["filled"].append(q)
        else:
            groups["closed"].append(q)
    for q in groups["open"]:
        if any(filled(q).values()):
            groups["filled"].append(q)
    tab = tab if tab in TABS else "open"
    chosen = sorted(groups[tab], key=lambda q: (q["start"], q["coin"], q["level"]), reverse=tab != "open")
    return {"tab": tab, "coin": coin, "counts": {t: len(v) for t, v in groups.items()},
            "rows": [row_of(q, now) for q in chosen[:limit]], "more": max(0, len(chosen) - limit)}


def _blank() -> dict[str, Any]:
    return {"quoted": 0, "filled": 0, "both": 0, "wins": 0, "losses": 0, "cost": D(0), "payout": D(0), "pnl": D(0), "open": 0}


def _finish(t: dict[str, Any]) -> dict[str, Any]:
    out = dict(t)
    out["fill_rate"] = round(t["filled"] / t["quoted"] * 100, 1) if t["quoted"] else None
    out["roi"] = round(float(t["pnl"] / t["cost"] * 100), 2) if t["cost"] else None
    for k in ("cost", "payout", "pnl"):
        out[k] = float(round(t[k], 4))
    return out


def journal(records: Iterable[dict], now: float) -> dict[str, Any]:
    """The review: totals (today, 7 days, all), by day, coin, starting hour, side and how long before the start the
    fills came, and the filled windows one by one."""
    today = day_of(now)
    week = {day_of(now - 86400 * i) for i in range(7)}
    totals = {"today": _blank(), "7d": _blank(), "all": _blank()}
    by_day: dict[str, dict] = defaultdict(_blank)
    by_coin: dict[str, dict] = defaultdict(_blank)
    by_hour: dict[int, dict] = defaultdict(_blank)
    by_side = {s: {"side": mk.SIDES_CN[s], "shares": D(0), "cost": D(0), "won": D(0)} for s in mk.SIDES}
    by_lead: dict[str, dict] = {name: {"bucket": name, "fills": 0, "shares": D(0), "cost": D(0), "won": D(0)} for _, name in LEAD_BUCKETS}
    quoted: set[tuple[str, str]] = set()
    detail = []
    for q in records:
        if not q.get("placed") and not any(filled(q).values()):
            continue  # never rested: not a quote
        day = day_of(q["start"])
        buckets = [totals["all"], by_day[day], by_coin[mk.name_of(q["coin"])], by_hour[hour_of(q["start"])]]
        if day == today:
            buckets.append(totals["today"])
        if day in week:
            buckets.append(totals["7d"])
        window = (q["slug"], day)
        if window not in quoted:
            quoted.add(window)
            for b in buckets:
                b["quoted"] += 1
        f = filled(q)
        if not any(f.values()):
            continue
        price = num(q["price"])
        result = q.get("result")
        settled = q.get("phase") == "settled" and result in mk.SIDES
        cost = sum((v * price for v in f.values()), D(0))
        payout = f.get(result, D(0)) if settled else D(0)
        for b in buckets:
            b["filled"] += 1
            b["both"] += 1 if all(f.values()) else 0
            if settled:
                b["cost"] += cost
                b["payout"] += payout
                b["pnl"] += payout - cost
                b["wins"] += 1 if payout > cost else 0
                b["losses"] += 1 if payout < cost else 0
            else:
                b["open"] += 1
        for side, shares in f.items():
            if shares <= 0:
                continue
            by_side[side]["shares"] += shares
            if settled:
                by_side[side]["cost"] += shares * price
                by_side[side]["won"] += shares if side == result else D(0)
            first = q["orders"][side].get("first_fill")
            if first:
                lead = by_lead[lead_bucket(q["start"] - float(first))]
                lead["fills"] += 1
                lead["shares"] += shares
                if settled:
                    lead["cost"] += shares * price
                    lead["won"] += shares if side == result else D(0)
        detail.append(q)
    detail.sort(key=lambda q: -q["start"])

    def money(x: dict) -> dict:
        return {**x, **{k: float(round(v, 4)) for k, v in x.items() if isinstance(v, D)},
                "pnl": float(round(x["won"] - x["cost"], 4))}
    return {
        "totals": {k: _finish(v) for k, v in totals.items()},
        "by_day": [{"day": d, **_finish(v)} for d, v in sorted(by_day.items(), reverse=True)][:90],
        "by_coin": [{"coin": c, **_finish(v)} for c, v in sorted(by_coin.items())],
        "by_hour": [{"hour": h, **_finish(by_hour[h])} for h in range(24) if h in by_hour],
        "by_side": [money(v) for v in by_side.values()],
        "by_lead": [money(v) for v in by_lead.values() if v["fills"]],
        "detail": [detail_row(q) for q in detail[:200]],
        "detail_more": max(0, len(detail) - 200),
    }


def detail_row(q: dict) -> dict[str, Any]:
    f = filled(q)
    price = num(q["price"])
    firsts = [float(o["first_fill"]) for o in q["orders"].values() if o.get("first_fill")]
    return {
        "when": mk.hm(q["start"]), "day": day_of(q["start"]), "coin": mk.name_of(q["coin"]), "level": int(q["level"]) + 1,
        "price": float(price * 100), "shares": float(num(q["shares"])), "attempt": int(q.get("attempt") or 0) + 1,
        "up": float(f.get("up", 0)), "down": float(f.get("down", 0)), "cost": float(sum((v * price for v in f.values()), D(0))),
        "result": mk.SIDES_CN.get(str(q.get("result") or ""), "待结算"), "pnl": q.get("pnl"),
        "placed_lead_s": round(q["start"] - float(q["placed"])) if q.get("placed") else None,
        "cancel_lead_s": q.get("cancel_lead"),
        "first_fill_lead_s": round(q["start"] - min(firsts)) if firsts else None,
        "redeem": q.get("redeem") or "",
    }


def detail_csv(records: Iterable[dict]) -> str:
    rows = sorted((q for q in records if any(filled(q).values())), key=lambda q: -q["start"])
    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(["场次（北京时间）", "日期", "币种", "档", "价格¢", "份数", "第几次报价", "涨成交", "跌成交", "成本$", "结果", "盈亏$",
                "挂上时距开场(秒)", "撤单确认距开场(秒)", "首次成交距开场(秒)", "领取"])
    for q in rows:
        d = detail_row(q)
        w.writerow([d["when"], d["day"], d["coin"], d["level"], d["price"], d["shares"], d["attempt"], d["up"], d["down"],
                    round(d["cost"], 4), d["result"], "" if d["pnl"] is None else round(float(d["pnl"]), 4),
                    d["placed_lead_s"] if d["placed_lead_s"] is not None else "", d["cancel_lead_s"] if d["cancel_lead_s"] is not None else "",
                    d["first_fill_lead_s"] if d["first_fill_lead_s"] is not None else "", d["redeem"]])
    return "﻿" + out.getvalue()  # Excel reads UTF-8 with the BOM


def daily_csv(review: dict) -> str:
    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(["日期", "报价场次", "成交场次", "成交率%", "双边成交", "赢", "输", "待结算", "成本$", "回款$", "盈亏$", "收益率%"])
    for d in review["by_day"]:
        w.writerow([d["day"], d["quoted"], d["filled"], d["fill_rate"] if d["fill_rate"] is not None else "", d["both"], d["wins"],
                    d["losses"], d["open"], d["cost"], d["payout"], d["pnl"], d["roi"] if d["roi"] is not None else ""])
    return "﻿" + out.getvalue()
