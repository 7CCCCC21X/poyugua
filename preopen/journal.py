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
CANCEL_CHOICES = (10, 15, 20, 30, 45, 60, 90, 120, 180, 240, 300, 420, 600, 900, 1800)   # seconds before the start
FILL_TIMES = ((0, "开场之后（不该有）"), (15, "开场前 0～15 秒"), (30, "15～30 秒"), (60, "30～60 秒"), (120, "1～2 分钟"),
              (300, "2～5 分钟"), (900, "5～15 分钟"), (float("inf"), "15 分钟以上"))
CANCEL_SAMPLE = 20        # filled windows before the review names a best cancel time
END_SLACK_S = 2.0         # a cancel confirmed this much earlier than a candidate still counts as resting until it


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


def journal(records: Iterable[dict], now: float, current: int | None = None) -> dict[str, Any]:
    """The review: totals (today, 7 days, all), by day, coin, starting hour, side and how long before the start the
    fills came, and the filled windows one by one."""
    records = list(records)
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
        "cancel": cancel_review(records, now, current),
        "detail_more": max(0, len(detail) - 200),
    }


def fill_events(q: dict) -> tuple[dict[str, list[tuple[float, D]]], bool]:
    """Each filled side's fills as (seconds before the start, shares), and whether the times are exact (the trades'
    match times) rather than when a snapshot first saw them (up to ~10 s late, so on the safe side when replayed)."""
    out: dict[str, list[tuple[float, D]]] = {}
    exact = True
    for side, o in (q.get("orders") or {}).items():
        total = num(o.get("filled"))
        if total <= 0:
            continue
        log = [(q["start"] - float(t), num(sh)) for t, sh in (o.get("fills_at") or [])]
        got = sum((sh for _, sh in log), D(0))
        if not log:
            seen = o.get("first_fill") or o.get("gone")
            log, exact = [(q["start"] - float(seen) if seen else 0.0, total)], False
        elif got < total:  # the trades show less than the book did: the rest at the latest time seen
            log.append((min(lead for lead, _ in log), total - got))
            exact = False
        out[side] = log
    return out, exact


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


def cancel_review(records: Iterable[dict], now: float, current: int | None = None) -> dict[str, Any]:
    """撤单提前多少秒: every finished window replayed as if the cancel rule had been X seconds before the start (the fills
    after that dropped), one set of windows for every X so the rows compare. A record the cancel rule ended can only be
    replayed for an X no later than its own cancel (it rested no longer); a record something else ended (a restart, a
    settings change, a refusal) would have ended the same way, and replays for every X. Also when the fills came, how
    often the side bought then won against its breakeven (the price), fills that came after the start or after a
    confirmed cancel (there should be none), and the coins."""
    eligible = []
    for q in records:
        f = filled(q)
        if not q.get("placed") and not any(f.values()):
            continue  # never rested
        if q["start"] + 300 > now:
            continue  # its story is not over
        if any(f.values()) and not (q.get("phase") == "settled" and q.get("result") in mk.SIDES):
            continue  # a fill without its result yet
        events, exact = fill_events(q)
        eligible.append((q, ended_by_cancel(q), events, exact))
    out: dict[str, Any] = {"windows": 0, "rows": [], "actual": None, "by_fill_time": [], "by_coin": [], "current": current,
                           "best": None, "advice": "", "base": None, "excluded": 0, "approx": 0, "filled": 0,
                           "after_start": 0, "after_cancel": 0, "losses": []}
    if not eligible:
        out["advice"] = "还没有结束的场次可以复盘"
        return out
    ends = [e for _, e, _, _ in eligible if e is not None]

    def covered(c: float) -> int:
        return sum(1 for e in ends if e <= c + END_SLACK_S)
    base = next((c for c in CANCEL_CHOICES if covered(c) >= 0.8 * len(ends)), CANCEL_CHOICES[-1]) if ends else CANCEL_CHOICES[0]
    common = [x for x in eligible if x[1] is None or x[1] <= base + END_SLACK_S]
    windows = {(q["slug"], int(q["level"])) for q, _, _, _ in common}

    def replay(cut: float | None) -> dict[str, Any]:
        """The common set with the fills earlier than `cut` seconds before the start kept (None: all of them)."""
        kept: dict[tuple, dict[str, D]] = defaultdict(lambda: defaultdict(D))
        result: dict[tuple, str] = {}
        cost = payout = D(0)
        for q, _, events, _ in common:
            key = (q["slug"], int(q["level"]))
            price = num(q["price"])
            for side, ev in events.items():
                sh = sum((v for lead, v in ev if cut is None or lead >= cut), D(0))
                if sh > 0:
                    kept[key][side] += sh
                    cost += sh * price
                    payout += sh if side == q.get("result") else D(0)
            result[key] = str(q.get("result") or "")
        filled_w = [k for k, v in kept.items() if any(x > 0 for x in v.values())]
        return {"windows": len(windows), "filled": len(filled_w),
                "both": sum(1 for k in filled_w if all(kept[k].get(sd, D(0)) > 0 for sd in mk.SIDES)),
                "shares": sum((sum(v.values(), D(0)) for v in kept.values()), D(0)), "cost": cost, "payout": payout,
                "pnl": payout - cost}
    out.update(base=base, windows=len(windows), excluded=len({(q["slug"], int(q["level"])) for q, _, _, _ in eligible}) - len(windows),
               approx=sum(1 for x in common if x[2] and not x[3]))
    out["actual"] = replay(None)
    out["filled"] = out["actual"]["filled"]
    for c in CANCEL_CHOICES:
        if c >= base:
            out["rows"].append({"cancel_s": c, **replay(c)})
    if out["rows"] and out["filled"] >= CANCEL_SAMPLE:
        out["best"] = max(out["rows"], key=lambda r: (r["pnl"], -r["cancel_s"]))["cancel_s"]
    times = {name: {"bucket": name, "shares": D(0), "cost": D(0), "won": D(0)} for _, name in FILL_TIMES}
    coins: dict[str, dict] = defaultdict(lambda: {"coin": "", "filled": 0, "cost": D(0), "won": D(0)})
    for q, end, events, _ in eligible:
        price = num(q["price"])
        coin = coins[mk.name_of(q["coin"])]
        coin["coin"] = mk.name_of(q["coin"])
        coin["filled"] += 1 if events else 0
        for side, ev in events.items():
            for lead, sh in ev:
                t = times[next(name for limit, name in FILL_TIMES if lead < limit)]
                won = sh if side == q.get("result") else D(0)
                t["shares"] += sh
                t["cost"] += sh * price
                t["won"] += won
                coin["cost"] += sh * price
                coin["won"] += won
                out["after_start"] += 1 if lead < 0 else 0
                out["after_cancel"] += 1 if end is not None and lead < end - END_SLACK_S else 0
    for t in times.values():
        if t["shares"] > 0:
            out["by_fill_time"].append({**t, "win_rate": float(t["won"] / t["shares"]), "breakeven": float(t["cost"] / t["shares"]),
                                        "pnl": t["won"] - t["cost"]})
    out["by_coin"] = sorted(({**c, "pnl": c["won"] - c["cost"]} for c in coins.values() if c["filled"]), key=lambda c: c["pnl"])
    out["losses"] = sorted((t for t in out["by_fill_time"] if t["pnl"] < 0), key=lambda t: t["pnl"])[:3]
    rows = {r["cancel_s"]: r for r in out["rows"]}
    actual = out["actual"]
    if out["filled"] < CANCEL_SAMPLE:
        out["advice"] = f"样本还少（{out['windows']} 场里只有 {out['filled']} 场有成交），先别改，攒到 {CANCEL_SAMPLE} 场以上再看"
    else:
        best = rows[out["best"]]
        gain = best["pnl"] - actual["pnl"]
        if gain <= D("0.01"):
            out["advice"] = f"现在的撤单时间已经是这些记录里最好的（实际盈亏 {actual['pnl']:+.2f} 美元）"
        else:
            out["advice"] = (f"改成开场前 {secs_label(out['best'])}撤单最好：这 {out['windows']} 场会是 {best['pnl']:+.2f} 美元，"
                             f"实际是 {actual['pnl']:+.2f}（{'少亏' if actual['pnl'] < 0 and best['pnl'] <= 0 else '多赚'} {gain:.2f}）")
    for r in [*out["rows"], actual]:
        for k in ("shares", "cost", "payout", "pnl"):
            r[k] = float(round(r[k], 4))
    for t in out["by_fill_time"]:
        for k in ("shares", "cost", "won", "pnl"):
            t[k] = float(round(t[k], 4))
    for c in out["by_coin"]:
        for k in ("cost", "won", "pnl"):
            c[k] = float(round(c[k], 4))
    return out


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
        "result": mk.SIDES_CN.get(str(q.get("result") or ""), "待结算"), "pnl": q.get("pnl"),
        "placed_lead_s": round(q["start"] - float(q["placed"])) if q.get("placed") else None,
        "cancel_lead_s": q.get("cancel_lead"),
        "first_fill_lead_s": round(q["start"] - min(firsts)) if firsts else None,
        "redeem": q.get("redeem") or "",
        "up_times": fill_times_text(q, "up"), "down_times": fill_times_text(q, "down"),
        "exact": fill_events(q)[1],
    }


def detail_csv(records: Iterable[dict]) -> str:
    rows = sorted((q for q in records if any(filled(q).values())), key=lambda q: -q["start"])
    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(["场次（北京时间）", "日期", "币种", "档", "价格¢", "份数", "第几次报价", "涨成交", "跌成交", "成本$", "结果", "盈亏$",
                "挂上时距开场(秒)", "撤单确认距开场(秒)", "首次成交距开场(秒)", "涨成交时刻(开场前秒:份)", "跌成交时刻(开场前秒:份)",
                "成交时刻精确", "领取"])
    for q in rows:
        d = detail_row(q)
        w.writerow([d["when"], d["day"], d["coin"], d["level"], d["price"], d["shares"], d["attempt"], d["up"], d["down"],
                    round(d["cost"], 4), d["result"], "" if d["pnl"] is None else round(float(d["pnl"]), 4),
                    d["placed_lead_s"] if d["placed_lead_s"] is not None else "", d["cancel_lead_s"] if d["cancel_lead_s"] is not None else "",
                    d["first_fill_lead_s"] if d["first_fill_lead_s"] is not None else "", d["up_times"], d["down_times"],
                    "是" if d["exact"] else "否（快照看到的时间）", d["redeem"]])
    return "\ufeff" + out.getvalue()  # Excel reads UTF-8 with the BOM


def daily_csv(review: dict) -> str:
    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(["日期", "报价场次", "成交场次", "成交率%", "双边成交", "赢", "输", "待结算", "成本$", "回款$", "盈亏$", "收益率%"])
    for d in review["by_day"]:
        w.writerow([d["day"], d["quoted"], d["filled"], d["fill_rate"] if d["fill_rate"] is not None else "", d["both"], d["wins"],
                    d["losses"], d["open"], d["cost"], d["payout"], d["pnl"], d["roi"] if d["roi"] is not None else ""])
    return "\ufeff" + out.getvalue()


def quotes_csv(records: Iterable[dict]) -> str:
    """Every quote (each window level and attempt that rested or filled), filled or not: the full record to review
    fill rates, cancel times and refusals elsewhere."""
    rows = sorted((q for q in records if q.get("placed") or any(filled(q).values())), key=lambda q: (-q["start"], q["coin"], q["level"]))
    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(["场次（北京时间）", "日期", "币种", "档", "第几次报价", "价格¢", "份数", "状态", "结束原因", "挂上时距开场(秒)",
                "撤单确认距开场(秒)", "涨成交", "跌成交", "涨成交时刻(开场前秒:份)", "跌成交时刻(开场前秒:份)", "结果", "盈亏$"])
    for q in rows:
        f = filled(q)
        w.writerow([mk.hm(q["start"]), day_of(q["start"]), mk.name_of(q["coin"]), int(q["level"]) + 1, int(q.get("attempt") or 0) + 1,
                    float(num(q["price"]) * 100), float(num(q["shares"])), PHASES.get(q["phase"], q["phase"]), q.get("reason") or "",
                    round(q["start"] - float(q["placed"])) if q.get("placed") else "",
                    q.get("cancel_lead") if q.get("cancel_lead") is not None else "", float(f.get("up", 0)), float(f.get("down", 0)),
                    fill_times_text(q, "up"), fill_times_text(q, "down"), mk.SIDES_CN.get(str(q.get("result") or ""), ""),
                    "" if q.get("pnl") is None else round(float(q["pnl"]), 4)])
    return "\ufeff" + out.getvalue()


def cancel_csv(review: dict) -> str:
    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(["开场前几秒撤单", "场次", "有成交的场次", "双边成交", "成交份数", "成本$", "回款$", "盈亏$", "说明"])
    if review.get("actual"):
        a = review["actual"]
        w.writerow([f"实际（{review.get('current')} 秒）", a["windows"], a["filled"], a["both"], a["shares"], a["cost"], a["payout"],
                    a["pnl"], "实际发生的"])
    for r in review["rows"]:
        w.writerow([r["cancel_s"], r["windows"], r["filled"], r["both"], r["shares"], r["cost"], r["payout"], r["pnl"],
                    "★最好" if r["cancel_s"] == review.get("best") else ""])
    w.writerow([])
    w.writerow(["成交时距开场", "份数", "成本$", "赢回$", "盈亏$", "这一边赢的比例%", "保本比例%（=买价）"])
    for t in review["by_fill_time"]:
        w.writerow([t["bucket"], t["shares"], t["cost"], t["won"], t["pnl"], round(t["win_rate"] * 100, 1), round(t["breakeven"] * 100, 1)])
    w.writerow([])
    w.writerow(["币种", "有成交的场次", "成本$", "赢回$", "盈亏$"])
    for c in review.get("by_coin", []):
        w.writerow([c["coin"], c["filled"], c["cost"], c["won"], c["pnl"]])
    w.writerow([])
    w.writerow(["结论", review["advice"]])
    if review.get("after_start") or review.get("after_cancel"):
        w.writerow(["注意", f"{review['after_start']} 笔开场后成交、{review['after_cancel']} 笔撤单确认后成交：撤单或到期没及时生效"])
    if review["rows"]:
        w.writerow(["说明", f"只能复盘「比当时撤得更早」：开场前 {review['base']} 秒以后单子已经撤了，那段时间会不会成交没有记录。"
                    f"另有 {review['excluded']} 场当时撤得更早，没算在内；{review['approx']} 场的成交时刻是快照看到的时间（偏晚几秒）"])
    return "\ufeff" + out.getvalue()


def review_text(records: list[dict], now: float, current: int | None = None, scope: str = "",
                daily_limit: float = 0.0) -> str:
    """The review for Telegram: the conclusion and where the money went first, then the replay, the fill times, the
    coins and the totals."""
    j = journal(records, now)
    c = cancel_review(records, now, current)
    lines = [f"📈 复盘{f'（{scope}）' if scope else ''} · 北京时间 {mk.hms(now)}", f"结论：{c['advice']}"]
    if c["losses"]:
        worst = "；".join(f"{t['bucket']} {t['pnl']:+.2f}（买到的一边赢 {t['win_rate'] * 100:.0f}%，保本 {t['breakeven'] * 100:.0f}%）"
                         for t in c["losses"])
        lines.append(f"亏在哪：{worst}")
    late = "、".join(x for x in (f"{c['after_start']} 笔在开场后成交" if c["after_start"] else "",
                                 f"{c['after_cancel']} 笔在撤单确认后成交" if c["after_cancel"] else "") if x)
    if late:
        lines.append(f"⚠️ 有 {late}：撤单或到期没及时生效（比如那段时间撤单被交易所拒了），看「最近的问题」")
    today, alltime = j["totals"]["today"], j["totals"]["all"]
    if daily_limit <= 0 and today["pnl"] < 0:
        lines.append(f"⚠️ 今天已亏 {-today['pnl']:.2f} 美元，没设日亏损上限（网页「参数」→ 资金 → 日亏损上限）")
    lines.append("")
    if c["rows"]:
        a = c["actual"]
        lines.append(f"【如果提前撤单】{c['windows']} 场按不同撤单时间重算")
        lines.append(f"  实际（现在开场前 {current} 秒撤）：成交 {a['filled']} 场（双边 {a['both']}），盈亏 {a['pnl']:+.2f}")
        groups: list[list[dict]] = []  # rows with the same outcome side by side: one line for the range
        for r in c["rows"]:
            if groups and (groups[-1][-1]["filled"], groups[-1][-1]["both"], groups[-1][-1]["pnl"]) == (r["filled"], r["both"], r["pnl"]):
                groups[-1].append(r)
            else:
                groups.append([r])
        for g in groups:
            span = secs_label(g[0]["cancel_s"]) + (f"～{secs_label(g[-1]['cancel_s'])}" if len(g) > 1 else "")
            r = g[0]
            lines.append(f"  开场前 {span}：成交 {r['filled']} 场（双边 {r['both']}），盈亏 {r['pnl']:+.2f}"
                         + ("  ★最好" if any(x["cancel_s"] == c["best"] for x in g) else ""))
        notes = [f"比开场前 {secs_label(c['base'])}更晚撤算不出来，那时单子已经撤了"]
        if c["excluded"]:
            notes.append(f"{c['excluded']} 场当时撤得更早，没算")
        if c["approx"]:
            notes.append(f"{c['approx']} 条成交时刻是快照时间，偏晚几秒")
        lines.append("（" + "；".join(notes) + "）")
    if c["by_fill_time"]:
        lines += ["", "【什么时候成交的】买到的那一边最后赢的比例，低于保本（=买价）就亏"]
        for t in c["by_fill_time"]:
            mark = "✅" if t["win_rate"] >= t["breakeven"] else "⚠️"
            lines.append(f"  {mark} {t['bucket']}：{mk.fmt(D(str(round(t['shares']))))} 份，赢 {t['win_rate'] * 100:.0f}%"
                         f"（保本 {t['breakeven'] * 100:.0f}%），盈亏 {t['pnl']:+.2f}")
    if c["by_coin"]:
        lines += ["", "【按币种】" + "，".join(f"{x['coin']} {x['pnl']:+.2f}（{x['filled']} 场成交）" for x in c["by_coin"])]
    lines.append("")
    for k, name in (("today", "今天"), ("7d", "近 7 天"), ("all", "累计")):
        t = j["totals"][k]
        if k != "today" and (t["quoted"], t["pnl"]) == (today["quoted"], today["pnl"]):
            continue  # nothing before today
        lines.append(f"{name}：报价 {t['quoted']} 场，成交 {t['filled']} 场（双边 {t['both']}），赢 {t['wins']} 输 {t['losses']}"
                     + (f"，待结算 {t['open']}" if t["open"] else "") + f"，盈亏 {t['pnl']:+.2f} 美元")
    if (alltime["quoted"], alltime["pnl"]) == (today["quoted"], today["pnl"]):
        lines[-1] += "（全部记录都在今天）"
    return "\n".join(lines)
