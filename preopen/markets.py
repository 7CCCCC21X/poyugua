"""The 5-minute up/down windows: their slugs, which ones are in range, and what a Gamma listing must say to be quoted.

A window starting at epoch second t (a multiple of 300) is the market <coin>-updown-5m-<t>, e.g.
btc-updown-5m-1794536700 = "Bitcoin Up or Down - November 12, 9:25PM-9:30PM ET". Polymarket creates each one about
23 h 52.5 min before its start (all coins within the same second); it turns active, and the CLOB accepts orders, some
75-90 s later (measured 11-13; the acceptingOrdersTimestamp field is backdated to ~6 s after the creation).
"""
from __future__ import annotations

import datetime as dt
import json
from dataclasses import dataclass
from decimal import Decimal as D, InvalidOperation
from typing import Any

from .config import Settings

PERIOD_S = 300
BEIJING = dt.timezone(dt.timedelta(hours=8))
NAMES = {"btc": "BTC", "eth": "ETH", "sol": "SOL", "xrp": "XRP", "doge": "DOGE", "hype": "HYPE", "bnb": "BNB"}
SIDES = ("up", "down")
SIDES_CN = {"up": "涨", "down": "跌"}


def slug_of(coin: str, start: int) -> str:
    return f"{coin}-updown-5m-{start}"


def parse_slug(slug: str) -> tuple[str, int] | None:
    """(coin, start) of a <coin>-updown-5m-<start> slug; None for any other."""
    head, sep, tail = slug.rpartition("-updown-5m-")
    if not sep or not head or not tail.isdigit():
        return None
    start = int(tail)
    if start % PERIOD_S:
        return None
    return head, start


def name_of(coin: str) -> str:
    return NAMES.get(coin, coin.upper())


def on_weekend(start: int) -> bool:
    """Saturday 00:00 to Monday 08:00, Beijing time (the thinnest stretch; PREOPEN_WEEKEND=off skips it)."""
    t = dt.datetime.fromtimestamp(start, BEIJING)
    return t.weekday() == 5 or t.weekday() == 6 or (t.weekday() == 0 and t.hour < 8)


def window_starts(now: float, settings: Settings) -> list[int]:
    """The starts of the windows to quote, soonest first: after the new-order cutoff (entry_s), within the range
    (ahead_s), and off the weekend when PREOPEN_WEEKEND=off."""
    first = (int(now + settings.entry_s) // PERIOD_S + 1) * PERIOD_S
    last = int(now + settings.ahead_s)
    out = []
    for t in range(first, last + 1, PERIOD_S):
        if not settings.weekend and on_weekend(t):
            continue
        out.append(t)
    return out


def iso_ts(value: Any) -> float | None:
    """Epoch seconds of an ISO time as Gamma writes it ('2026-11-13T02:25:00Z', '2026-11-13 02:31:19+00')."""
    if not value or not isinstance(value, str):
        return None
    text = value.strip().replace(" ", "T")
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    if text.endswith("+00"):
        text += ":00"
    try:
        t = dt.datetime.fromisoformat(text)
    except ValueError:
        return None
    if t.tzinfo is None:
        t = t.replace(tzinfo=dt.timezone.utc)
    return t.timestamp()


def json_list(value: Any) -> list | None:
    if isinstance(value, list):
        return value
    if isinstance(value, str) and value.strip().startswith("["):
        try:
            parsed = json.loads(value)
        except ValueError:
            return None
        return parsed if isinstance(parsed, list) else None
    return None


def dec(value: Any) -> D | None:
    if value is None or value == "":
        return None
    try:
        d = D(str(value))
    except InvalidOperation:
        return None
    return d if d.is_finite() else None


@dataclass
class Listing:
    """A listed window, as the discovery read it from Gamma (fields that do not change: the ids, the tokens)."""
    slug: str
    coin: str
    start: int
    condition_id: str
    market_id: str
    question: str
    up: str                  # the Up outcome's asset id (CTF token id; a position id on a Protocol V2 market)
    down: str
    tick: D
    min_size: D
    neg_risk: bool
    version: str
    created: float | None    # Gamma createdAt
    accepting: bool          # Gamma acceptingOrders when read
    maker_fee: bool          # the fee schedule charges makers too
    rewards_min: D | None    # liquidity rewards: the smallest qualifying size
    seen: float = 0.0        # when this process first read it listed
    fee_rate: D = D(0)       # the taker fee schedule (feeSchedule.rate / exponent), handed to the SDK's metadata cache
    fee_exponent: D = D(0)
    opened: float = 0.0      # when this process first saw the CLOB accept orders on it (0: accepting when read, or not yet)

    @property
    def end(self) -> int:
        return self.start + PERIOD_S

    def asset(self, side: str) -> str:
        return self.up if side == "up" else self.down

    def side_of(self, asset: str) -> str | None:
        return "up" if asset == self.up else "down" if asset == self.down else None


def parse_listing(m: dict, coin: str, start: int, now: float = 0.0) -> tuple[Listing | None, str]:
    """The Listing of a Gamma market for window (coin, start), or (None, why not): the identity (slug, start and end
    times), the two outcomes Up and Down mapped to their tokens by name, and an order book that is on."""
    slug = slug_of(coin, start)
    if str(m.get("slug") or "") != slug:
        return None, f"链接不符（{m.get('slug')}）"
    begins = iso_ts(m.get("eventStartTime"))
    ends = iso_ts(m.get("endDate"))
    if begins is not None and int(begins) != start:
        return None, f"开始时间不符（{m.get('eventStartTime')}）"
    if ends is not None and int(ends) != start + PERIOD_S:
        return None, f"结束时间不符（{m.get('endDate')}）"
    if begins is None and ends is None:
        return None, "缺开始/结束时间"
    outcomes = json_list(m.get("outcomes"))
    version = str(m.get("version") or "v1").lower()
    ids = json_list(m.get("positionIds")) if version == "v2" else json_list(m.get("clobTokenIds"))
    if not outcomes or not ids or len(outcomes) != 2 or len(ids) != 2:
        return None, "缺结果或代币"
    by_name = {str(o).strip().lower(): str(i).strip() for o, i in zip(outcomes, ids)}
    up, down = by_name.get("up", ""), by_name.get("down", "")
    if not up or not down or up == down or not up.isdigit() or not down.isdigit():
        return None, f"结果不是涨/跌（{outcomes}）"
    condition = str(m.get("conditionId") or "")
    if not (condition.startswith("0x") and len(condition) == 66):
        return None, "缺 conditionId"
    if m.get("closed") is True or m.get("archived") is True:
        return None, "已关闭"
    # `active` is false and `acceptingOrders` null for the first seconds of a listing (measured 11-13 03:17:30): not a
    # reason to set it aside; whether orders are accepted is the CLOB's own flag, asked before placing (Engine.place_window)
    if m.get("enableOrderBook") is False:
        return None, "没开订单簿"
    tick = dec(m.get("orderPriceMinTickSize"))
    min_size = dec(m.get("orderMinSize"))
    if tick is None or tick <= 0 or min_size is None or min_size <= 0:
        return None, "缺最小价位/最小份数"
    schedule = m.get("feeSchedule") if isinstance(m.get("feeSchedule"), dict) else {}
    maker_fee = bool(m.get("feesEnabled")) and schedule.get("takerOnly") is False
    return Listing(
        slug=slug, coin=coin, start=start, condition_id=condition, market_id=str(m.get("id") or ""),
        question=str(m.get("question") or ""), up=up, down=down, tick=tick, min_size=min_size,
        neg_risk=bool(m.get("negRisk")), version=version, created=iso_ts(m.get("createdAt")),
        accepting=bool(m.get("acceptingOrders")), maker_fee=maker_fee, rewards_min=dec(m.get("rewardsMinSize")),
        seen=now, fee_rate=dec(schedule.get("rate")) or D(0), fee_exponent=dec(schedule.get("exponent")) or D(0),
    ), ""


def level_problem(listing: Listing, price: D, shares: D) -> str:
    """Why a level cannot be quoted on this market: off the price grid, under its minimum size."""
    if (price / listing.tick) != (price / listing.tick).to_integral_value():
        return f"价格 {price} 不在最小价位 {listing.tick} 上"
    if shares < listing.min_size:
        return f"份数 {shares} 少于市场最小 {listing.min_size}"
    return ""


def winner(m: dict) -> str | None:
    """The winning side of a resolved market from its Gamma record ('up' / 'down'), None while unresolved."""
    if not m.get("closed"):
        return None
    outcomes = json_list(m.get("outcomes")) or []
    prices = json_list(m.get("outcomePrices")) or []
    if len(outcomes) != 2 or len(prices) != 2:
        return None
    for outcome, price in zip(outcomes, prices):
        if dec(price) == 1:
            name = str(outcome).strip().lower()
            if name in SIDES:
                return name
    return None


def hm(ts: float) -> str:
    """Beijing time, as the operator reads it: 11-14 10:45."""
    return dt.datetime.fromtimestamp(ts, BEIJING).strftime("%m-%d %H:%M")


def hms(ts: float) -> str:
    return dt.datetime.fromtimestamp(ts, BEIJING).strftime("%H:%M:%S")


def window_name(coin: str, start: int) -> str:
    return f"{name_of(coin)} {hm(start)}"


def fmt(value: Any) -> str:
    """A number without trailing zeros: 10, 0.5, 49."""
    d = dec(value)
    if d is None:
        return str(value)
    text = format(d.normalize(), "f")
    return text


def span(seconds: float) -> str:
    """A duration as the operator reads it: 45 秒, 12 分, 3 小时 5 分."""
    s = int(round(seconds))
    if s < 0:
        return f"-{span(-s)}"
    if s < 60:
        return f"{s} 秒"
    if s < 3600:
        return f"{s // 60} 分"
    h, m = divmod(s // 60, 60)
    return f"{h} 小时" + (f" {m} 分" if m else "")
