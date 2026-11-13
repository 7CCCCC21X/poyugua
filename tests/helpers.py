"""Fixtures for the offline suites: a clock the test moves, a Gamma that answers from a dict, an engine on a paper book."""
from __future__ import annotations

import asyncio
import json
import os
import sys
from decimal import Decimal as D
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import httpx  # noqa: E402

_real_send = httpx.AsyncClient.send


async def _no_network(self, request, *args, **kwargs):  # any real request fails: results never depend on the network
    if isinstance(getattr(self, "_transport", None), httpx.MockTransport):
        return await _real_send(self, request, *args, **kwargs)
    raise OSError(f"network disabled in tests: {request.url}")

httpx.AsyncClient.send = _no_network

from preopen import config, markets as mk  # noqa: E402
from preopen.engine import Engine  # noqa: E402
from preopen.exchange import PaperExchange  # noqa: E402
from preopen.ledger import Ledger  # noqa: E402

T0 = 1794537000  # 2026-11-13 02:30:00 UTC, a window boundary (a Friday, 10:30 Beijing time)


class Clock:
    def __init__(self, t: float = T0 + 1):
        self.t = float(t)

    def __call__(self) -> float:
        return self.t

    def advance(self, seconds: float) -> None:
        self.t += seconds


def iso(ts: float) -> str:
    import datetime as dt
    return dt.datetime.fromtimestamp(ts, dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def token(coin: str, start: int, side: str) -> str:
    return str(abs(hash((coin, start, side))) % 10**70 + 10**70)


def gamma_market(coin: str, start: int, created: float | None = None, accepting: bool = True, tick: float = 0.01,
                 min_size: float = 5, outcomes=("Up", "Down"), closed: bool = False, prices=None, **extra) -> dict:
    created = start - 86400 + 450 if created is None else created
    m = {
        "id": str(start % 10**7), "question": f"{coin} Up or Down", "slug": mk.slug_of(coin, start),
        "conditionId": "0x" + format(abs(hash((coin, start))) % 16**64, "064x"),
        "eventStartTime": iso(start), "endDate": iso(start + 300), "createdAt": iso(created),
        "outcomes": json.dumps(list(outcomes)),
        "clobTokenIds": json.dumps([token(coin, start, o.lower()) for o in outcomes]),
        "orderPriceMinTickSize": tick, "orderMinSize": min_size, "negRisk": False, "active": True, "closed": closed,
        "enableOrderBook": True, "acceptingOrders": accepting, "feesEnabled": True,
        "feeSchedule": {"exponent": 1, "rate": 0.07, "takerOnly": True, "rebateRate": 0.2}, "rewardsMinSize": 50,
        "version": "v1",
    }
    if prices is not None:
        m["outcomePrices"] = json.dumps(prices)
    m.update(extra)
    return m


class FakePublic:
    def __init__(self, clock: Clock):
        self.clock = clock
        self.listed: dict[str, dict] = {}
        self.results: dict[str, dict] = {}
        self.path_asked: list[list[str]] = []
        self.not_accepting: set[str] = set()
        self.asked: list[list[str]] = []
        self.accept_asks = 0

    def list_window(self, coin: str, start: int, **kw) -> dict:
        m = gamma_market(coin, start, **kw)
        self.listed[m["slug"]] = m
        return m

    def list_range(self, coins, starts, **kw) -> None:
        for c in coins:
            for t in starts:
                self.list_window(c, t, **kw)

    async def listings(self, slugs, closed=False):
        self.asked.append(list(slugs))
        return {s: self.listed[s] for s in slugs if s in self.listed}

    async def market(self, slug):
        return self.results.get(slug) or self.listed.get(slug)

    async def markets(self, slugs):
        self.path_asked.append(list(slugs))
        return {s: self.listed[s] for s in slugs if s in self.listed}

    async def accepting(self, condition_id):
        self.accept_asks += 1
        return condition_id not in self.not_accepting

    async def book(self, asset):
        return None

    async def server_time(self):
        return None

    async def close(self):
        return None


def settings(**kw) -> config.Settings:
    env = {"PREOPEN_MARKETS": "btc,eth", "PREOPEN_AHEAD_MINUTES": "60"}
    env.update({k: str(v) for k, v in kw.items()})
    return config.from_env(env)


def make_engine(s: config.Settings | None = None, clock: Clock | None = None, ledger: Ledger | None = None,
                exchange: PaperExchange | None = None, public: FakePublic | None = None, balance: str = "1000"):
    clock = clock or Clock()
    s = s or settings()
    public = public or FakePublic(clock)
    exchange = exchange or PaperExchange(D(balance), clock=clock)
    ledger = ledger or Ledger(":memory:")
    notes: list[str] = []
    e = Engine(s, exchange, public, ledger, notify=lambda text, key="": notes.append(text), clock=clock)
    e.notes = notes  # type: ignore[attr-defined]
    return e, clock, public, exchange


async def drain(e: Engine) -> None:
    """Run the queued windows through the workers' path until the queue is empty (parked ones stay parked)."""
    while not e.queue.empty():
        *_, slug = e.queue.get_nowait()
        e.queued.discard(slug)
        await e.process(slug)


async def tick(e: Engine, seconds: float = 0.0, account: bool = True) -> None:
    """Move the clock and run one pass of every loop (account, discovery, workers, cancel, reconcile, settle); the
    retries parked during the previous tick fire (time has passed)."""
    e.clock.advance(seconds)
    if account:
        await e.account_once()
    unpark(e)
    await e.discover()
    await drain(e)
    await e.cancel_pass()
    await e.reconcile_pass()
    await e.settle_pass()


def run(coro):
    return asyncio.run(coro)


def unpark(e: Engine) -> None:
    """Fire every parked retry now (the timers would after their delay)."""
    for slug, handle in list(e.timers.items()):
        handle.cancel()
        e.timers.pop(slug, None)
        e.enqueue(slug)


os.environ.pop("LIVE", None)
