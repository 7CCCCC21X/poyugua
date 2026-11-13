"""Public reads (no key): Gamma for the listings and results, the CLOB for "accepting orders" and the books (probe).

Gamma's GET /markets takes many slug= at once (open markets only unless closed=true; `limit` must cover them all),
so a pass over a day of windows is a few dozen requests and the next listing of every coin is one request.

Gamma answers carry `cache-control: public, max-age=300` and CloudFront serves them from its cache: a slug asked
before its listing keeps answering [] for up to five minutes (measured 11-13: "Hit from cloudfront, age: 264" while
the market had been listed for four minutes). Every read therefore carries a parameter of its own (_=<ns>).
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any

import httpx

LOG = logging.getLogger(__name__)
GAMMA = "https://gamma-api.polymarket.com"
CLOB = "https://clob.polymarket.com"
SLUGS_PER_REQUEST = 40
UA = "poyugua-preopen/1.0"


class PublicApi:
    def __init__(self, timeout: float = 10.0, client: httpx.AsyncClient | None = None):
        self.http = client or httpx.AsyncClient(timeout=timeout, headers={"User-Agent": UA},
                                                limits=httpx.Limits(max_connections=20, max_keepalive_connections=10))
        self.requests = 0

    async def close(self) -> None:
        await self.http.aclose()

    async def _get(self, url: str, params: Any = None) -> Any:
        self.requests += 1
        r = await self.http.get(url, params=params)
        if r.status_code == 404:
            return None
        r.raise_for_status()
        return r.json()

    async def listings(self, slugs: list[str], closed: bool = False) -> dict[str, dict]:
        """The Gamma markets of these slugs that exist (open ones; closed ones too with closed=True), by slug."""
        out: dict[str, dict] = {}
        chunks = [slugs[i:i + SLUGS_PER_REQUEST] for i in range(0, len(slugs), SLUGS_PER_REQUEST)]

        async def one(chunk: list[str]) -> None:
            params = [("slug", s) for s in chunk] + [("limit", str(len(chunk) + 5)), ("_", str(time.time_ns()))]
            if closed:
                params.append(("closed", "true"))
            data = await self._get(f"{GAMMA}/markets", params=params)
            for m in data or []:
                if isinstance(m, dict) and m.get("slug") in chunk:
                    out[str(m["slug"])] = m
        gate = asyncio.Semaphore(4)

        async def gated(chunk: list[str]) -> None:
            async with gate:
                await one(chunk)
        await asyncio.gather(*(gated(c) for c in chunks))
        return out

    async def market(self, slug: str) -> dict | None:
        """One market by slug, closed or not. This path is a direct lookup: a new market answers here within a second
        of its creation, while the GET /markets?slug= query shows it only ~88 s later (measured 11-13 03:12:30:
        /markets/slug +0.6 s, /events/slug +0.8 s, /markets?slug= and /events?slug= +88 s)."""
        data = await self._get(f"{GAMMA}/markets/slug/{slug}", params={"_": str(time.time_ns())})
        return data if isinstance(data, dict) else None

    async def markets(self, slugs: list[str]) -> dict[str, dict]:
        """Several markets by the direct path, side by side (each coin's next window); errors count as not found."""
        answers = await asyncio.gather(*(self.market(s) for s in slugs), return_exceptions=True)
        return {s: a for s, a in zip(slugs, answers) if isinstance(a, dict) and a.get("slug") == s}

    async def accepting(self, condition_id: str) -> bool | None:
        """Whether the CLOB accepts orders on this market now (its own flag, ahead of Gamma's)."""
        data = await self._get(f"{CLOB}/clob-markets/{condition_id}", params={"_": str(time.time_ns())})
        if not isinstance(data, dict):
            return None
        value = data.get("ao", data.get("accepting_orders"))
        return bool(value) if value is not None else None

    async def book(self, asset: str) -> dict | None:
        data = await self._get(f"{CLOB}/book", params={"token_id": asset})
        return data if isinstance(data, dict) else None

    async def server_time(self) -> float | None:
        try:
            data = await self._get(f"{CLOB}/time")
        except Exception as error:
            LOG.debug("clob time: %s", error)
            return None
        try:
            return float(json.loads(json.dumps(data)))
        except (TypeError, ValueError):
            return None


def best(book: dict | None, side: str) -> tuple[float, float] | None:
    """The best bid or ask of a CLOB book answer: (price, size)."""
    if not book:
        return None
    levels = [(float(x["price"]), float(x["size"])) for x in book.get(side, []) if isinstance(x, dict)]
    if not levels:
        return None
    return max(levels) if side == "bids" else min(levels)


def depth_at(book: dict | None, side: str, price: float) -> float:
    if not book:
        return 0.0
    return sum(float(x["size"]) for x in book.get(side, []) if isinstance(x, dict) and abs(float(x["price"]) - price) < 1e-9)
