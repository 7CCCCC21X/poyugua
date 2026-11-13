"""Public reads (no key): Gamma for the listings and results, the CLOB for "accepting orders" and the books (probe).

Gamma's GET /markets takes many slug= at once (open markets only unless closed=true; `limit` must cover them all),
so a pass over a day of windows is a few dozen requests and the next listing of every coin is one request.

Gamma answers carry `cache-control: public, max-age=300` and CloudFront serves them from its cache: a slug asked
before its listing keeps answering [] for up to five minutes (measured 11-13: "Hit from cloudfront, age: 264" while
the market had been listed for four minutes). Every read therefore carries a parameter of its own (_=<ns>).

A 429 is obeyed: the host is left alone for its Retry-After (10 s without one) and every read of it meanwhile raises
PublicThrottled at once, without a request. A timeout, a 429 or a 5xx is an error, never "no such market": the callers
ask again later rather than conclude anything from it.
"""
from __future__ import annotations

import asyncio
import collections
import email.utils
import json
import logging
import math
import time
from typing import Any
from urllib.parse import urlsplit

import httpx

LOG = logging.getLogger(__name__)
GAMMA = "https://gamma-api.polymarket.com"
CLOB = "https://clob.polymarket.com"
SLUGS_PER_REQUEST = 40
UA = "poyugua-preopen/1.0"


class PublicThrottled(Exception):
    """A public host answered 429: left alone for `seconds`."""

    def __init__(self, host: str, seconds: float):
        super().__init__(f"{host} 限流，{seconds:.0f} 秒后再问")
        self.host, self.seconds = host, seconds


def retry_after(response: httpx.Response, default: float = 10.0) -> float:
    """The Retry-After of an answer, in seconds (a number or an HTTP date), within 1 s to 10 minutes."""
    raw = (response.headers.get("retry-after") or "").strip()
    seconds: float | None = None
    if raw:
        try:
            seconds = float(raw)
        except ValueError:
            try:
                seconds = email.utils.parsedate_to_datetime(raw).timestamp() - time.time()
            except (TypeError, ValueError):
                seconds = None
    if seconds is None or not math.isfinite(seconds):
        seconds = default
    return max(1.0, min(seconds, 600.0))


class PublicApi:
    def __init__(self, timeout: float = 10.0, client: httpx.AsyncClient | None = None, clock=time.monotonic):
        self.http = client or httpx.AsyncClient(timeout=timeout, headers={"User-Agent": UA},
                                                limits=httpx.Limits(max_connections=20, max_keepalive_connections=10))
        self.clock = clock
        self.requests = 0
        self.blocked_until: dict[str, float] = {}       # host -> when its 429 lifts
        self.errors: collections.Counter = collections.Counter()   # host -> failed reads (timeouts, 5xx, 429)
        self.last_error = ""

    async def close(self) -> None:
        await self.http.aclose()

    def health(self) -> str:
        now = self.clock()
        blocked = [f"{h} 限流还剩 {until - now:.0f} 秒" for h, until in self.blocked_until.items() if until > now]
        failed = "，".join(f"{h} 失败 {n} 次" for h, n in self.errors.most_common(3))
        return "；".join(x for x in (f"{self.requests} 次请求", *blocked, failed, self.last_error and f"最近：{self.last_error[:120]}") if x)

    async def _get(self, url: str, params: Any = None) -> Any:
        host = urlsplit(url).netloc
        wait = self.blocked_until.get(host, 0.0) - self.clock()
        if wait > 0:
            raise PublicThrottled(host, wait)
        self.requests += 1
        try:
            r = await self.http.get(url, params=params)
        except httpx.HTTPError as error:
            self.errors[host] += 1
            self.last_error = f"{host} {type(error).__name__} {error}"
            raise
        if r.status_code == 429:
            seconds = retry_after(r)
            self.blocked_until[host] = self.clock() + seconds
            self.errors[host] += 1
            self.last_error = f"{host} 429（{seconds:.0f} 秒）"
            LOG.warning("公共接口 %s 限流：%.0f 秒内不再请求", host, seconds)
            raise PublicThrottled(host, seconds)
        if r.status_code == 404:
            return None
        if r.status_code >= 400:
            self.errors[host] += 1
            self.last_error = f"{host} HTTP {r.status_code}"
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
        """Several markets by the direct path, side by side (each coin's next window). A slug whose read failed is
        left out (asked again at the next poll, never taken as unlisted for good); a 429 is raised for the caller to
        back off."""
        answers = await asyncio.gather(*(self.market(s) for s in slugs), return_exceptions=True)
        for a in answers:
            if isinstance(a, PublicThrottled):
                raise a
            if isinstance(a, BaseException) and not isinstance(a, Exception):
                raise a
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
