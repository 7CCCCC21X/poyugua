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
VERSIONS = ("v1", "v2")   # Gamma's market protocols: a batch query lists one of them, so each is asked
V2_PREFIX = "polyv2"      # the temporary V2 test markets (11-20) were named polyv2<coin>-updown-5m-<t>, beside the live
                          # <coin>-updown-5m-<t>: a market is looked for under that name too, and kept under the bot's own
OTHER_NAME_S = 5.0        # the fast path asks a coin's other name this often (the name its markets have, every poll)
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
        self.prefix: dict[str, str] = {}         # coin -> the name its latest market was found under ("" or V2_PREFIX)
        self.other_asked: dict[str, float] = {}  # coin -> when the fast path last asked its other name

    def kept(self, m: dict, slug: str) -> dict:
        """A market found for the bot's `slug`, under that name or the V2 test one: kept under `slug` (its own name in
        listedSlug); the coin's next markets asked first by the name this one had."""
        name = str(m.get("slug") or "")
        self.prefix[coin_of(slug)] = "" if name == slug else V2_PREFIX
        return m if name == slug else {**m, "slug": slug, "listedSlug": name}

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
        """The Gamma markets of these slugs that exist (open ones; closed ones too with closed=True), by slug, of both
        protocols: GET /markets?slug= lists the CTF (v1) markets alone unless asked for version=v2 (and takes no
        "all"; measured 11-22), and from 11-23 (BNB, DOGE, HYPE, SOL, XRP, ZEC) and 11-24 (BTC, ETH) the new
        Up/Down markets are Polymarket Protocol V2 ones."""
        out: dict[str, dict] = {}
        half = SLUGS_PER_REQUEST // 2   # each slug asked under both names
        chunks = [slugs[i:i + half] for i in range(0, len(slugs), half)]

        async def one(chunk: list[str], version: str) -> None:
            names = [n for s in chunk for n in (s, V2_PREFIX + s)]
            params = [("slug", n) for n in names] + [("limit", str(len(names) + 5)), ("version", version),
                                                      ("_", str(time.time_ns()))]
            if closed:
                params.append(("closed", "true"))
            data = await self._get(f"{GAMMA}/markets", params=params)
            wanted = set(chunk)
            for m in data or []:
                if not isinstance(m, dict):
                    continue
                name = str(m.get("slug") or "")
                slug = name[len(V2_PREFIX):] if name.startswith(V2_PREFIX) else name
                if slug in wanted and (name == slug or slug not in out):   # both there: the bot's own name
                    out[slug] = self.kept(m, slug)
        gate = asyncio.Semaphore(4)

        async def gated(chunk: list[str], version: str) -> None:
            async with gate:
                await one(chunk, version)
        await asyncio.gather(*(gated(c, v) for c in chunks for v in VERSIONS))
        return out

    async def path(self, name: str) -> dict | None:
        """GET /markets/slug/<name>: a direct lookup, closed or not. A new market answers here within a second of its
        creation, while the GET /markets?slug= query shows it only ~88 s later (measured 11-13 03:12:30:
        /markets/slug +0.6 s, /events/slug +0.8 s, /markets?slug= and /events?slug= +88 s)."""
        data = await self._get(f"{GAMMA}/markets/slug/{name}", params={"_": str(time.time_ns())})
        return data if isinstance(data, dict) and data.get("slug") == name else None

    async def market(self, slug: str) -> dict | None:
        """One market by the bot's slug, closed or not: under that name, else under the V2 test one."""
        for name in (slug, V2_PREFIX + slug):
            if (m := await self.path(name)) is not None:
                return self.kept(m, slug)
        return None

    async def markets(self, slugs: list[str]) -> dict[str, dict]:
        """Several markets by the direct path, side by side (each coin's next window): each under the name its coin's
        markets have, and the other name every OTHER_NAME_S (a coin moved to V2 found within seconds, at no double
        rate). A slug whose read failed is left out (asked again at the next poll, never taken as unlisted for good);
        a 429 is raised for the caller to back off."""
        now = self.clock()
        asks: list[tuple[str, str]] = []
        for s in slugs:
            coin = coin_of(s)
            mine = self.prefix.get(coin, "")
            asks.append((s, mine + s))
            if now - self.other_asked.get(coin, float("-inf")) >= OTHER_NAME_S:
                self.other_asked[coin] = now
                asks.append((s, ("" if mine else V2_PREFIX) + s))
        answers = await asyncio.gather(*(self.path(name) for _, name in asks), return_exceptions=True)
        for a in answers:
            if isinstance(a, PublicThrottled):
                raise a
            if isinstance(a, BaseException) and not isinstance(a, Exception):
                raise a
        out: dict[str, dict] = {}
        for (s, name), a in zip(asks, answers):
            if isinstance(a, dict) and (name == s or s not in out):   # both there: the bot's own name
                out[s] = self.kept(a, s)
        return out

    async def clob_market(self, condition_id: str) -> dict | None:
        """The CLOB's own record of a market: `ao` (accepting orders), `t` (its tokens), None when it has none."""
        data = await self._get(f"{CLOB}/clob-markets/{condition_id}", params={"_": str(time.time_ns())})
        return data if isinstance(data, dict) else None

    async def accepting(self, condition_id: str) -> bool | None:
        """Whether the CLOB accepts orders on this market now (its own flag, ahead of Gamma's)."""
        data = await self.clob_market(condition_id)
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


def coin_of(slug: str) -> str:
    """The coin of a <coin>-updown-5m-<start> slug."""
    return slug.split("-updown-", 1)[0]


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
