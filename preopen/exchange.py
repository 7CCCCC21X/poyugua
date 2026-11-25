"""The exchange behind the engine: the live one over Polymarket's unified SDK (polymarket-client), the paper one for
LIVE=off and the tests. The engine sees only this module's types, so an SDK change touches this file alone.

Outcomes of a placement, per order:
    open       the CLOB accepted it (its id is known)
    rejected   the CLOB refused it outright (post-only would cross, an invalid field, the balance): nothing rests
    not_sent   nothing was processed: signing failed, the request never left, or the answer says in so many words that it
               was turned away before processing (429, 425 restarting, 503 cancel-only / post-only / trading disabled,
               401/403 at the gate)
    unknown    sent, and whether it was processed is not known (a timeout, a lost answer, any other 5xx): it may rest

Every order is signed before anything is sent and the caller is handed the orders' hashes (`on_signed`) before the first
POST: the caller writes them down first, so an order the process loses track of (a crash mid-send) is still found by its
hash, cancelled by it and its fills read from the trades.
"""
from __future__ import annotations

import asyncio
import collections
import contextlib
import logging
import time
from dataclasses import dataclass, field
from decimal import Decimal as D
from typing import Any, Callable, Protocol

LOG = logging.getLogger(__name__)
BATCH = 15                      # POST /orders takes 1 to 15 orders
POSTS_IN_FLIGHT = 1             # the account's order POSTs at a time: the CLOB times out bursts of concurrent submissions
                                # from one account ("order timed out", docs); what waits meanwhile goes in the next batch
TIMED_OUT_WAIT_S, TIMED_OUT_MAX_S = 5.0, 60.0   # after "order timed out": no new order this long, doubling while it lasts
# the per-signer order bucket of each tier (docs: api-reference/trading-rate-limits): tokens/s, burst
TIER_LIMITS = {"standard": (40, 60), "copper": (60, 90), "bronze": (80, 120), "silver": (200, 300), "gold": (400, 600),
               "platinum": (450, 675), "diamond": (525, 787), "elite": (600, 900)}
RATE_MARGIN = 0.9         # the local order bucket never above this share of the tier's rate (nor its burst less 5)
MIN_ORDER_RATE = 5.0      # a 429 halves the local order rate down to this...
RATE_RECOVER_S = 30       # ...and every this long without one it grows half again, back to the cap
CANCEL_CHUNK = 200              # DELETE /orders takes up to 1000 ids; a chunk is also kept within the local cancel bucket's burst
# The CLOB's refusal messages (docs: resources/error-codes, "Order Processing Errors"). They come per order in a 200, or
# for the whole request as a 400 (`not enough balance / allowance` among them): the words decide the code, not the status.
REFUSALS = (
    ("not enough balance / allowance", "not_enough_balance"),
    ("crosses book", "post_only_would_cross"),
    ("crosses the book", "post_only_would_cross"),
    ("minimum tick size", "invalid_tick"),
    ("lower than the minimum", "invalid_size"),
    ("duplicated", "duplicated"),
    ("invalid expiration", "invalid_expiration"),
    ("not yet ready to process", "market_not_ready"),
    ("match delayed", "delayed"),
    ("canceled in the ctf exchange", "canceled_onchain"),
    ("address banned", "banned"),
    ("closed only mode", "closed_only"),
    ("too many orders in payload", "batch_too_large"),
    ("invalid order payload", "invalid_payload"),
    ("owner of the api key", "auth"),
    ("address of the api key", "auth"),
    ("order timed out", "timed_out"),
    ("invalid token id", "token_not_ready"),       # the matching engine has not registered the market's tokens yet
    ("no orderbook exists", "token_not_ready"),
    ("could not insert order", "insert_failed"),
    ("cancel-only", "cancel_only"),
    ("post-only mode", "post_only_mode"),
)
# Refusals about this order on this market's book (its price crossing, its tick or size, the market's own state): counted
# against the window, which is quoted again up to PREOPEN_MAX_ATTEMPTS. Any other refusal is not the window's doing (the
# account's balance, an exchange rule, a restriction, a lost answer, words not recognised): it uses no attempt, and the
# engine holds all new placement for a while instead (Engine.refused), so one account-wide refusal never strikes off
# every window it meets.
WINDOW_CODES = {"post_only_would_cross", "invalid_tick", "invalid_size", "invalid", "canceled_onchain", "unmatched",
                "fok_not_filled", "fak_not_filled"}


TRANSIENT_WAIT = {"insert_failed": 5.0}   # the exchange's own hiccup under load: not inserted; sent again after this
NO_SPLIT = {"auth", "banned", "closed_only"}   # a whole request refused for these: the account's, not one window's


def refusal_code(message: str, fallback: str) -> str:
    text = (message or "").lower()
    for needle, code in REFUSALS:
        if needle in text:
            return code
    return fallback


def retryable(code: str) -> bool:
    """Not the window's own doing: the window is placed again without using an attempt."""
    return code not in WINDOW_CODES


def unrecognised(code: str) -> bool:
    """A refusal whose words were not recognised: the account's or the window's, told apart by what follows."""
    return code == "unknown" or (code.startswith("http_4") and code not in ("http_408", "http_409"))


OnSigned = Callable[[list[tuple[str, str, int]]], None]   # [(ref, hash, salt)] of the orders signed, before any is sent
TERMINAL = {"MATCHED", "INVALID", "EXPIRED", "FILLED", "REJECTED"}  # ...and every CANCELED* status

STANDARD_EXCHANGE = "0xE111180000d2663C0091e4f400237545B87B996B"
NEG_RISK_EXCHANGE = "0xe2222d279d744050d28e00520010520000310F59"
V2_EXCHANGE = "0xe3333700cA9d93003F00f0F71f8515005F6c00Aa"


@dataclass
class OrderSpec:
    ref: str            # the caller's handle: <quote key>/<side>
    asset: str          # the outcome's asset id
    price: D
    size: D
    expiration: int     # the stated GTD expiration; the order lives until 60 s before it
    neg_risk: bool = False
    market: str = ""    # the condition id (the paper book keeps it; the CLOB knows it from the token)


@dataclass
class Placed:
    ref: str
    status: str                 # open | rejected | not_sent | unknown
    order_id: str = ""
    code: str = ""
    message: str = ""
    hash: str = ""              # the order's EIP-712 hash, computed here (the CLOB's order id, as far as is known)
    salt: int = 0
    matched: D = D(0)
    wait: float = 0.0           # > 0: the request was refused before processing (429, restart): hold this long
    whole: bool = False         # the refusal was for the whole request (an HTTP error), not for this order alone
    batch: int = 0              # orders in the request it went in
    http: int = 0               # the HTTP status of a refused request


@dataclass
class Row:
    """An order as the CLOB shows it."""
    id: str
    asset: str
    market: str
    side: str
    price: D
    size: D
    matched: D
    status: str
    created: float | None = None

    @property
    def state(self) -> str:
        return self.status.upper().removeprefix("ORDER_STATUS_")

    @property
    def resting(self) -> bool:
        """Whether the order may still rest. An unknown status counts as resting while anything is unfilled: wrongly
        thinking an order gone stops it being cancelled; wrongly thinking it rests only costs another cancel."""
        state = self.state
        if self.matched >= self.size or state in TERMINAL or state.startswith("CANCEL"):
            return False
        return True


@dataclass
class Cancelled:
    canceled: set[str] = field(default_factory=set)
    not_canceled: dict[str, str] = field(default_factory=dict)
    throttle: tuple[str, float] | None = None   # a later chunk was refused (429, restart): (reason, seconds); its ids are in neither list
    unanswered: set[str] = field(default_factory=set)   # ids of the chunks not answered (refused, failed): asked again
    error: str = ""                                     # why a later chunk failed (not a refusal before processing)


class Throttled(Exception):
    """The whole request was refused before it was processed (a 429, a restart, cancel-only): wait `seconds`."""

    def __init__(self, reason: str, seconds: float, code: str = "rate_limited"):
        super().__init__(reason)
        self.reason, self.seconds, self.code = reason, seconds, code


class Exchange(Protocol):
    wallet: str
    live: bool

    async def start(self) -> None: ...
    async def close(self) -> None: ...
    def prime(self, listing: Any) -> None: ...
    async def place(self, specs: list[OrderSpec], on_signed: OnSigned | None = None) -> list[Placed]: ...
    async def cancel(self, ids: list[str]) -> Cancelled: ...
    async def cancel_market(self, condition_id: str) -> Cancelled: ...   # every order of the API key on a market
    async def cancel_all(self) -> Cancelled: ...                          # every order of the API key
    async def open_orders(self) -> list[Row]: ...
    async def order(self, order_id: str) -> Row | None: ...
    async def fills(self, condition_id: str) -> dict[str, D]: ...       # order id -> shares bought, from the trades
    async def trade_log(self, condition_id: str) -> dict[str, list[tuple[float, D]]]: ...  # order id -> [(matched at, shares)]
    async def balance(self) -> D: ...
    async def redeem(self, condition_id: str) -> str: ...
    def redeem_problem(self) -> str: ...                                  # why winnings cannot be claimed from here ("")
    def identity(self) -> str: ...                                        # the account the orders belong to


class Bucket:
    """A local token bucket kept under the CLOB's per-signer limits (order and cancel buckets are separate)."""

    def __init__(self, rate: float, burst: int, clock=time.monotonic):
        self.rate, self.burst, self.clock = rate, burst, clock
        self.tokens, self.at = float(burst), clock()
        self.lock = asyncio.Lock()

    def _refill(self) -> None:
        now = self.clock()
        self.tokens = min(self.burst, self.tokens + (now - self.at) * self.rate)
        self.at = now

    async def take(self, n: int) -> None:
        n = min(n, self.burst)
        async with self.lock:
            while True:
                self._refill()
                if self.tokens >= n:
                    self.tokens -= n
                    return
                await asyncio.sleep((n - self.tokens) / self.rate)


def order_hash(signed: Any, *, neg_risk: bool, chain_id: int = 137) -> str:
    """keccak(0x1901 || domain || hashStruct(Order)) of a signed order: the CTF Exchange's hashOrder, which the CLOB
    uses as the order id. Computed from the public SignedOrder fields; "" if anything is off."""
    try:
        from eth_abi.abi import encode
        from eth_utils.crypto import keccak
        order_type = ("Order(uint256 salt,address maker,address signer,uint256 tokenId,uint256 makerAmount,"
                      "uint256 takerAmount,uint8 side,uint8 signatureType,uint256 timestamp,bytes32 metadata,bytes32 builder)")
        token = int(str(signed.token_id))
        v2 = token & ((1 << 256) - 1) and _is_v2_position(str(signed.token_id))
        version = "3" if v2 else "2"
        exchange = V2_EXCHANGE if v2 else NEG_RISK_EXCHANGE if neg_risk else STANDARD_EXCHANGE
        domain = keccak(encode(["bytes32", "bytes32", "bytes32", "uint256", "address"], [
            keccak(b"EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)"),
            keccak(b"Polymarket CTF Exchange"), keccak(version.encode()), chain_id, exchange]))
        side = 0 if str(signed.side).upper() == "BUY" else 1
        struct = keccak(encode(
            ["bytes32", "uint256", "address", "address", "uint256", "uint256", "uint256", "uint8", "uint8", "uint256", "bytes32", "bytes32"],
            [keccak(order_type.encode()), int(signed.salt), str(signed.maker), str(signed.signer), token, int(signed.maker_amount),
             int(signed.taker_amount), side, int(signed.signature_type), int(signed.timestamp),
             bytes.fromhex(str(signed.metadata)[2:]), bytes.fromhex(str(signed.builder)[2:])]))
        return "0x" + keccak(b"\x19\x01" + domain + struct).hex()
    except Exception as error:  # the hash is a convenience: an order without it is still placed and found by its id
        LOG.debug("order hash: %s", error)
        return ""


def signing_speed(rounds: int = 5) -> tuple[str, float]:
    """The ECDSA backend eth-keys signs with, and the milliseconds one order takes to sign: measured on a throwaway key
    over a message of an order's shape. coincurve (libsecp256k1, in requirements.lock) ~1 ms; eth-keys' own pure-Python
    backend, which it falls back to when coincurve cannot be imported, ~6 ms: the same signatures, byte for byte.
    ("", 0.0) when it cannot be measured."""
    try:
        from eth_account import Account
        from eth_keys.backends import get_backend
        account = Account.create()
        message = {
            "types": {
                "EIP712Domain": [{"name": "name", "type": "string"}, {"name": "version", "type": "string"},
                                 {"name": "chainId", "type": "uint256"}, {"name": "verifyingContract", "type": "address"}],
                "Order": [{"name": n, "type": t} for n, t in (
                    ("salt", "uint256"), ("maker", "address"), ("signer", "address"), ("tokenId", "uint256"),
                    ("makerAmount", "uint256"), ("takerAmount", "uint256"), ("side", "uint8"), ("signatureType", "uint8"),
                    ("timestamp", "uint256"), ("metadata", "bytes32"), ("builder", "bytes32"))]},
            "primaryType": "Order",
            "domain": {"name": "Polymarket CTF Exchange", "version": "3", "chainId": 137, "verifyingContract": V2_EXCHANGE},
            "message": {"salt": 1, "maker": account.address, "signer": account.address, "tokenId": 10**70,
                        "makerAmount": 4_900_000, "takerAmount": 10_000_000, "side": 0, "signatureType": 3,
                        "timestamp": 1795500000000, "metadata": b"\x00" * 32, "builder": b"\x00" * 32}}
        account.sign_typed_data(full_message=message)   # the first one loads the library
        began = time.perf_counter()
        for _ in range(rounds):
            account.sign_typed_data(full_message=message)
        return type(get_backend()).__name__, (time.perf_counter() - began) / rounds * 1000
    except Exception as error:
        LOG.debug("signing speed: %s", error)
        return "", 0.0


def v2_allowance_of(allowances: Any) -> D | None:
    """The pUSD that ExchangeV3 (Polymarket Protocol V2's exchange) may spend for the account, from the CLOB's COLLATERAL
    allowances by spender; None when the CLOB names no allowance for it (not known). A V2 BUY needs it: the CTF
    approvals do not cover it (docs: Polymarket Protocol V2 API Migration, 11-22)."""
    if not isinstance(allowances, dict):
        return None
    for spender, amount in allowances.items():
        if str(spender).lower() == V2_EXCHANGE.lower():
            try:
                return D(int(amount)) / D(1_000_000)
            except (TypeError, ValueError):
                return None
    return None


def _is_v2_position(asset: str) -> bool:
    try:
        from polymarket._internal.protocol import is_v2_position_id
        return bool(is_v2_position_id(asset))
    except Exception:
        return False


DISABLED_TEXTS = ("trading is currently disabled", "trading disabled", "trading is disabled")


def refused_before_processing(error: Any) -> str:
    """The code of an HTTP refusal that says the request was turned away before any order in it was processed, or "".
    425 (the matching engine restarting) and a 503 the SDK recognised as cancel-only or post-only mode, or whose text
    says trading is disabled; 401/403 (the key, the signature or the region refused at the gate). Any other 5xx (a
    gateway timing out, a proxy's 502/503/504) may come after the orders were processed: not this."""
    if error.restriction:
        return str(error.restriction)
    if error.status == 425:
        return "restarting"
    if error.status in (401, 403):
        return "auth"
    if error.status == 503 and any(t in str(error).lower() for t in DISABLED_TEXTS):
        return "trading_disabled"
    return ""


def _no_such_order(error: BaseException) -> bool:
    """Whether an unreadable single-order answer was an empty one (null, {}): the CLOB has no order with that id."""
    cause = error.__cause__
    try:
        from pydantic import ValidationError
    except ImportError:  # pragma: no cover
        return False
    if not isinstance(cause, ValidationError):
        return False
    inputs = [e.get("input") for e in cause.errors()]
    return bool(inputs) and all(i is None or i == {} or i == "" or i == [] for i in inputs)


def _cause_not_sent(error: BaseException) -> bool:
    """Whether a transport failure happened before the request left (no connection, a pool timeout)."""
    try:
        import httpx
    except ImportError:  # pragma: no cover
        return False
    cause = error.__cause__
    return isinstance(cause, (httpx.ConnectError, httpx.ConnectTimeout, httpx.PoolTimeout))


def _row(o: Any) -> Row:
    created = getattr(o, "created_at", None)
    return Row(id=str(o.id), asset=str(o.asset_id), market=str(o.condition_id), side=str(o.side).upper(), price=D(str(o.price)),
               size=D(str(o.original_size)), matched=D(str(o.size_matched)), status=str(o.status or "").upper(),
               created=created.timestamp() if created is not None else None)


class LiveExchange:
    """Polymarket through polymarket-client's AsyncSecureClient: signing, batches of 15, cancels by id and by market."""
    live = True

    def __init__(self, settings: Any, *, order_bucket: Bucket | None = None, cancel_bucket: Bucket | None = None):
        self.settings = settings
        self.client: Any = None
        self.wallet = ""
        self.wallet_type = ""
        self.v2_allowance: D | None = None   # what ExchangeV3 may spend (None: the CLOB does not say), read with the balance
        self.signing: tuple[str, float] = ("", 0.0)   # the ECDSA backend and the ms an order takes to sign (start())
        self.tier = ""
        self.rate_warning = False
        self.wallet_list_off_until = 0.0
        self.orders = order_bucket or Bucket(settings.order_rate, settings.order_burst)
        self.cancels = cancel_bucket or Bucket(settings.cancel_rate, settings.cancel_burst)
        self.configured = (float(settings.order_rate), int(settings.order_burst))   # the settings' order bucket
        self.sent: collections.deque = collections.deque(maxlen=4000)   # [monotonic, orders, tokens taken?] of every order request
        self.limited: collections.deque = collections.deque(maxlen=200)  # each 429 of the order bucket, with the bot's own pace
        self.backoff_at = 0.0
        self._outbox: list[tuple[list[tuple[OrderSpec, Any, str]], asyncio.Future]] = []   # signed, waiting for the sender
        self._senders: list[asyncio.Task] = []
        self._wake: asyncio.Event | None = None
        self._timeouts = 0              # "order timed out" answers in a row

    def _on_rate(self, update: Any) -> None:
        if getattr(update, "tier", None) and str(update.tier) != self.tier:
            self.tier = str(update.tier)
            self.set_order_limits(*self.configured)  # the tier known: the local bucket kept under it
        if getattr(update, "warning", False):
            self.rate_warning = True

    def order_cap(self) -> tuple[float, int]:
        """The settings' order bucket, kept under the tier's own (its rate x 0.9, its burst less 5) once it is known."""
        rate, burst = self.configured
        tier = TIER_LIMITS.get(self.tier.lower())
        if tier:
            rate, burst = min(rate, tier[0] * RATE_MARGIN), min(burst, tier[1] - 5)
        return rate, burst

    def set_order_limits(self, rate: float, burst: int) -> None:
        """New settings (or the tier learned): the local order bucket at them, capped by the tier."""
        self.configured = (float(rate), int(burst))
        cap_rate, cap_burst = self.order_cap()
        self.orders.rate, self.orders.burst = cap_rate, cap_burst
        self.orders.tokens = min(self.orders.tokens, float(cap_burst))

    def own_left(self, now: float) -> float:
        """What the signer's order bucket would hold now had this program alone spent from it: the tier's burst, filled
        again at the tier's rate, less every order this program sent (docs, trading rate limits: POST /orders costs a
        token an order, whatever becomes of it; a request answered 429 is not processed and takes none). Below zero:
        this program sent more than the tier admits."""
        rate, burst = TIER_LIMITS.get(self.tier.lower(), TIER_LIMITS["standard"])
        level, at = float(burst), None
        for t, n, taken in self.sent:
            if at is not None:
                level = min(float(burst), level + (t - at) * rate)
            at = t
            if taken:
                level -= n
        return level if at is None else min(float(burst), level + (now - at) * rate)

    def note_limited(self, error: Any, orders: int = 0) -> None:
        """A 429 on an order request of `orders` orders: kept with what tells whose doing it was. `own_left`: what this
        program's own orders had left in the signer's bucket; `remaining`: what the exchange said was left (its
        Poly-RateLimit-Remaining; None when the answer carried no such state: not the per-signer limiter's). The
        request fitting by the program's own count, the 429 was not its doing (11-25: 35 an hour, every one at a
        market's opening, the last with 8 orders sent in ten seconds: something else spends this signer's tokens, or
        another limiter answered). Only when it was the program's own is its pace halved and its bucket emptied:
        slowing down for someone else's orders left it at 5 orders a second through every opening."""
        now = time.monotonic()
        if self.sent:
            self.sent[-1][2] = False   # the request just answered (one POST is in flight at a time): not processed
        own = sum(n for t, n, _ in self.sent if now - t <= 10)
        left = self.own_left(now)
        tier = TIER_LIMITS.get(self.tier.lower(), TIER_LIMITS["standard"])
        state = getattr(error, "rate_limit", None)
        mine = left < orders
        self.limited.append({"at": time.time(), "orders": orders, "own10": own, "own_left": round(left, 1),
                             "allowance": tier[1] + 10 * tier[0], "burst": tier[1], "per_s": tier[0],
                             "remaining": getattr(state, "remaining", None), "retry": float(error.retry_after or 0),
                             "rate": round(self.orders.rate, 1), "mine": mine})
        if mine:
            self.orders.rate = max(MIN_ORDER_RATE, self.orders.rate / 2)
            self.orders.tokens = 0.0
            self.backoff_at = now

    def recover(self) -> None:
        cap, _ = self.order_cap()
        now = time.monotonic()
        if self.orders.rate < cap and now - self.backoff_at >= RATE_RECOVER_S:
            self.orders.rate = min(cap, self.orders.rate * 1.5)
            self.backoff_at = now

    def rate_status(self) -> dict[str, Any]:
        """The order pace for /check and 🧾 拒单: now, the cap, the tier, the 429s of the last hour, how many of them the
        bot's own orders do not explain (`others`; of those, `unreported`: answered without the signer's rate-limit
        state) and the latest of each kind."""
        hour = [x for x in self.limited if time.time() - x["at"] <= 3600]
        cap, _ = self.order_cap()
        tier = TIER_LIMITS.get(self.tier.lower())
        others = [x for x in hour if not x.get("mine")]
        return {"rate": round(self.orders.rate, 1), "cap": round(cap, 1), "configured": self.configured[0], "tier": self.tier,
                "tier_rate": tier[0] if tier else None, "limited_hour": len(hour), "others": len(others),
                "unreported": sum(1 for x in others if x.get("remaining") is None),
                "last": hour[-1] if hour else None, "last_other": others[-1] if others else None}

    async def start(self) -> None:
        from polymarket import ApiKeyCreds, AsyncSecureClient, RelayerApiKey
        s = self.settings
        kwargs: dict[str, Any] = {"private_key": s.private_key, "on_rate_limit_update": self._on_rate,
                                  "logger": logging.getLogger("polymarket")}
        if s.wallet:
            kwargs["wallet"] = s.wallet
        if s.clob_key:
            kwargs["credentials"] = ApiKeyCreds(key=s.clob_key, secret=s.clob_secret, passphrase=s.clob_passphrase)
        if s.relayer_key:
            kwargs["api_key"] = RelayerApiKey(key=s.relayer_key, address=s.relayer_address)
        self.client = await AsyncSecureClient.create(**kwargs)
        self.wallet = str(self.client.wallet)
        self.wallet_type = str(self.client.wallet_type)
        LOG.info("Polymarket 账户 %s（%s），签名地址 %s", self.wallet, self.wallet_type, self.client.signer)
        self.signing = signing_speed()
        LOG.info("签名：%s，每张单约 %.1f 毫秒", *self.signing)

    async def close(self) -> None:
        for task in self._senders:
            task.cancel()
        if self.client is not None:
            with contextlib.suppress(Exception):
                await self.client.close()

    def prime(self, listing: Any) -> None:
        """Seed the SDK's market-metadata cache from the Gamma listing (tick size, neg risk, the two tokens), so the
        first orders of a fresh listing are signed without the SDK's two reads (/markets-by-token, /clob-markets):
        the race for the queue starts the moment Gamma shows the market. Pinned-SDK internals; on any mismatch the
        SDK reads them itself as usual."""
        try:
            from polymarket._internal.actions.orders.market_data import MarketInfo, PlatformFeeInfo
            cache = self.client._ctx.order_metadata
            info = MarketInfo(fee_info=PlatformFeeInfo(rate=D(str(listing.fee_rate)), exponent=D(str(listing.fee_exponent))),
                              neg_risk=bool(listing.neg_risk), tick_size=D(str(listing.tick)),
                              token_ids=frozenset({listing.up, listing.down}))
            for asset in (listing.up, listing.down):
                cache._conditions.set(asset, listing.condition_id)
            cache._markets.set(listing.condition_id, info)
        except Exception as error:
            LOG.debug("prime %s: %s", getattr(listing, "slug", "?"), error)

    async def place(self, specs: list[OrderSpec], on_signed: OnSigned | None = None) -> list[Placed]:
        """Sign every order, hand the hashes to `on_signed` (the caller writes them down: if that raises, nothing is
        sent), then POST them: one POST of the account at a time, up to 15 orders, other windows' orders with them."""
        from polymarket import PolymarketError, UserInputError
        out: dict[str, Placed] = {}
        signed: list[tuple[OrderSpec, Any, str]] = []
        for spec in specs:
            try:
                order = await self.client.create_limit_order(asset_id=spec.asset, price=str(spec.price), size=str(spec.size),
                                                             side="BUY", post_only=True, expiration=int(spec.expiration))
            except UserInputError as error:  # off the tick grid, under the size, an expiration too near: this order cannot be
                out[spec.ref] = Placed(spec.ref, "rejected", code="invalid", message=str(error))
                continue
            except PolymarketError as error:  # the market's metadata could not be read: nothing sent, placed again later
                out[spec.ref] = Placed(spec.ref, "not_sent", code="sign_failed", message=str(error) or type(error).__name__)
                continue
            signed.append((spec, order, order_hash(order, neg_risk=spec.neg_risk)))
        if signed and on_signed is not None:
            on_signed([(spec.ref, h, int(o.salt)) for spec, o, h in signed])  # durable before the first byte is sent
        if signed:
            out.update(await self._submit(signed))
        return [out[s.ref] for s in specs]

    async def _submit(self, signed: list[tuple[OrderSpec, Any, str]]) -> dict[str, Placed]:
        """Hand signed orders to the sender and wait for their answers. One POST of the account is in flight at a time;
        orders signed meanwhile by the other workers wait and go together, up to 15 to a POST (a window's orders are
        never split while they fit)."""
        loop = asyncio.get_running_loop()
        future: asyncio.Future = loop.create_future()
        self._outbox.append((signed, future))
        if self._wake is None:
            self._wake = asyncio.Event()
        self._senders = [t for t in self._senders if not t.done()]
        while len(self._senders) < POSTS_IN_FLIGHT:
            self._senders.append(loop.create_task(self._sender(), name="order-sender"))
        self._wake.set()
        return await future

    async def _sender(self) -> None:
        while True:
            if not self._outbox:
                self._wake.clear()
                await self._wake.wait()
                continue
            groups = [self._outbox.pop(0)]
            size = len(groups[0][0])
            while self._outbox and size + len(self._outbox[0][0]) <= BATCH:
                group = self._outbox.pop(0)
                groups.append(group)
                size += len(group[0])
            groups = [g for g in groups if not g[1].done()]  # a caller that gave up (the exit stopped it) is not sent for:
            if not groups:                                    # no one would read the answer, nor know what rests
                continue
            items = [item for signed, _ in groups for item in signed]
            try:
                results = await self._post(items)
                if len(groups) > 1 and any(r.whole and r.status == "rejected" and r.code not in NO_SPLIT for r in results.values()):
                    # the whole request refused, perhaps for one window's order alone (a token the matching engine
                    # does not know yet, one market's balance): each window goes again on its own, so that the others
                    # are not held back by it. A request refused as a whole was not processed: sending again is safe.
                    results = await self._post_apart([signed for signed, _ in groups])
            except asyncio.CancelledError:
                for _, future in groups:
                    future.cancel()
                raise
            except BaseException as error:  # the request itself blew up: every caller of the batch sees it
                for _, future in groups:
                    if not future.done():
                        future.set_exception(error)
                continue
            for signed, future in groups:
                if not future.done():
                    future.set_result({spec.ref: results[spec.ref] for spec, _, _ in signed})
            stopped = next((r for r in results.values() if r.status == "not_sent" and r.wait > 0), None)
            if stopped is not None:  # refused before processing (429, a restart, timed out): what waits is not sent
                waiting, self._outbox = self._outbox, []
                for signed, future in waiting:
                    if not future.done():
                        future.set_result({spec.ref: Placed(spec.ref, "not_sent", code=stopped.code, message="前一批被拒，没发",
                                                            hash=h, salt=int(o.salt), wait=stopped.wait)
                                           for spec, o, h in signed})

    async def _post_apart(self, groups: list[list[tuple[OrderSpec, Any, str]]]) -> dict[str, Placed]:
        """Each group (a window's orders) in a request of its own; after a refusal before processing (429, a restart)
        the rest is not sent."""
        out: dict[str, Placed] = {}
        stopped: Placed | None = None
        for signed in groups:
            if stopped is not None:
                out.update({spec.ref: Placed(spec.ref, "not_sent", code=stopped.code, message="前一批被拒，没发", hash=h,
                                             salt=int(o.salt), wait=stopped.wait) for spec, o, h in signed})
                continue
            got = await self._post(signed)
            out.update(got)
            stopped = next((r for r in got.values() if r.status == "not_sent" and r.wait > 0), None)
        return out

    async def _post(self, signed: list[tuple[OrderSpec, Any, str]]) -> dict[str, Placed]:
        """POST signed orders in batches of 15 and read each answer."""
        from polymarket import PolymarketError, RateLimitError, RequestRejectedError
        out: dict[str, Placed] = {}
        stopped: Placed | None = None
        for i in range(0, len(signed), BATCH):
            batch = signed[i:i + BATCH]
            if stopped is not None:  # an earlier batch was refused before processing: the rest is not sent
                for spec, o, h in batch:
                    out[spec.ref] = Placed(spec.ref, "not_sent", code=stopped.code, message="前一批被拒，没发", hash=h,
                                           salt=int(o.salt), wait=stopped.wait)
                continue
            self.recover()
            await self.orders.take(len(batch))
            self.sent.append([time.monotonic(), len(batch), True])
            try:
                responses = await self.client.post_orders([o for _, o, _ in batch])
            except RateLimitError as error:  # refused at the gate: nothing processed
                self.note_limited(error, len(batch))
                stopped = Placed("", "not_sent", code="rate_limited", message=f"下单被限流（429）：{error}",
                                 wait=float(error.retry_after or 10.0))
                for spec, o, h in batch:
                    out[spec.ref] = Placed(spec.ref, "not_sent", code=stopped.code, message=stopped.message, hash=h,
                                           salt=int(o.salt), wait=stopped.wait)
                continue
            except RequestRejectedError as error:
                code = refused_before_processing(error)
                if code:
                    wait = float(error.retry_after or (2.0 if code == "restarting" else 60.0 if code == "auth" else 30.0))
                    stopped = Placed("", "not_sent", code=code, message=f"下单被拒（{error.status} {code}）：{error}", wait=wait)
                    for spec, o, h in batch:
                        out[spec.ref] = Placed(spec.ref, "not_sent", code=code, message=stopped.message, hash=h,
                                               salt=int(o.salt), wait=wait, whole=True, batch=len(batch), http=error.status)
                    continue
                code = refusal_code(str(error), f"http_{error.status}")
                if code == "timed_out":  # 500 "order timed out": rejected before reaching the book, safe to send again
                    self._timeouts += 1
                    wait = min(TIMED_OUT_MAX_S, TIMED_OUT_WAIT_S * 2 ** (self._timeouts - 1))
                    stopped = Placed("", "not_sent", code=code, message=f"下单超时被拒（{error.status}，连续第 {self._timeouts} 次）：{error}",
                                     wait=wait)
                    for spec, o, h in batch:
                        out[spec.ref] = Placed(spec.ref, "not_sent", code=code, message=stopped.message, hash=h,
                                               salt=int(o.salt), wait=stopped.wait)
                    continue
                # a 4xx is the request refused as it stood (nothing rests), "Duplicated" aside (the very order is on the
                # book already); a 5xx without those words, a 408 or a 409 may come after processing: unknown, looked for
                # by the hash
                refused = 400 <= error.status < 500 and error.status not in (408, 409) and code != "duplicated"
                for spec, o, h in batch:
                    out[spec.ref] = Placed(spec.ref, "rejected" if refused else "unknown", code=code,
                                           message=str(error), hash=h, salt=int(o.salt), wait=TRANSIENT_WAIT.get(code, 0.0),
                                           whole=True, batch=len(batch), http=error.status)
                continue
            except PolymarketError as error:  # a transport failure or a malformed answer: sent or not, unknown
                status = "not_sent" if _cause_not_sent(error) else "unknown"
                for spec, o, h in batch:
                    out[spec.ref] = Placed(spec.ref, status, code="transport", message=str(error) or type(error).__name__,
                                           hash=h, salt=int(o.salt))
                continue
            if len(responses) != len(batch):
                for spec, o, h in batch:
                    out[spec.ref] = Placed(spec.ref, "unknown", code="malformed", message=f"{len(responses)} 个应答对 {len(batch)} 张单",
                                           hash=h, salt=int(o.salt))
                continue
            self._timeouts = 0
            for (spec, o, h), r in zip(batch, responses):
                if getattr(r, "ok", False):
                    out[spec.ref] = Placed(spec.ref, "open", order_id=str(r.order_id), hash=h, salt=int(o.salt),
                                           matched=D(str(getattr(r, "taking_amount", 0) or 0)) if r.status == "matched" else D(0))
                else:
                    message = str(getattr(r, "message", ""))
                    code = str(getattr(r, "code", "unknown"))
                    if code == "unknown":  # words the SDK does not map: the documented ones are
                        code = refusal_code(message, code)
                    out[spec.ref] = Placed(spec.ref, "unknown" if code == "duplicated" else "rejected", code=code,
                                           message=message, hash=h, salt=int(o.salt), wait=TRANSIENT_WAIT.get(code, 0.0),
                                           batch=len(batch))
        return out

    async def cancel(self, ids: list[str]) -> Cancelled:
        """Cancel by id in chunks the local bucket admits whole. A refusal before processing (429, a restart) ends the
        call with what earlier chunks did kept and `throttle` set; the ids not answered are in neither list."""
        from polymarket import RateLimitError, RequestRejectedError
        result = Cancelled()
        size = max(1, min(CANCEL_CHUNK, self.cancels.burst))
        for i in range(0, len(ids), size):
            chunk = ids[i:i + size]
            await self.cancels.take(len(chunk))
            try:
                r = await self.client.cancel_orders(order_ids=chunk)
            except RateLimitError as error:
                result.throttle = ("撤单被限流（429）", float(error.retry_after or 5.0))
                result.unanswered.update(ids[i:])
                return result
            except RequestRejectedError as error:
                if error.restriction == "restarting" or error.status in (425, 503):
                    result.throttle = (f"撤单被拒（{error.status}）：{error}", float(error.retry_after or 2.0))
                    result.unanswered.update(ids[i:])
                    return result
                if not result.canceled and not result.not_canceled:
                    raise
                result.unanswered.update(ids[i:])  # the rest is retried by the caller
                result.error = str(error) or type(error).__name__
                LOG.warning("撤单：后面 %d 张没收到答复（%s），稍后重撤", len(ids) - i, result.error)
                return result
            except Exception as error:
                if not result.canceled and not result.not_canceled:
                    raise
                result.unanswered.update(ids[i:])
                result.error = str(error) or type(error).__name__
                LOG.warning("撤单：后面 %d 张没收到答复（%s），稍后重撤", len(ids) - i, result.error)
                return result
            result.canceled.update(str(x) for x in r.canceled)
            result.not_canceled.update({str(k): str(v) for k, v in r.not_canceled.items()})
        return result

    async def cancel_market(self, condition_id: str) -> Cancelled:
        from polymarket import RateLimitError
        await self.cancels.take(1)
        try:
            r = await self.client.cancel_market_orders(market=condition_id)
        except RateLimitError as error:
            raise Throttled("撤单被限流（429）", error.retry_after or 5.0) from error
        return Cancelled(set(str(x) for x in r.canceled), {str(k): str(v) for k, v in r.not_canceled.items()})

    async def cancel_all(self) -> Cancelled:
        """Every open order of this API key (the emergency stop; a switch away from live trading)."""
        from polymarket import RateLimitError
        await self.cancels.take(1)
        try:
            r = await self.client.cancel_all()
        except RateLimitError as error:
            raise Throttled("撤单被限流（429）", error.retry_after or 5.0) from error
        return Cancelled(set(str(x) for x in r.canceled), {str(k): str(v) for k, v in r.not_canceled.items()})

    async def open_orders(self) -> list[Row]:
        """The open orders. On a Deposit Wallet the wallet-wide list (every signer's: the website's too), so the
        budget sees all the pUSD resting buys hold; elsewhere, or if that list fails, this key's own orders."""
        if self.wallet_type == "DEPOSIT_WALLET" and time.monotonic() >= self.wallet_list_off_until:
            try:
                rows = [_row(o) async for o in self.client.list_wallet_open_orders().iter_items()]
                return rows
            except Exception as error:
                self.wallet_list_off_until = time.monotonic() + 600
                LOG.warning("钱包级挂单列表读不到（%s），十分钟内改读本凭证的挂单", error)
        rows = []
        async for page in self.client.list_open_orders():
            rows.extend(_row(o) for o in page.items)
        return rows

    async def order(self, order_id: str) -> Row | None:
        """One order by id, open or not; None when the CLOB has no order with that id (404, 400, an empty answer)."""
        from polymarket import RequestRejectedError, UnexpectedResponseError
        try:
            return _row(await self.client.get_order(order_id=order_id))
        except RequestRejectedError as error:
            if error.status in (400, 404):
                return None
            raise
        except UnexpectedResponseError as error:
            if _no_such_order(error):
                return None
            raise

    async def fills(self, condition_id: str) -> dict[str, D]:
        """What each of this account's orders bought on a market, from its trade history (order id -> shares)."""
        return {oid: sum((shares for _, shares in log), D(0)) for oid, log in (await self.trade_log(condition_id)).items()}

    async def trade_log(self, condition_id: str) -> dict[str, list[tuple[float, D]]]:
        """Each fill of this account's orders on a market with the moment it matched (order id -> [(epoch s, shares)]):
        the bot's orders are post-only, so they are the maker orders of the trades. A trade without a time has 0."""
        out: dict[str, list[tuple[float, D]]] = {}
        async for page in self.client.list_account_trades(market=condition_id):
            for t in page.items:
                if str(getattr(t, "status", "")).upper() == "FAILED":
                    continue  # never settled on chain: no shares came of it
                at = getattr(t, "matched_at", None)
                ts = at.timestamp() if hasattr(at, "timestamp") else 0.0
                for m in t.maker_orders:
                    out.setdefault(str(m.order_id), []).append((ts, D(str(m.matched_amount))))
        return out

    def identity(self) -> str:
        """The account the orders and records belong to: the wallet (the funds) and the signer (the API key's owner,
        which alone can cancel them)."""
        signer = str(getattr(self.client, "signer", "") or "")
        return f"{self.wallet.lower()}|{signer.lower()}"

    async def balance(self) -> D:
        """The pUSD balance; the V2 exchange's allowance read with it (v2_allowance)."""
        b = await self.client.get_balance_allowance(asset_type="COLLATERAL")
        self.v2_allowance = v2_allowance_of(getattr(b, "allowances", None))
        return D(int(b.balance)) / D(1_000_000)

    async def refresh_allowance(self) -> None:
        """Have the CLOB read the account's pUSD allowances afresh (an approval made on the website shows then). The
        SDK's own refresh after an approval, through its internals: on any mismatch nothing is refreshed."""
        try:
            from polymarket._internal.actions import account as account_actions
            from polymarket._internal.wallet import signature_type_for
            ctx = self.client._ctx
            path, params = account_actions.build_update_balance_allowance_request(
                asset_type="COLLATERAL", signature_type=signature_type_for(ctx.wallet_type))
            await ctx.secure_clob.get_bytes(path, params=params)
        except Exception as error:
            LOG.debug("refresh allowance: %s", error)

    def redeem_problem(self) -> str:
        """Why winnings cannot be claimed from here ("" when they can): a smart wallet (the website's) claims through
        the Relayer, which takes a Relayer API key; an EOA sends its own transaction."""
        if self.wallet_type != "EOA" and not self.settings.relayer_key:
            return ("没配 Relayer Key，领取不了：网页钱包（智能钱包）领取要走 Relayer。到 polymarket.com → Settings → API Keys → Relayer API Keys 创建一个，把 API Key 和 Signer Address 填到 Railway 的 POLY_RELAYER_API_KEY 和 POLY_RELAYER_API_KEY_ADDRESS（重启后自动补领）；或者先到网页上手动领取")
        return ""

    async def redeem(self, condition_id: str) -> str:
        if why := self.redeem_problem():
            raise RuntimeError(why)
        from polymarket import UserInputError
        try:
            handle = await self.client.redeem_positions(condition_id=condition_id)
        except UserInputError as error:  # a V2 market whose positions hold nothing: redeemed already (the auto-redeem)
            if "no balance to redeem" in str(error):
                return "已领取（账户里这一场已经没有持仓）"
            raise
        result = await asyncio.wait_for(handle.wait(), timeout=180)
        return str(getattr(result, "transaction_hash", "") or getattr(result, "state", "") or "ok")


class PaperExchange:
    """LIVE=off: orders are kept in memory and never fill; the discovery, the timing and the bookkeeping run as live.
    The balance rule is Polymarket's, market by market: a market's open BUYs, a new one included, add up to at most the
    balance, and other markets' orders do not count (funds are not shared); an order over it is refused with "not
    enough balance / allowance". When the balance falls under a market's open BUYs (fills elsewhere spent it), that
    market's orders are cancelled (CANCELED_NOT_ENOUGH_BALANCE)."""
    live = False

    def __init__(self, balance: D = D("1000"), clock=time.time):
        self.wallet = "paper"
        self.clock = clock
        self.cash = balance
        self.book: dict[str, Row] = {}
        self.gone: dict[str, Row] = {}
        self.seq = 0
        self.placed: list[OrderSpec] = []      # every order ever placed (the tests read it)
        self.cancel_calls: list[list[str]] = []
        self.market_cancels: list[str] = []
        self.reject: dict[str, tuple[str, str]] = {}   # ref -> (status, code) the next placement of that ref answers (tests)
        self.refuse_market: dict[str, tuple[str, str]] = {}  # condition id -> (status, code) every order there answers (tests)
        self.fail_place: Exception | None = None
        self.fail_cancel: Exception | None = None
        self.fail_read: Exception | None = None      # set: every order() and fills() raises it (tests)
        self.fail_balance: Exception | None = None   # set: every balance() raises it (tests)
        self.markets: dict[str, str] = {}      # asset -> condition id (rows of the paper book carry it)
        self.fill_log: dict[str, list[tuple[float, D]]] = {}   # order id -> [(when, shares)] of fill() (tests)
        self.v2_allowance: D | None = None   # V2 approvals do not exist on paper; a test sets D(0) to play them missing

    async def start(self) -> None:
        return None

    async def close(self) -> None:
        return None

    def prime(self, listing: Any) -> None:
        return None

    def identity(self) -> str:
        return "paper"

    def redeem_problem(self) -> str:
        return ""

    async def place(self, specs: list[OrderSpec], on_signed: OnSigned | None = None) -> list[Placed]:
        """As live: every order "signed" (its id, which is its hash, drawn) and handed to `on_signed` before any rests."""
        if self.fail_place is not None:
            error, self.fail_place = self.fail_place, None
            raise error
        ids = []
        for _ in specs:
            self.seq += 1
            ids.append(("0x" + format(self.seq, "064x"), self.seq))
        if on_signed is not None:
            on_signed([(spec.ref, oid, salt) for spec, (oid, salt) in zip(specs, ids)])
        out = []
        for spec, (oid, salt) in zip(specs, ids):
            if spec.ref in self.reject or (spec.market and spec.market in self.refuse_market):
                status, code = self.reject.pop(spec.ref) if spec.ref in self.reject else self.refuse_market[spec.market]
                out.append(Placed(spec.ref, status, code=code, message=code, hash=oid, salt=salt))
                continue
            market = spec.market or self.markets.get(spec.asset, "")
            if self.market_open(market) + spec.price * spec.size > self.cash:  # this market's BUYs against the balance
                out.append(Placed(spec.ref, "rejected", code="not_enough_balance", message="not enough balance / allowance",
                                  hash=oid, salt=salt))
                continue
            self.placed.append(spec)
            self.book[oid] = Row(id=oid, asset=spec.asset, market=market, side="BUY", price=spec.price,
                                 size=spec.size, matched=D(0), status="LIVE", created=self.clock())
            out.append(Placed(spec.ref, "open", order_id=oid, hash=oid, salt=salt))
        return out

    async def cancel(self, ids: list[str]) -> Cancelled:
        if self.fail_cancel is not None:
            error, self.fail_cancel = self.fail_cancel, None
            raise error
        self.cancel_calls.append(list(ids))
        result = Cancelled()
        for oid in ids:
            row = self.book.pop(oid, None)
            if row is None:
                result.not_canceled[oid] = "order not found"
                continue
            row.status = "CANCELED" if row.matched < row.size else "MATCHED"
            self.gone[oid] = row
            result.canceled.add(oid)
        return result

    async def refresh_allowance(self) -> None:
        return None

    async def cancel_market(self, condition_id: str) -> Cancelled:
        self.market_cancels.append(condition_id)
        ids = [oid for oid, row in self.book.items() if row.market == condition_id]
        return await self.cancel(ids) if ids else Cancelled()

    async def cancel_all(self) -> Cancelled:
        return await self.cancel(list(self.book)) if self.book else Cancelled()

    async def open_orders(self) -> list[Row]:
        return [Row(**vars(r)) for r in self.book.values()]

    async def order(self, order_id: str) -> Row | None:
        if self.fail_read is not None:
            raise self.fail_read
        row = self.book.get(order_id) or self.gone.get(order_id)
        return Row(**vars(row)) if row else None

    async def fills(self, condition_id: str) -> dict[str, D]:
        if self.fail_read is not None:
            raise self.fail_read
        return {oid: row.matched for oid, row in {**self.gone, **self.book}.items()
                if row.market == condition_id and row.matched > 0}

    async def trade_log(self, condition_id: str) -> dict[str, list[tuple[float, D]]]:
        if self.fail_read is not None:
            raise self.fail_read
        rows = {**self.gone, **self.book}
        return {oid: list(log) for oid, log in self.fill_log.items() if oid in rows and rows[oid].market == condition_id}

    async def balance(self) -> D:
        if self.fail_balance is not None:
            raise self.fail_balance
        return self.cash

    async def redeem(self, condition_id: str) -> str:
        return "paper"

    def market_open(self, market: str) -> D:
        return sum((r.price * (r.size - r.matched) for r in self.book.values() if r.market == market and r.side == "BUY"), D(0))

    def enforce(self) -> list[str]:
        """The CLOB keeps checking: a market whose open BUYs add up to more than the balance has them cancelled."""
        gone = []
        for market in {r.market for r in self.book.values()}:
            if self.market_open(market) > self.cash:
                for oid in [i for i, r in self.book.items() if r.market == market]:
                    row = self.book.pop(oid)
                    row.status = "CANCELED_NOT_ENOUGH_BALANCE"
                    self.gone[oid] = row
                    gone.append(oid)
        return gone

    def cancel_elsewhere(self, ids: list[str] | None = None) -> list[str]:
        """Test helper: orders cancelled from outside the bot (the website's 取消所有 when `ids` is None)."""
        gone = []
        for oid in list(self.book) if ids is None else ids:
            if oid in self.book:
                row = self.book.pop(oid)
                row.status = "CANCELED"
                self.gone[oid] = row
                gone.append(oid)
        return gone

    # --- test helpers ---
    def fill(self, order_id: str, shares: D) -> None:
        row = self.book[order_id]
        self.fill_log.setdefault(order_id, []).append((self.clock(), min(shares, row.size - row.matched)))
        row.matched = min(row.size, row.matched + shares)
        self.cash -= shares * row.price
        if row.matched >= row.size:
            row.status = "MATCHED"
            self.gone[order_id] = self.book.pop(order_id)
        self.enforce()
