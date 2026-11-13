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
import contextlib
import logging
import time
from dataclasses import dataclass, field
from decimal import Decimal as D
from typing import Any, Callable, Protocol

LOG = logging.getLogger(__name__)
BATCH = 15                      # POST /orders takes 1 to 15 orders
CANCEL_CHUNK = 200              # DELETE /orders takes up to 1000 ids; a chunk is also kept within the local cancel bucket's burst
RETRYABLE_CODES = {"market_not_ready", "post_only_mode", "post_only", "rate_limited", "restarting", "cancel_only",
                   "trading_disabled", "auth", "sign_failed", "not_enough_balance", "transport", "exception", "malformed",
                   "missing"}
# ^ a refusal (or a lost answer) that is not the window's own doing: placed again, not counted


def retryable(code: str) -> bool:
    return code in RETRYABLE_CODES or code.startswith("http_5")


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
    async def balance(self) -> D: ...
    async def redeem(self, condition_id: str) -> str: ...
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
        self.tier = ""
        self.rate_warning = False
        self.wallet_list_off_until = 0.0
        self.orders = order_bucket or Bucket(settings.order_rate, settings.order_burst)
        self.cancels = cancel_bucket or Bucket(settings.cancel_rate, settings.cancel_burst)

    def _on_rate(self, update: Any) -> None:
        if getattr(update, "tier", None):
            self.tier = str(update.tier)
        if getattr(update, "warning", False):
            self.rate_warning = True

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

    async def close(self) -> None:
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
        sent), then POST them in batches of 15."""
        from polymarket import PolymarketError, RateLimitError, RequestRejectedError, UserInputError
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
        stopped: Placed | None = None
        for i in range(0, len(signed), BATCH):
            batch = signed[i:i + BATCH]
            if stopped is not None:  # an earlier batch was refused before processing: the rest is not sent
                for spec, o, h in batch:
                    out[spec.ref] = Placed(spec.ref, "not_sent", code=stopped.code, message="前一批被拒，没发", hash=h,
                                           salt=int(o.salt), wait=stopped.wait)
                continue
            await self.orders.take(len(batch))
            try:
                responses = await self.client.post_orders([o for _, o, _ in batch])
            except RateLimitError as error:  # refused at the gate: nothing processed
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
                                               salt=int(o.salt), wait=wait)
                    continue
                # a 4xx is the request refused as it stood (nothing rests); a 5xx without those words, a 408 or a 409
                # may come after processing: unknown, looked for by the hash
                refused = 400 <= error.status < 500 and error.status not in (408, 409)
                for spec, o, h in batch:
                    out[spec.ref] = Placed(spec.ref, "rejected" if refused else "unknown", code=f"http_{error.status}",
                                           message=str(error), hash=h, salt=int(o.salt))
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
            for (spec, o, h), r in zip(batch, responses):
                if getattr(r, "ok", False):
                    out[spec.ref] = Placed(spec.ref, "open", order_id=str(r.order_id), hash=h, salt=int(o.salt),
                                           matched=D(str(getattr(r, "taking_amount", 0) or 0)) if r.status == "matched" else D(0))
                else:
                    out[spec.ref] = Placed(spec.ref, "rejected", code=str(getattr(r, "code", "unknown")),
                                           message=str(getattr(r, "message", "")), hash=h, salt=int(o.salt))
        return [out[s.ref] for s in specs]

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
                return result
            except RequestRejectedError as error:
                if error.restriction == "restarting" or error.status in (425, 503):
                    result.throttle = (f"撤单被拒（{error.status}）：{error}", float(error.retry_after or 2.0))
                    return result
                if not result.canceled and not result.not_canceled:
                    raise
                return result  # the rest is retried by the caller
            except Exception:
                if not result.canceled and not result.not_canceled:
                    raise
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
        """What each of this account's orders bought on a market, from its trade history (order id -> shares): the
        bot's orders are post-only, so they are the maker orders of the trades."""
        out: dict[str, D] = {}
        async for page in self.client.list_account_trades(market=condition_id):
            for t in page.items:
                if str(getattr(t, "status", "")).upper() == "FAILED":
                    continue  # never settled on chain: no shares came of it
                for m in t.maker_orders:
                    oid = str(m.order_id)
                    out[oid] = out.get(oid, D(0)) + D(str(m.matched_amount))
        return out

    def identity(self) -> str:
        """The account the orders and records belong to: the wallet (the funds) and the signer (the API key's owner,
        which alone can cancel them)."""
        signer = str(getattr(self.client, "signer", "") or "")
        return f"{self.wallet.lower()}|{signer.lower()}"

    async def balance(self) -> D:
        b = await self.client.get_balance_allowance(asset_type="COLLATERAL")
        return D(int(b.balance)) / D(1_000_000)

    async def redeem(self, condition_id: str) -> str:
        if self.wallet_type != "EOA" and not self.settings.relayer_key:
            raise RuntimeError("缺少 POLY_RELAYER_API_KEY：智能钱包的领取要走 Relayer（或到网页上手动领取）")
        handle = await self.client.redeem_positions(condition_id=condition_id)
        result = await asyncio.wait_for(handle.wait(), timeout=180)
        return str(getattr(result, "transaction_hash", "") or getattr(result, "state", "") or "ok")


class PaperExchange:
    """LIVE=off: orders are kept in memory and never fill; the discovery, the timing and the bookkeeping run as live."""
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
        self.fail_place: Exception | None = None
        self.fail_cancel: Exception | None = None
        self.fail_read: Exception | None = None      # set: every order() and fills() raises it (tests)
        self.fail_balance: Exception | None = None   # set: every balance() raises it (tests)
        self.markets: dict[str, str] = {}      # asset -> condition id (rows of the paper book carry it)

    async def start(self) -> None:
        return None

    async def close(self) -> None:
        return None

    def prime(self, listing: Any) -> None:
        return None

    def identity(self) -> str:
        return "paper"

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
            if spec.ref in self.reject:
                status, code = self.reject.pop(spec.ref)
                out.append(Placed(spec.ref, status, code=code, message=code, hash=oid, salt=salt))
                continue
            self.placed.append(spec)
            self.book[oid] = Row(id=oid, asset=spec.asset, market=spec.market or self.markets.get(spec.asset, ""), side="BUY", price=spec.price,
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

    async def balance(self) -> D:
        if self.fail_balance is not None:
            raise self.fail_balance
        return self.cash

    async def redeem(self, condition_id: str) -> str:
        return "paper"

    # --- test helpers ---
    def fill(self, order_id: str, shares: D) -> None:
        row = self.book[order_id]
        row.matched = min(row.size, row.matched + shares)
        self.cash -= shares * row.price
        if row.matched >= row.size:
            row.status = "MATCHED"
            self.gone[order_id] = self.book.pop(order_id)
