"""The engine end to end over the live adapter (LiveExchange, as it runs with LIVE=on) and a fake CLOB built on the SDK's
own models: the review's scenarios as it reproduced them — the CLOB takes the orders, then the process dies or the
gateway answers a bare 503; another strategy's identical orders on the same API key; a balance endpoint down for ten
minutes — run through the code paths that run live (signing, the order hash, batches, lookups, cancels)."""
import contextlib
import datetime as dt
import json
import tempfile
import unittest
from decimal import Decimal as D
from pathlib import Path
from types import SimpleNamespace as N

import httpx
from helpers import T0, Clock, FakePublic, drain, make_engine, settings, tick

from polymarket import RequestRejectedError, TransportError
from polymarket.models.clob.cancel import CancelOrdersResponse
from polymarket.models.clob.order_response import AcceptedOrder
from polymarket.models.clob.orders import SignedOrder
from preopen.exchange import Bucket, LiveExchange, order_hash
from preopen.ledger import Ledger

WALLET = "0x1111111111111111111111111111111111111111"
ZERO32 = "0x" + "00" * 32


class Crash(BaseException):
    """The process dies (a deploy, an OOM kill): nothing after it runs."""


class Pages:
    def __init__(self, items):
        self.items = list(items)

    def __aiter__(self):
        async def pages():
            yield N(items=self.items)
        return pages()


class FakeClob:
    """The CLOB as the SDK client shows it. An order's id is its EIP-712 hash; it rests until cancelled or filled; a fill
    is a trade with the order as its maker. `answers` scripts what the next POSTs answer once the CLOB took the orders:
    "ok", "503" / "502" (a bare gateway error), "crash" (the process dies before reading the answer), or "lost" (a read timeout
    on a POST the CLOB never processed)."""

    def __init__(self, clock):
        self.clock = clock
        self.wallet, self.wallet_type, self.signer = WALLET, "EOA", WALLET
        self.orders: dict[str, N] = {}
        self.trades: list[N] = []
        self.markets: dict[str, str] = {}
        self.answers: list[str] = []
        self.posted = 0
        self.balance_usd = D("1000")
        self.balance_error: Exception | None = None
        self.missing = "404"
        self.market_cancels: list[str] = []
        self.cancel_alls = 0
        self.salt = 0

    def know(self, m: dict) -> dict:
        for t in json.loads(m["clobTokenIds"]):
            self.markets[t] = m["conditionId"]
        return m

    async def create_limit_order(self, *, asset_id, price, size, side, post_only, expiration):
        self.salt += 1
        p, s = D(price), D(size)
        return SignedOrder(builder=ZERO32, expiration=expiration, maker=WALLET, maker_amount=int(p * s * 10**6), metadata=ZERO32,
                           order_type="GTD", salt=self.salt, side=side, signature="0x" + "ab" * 65, signature_type=0, signer=WALLET,
                           taker_amount=int(s * 10**6), timestamp=int(self.clock() * 1000), token_id=asset_id, post_only=post_only)

    def book(self, o: SignedOrder) -> str:
        oid = order_hash(o, neg_risk=False)
        self.orders[oid] = N(id=oid, asset_id=str(o.token_id), condition_id=self.markets.get(str(o.token_id), ""), side="BUY",
                             price=str(D(o.maker_amount) / D(o.taker_amount)), original_size=str(D(o.taker_amount) / 10**6),
                             size_matched="0", status="LIVE", created_at=dt.datetime.fromtimestamp(self.clock(), dt.timezone.utc))
        return oid

    async def post_orders(self, orders):
        answer = self.answers.pop(0) if self.answers else "ok"
        self.posted += len(orders)
        if answer == "lost":
            error = TransportError("timed out")
            error.__cause__ = httpx.ReadTimeout("read")
            raise error
        ids = [self.book(o) for o in orders]
        if answer == "crash":
            raise Crash()
        if answer in ("502", "503"):
            raise RequestRejectedError("Bad Gateway" if answer == "502" else "Service Unavailable", status=int(answer))
        return [AcceptedOrder(order_id=i, status="live", making_amount=D(0), taking_amount=D(0), trade_ids=(),
                              transactions_hashes=()) for i in ids]

    async def get_order(self, *, order_id):
        o = self.orders.get(order_id)
        if o is None:
            if self.missing == "null":
                from polymarket.models.clob.account import OpenOrder
                return OpenOrder.parse_response(None)
            raise RequestRejectedError("order not found", status=404)
        return o

    def list_open_orders(self, **kw):
        return Pages(o for o in self.orders.values() if o.status == "LIVE")

    async def cancel_orders(self, *, order_ids):
        canceled, refused = [], {}
        for i in order_ids:
            o = self.orders.get(i)
            if o is None:
                refused[i] = "order not found"
            elif o.status != "LIVE":
                refused[i] = f"order already {o.status.lower()}"
            else:
                o.status = "CANCELED"
                canceled.append(i)
        return CancelOrdersResponse(canceled=tuple(canceled), not_canceled=refused)

    async def cancel_market_orders(self, *, market):
        self.market_cancels.append(market)
        return await self.cancel_orders(order_ids=[i for i, o in self.orders.items() if o.condition_id == market and o.status == "LIVE"])

    async def cancel_all(self):
        self.cancel_alls += 1
        return await self.cancel_orders(order_ids=[i for i, o in self.orders.items() if o.status == "LIVE"])

    def list_account_trades(self, *, market):
        return Pages(t for t in self.trades if t.market == market)

    async def get_balance_allowance(self, *, asset_type):
        if self.balance_error is not None:
            raise self.balance_error
        return N(balance=int(self.balance_usd * 10**6))

    async def close(self):
        return None

    # --- the market's side ---
    def fill(self, oid: str, shares: D) -> None:
        o = self.orders[oid]
        matched = D(o.size_matched) + shares
        o.size_matched = str(matched)
        if matched >= D(o.original_size):
            o.status = "MATCHED"
        self.trades.append(N(market=o.condition_id, status="CONFIRMED", maker_orders=[N(order_id=oid, matched_amount=str(shares))]))
        self.balance_usd -= shares * D(o.price)

    def live(self) -> list[N]:
        return [o for o in self.orders.values() if o.status == "LIVE"]


def adapter(fake: FakeClob, s) -> LiveExchange:
    x = LiveExchange(s, order_bucket=Bucket(1000, 1000), cancel_bucket=Bucket(1000, 1000))
    x.client, x.wallet, x.wallet_type = fake, WALLET, "EOA"
    return x


def quotes(e):
    return sorted(e.ledger.rows.values(), key=lambda q: (q["start"], q["coin"], q["level"], q["attempt"]))


class LiveEndToEndTest(unittest.IsolatedAsyncioTestCase):
    def engine(self, clock, fake, pub, ledger=None, **env):
        s = settings(**env)
        e, _, _, _ = make_engine(s, clock=clock, ledger=ledger, exchange=adapter(fake, s), public=pub)
        return e

    async def test_review1_the_process_dies_after_the_clob_took_the_orders(self):
        """单边成交 10 份、实际支出 $4.90，重启后账本仍记录零成交，随后又发出两张新单 — no longer."""
        with tempfile.TemporaryDirectory() as d:
            path = str(Path(d) / "live.sqlite3")
            clock, start = Clock(), T0 + 1800
            fake, pub = FakeClob(clock), FakePublic(clock)
            m = fake.know(pub.list_window("btc", start))
            first = Ledger(path)
            e = self.engine(clock, fake, pub, ledger=first)
            await e.account_once()
            await e.discover()
            fake.answers = ["crash"]
            with self.assertRaises(Crash):
                await drain(e)
            up = next(o.id for o in fake.orders.values() if o.asset_id == json.loads(m["clobTokenIds"])[0])
            fake.fill(up, D("10"))  # filled in full while the process was down: $4.90 spent
            again = Ledger(path)  # a new process: the same ledger file, the same CLOB
            e2 = self.engine(clock, fake, pub, ledger=again)
            e2.take_over()
            for _ in range(30):  # five minutes of the restarted process
                await tick(e2, 10)
            (q,) = quotes(e2)
            self.assertEqual(D(q["orders"]["up"]["filled"]), D("10"))  # the fill is on the record
            self.assertEqual(q["orders"]["down"]["state"], "open")
            self.assertEqual(fake.posted, 2)                            # and nothing was sent again
            first.close()
            again.close()

    async def test_review2_a_bare_503_after_the_clob_took_the_orders(self):
        """模拟交易所接受两单后返回普通 503，最终账本记录数为 0、预留金额为 0，订单仍然存在 — no longer."""
        clock = Clock()
        fake, pub = FakeClob(clock), FakePublic(clock)
        fake.know(pub.list_window("btc", T0 + 1800))
        e = self.engine(clock, fake, pub)
        await e.account_once()
        await e.discover()
        fake.answers = ["503"]
        await drain(e)
        self.assertEqual(len(quotes(e)), 1)           # the record stays...
        self.assertEqual(e.reserved(), D("9.80"))     # ...and so does its money
        for _ in range(30):
            await tick(e, 10)
        (q,) = quotes(e)
        self.assertEqual({o["state"] for o in q["orders"].values()}, {"open"})  # found by hash, managed
        self.assertEqual({o["id"] for o in q["orders"].values()}, {o.id for o in fake.live()})
        self.assertEqual(fake.posted, 2)

    async def test_review3_and_6_another_strategy_on_the_same_key(self):
        """其他订单被撤、机器人自己的订单反而继续挂着; 市场级撤单会影响其他策略 — no longer."""
        clock, start = Clock(), T0 + 1800
        fake, pub = FakeClob(clock), FakePublic(clock)
        m = fake.know(pub.list_window("btc", start))
        up_token, down_token = json.loads(m["clobTokenIds"])
        expiration = start - 30 + 60
        theirs = {fake.book(await fake.create_limit_order(asset_id=up_token, price="0.49", size="10", side="BUY",
                                                          post_only=True, expiration=expiration)),   # the very same order
                  fake.book(await fake.create_limit_order(asset_id=down_token, price="0.30", size="20", side="BUY",
                                                          post_only=True, expiration=expiration))}
        e = self.engine(clock, fake, pub)
        await e.account_once()
        await e.discover()
        fake.answers = ["502"]  # the answer to the bot's own orders is lost (a gateway's 502: unknown to every version)
        await drain(e)
        await tick(e, 5)
        (q,) = quotes(e)
        mine = {o["id"] for o in q["orders"].values()}
        self.assertEqual(len(mine), 2)
        self.assertFalse(mine & theirs)  # claimed its own orders, exactly
        clock.t = start - 60
        await tick(e)
        await tick(e, 2)
        self.assertEqual({o.id for o in fake.live()}, theirs)  # its own cancelled, theirs untouched
        self.assertEqual(fake.market_cancels, [])
        await e.emergency()
        self.assertEqual(fake.cancel_alls, 0)
        self.assertEqual({o.id for o in fake.live()}, theirs)

    async def test_review4_a_balance_endpoint_down_for_ten_minutes(self):
        """余额过期 600 秒后仍发出新订单 — no longer."""
        clock = Clock()
        fake, pub = FakeClob(clock), FakePublic(clock)
        e = self.engine(clock, fake, pub)
        await e.account_once()
        fake.balance_error = RuntimeError("balance endpoint down")
        for _ in range(60):  # ten minutes: the open orders read fine, the balance never
            clock.advance(10)
            with contextlib.suppress(RuntimeError):
                await e.account_once()
        fake.know(pub.list_window("btc", T0 + 3000))
        await tick(e, account=False)
        self.assertEqual(fake.posted, 0)
        self.assertIn("余额过期", e.hold())
        fake.balance_error = None
        await tick(e)
        await tick(e)
        self.assertEqual(fake.posted, 2)

    async def test_an_unprocessed_post_is_given_up_on_the_clobs_word_and_quoted_once_more(self):
        for missing in ("404", "null"):
            with self.subTest(missing=missing):
                clock = Clock()
                fake, pub = FakeClob(clock), FakePublic(clock)
                fake.missing = missing
                fake.know(pub.list_window("btc", T0 + 1800))
                e = self.engine(clock, fake, pub)
                await tick(e)  # the first orders answered: the id is learned to be the hash
                self.assertTrue(e.id_is_hash)
                fake.know(pub.list_window("eth", T0 + 1800))
                fake.answers = ["lost"]  # a read timeout on a POST the CLOB never processed
                await tick(e, 61)        # (found by the next full pass: not the coin's next window)
                self.assertEqual(fake.posted, 4)
                for _ in range(20):  # the CLOB says "no such order" for two minutes: given up, quoted again once
                    await tick(e, 10)
                self.assertEqual(fake.posted, 6)
                self.assertEqual(len(fake.live()), 4)
                eth = [q for q in quotes(e) if q["coin"] == "eth"]
                self.assertEqual([q["attempt"] for q in eth], [0, 1])
                self.assertEqual({o["state"] for o in eth[0]["orders"].values()}, {"not_found"})


if __name__ == "__main__":
    unittest.main()
