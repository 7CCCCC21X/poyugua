"""The engine end to end over the live adapter (LiveExchange, as it runs with LIVE=on) and a fake CLOB built on the SDK's
own models: the review's scenarios as it reproduced them — the CLOB takes the orders, then the process dies or the
gateway answers a bare 503; another strategy's identical orders on the same API key; a balance endpoint down for ten
minutes — run through the code paths that run live (signing, the order hash, batches, lookups, cancels)."""
import asyncio
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
import preopen.markets as mk
from preopen.exchange import Bucket, LiveExchange, OrderSpec, order_hash
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
        self.elsewhere = D(0)   # held in every market by orders this key does not list (another key): counted all the same
        self.refused = 0
        self.redeemed: list[str] = []
        self.signer_mismatch = False
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
        if self.signer_mismatch:   # the API key belongs to another address than the orders' signer (docs: POST /order)
            raise RequestRejectedError("the order signer address has to be the address of the API KEY", status=400)
        if answer == "timed_out":  # docs: rejected before reaching the book, safe to send again
            raise RequestRejectedError("order timed out", status=500)
        # each market's open BUYs against the balance on its own (11-13: three $49 orders in three markets rest on
        # $75.10); over it, the batch is refused with the documented 400
        need: dict[str, D] = {}
        for o in orders:
            market = self.markets.get(str(o.token_id), "")
            need[market] = need.get(market, D(0)) + D(o.maker_amount) / 10**6
        for market, amount in need.items():
            held = sum(D(o.price) * (D(o.original_size) - D(o.size_matched)) for o in self.live() if o.condition_id == market)
            if held + self.elsewhere + amount > self.balance_usd:
                self.refused += len(orders)
                raise RequestRejectedError("not enough balance / allowance", status=400)
        if answer == "lost":
            error = TransportError("timed out")
            error.__cause__ = httpx.ReadTimeout("read")
            raise error
        ids = [self.book(o) for o in orders]
        if answer == "crash":
            raise Crash()
        if answer == "duplicated":  # the very orders are on the book already
            raise RequestRejectedError(f"order {ids[0]} is invalid. Duplicated.", status=400)
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

    async def redeem_positions(self, *, condition_id):
        self.redeemed.append(condition_id)

        async def wait():
            return N(transaction_hash="0x" + "cd" * 32)
        return N(wait=wait)

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

    def website_cancel_all(self) -> None:
        """取消所有 on the website: every open order of the account, plain CANCELED."""
        for o in self.live():
            o.status = "CANCELED"


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


def seven_coins(clock, fake, pub, ahead="120"):
    s = settings(PREOPEN_MARKETS="btc,eth,sol,xrp,doge,hype,bnb", PREOPEN_AHEAD_MINUTES=ahead, PREOPEN_PRICE_CENTS="47",
                 PREOPEN_SHARES="10", PREOPEN_CANCEL_SECONDS="15", PREOPEN_EXPIRY_SECONDS="10", PREOPEN_ENTRY_SECONDS="300")
    for coin in s.coins:
        for t in mk.window_starts(clock.t, s):
            fake.know(pub.list_window(coin, t))
    return s


class BalanceEndToEndTest(unittest.IsolatedAsyncioTestCase):
    """The 11-13 live case: $75.10, seven coins at 47¢ x 10 ($9.40 a window), the weekend off on a Friday afternoon (no
    window enters the range later). Each market is checked against the balance on its own, so every window is quoted;
    whatever the exchange refuses or cancels, no window is struck off without a word."""

    def engine(self, clock, fake, pub, s, ledger=None):
        return make_engine(s, clock=clock, ledger=ledger, exchange=adapter(fake, s), public=pub)[0]

    async def test_every_window_is_quoted_on_75_dollars(self):
        clock = Clock()
        fake, pub = FakeClob(clock), FakePublic(clock)
        fake.balance_usd = D("75.10")
        s = seven_coins(clock, fake, pub)
        starts = mk.window_starts(clock.t, s)
        e = self.engine(clock, fake, pub, s)
        for _ in range(240):  # twenty minutes in 5 s steps
            await tick(e, 5)
        self.assertEqual(fake.refused, 0)              # nothing a market could not take was ever sent
        ahead = [t for t in starts if t - s.cancel_s > clock.t]
        self.assertEqual(len(fake.live()), 2 * 7 * len(ahead))  # every window not yet at its cancel time
        self.assertGreater(sum(D(o.price) * D(o.original_size) for o in fake.live()), 10 * D("75.10"))
        cov = e.coverage()  # the range: windows still open to new orders (more than entry_s ahead)
        listed = [t for t in mk.window_starts(clock.t, s) if t in starts]  # the range moved on past what was listed
        self.assertEqual(cov["counts"]["resting"], 7 * len(listed))
        self.assertNotIn("stopped", cov["counts"])     # no window struck off

    async def test_a_balance_refusal_the_bot_could_not_see_coming_strikes_off_nothing(self):
        clock = Clock()
        fake, pub = FakeClob(clock), FakePublic(clock)
        fake.balance_usd, fake.elsewhere = D("75.10"), D("70")  # $70 in every market held by orders this key does not list
        s = seven_coins(clock, fake, pub)
        e = self.engine(clock, fake, pub, s)
        await tick(e)
        await tick(e, 1)
        self.assertGreater(fake.refused, 0)
        self.assertEqual(fake.live(), [])              # $5.10 left in each market: no window fits
        self.assertIn("交易所说余额不够", e.hold())       # a minute's hold, then the balance read again
        self.assertEqual(quotes(e), [])                # the refused: no record, no attempt used
        fake.elsewhere = D(0)
        for _ in range(9):
            await tick(e, 10)
        self.assertGreater(len(fake.live()), 100)      # every window quoted once the money is there
        self.assertNotIn("stopped", e.coverage()["counts"])

    async def test_the_stuck_ledger_comes_back_after_the_fix(self):
        """What the 11-13 ledger holds: windows closed for good on a 400 "not enough balance / allowance" for the whole
        batch. Started on it, the bot quotes them again; a manual withdrawal and a refusal about the order stay."""
        with tempfile.TemporaryDirectory() as d:
            path = str(Path(d) / "live.sqlite3")
            clock = Clock()
            fake, pub = FakeClob(clock), FakePublic(clock)
            fake.balance_usd = D("75.10")
            s = seven_coins(clock, fake, pub)
            ledger = Ledger(path)
            starts = mk.window_starts(clock.t, s)
            for coin in s.coins:
                for t in starts:
                    slug = mk.slug_of(coin, t)
                    code, error, extra = "http_400", "not enough balance / allowance", {}
                    if (coin, t) == ("btc", starts[0]):
                        extra = {"manual": True, "reason": "网页手动撤单"}
                    if (coin, t) == ("eth", starts[0]):
                        code, error = "post_only_would_cross", "invalid post-only order: order crosses book"
                    ledger.save({"key": f"{slug}#0@0", "slug": slug, "coin": coin, "start": t, "level": 0, "attempt": 0,
                                 "price": "0.47", "shares": "10", "expiration": t + 50, "condition_id": f"0x{t:064x}",
                                 "market_id": "", "neg_risk": False, "listed": None, "seen": clock.t, "created": clock.t - 600,
                                 "phase": "settled", "reason": f"单边没挂上：涨 rejected {code}", "counted": True,
                                 "permanent": True, "closed_at": clock.t - 600, "pnl": 0.0,
                                 "orders": {side: {"asset": "", "state": "rejected", "id": "", "hash": "", "salt": 0,
                                                   "filled": "0", "code": code, "error": error, "sent": clock.t - 600}
                                            for side in ("up", "down")}, **extra})
            e = self.engine(clock, fake, pub, s, ledger=ledger)
            await e.discover()
            self.assertEqual(e.coverage()["counts"].get("stopped"), 7 * len(starts))  # where 11-13 stood: all struck off
            e.take_over()
            await tick(e)
            await tick(e, 1)
            first = {q["coin"] for q in quotes(e) if q["phase"] == "active" and q["start"] == starts[0]}
            self.assertEqual(first, {"sol", "xrp", "doge", "hype", "bnb"})  # btc withdrawn by hand, eth's own refusal
            self.assertEqual(len(fake.live()), 2 * (7 * len(starts) - 2))  # every other window quoted again
            self.assertIn("手动撤了", e.coverage()["text"])
            ledger.close()


class CancelledElsewhereTest(unittest.IsolatedAsyncioTestCase):
    async def test_the_websites_cancel_all_pauses_the_bot(self):
        clock = Clock()
        fake, pub = FakeClob(clock), FakePublic(clock)
        fake.balance_usd = D("75.10")
        s = seven_coins(clock, fake, pub)
        e = make_engine(s, clock=clock, exchange=adapter(fake, s), public=pub)[0]
        await tick(e)
        await tick(e, 1)
        placed = len(fake.live())
        self.assertGreater(placed, 100)
        fake.website_cancel_all()
        for _ in range(4):
            await tick(e, 6)
        self.assertTrue(e.paused)
        self.assertEqual(fake.live(), [])
        self.assertEqual(fake.posted, placed)          # nothing placed again while paused
        self.assertFalse(any(q.get("counted") for q in quotes(e)))
        e.resume()
        await tick(e, 5)
        await tick(e, 1)
        self.assertGreater(len(fake.live()), 100)      # quoted again once resumed


class SenderTest(unittest.IsolatedAsyncioTestCase):
    """One order POST of the account at a time (the CLOB times out concurrent submissions from one account, docs):
    windows signed meanwhile wait and go together, up to 15 orders to a POST."""

    def specs(self, fake, pub, clock, n):
        out = []
        for i in range(n):
            m = fake.know(pub.list_window("btc", T0 + 600 + 300 * i))
            up, down = json.loads(m["clobTokenIds"])
            out.append([OrderSpec(f"w{i}/{side}", token, D("0.47"), D("10"), T0 + 650 + 300 * i, market=m["conditionId"])
                        for side, token in (("up", up), ("down", down))])
        return out

    async def test_windows_signed_together_share_posts_one_at_a_time(self):
        clock = Clock()
        fake, pub = FakeClob(clock), FakePublic(clock)
        flying, most, sizes = 0, 0, []
        real = fake.post_orders

        async def slow(orders):
            nonlocal flying, most
            flying += 1
            most = max(most, flying)
            sizes.append(len(orders))
            await asyncio.sleep(0.05)
            try:
                return await real(orders)
            finally:
                flying -= 1
        fake.post_orders = slow
        x = adapter(fake, settings())
        results = await asyncio.gather(*(x.place(group) for group in self.specs(fake, pub, clock, 16)))  # 16 workers
        self.assertEqual(most, 1)                                   # never two POSTs in flight
        self.assertEqual(sizes, [14, 14, 4])                        # 7 windows to a POST, a window never split
        self.assertEqual({r.status for group in results for r in group}, {"open"})
        await x.close()

    async def test_order_timed_out_holds_what_waits_and_backs_off(self):
        clock = Clock()
        fake, pub = FakeClob(clock), FakePublic(clock)
        x = adapter(fake, settings())
        groups = self.specs(fake, pub, clock, 3)
        waits = []
        for _ in range(3):
            fake.answers = ["timed_out"]
            results = await asyncio.gather(*(x.place(g) for g in groups))
            self.assertEqual({r.status for group in results for r in group}, {"not_sent"})  # nothing rests: sent again later
            waits.append(results[0][0].wait)
        self.assertEqual(waits, [5.0, 10.0, 20.0])               # doubling while it lasts
        await x.place(groups[0])                                   # accepted: the next timeout starts again at 5 s
        fake.answers = ["timed_out"]
        self.assertEqual((await x.place(groups[1]))[0].wait, 5.0)
        await x.close()


class CredentialsTest(unittest.IsolatedAsyncioTestCase):
    async def test_an_api_key_of_another_account_stops_everything_loudly_and_strikes_off_nothing(self):
        """11-13 15:30: every order refused with "the order signer address has to be the address of the API KEY",
        windows retried every 5 s without a word, /check saying ⏳ 正在挂."""
        clock = Clock()
        fake, pub = FakeClob(clock), FakePublic(clock)
        fake.balance_usd = D("75.10")
        s = seven_coins(clock, fake, pub)
        e = make_engine(s, clock=clock, exchange=adapter(fake, s), public=pub)[0]
        fake.signer_mismatch = True
        await tick(e)
        sent = fake.posted
        for _ in range(5):
            await tick(e, 5)
        self.assertLess(fake.posted - sent, 30)                       # held, not hammering every 5 s
        report = e.check_text()
        verdict = report.splitlines()[1]
        self.assertTrue(verdict.startswith("⛔ 暂不新挂：交易所拒单：签名私钥和 API Key 不是同一个账户"))
        self.assertIn("the order signer address has to be the address of the API KEY", report)  # the exchange's words
        self.assertIn("POLY_API_KEY", verdict)                        # what to change, in the first line
        self.assertIn("⛔ 下单：这次启动后交易所一张都没接受", report)
        self.assertIn("签名地址", report)
        self.assertNotIn("挂着", e.coverage()["text"])               # windows being sent are not counted as resting
        self.assertTrue(any("refused-auth" in k for k in e.notified))  # said on Telegram
        self.assertEqual([q for q in quotes(e) if q.get("counted") or q.get("permanent")], [])
        fake.signer_mismatch = False                                  # the variables fixed
        for _ in range(30):
            await tick(e, 10)
        self.assertGreater(len(fake.live()), 100)
        self.assertTrue(e.check_text().splitlines()[1].startswith("✅"))


class RefusalWordsTest(unittest.IsolatedAsyncioTestCase):
    async def test_an_order_timed_out_is_sent_again_and_a_duplicate_is_looked_up(self):
        for answer in ("timed_out", "duplicated"):
            with self.subTest(answer=answer):
                clock = Clock()
                fake, pub = FakeClob(clock), FakePublic(clock)
                fake.know(pub.list_window("btc", T0 + 1800))
                s = settings()
                e = make_engine(s, clock=clock, exchange=adapter(fake, s), public=pub)[0]
                fake.answers = [answer]
                await tick(e)
                for _ in range(4):
                    await tick(e, 5)
                (q,) = [q for q in quotes(e) if q["phase"] == "active"]
                self.assertEqual({o["state"] for o in q["orders"].values()}, {"open"})
                self.assertEqual(len(fake.live()), 2)
                self.assertEqual(fake.posted, 4 if answer == "timed_out" else 2)  # sent again / found by its hash


class RedeemTest(unittest.IsolatedAsyncioTestCase):
    async def test_a_web_wallet_without_a_relayer_key_is_told_once_and_claims_after_a_restart(self):
        clock, start = Clock(), T0 + 1800
        fake, pub = FakeClob(clock), FakePublic(clock)
        m = fake.know(pub.list_window("btc", start))
        s = settings()
        x = adapter(fake, s)
        x.wallet_type = "DEPOSIT_WALLET"
        e = make_engine(s, clock=clock, exchange=x, public=pub)[0]
        await tick(e)
        (q,) = quotes(e)
        fake.fill(q["orders"]["down"]["id"], D("10"))
        clock.t = start + 300 + 61
        pub.results[m["slug"]] = {**m, "closed": True, "outcomePrices": '["0", "1"]'}
        for _ in range(5):
            await tick(e, 30)
        (q,) = quotes(e)
        self.assertEqual(q["redeem"], "manual")
        self.assertIn("Relayer", q["redeem_error"])
        self.assertEqual(fake.redeemed, [])
        s2 = settings(POLY_RELAYER_API_KEY="k", POLY_RELAYER_API_KEY_ADDRESS="0x" + "22" * 20)  # the key added, restarted
        x2 = adapter(fake, s2)
        x2.wallet_type = "DEPOSIT_WALLET"
        e2 = make_engine(s2, clock=clock, ledger=e.ledger, exchange=x2, public=pub)[0]
        self.assertEqual(e2.redeem_again(), 1)
        clock.advance(30)
        await e2.redeem_pass()
        self.assertEqual(fake.redeemed, [m["conditionId"]])
        self.assertEqual(quotes(e2)[0]["redeem"], "done")


if __name__ == "__main__":
    unittest.main()
