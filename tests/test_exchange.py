"""The live adapter's mapping of SDK answers (with a fake SDK client), the order hash, the public reads."""
import unittest
from decimal import Decimal as D

import httpx
from helpers import settings

from polymarket import RateLimitError, RequestRejectedError, TransportError
from polymarket.models.clob.cancel import CancelOrdersResponse
from polymarket.models.clob.order_response import AcceptedOrder, RejectedOrder
from polymarket.models.clob.orders import SignedOrder
from preopen import gamma
from preopen.exchange import Bucket, LiveExchange, OrderSpec, order_hash

WALLET = "0x1111111111111111111111111111111111111111"
ZERO32 = "0x" + "00" * 32


def signed(asset: str, salt: int = 7) -> SignedOrder:
    return SignedOrder(builder=ZERO32, expiration=1794624330, maker=WALLET, maker_amount=4_900_000, metadata=ZERO32,
                       order_type="GTD", salt=salt, side="BUY", signature="0x" + "ab" * 65, signature_type=3, signer=WALLET,
                       taker_amount=10_000_000, timestamp=1794537000123, token_id=asset, post_only=True)


class FakeClient:
    def __init__(self):
        self.created = []
        self.posted = []
        self.answers = []      # one per post_orders call: a list of responses or an exception
        self.sign_error = None

    async def create_limit_order(self, **kw):
        if self.sign_error:
            raise self.sign_error
        self.created.append(kw)
        return signed(kw["asset_id"], salt=len(self.created))

    async def post_orders(self, orders):
        self.posted.append(list(orders))
        answer = self.answers.pop(0)
        if isinstance(answer, Exception):
            raise answer
        return answer

    async def cancel_orders(self, order_ids):
        return CancelOrdersResponse(canceled=tuple(order_ids[:-1]), not_canceled={order_ids[-1]: "order not found"})


def specs(n: int) -> list[OrderSpec]:
    return [OrderSpec(ref=f"q{i}/{'up' if i % 2 == 0 else 'down'}", asset=str(10**70 + i), price=D("0.49"), size=D("10"),
                      expiration=1794624330) for i in range(n)]


def live() -> tuple[LiveExchange, FakeClient]:
    x = LiveExchange(settings(), order_bucket=Bucket(1000, 1000), cancel_bucket=Bucket(1000, 1000))
    client = FakeClient()
    x.client = client
    return x, client


def accepted(oid: str) -> AcceptedOrder:
    return AcceptedOrder(order_id=oid, status="live", making_amount=D(0), taking_amount=D(0), trade_ids=(), transactions_hashes=())


class LiveExchangeTest(unittest.IsolatedAsyncioTestCase):
    async def test_post_only_gtd_buy_and_mapping(self):
        x, c = live()
        c.answers = [[accepted("0x" + "01" * 32), RejectedOrder(code="post_only_would_cross", message="invalid post-only order: order crosses book")]]
        out = await x.place(specs(2))
        self.assertEqual(c.created[0], {"asset_id": str(10**70), "price": "0.49", "size": "10", "side": "BUY", "post_only": True,
                                        "expiration": 1794624330})
        self.assertEqual([r.status for r in out], ["open", "rejected"])
        self.assertEqual(out[0].order_id, "0x" + "01" * 32)
        self.assertEqual(out[1].code, "post_only_would_cross")
        self.assertTrue(out[0].hash.startswith("0x") and len(out[0].hash) == 66)

    async def test_batches_of_fifteen(self):
        x, c = live()
        c.answers = [[accepted(f"0x{i:064x}") for i in range(15)], [accepted(f"0x{i:064x}") for i in range(15, 20)]]
        out = await x.place(specs(20))
        self.assertEqual([len(b) for b in c.posted], [15, 5])
        self.assertTrue(all(r.status == "open" for r in out))

    async def test_rate_limit_marks_not_sent_and_stops_the_rest(self):
        x, c = live()
        c.answers = [RateLimitError("slow down", retry_after=7)]
        out = await x.place(specs(20))
        self.assertEqual(len(c.posted), 1)
        self.assertEqual({r.status for r in out}, {"not_sent"})
        self.assertEqual({r.wait for r in out}, {7.0})
        self.assertEqual({r.code for r in out}, {"rate_limited"})

    async def test_restart_and_auth_are_not_sent(self):
        for error, code in [(RequestRejectedError("restarting", status=425, restriction="restarting", retry_after=1), "restarting"),
                            (RequestRejectedError("unauthorized", status=401), "auth"),
                            (RequestRejectedError("trading is disabled", status=503), "trading_disabled")]:
            x, c = live()
            c.answers = [error]
            out = await x.place(specs(2))
            with self.subTest(code=code):
                self.assertEqual({r.status for r in out}, {"not_sent"})
                self.assertEqual({r.code for r in out}, {code})
                self.assertTrue(all(r.wait > 0 for r in out))

    async def test_bad_request_rejects_and_server_error_is_unknown(self):
        x, c = live()
        c.answers = [RequestRejectedError("bad", status=400), RequestRejectedError("oops", status=502)]
        out = await x.place(specs(16))
        self.assertEqual({r.status for r in out[:15]}, {"rejected"})
        self.assertEqual(out[15].status, "unknown")

    async def test_transport_error_is_unknown_unless_never_connected(self):
        x, c = live()
        err = TransportError("timed out")
        err.__cause__ = httpx.ReadTimeout("read")
        c.answers = [err]
        self.assertEqual((await x.place(specs(1)))[0].status, "unknown")
        x, c = live()
        err = TransportError("no route")
        err.__cause__ = httpx.ConnectError("refused")
        c.answers = [err]
        self.assertEqual((await x.place(specs(1)))[0].status, "not_sent")

    async def test_signing_failure_sends_nothing(self):
        from polymarket import UnexpectedResponseError, UserInputError
        x, c = live()
        c.sign_error = UnexpectedResponseError("metadata")
        out = await x.place(specs(2))
        self.assertEqual({r.status for r in out}, {"not_sent"})
        self.assertEqual(c.posted, [])
        c.sign_error = UserInputError("price not on tick")
        out = await x.place(specs(1))
        self.assertEqual(out[0].status, "rejected")

    async def test_cancel_maps_both_lists(self):
        x, c = live()
        r = await x.cancel(["a", "b", "c"])
        self.assertEqual(r.canceled, {"a", "b"})
        self.assertEqual(r.not_canceled, {"c": "order not found"})


class HashFirstTest(unittest.IsolatedAsyncioTestCase):
    async def test_the_hashes_are_handed_over_before_the_first_post(self):
        x, c = live()
        c.answers = [[accepted("0x" + "01" * 32), accepted("0x" + "02" * 32)]]
        seen = []
        out = await x.place(specs(2), on_signed=lambda hashes: seen.append((list(hashes), len(c.posted))))
        (hashes, posted_then), = seen
        self.assertEqual(posted_then, 0)  # written down while nothing had been sent
        self.assertEqual([ref for ref, _, _ in hashes], ["q0/up", "q1/down"])
        self.assertEqual([h for _, h, _ in hashes], [r.hash for r in out])
        self.assertEqual([salt for _, _, salt in hashes], [1, 2])

    async def test_a_failed_write_sends_nothing(self):
        x, c = live()
        c.answers = [[accepted("0x" + "01" * 32)]]

        def disk_full(hashes):
            raise OSError("disk full")
        with self.assertRaises(OSError):
            await x.place(specs(1), on_signed=disk_full)
        self.assertEqual(c.posted, [])

    async def test_only_an_explicit_refusal_counts_as_not_sent(self):
        cases = [
            (RequestRejectedError("Service Unavailable", status=503), "unknown", "http_503"),  # a bare 503: maybe processed
            (RequestRejectedError("Trading is currently disabled. Check polymarket.com for updates", status=503), "not_sent", "trading_disabled"),
            (RequestRejectedError("Trading is currently cancel-only", status=503, restriction="cancel_only"), "not_sent", "cancel_only"),
            (RequestRejectedError("post only mode", status=503, code="post_only_mode", restriction="post_only"), "not_sent", "post_only"),
            (RequestRejectedError("restarting", status=425, restriction="restarting"), "not_sent", "restarting"),
            (RequestRejectedError("forbidden", status=403), "not_sent", "auth"),
            (RequestRejectedError("bad gateway", status=502), "unknown", "http_502"),
            (RequestRejectedError("gateway timeout", status=504), "unknown", "http_504"),
            (RequestRejectedError("request timeout", status=408), "unknown", "http_408"),
            (RequestRejectedError("conflict", status=409), "unknown", "http_409"),
            (RequestRejectedError("bad", status=400), "rejected", "http_400"),
        ]
        for error, status, code in cases:
            x, c = live()
            c.answers = [error]
            out = await x.place(specs(2))
            with self.subTest(error=str(error), status=error.status):
                self.assertEqual({r.status for r in out}, {status})
                self.assertEqual({r.code for r in out}, {code})
                self.assertTrue(all(r.hash for r in out))  # signed: its hash known whatever became of it

    async def test_an_empty_order_answer_is_no_such_order(self):
        from polymarket import UnexpectedResponseError
        from polymarket.models.clob.account import OpenOrder
        x, c = live()

        async def empty(order_id):
            return OpenOrder.parse_response(None)  # what the SDK makes of a null body
        c.get_order = empty
        self.assertIsNone(await x.order("0x" + "ab" * 32))

        async def garbled(order_id):
            raise UnexpectedResponseError("something else")
        c.get_order = garbled
        with self.assertRaises(UnexpectedResponseError):  # not an answer: no conclusion
            await x.order("0x" + "ab" * 32)

    async def test_fills_add_up_the_maker_orders_of_the_trades(self):
        from types import SimpleNamespace as N
        x, c = live()
        trades = [N(status="CONFIRMED", maker_orders=[N(order_id="0xa", matched_amount="4"), N(order_id="0xb", matched_amount="1")]),
                  N(status="MATCHED", maker_orders=[N(order_id="0xa", matched_amount="6")]),
                  N(status="FAILED", maker_orders=[N(order_id="0xa", matched_amount="10")])]  # never settled
        asked = []

        def list_account_trades(market):
            asked.append(market)
            return _Paginator(trades)
        c.list_account_trades = list_account_trades
        self.assertEqual(await x.fills("0xc"), {"0xa": D("10"), "0xb": D("1")})
        self.assertEqual(asked, ["0xc"])


class _Page:
    def __init__(self, items):
        self.items = items


class _Paginator:
    def __init__(self, items, fail=False):
        self.items, self.fail = items, fail

    async def _gen(self):
        if self.fail:
            raise RuntimeError("nope")
        for item in self.items:
            yield item

    def iter_items(self):
        return self._gen()

    def __aiter__(self):
        async def pages():
            yield _Page(self.items)
        return pages()


class OpenOrdersTest(unittest.IsolatedAsyncioTestCase):
    async def test_deposit_wallet_lists_every_signers_orders_and_falls_back(self):
        from types import SimpleNamespace
        import datetime as dt
        o = lambda oid: SimpleNamespace(id=oid, asset_id="1", condition_id="0xc", side="BUY", price="0.49", original_size="10",
                                        size_matched="0", status="LIVE", created_at=dt.datetime.now(dt.timezone.utc))
        x, c = live()
        x.wallet_type = "DEPOSIT_WALLET"
        c.list_wallet_open_orders = lambda: _Paginator([o("w1"), o("w2")])
        c.list_open_orders = lambda: _Paginator([o("k1")])
        self.assertEqual([r.id for r in await x.open_orders()], ["w1", "w2"])
        c.list_wallet_open_orders = lambda: _Paginator([], fail=True)
        self.assertEqual([r.id for r in await x.open_orders()], ["k1"])
        x.wallet_type = "EOA"
        x.wallet_list_off_until = 0
        self.assertEqual([r.id for r in await x.open_orders()], ["k1"])


class RowAndCancelTest(unittest.IsolatedAsyncioTestCase):
    def test_resting(self):
        from preopen.exchange import Row
        row = lambda status, matched="0": Row("0x1", "1", "0xc", "BUY", D("0.49"), D("10"), D(matched), status)
        self.assertTrue(row("LIVE").resting)
        self.assertTrue(row("ORDER_STATUS_LIVE").resting)
        self.assertTrue(row("SOMETHING_NEW").resting)  # unknown: assumed resting (another cancel costs nothing)
        for status in ("MATCHED", "CANCELED", "CANCELED_NOT_ENOUGH_BALANCE", "CANCELED_EXPIRED_ORDER", "INVALID"):
            self.assertFalse(row(status).resting, status)
        self.assertFalse(row("LIVE", "10").resting)

    async def test_a_429_midway_keeps_the_earlier_chunks(self):
        x, c = live()
        x.cancels = Bucket(1000, 2)  # chunks of two ids
        calls = []

        async def cancel_orders(order_ids):
            calls.append(list(order_ids))
            if len(calls) == 2:
                raise RateLimitError("slow down", retry_after=3)
            return CancelOrdersResponse(canceled=tuple(order_ids), not_canceled={})
        c.cancel_orders = cancel_orders
        r = await x.cancel(["a", "b", "c", "d", "e"])
        self.assertEqual(calls, [["a", "b"], ["c", "d"]])
        self.assertEqual(r.canceled, {"a", "b"})
        self.assertEqual(r.throttle, ("撤单被限流（429）", 3.0))


class OrderHashTest(unittest.TestCase):
    def test_matches_the_sdk_struct_and_domain(self):
        from eth_utils.crypto import keccak
        from polymarket._internal.actions.orders.typed_data import _app_domain_separator, _order_contents_hash
        from polymarket._internal.actions.orders.types import UnsignedOrder
        s = signed(str(10**70 + 5), salt=123456789)
        u = UnsignedOrder(chain_id=137, builder=s.builder, exchange_address="0xE111180000d2663C0091e4f400237545B87B996B",
                          expiration=s.expiration, maker=s.maker, maker_amount=s.maker_amount, metadata=s.metadata,
                          order_type="GTD", salt=s.salt, side="BUY", signature_type=3, signer=s.signer,
                          taker_amount=s.taker_amount, timestamp=s.timestamp, token_id=s.token_id)
        sdk = "0x" + keccak(b"\x19\x01" + bytes.fromhex(_app_domain_separator(u, protocol_version="2")[2:])
                            + _order_contents_hash(u)).hex()
        self.assertEqual(order_hash(s, neg_risk=False), sdk)
        self.assertNotEqual(order_hash(s, neg_risk=True), sdk)  # another exchange, another domain

    def test_garbage_gives_empty(self):
        self.assertEqual(order_hash(object(), neg_risk=False), "")


class PublicApiTest(unittest.IsolatedAsyncioTestCase):
    async def test_listings_bust_the_cache_and_cover_the_limit(self):
        seen = []

        def handler(request: httpx.Request) -> httpx.Response:
            seen.append(request.url)
            slugs = request.url.params.get_list("slug")
            return httpx.Response(200, json=[{"slug": s} for s in slugs if s.endswith("0")])
        api = gamma.PublicApi(client=httpx.AsyncClient(transport=httpx.MockTransport(handler)))
        slugs = [f"btc-updown-5m-{1794537000 + 300 * i}" for i in range(50)]
        found = await api.listings(slugs)
        self.assertEqual(len(seen), 2)  # 40 + 10
        self.assertEqual(set(found), {s for s in slugs if s.endswith("0")})
        for url in seen:
            self.assertTrue(url.params.get("_"))
            self.assertGreaterEqual(int(url.params["limit"]), len(url.params.get_list("slug")))
        self.assertNotEqual(seen[0].params["_"], seen[1].params["_"])
        await api.close()

    async def test_accepting_reads_the_clob_flag(self):
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json={"ao": request.url.path.endswith("yes")})
        api = gamma.PublicApi(client=httpx.AsyncClient(transport=httpx.MockTransport(handler)))
        self.assertTrue(await api.accepting("yes"))
        self.assertFalse(await api.accepting("no"))
        await api.close()


if __name__ == "__main__":
    unittest.main()
