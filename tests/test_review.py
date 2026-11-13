"""The review of 7a14cc4, item by item, and its suggestions: each test is the failure the review described, shown not to
happen. (1) a crash mid-send; (2) a bare 503; (3) claiming another strategy's order; (4) a stale balance; (5) Telegram
group members; (6) a shared API key swept by market; then the instance lock, the ledger's account, the scans kept apart,
the public API's 429 and the risk limits."""
import asyncio
import contextlib
import tempfile
import time
import unittest
from decimal import Decimal as D
from pathlib import Path

import httpx
from helpers import T0, Clock, FakePublic, drain, make_engine, settings, tick

import main
from preopen import config, gamma, markets as mk
from preopen.exchange import OrderSpec, Placed
from preopen.ledger import Ledger
from preopen.telegram import CONFIRM_PAUSE, Telegram


def quotes(e, slug=None, phase=None):
    return sorted((q for q in e.ledger.rows.values() if (slug is None or q["slug"] == slug) and (phase is None or q["phase"] == phase)),
                  key=lambda q: (q["start"], q["coin"], q["level"], q["attempt"]))


class Crash(BaseException):
    """The process dies (a deploy, an OOM kill): nothing after it runs."""


async def ready(e, pub, coin="btc", start=T0 + 1800):
    pub.list_window(coin, start)
    await e.account_once()
    await e.discover()
    return mk.slug_of(coin, start)


class CrashMidSendTest(unittest.IsolatedAsyncioTestCase):
    """Review 1: the hash was written only after the POST came back; a process stopped in between left orders it could
    not find again, and a filled one was given up and its window quoted a second time."""

    async def test_the_hashes_are_on_disk_before_the_send_and_a_fill_made_meanwhile_is_found(self):
        with tempfile.TemporaryDirectory() as d:
            path = str(Path(d) / "preopen.sqlite3")
            clock = Clock()
            e, _, pub, x = make_engine(clock=clock, ledger=Ledger(path))
            await ready(e, pub)
            real = x.place

            async def dies_after_the_post(specs, on_signed=None):
                await real(specs, on_signed)  # the CLOB took both orders...
                raise Crash()                 # ...and the process died before the answer was written down
            x.place = dies_after_the_post
            with self.assertRaises(Crash):
                await drain(e)
            disk = Ledger(path)  # what a new process finds
            (q,) = disk.rows.values()
            self.assertEqual({o["state"] for o in q["orders"].values()}, {"placing"})
            self.assertEqual({o["hash"] for o in q["orders"].values()}, set(x.book))  # the ids, written before the send
            x.fill(q["orders"]["up"]["hash"], D("10"))  # while it was down, the up side filled in full (off the book)
            x.place = real
            e2, _, _, _ = make_engine(clock=clock, ledger=disk, exchange=x, public=pub)
            e2.take_over()
            await tick(e2, 5)
            await tick(e2, 6)
            (q,) = quotes(e2)
            self.assertEqual(D(q["orders"]["up"]["filled"]), D("10"))  # the fill is on the record
            self.assertEqual(q["orders"]["down"]["state"], "open")      # the other side found resting, kept
            self.assertEqual(q["phase"], "active")
            for _ in range(20):  # minutes on: nothing given up, nothing quoted a second time
                await tick(e2, 10)
            self.assertEqual(len(quotes(e2)), 1)
            self.assertEqual(len(x.placed), 2)
            e.ledger.close()
            disk.close()

    async def test_an_order_never_found_is_kept_unknown_until_its_expiry_without_the_ids_word(self):
        e, clock, pub, x = make_engine()
        slug = await ready(e, pub)

        async def lost(specs, on_signed=None):
            on_signed([(s.ref, "0x" + format(700 + i, "064x"), 700 + i) for i, s in enumerate(specs)])
            raise TimeoutError("read timeout")
        x.place = lost
        await drain(e)
        for _ in range(30):  # five minutes: the id was never confirmed to be the hash, so "not found" proves nothing
            await tick(e, 10)
        q = quotes(e, slug)[0]
        self.assertEqual({o["state"] for o in q["orders"].values()}, {"unknown"})
        self.assertEqual(len(quotes(e, slug)), 1)  # and the window is not quoted again meanwhile


class Bare503Test(unittest.IsolatedAsyncioTestCase):
    """Review 2: a bare 503 was taken for "not sent": the record dropped and its money freed while the orders rested."""

    async def test_orders_answered_with_a_bare_503_stay_on_record_and_are_found(self):
        e, clock, pub, x = make_engine()
        slug = await ready(e, pub)
        real = x.place

        async def processed_then_503(specs, on_signed=None):
            got = await real(specs, on_signed)  # processed (resting)...
            return [Placed(p.ref, "unknown", code="http_503", message="Service Unavailable", hash=p.hash, salt=p.salt) for p in got]
        x.place = processed_then_503
        await drain(e)
        (q,) = quotes(e, slug)
        self.assertEqual({o["state"] for o in q["orders"].values()}, {"unknown"})
        self.assertEqual(e.reserved(), D("9.80"))  # its money stays reserved while its state is not known
        await tick(e, 5)
        (q,) = quotes(e, slug)
        self.assertEqual({o["state"] for o in q["orders"].values()}, {"open"})
        self.assertEqual(q["phase"], "active")

    async def test_a_refusal_misread_as_not_sent_is_still_cancelled_by_its_hash(self):
        e, clock, pub, x = make_engine()
        slug = await ready(e, pub)
        real = x.place

        async def rested_but_said_refused(specs, on_signed=None):
            got = await real(specs, on_signed)
            return [Placed(p.ref, "not_sent", code="rate_limited", message="429", hash=p.hash, salt=p.salt) for p in got]
        x.place = rested_but_said_refused
        await drain(e)
        self.assertEqual(quotes(e, slug), [])  # judged refused: no record...
        self.assertEqual(len(x.book), 2)
        await e.cancel_pass()
        self.assertEqual(x.book, {})           # ...yet nothing it signed is left on the book
        self.assertTrue(any("其实挂上了" in text for _, text in e.errors))


class ClaimTest(unittest.IsolatedAsyncioTestCase):
    """Review 3: an unknown order was claimed by its look (token, side, price, size) before its hash: another strategy's
    identical order could be taken for it, then cancelled."""

    async def setup_lost_with_a_lookalike(self, e, pub, x):
        slug = await ready(e, pub)
        await drain(e)
        (q,) = quotes(e, slug)
        o = q["orders"]["up"]
        other = (await x.place([OrderSpec("other/up", o["asset"], D("0.49"), D("10"), q["expiration"], market=q["condition_id"])]))[0]
        mine = o["id"]
        e.by_id.pop(mine)
        o.update(state="unknown", id="", unknown_at=e.now(), sent=e.now() - 10)  # its answer was lost; its hash is known
        e.save(q)
        await e.account_once()
        return q, mine, other.order_id

    async def test_the_hash_decides_and_the_lookalike_is_left_alone(self):
        e, clock, pub, x = make_engine()
        q, mine, other = await self.setup_lost_with_a_lookalike(e, pub, x)
        self.assertTrue(e.claim(q, "up"))
        self.assertEqual(q["orders"]["up"]["id"], mine)  # its own order, exactly
        self.assertIn(other, x.book)

    async def test_without_a_usable_hash_two_lookalikes_are_not_guessed_between(self):
        e, clock, pub, x = make_engine()
        q, mine, other = await self.setup_lost_with_a_lookalike(e, pub, x)
        e.id_is_hash = False  # the id is known not to be the hash: only the look is left
        q["orders"]["up"]["hash"] = "0x" + "77" * 32  # (so the hash is not the id either)
        e.save(q)
        self.assertFalse(e.claim(q, "up"))  # two orders fit: neither is claimed
        self.assertIn("不自动认领", q["orders"]["up"]["claim_note"])
        await x.cancel([other])
        await e.account_once()
        self.assertTrue(e.claim(q, "up"))  # one fits: that one
        self.assertEqual(q["orders"]["up"]["id"], mine)


class StaleBalanceTest(unittest.IsolatedAsyncioTestCase):
    """Review 4: the snapshot was marked fresh before the balance was read: a balance failing for ten minutes was still
    placed against."""

    async def test_a_balance_that_cannot_be_read_stops_new_orders(self):
        e, clock, pub, x = make_engine()
        await e.account_once()
        x.fail_balance = RuntimeError("balance endpoint down")
        for _ in range(12):  # two minutes of failed balance reads (the open orders read fine)
            clock.advance(10)
            with contextlib.suppress(RuntimeError):
                await e.account_once()
        self.assertIn("余额过期", e.hold())
        pub.list_window("btc", T0 + 1800)
        await tick(e, account=False)
        self.assertEqual(x.placed, [])
        x.fail_balance = None
        await tick(e)
        self.assertEqual(e.hold(), "")
        await tick(e)
        self.assertEqual(len(x.placed), 2)


class FakeTelegram(Telegram):
    def __init__(self, chat, admins):
        super().__init__("123456:TEST", chat, admins=admins)
        self.posts = []

    async def _post(self, method, **payload):
        self.posts.append((method, payload))
        return True


class TelegramGroupTest(unittest.IsolatedAsyncioTestCase):
    """Review 5: in a group chat any member could pause, resume and read the balances (only the chat was checked)."""

    async def test_in_a_group_only_the_admins_are_obeyed(self):
        group, admin, member = "-1001234567890", 777, 555
        tg = FakeTelegram(group, admins=(str(admin),))
        seen = []

        async def handle(cmd, args):
            seen.append(cmd)
            return "ok"
        await tg.on_message({"chat": {"id": int(group)}, "from": {"id": member}, "text": "/resume"}, handle)
        await tg.on_message({"chat": {"id": int(group)}, "from": {"id": member}, "text": "📊 状态"}, handle)
        self.assertEqual(seen, [])
        await tg.on_callback({"id": "1", "data": CONFIRM_PAUSE, "from": {"id": member},
                              "message": {"chat": {"id": int(group)}, "message_id": 3}}, handle)
        self.assertEqual(seen, [])
        self.assertEqual(tg.posts[-1], ("answerCallbackQuery", {"callback_query_id": "1", "text": "只接受管理员的操作"}))
        await tg.on_message({"chat": {"id": int(group)}, "from": {"id": admin}, "text": "/status"}, handle)
        self.assertEqual(seen, ["/status"])
        await tg.on_message({"chat": {"id": admin}, "from": {"id": admin}, "text": "/status"}, handle)  # another chat
        self.assertEqual(seen, ["/status"])

    def test_admins_default_to_the_private_chat_and_a_group_needs_them_named(self):
        self.assertEqual(settings(TELEGRAM_BOT_TOKEN="1:x", TELEGRAM_CHAT_ID="777").telegram_admins, ("777",))
        s = settings(TELEGRAM_BOT_TOKEN="1:x", TELEGRAM_CHAT_ID="-100123")
        self.assertEqual(s.telegram_admins, ())
        self.assertTrue(any("TELEGRAM_ADMIN_IDS" in n for n in s.notes))
        self.assertEqual(settings(TELEGRAM_CHAT_ID="-100123", TELEGRAM_ADMIN_IDS="777, 888").telegram_admins, ("777", "888"))
        with self.assertRaises(config.ConfigError):
            settings(TELEGRAM_ADMIN_IDS="@someone")
        self.assertEqual(Telegram("1:x", "-100123").admins, set())  # a group's id names no user


class SharedKeyTest(unittest.IsolatedAsyncioTestCase):
    """Review 6: the market sweep and the emergency cancel-all took every order of the API key, another strategy's too."""

    async def place_with_a_neighbour(self, e, pub, x):
        slug = await ready(e, pub)
        await drain(e)
        (q,) = quotes(e, slug)
        other = (await x.place([OrderSpec("other/down", q["orders"]["down"]["asset"], D("0.30"), D("20"), q["expiration"],
                                          market=q["condition_id"])]))[0]
        return q, other.order_id

    async def test_by_default_only_the_bots_own_orders_are_cancelled(self):
        e, clock, pub, x = make_engine()
        q, other = await self.place_with_a_neighbour(e, pub, x)
        text = await e.emergency()
        self.assertEqual(set(x.book), {other})  # its own two gone, the neighbour kept
        self.assertIn("都已撤掉", text)
        clock.t = q["start"] - 60
        await tick(e)
        self.assertEqual(x.market_cancels, [])
        self.assertEqual(set(x.book), {other})

    async def test_an_exclusive_key_is_cleared_at_once(self):
        e, clock, pub, x = make_engine(settings(PREOPEN_KEY_EXCLUSIVE="on"))
        q, other = await self.place_with_a_neighbour(e, pub, x)
        await e.emergency()
        self.assertEqual(x.book, {})


class InstanceLockTest(unittest.TestCase):
    def test_a_second_process_on_the_same_ledger_is_refused(self):
        with tempfile.TemporaryDirectory() as d:
            db = str(Path(d) / "preopen.sqlite3")
            first = main.acquire_lock(db)
            with self.assertRaises(main.InstanceLocked):
                main.acquire_lock(db)
            first.close()
            main.acquire_lock(db).close()  # released with its holder
            self.assertIsNone(main.acquire_lock(":memory:"))


class LedgerAccountTest(unittest.IsolatedAsyncioTestCase):
    async def test_another_accounts_ledger_is_not_taken_over_until_rebound(self):
        e, clock, pub, x = make_engine()
        slug = await ready(e, pub)
        await drain(e)
        e.ledger.put("identity", "0x" + "aa" * 20 + "|0x" + "bb" * 20)  # written by another account
        e.blocked = e.check_identity()
        self.assertIn("账本属于另一个账户", e.blocked)
        self.assertEqual(e.hold(), e.blocked)
        e.pause()
        await e.cancel_pass()
        self.assertEqual(quotes(e, slug)[0]["phase"], "active")  # its records left exactly as they were
        self.assertEqual(len(x.book), 2)
        text = e.rebind()
        self.assertIn("改绑", text)
        self.assertEqual(e.ledger.get("identity"), "paper")
        (q,) = quotes(e, slug)
        self.assertTrue(q["foreign"])
        self.assertEqual(q["phase"], "closed")
        self.assertTrue(e.restart and e.stop.is_set())

    async def test_a_blocked_run_waits_without_touching_anything(self):
        e, clock, pub, x = make_engine()
        e.ledger.put("identity", "0x" + "aa" * 20 + "|0x" + "bb" * 20)
        task = asyncio.create_task(e.run())
        await asyncio.sleep(0.05)
        self.assertTrue(e.blocked)
        self.assertEqual(x.placed, [])
        e.stop.set()
        await asyncio.wait_for(task, 2)

    async def test_the_first_run_binds_the_ledger(self):
        e, clock, pub, x = make_engine()
        self.assertEqual(e.check_identity(), "")
        self.assertEqual(e.ledger.get("identity"), "paper")
        self.assertEqual(e.check_identity(), "")


class ScanTest(unittest.IsolatedAsyncioTestCase):
    async def test_the_fast_poll_and_the_full_pass_are_apart(self):
        e, clock, pub, x = make_engine()
        await e.scan_front()
        self.assertEqual((len(pub.asked), len(pub.path_asked)), (0, 1))  # the next windows only
        await e.scan_full()
        self.assertEqual((len(pub.asked), len(pub.path_asked)), (1, 1))  # the batched pass only
        await e.scan_full()
        self.assertEqual(len(pub.asked), 1)  # once a minute

    async def test_a_hung_full_pass_does_not_hold_the_fast_poll(self):
        e, clock, pub, x = make_engine()
        gate = asyncio.Event()

        async def hung(slugs, closed=False):
            await gate.wait()
            return {}
        pub.listings = hung
        full = asyncio.create_task(e.scan_full())
        await asyncio.sleep(0)
        start = mk.window_starts(clock.t, e.s)[0]
        pub.list_window("btc", start, created=clock.t)
        await asyncio.wait_for(e.scan_front(), 1)
        self.assertIn(mk.slug_of("btc", start), e.listings)
        gate.set()
        await full


class PublicThrottleTest(unittest.IsolatedAsyncioTestCase):
    async def test_a_429_is_obeyed_and_an_error_is_not_taken_for_no_market(self):
        hits = []
        mode = {"status": 429}

        def handler(request: httpx.Request) -> httpx.Response:
            hits.append(request.url.path)
            if mode["status"] == 429:
                return httpx.Response(429, headers={"Retry-After": "7"})
            if mode["status"] == 500:
                return httpx.Response(500)
            return httpx.Response(200, json={"slug": request.url.path.rsplit("/", 1)[-1]})
        now = [100.0]
        api = gamma.PublicApi(client=httpx.AsyncClient(transport=httpx.MockTransport(handler)), clock=lambda: now[0])
        with self.assertRaises(gamma.PublicThrottled) as caught:
            await api.market("btc-updown-5m-1")
        self.assertEqual(caught.exception.seconds, 7.0)
        with self.assertRaises(gamma.PublicThrottled):
            await api.markets(["btc-updown-5m-1", "eth-updown-5m-1"])
        self.assertEqual(len(hits), 1)  # nothing sent while it lasts
        now[0] += 8
        mode["status"] = 500
        with self.assertRaises(httpx.HTTPStatusError):  # an error, never "no such market"
            await api.market("btc-updown-5m-1")
        self.assertEqual(await api.markets(["btc-updown-5m-1"]), {})  # left out, asked again next poll
        mode["status"] = 200
        self.assertEqual(set(await api.markets(["btc-updown-5m-1"])), {"btc-updown-5m-1"})
        self.assertIn("失败", api.health())
        await api.close()

    def test_retry_after_forms(self):
        r = lambda v: httpx.Response(429, headers={"Retry-After": v} if v is not None else {})
        self.assertEqual(gamma.retry_after(r("3")), 3.0)
        self.assertEqual(gamma.retry_after(r(None)), 10.0)
        self.assertEqual(gamma.retry_after(r("99999")), 600.0)
        future = time.strftime("%a, %d %b %Y %H:%M:%S GMT", time.gmtime(time.time() + 30))
        self.assertTrue(25 <= gamma.retry_after(r(future)) <= 31)

    async def test_the_engine_backs_off(self):
        e, clock, pub, x = make_engine()

        async def throttled(slugs):
            raise gamma.PublicThrottled("gamma-api.polymarket.com", 0.01)
        pub.markets = throttled
        e.stop = asyncio.Event()
        task = asyncio.create_task(e.discovery_loop())
        await asyncio.sleep(0.05)
        e.stop.set()
        await asyncio.wait_for(task, 2)
        self.assertTrue(any("限流" in text for _, text in e.errors))


class RiskTest(unittest.IsolatedAsyncioTestCase):
    async def test_exposure_is_the_unpaired_shares_cost(self):
        e, clock, pub, x = make_engine()
        starts = mk.window_starts(clock.t, e.s)[:2]
        pub.list_range(["btc"], starts)
        await tick(e)
        a, b = quotes(e)
        x.fill(a["orders"]["up"]["id"], D("10"))   # one-sided: the 4.90 paid may all be lost
        x.fill(b["orders"]["up"]["id"], D("10"))
        x.fill(b["orders"]["down"]["id"], D("10"))  # a full pair: pays $1 whatever happens
        await tick(e, 10)  # off the book...
        await tick(e, 6)   # ...and their final records read
        self.assertEqual(e.exposure(), D("4.90"))

    async def test_the_exposure_cap_withdraws_the_unfilled_quotes_and_keeps_the_hedge(self):
        e, clock, pub, x = make_engine(settings(PREOPEN_MAX_EXPOSURE_USD="4"))
        pub.list_range(["btc", "eth"], mk.window_starts(clock.t, e.s)[:2])
        await tick(e)
        self.assertEqual(len(quotes(e, phase="active")), 4)
        hit = quotes(e)[0]
        x.fill(hit["orders"]["up"]["id"], D("10"))  # $4.90 one-sided: over the $4 cap
        await tick(e, 10)
        await tick(e, 6)
        self.assertIn("最坏亏损", e.hold())
        await e.cancel_pass()  # the next cancel pass (every second) withdraws
        active = quotes(e, phase="active")
        self.assertEqual([q["key"] for q in active], [hit["key"]])  # its unfilled side may still even it out
        self.assertEqual(active[0]["orders"]["down"]["state"], "open")
        await tick(e, 6)
        await tick(e, 30)
        self.assertEqual(len(quotes(e, phase="active")), 1)  # nothing new while the limit holds

    async def test_the_daily_loss_limit(self):
        e, clock, pub, x = make_engine(settings(PREOPEN_MAX_DAILY_LOSS_USD="5"))
        start = T0 - 600  # a window of today, already settled at a loss
        e.save({"key": "x#0@0", "slug": mk.slug_of("btc", start), "coin": "btc", "start": start, "level": 0, "attempt": 0,
                "price": "0.49", "shares": "20", "phase": "settled", "pnl": -9.8, "result": "down", "expiration": start,
                "condition_id": "0xc", "orders": {s: {"state": "final", "filled": "20" if s == "up" else "0"} for s in mk.SIDES}})
        self.assertIn("日亏损上限", e.hold())
        clock.advance(86400)  # the next Beijing day
        await e.account_once()
        self.assertNotIn("日亏损", e.hold())


if __name__ == "__main__":
    unittest.main()
