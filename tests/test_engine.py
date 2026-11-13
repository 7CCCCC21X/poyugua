"""The engine on a paper book: discovery, placement, the cancel before the start, fills, results, restarts."""
import asyncio
import unittest
from decimal import Decimal as D

from helpers import T0, Clock, FakePublic, drain, make_engine, settings, tick, unpark

from preopen import markets as mk
from preopen.exchange import PaperExchange
from preopen.ledger import Ledger


def quotes(e, slug=None, phase=None):
    return sorted((q for q in e.ledger.rows.values() if (slug is None or q["slug"] == slug) and (phase is None or q["phase"] == phase)),
                  key=lambda q: (q["start"], q["coin"], q["level"], q["attempt"]))


class PlacementTest(unittest.IsolatedAsyncioTestCase):
    async def test_fresh_listing_is_quoted_both_sides_post_only_gtd(self):
        e, clock, pub, x = make_engine()
        start = T0 + 1800
        pub.list_window("btc", start, created=clock.t - 1)
        await e.account_once()
        await e.discover()
        self.assertIn(mk.slug_of("btc", start), e.queued)
        await drain(e)
        qs = quotes(e, mk.slug_of("btc", start))
        self.assertEqual(len(qs), 1)
        q = qs[0]
        self.assertEqual(q["phase"], "active")
        self.assertEqual({o["state"] for o in q["orders"].values()}, {"open"})
        self.assertEqual(len(x.placed), 2)
        self.assertEqual({(s.price, s.size) for s in x.placed}, {(D("0.49"), D("10"))})
        # GTD: stated expiration = start - expiry (30 s) + the CLOB's 60 s threshold
        self.assertEqual({s.expiration for s in x.placed}, {start - 30 + 60})
        self.assertEqual(e.reserved(), D("9.80"))
        self.assertTrue(e.place_lags)

    async def test_whole_range_soonest_first_and_fresh_listing_jumps_the_queue(self):
        e, clock, pub, x = make_engine()
        starts = mk.window_starts(clock.t, e.s)
        pub.list_range(["btc", "eth"], starts)
        fresh = starts[-1] + 300
        await e.account_once()
        await e.discover()
        clock.advance(400)  # the range moved by a window: the newest one now listed, fresh
        await e.account_once()
        pub.list_window("btc", fresh, created=clock.t - 0.5)
        pub.list_window("eth", fresh, created=clock.t - 0.5)
        await e.discover()
        first = e.queue.get_nowait()
        self.assertEqual(first[0], 0)  # priority class 0: fresh listings first
        self.assertEqual(first[1], fresh)
        e.queue.put_nowait(first)
        await drain(e)
        placed_starts = [int(q["start"]) for q in quotes(e)]
        self.assertIn(fresh, placed_starts)
        self.assertEqual(len({q["slug"] for q in quotes(e)}), len(mk.window_starts(clock.t, e.s)) * 2)

    async def test_next_window_is_found_by_the_direct_path_between_full_scans(self):
        e, clock, pub, x = make_engine()
        await e.account_once()
        await e.discover()  # the full scan (batched query): nothing listed yet
        self.assertEqual(len(pub.asked), 1)
        starts = mk.window_starts(clock.t, e.s)
        nxt = starts[0]
        self.assertEqual(sorted(pub.path_asked[-1]), sorted(mk.slug_of(c, nxt) for c in ("btc", "eth")))
        pub.list_window("btc", nxt, created=clock.t)
        clock.advance(0.5)
        await e.discover()
        self.assertEqual(len(pub.asked), 1)  # no batched query between full scans
        self.assertIn(mk.slug_of("btc", nxt), e.listings)
        self.assertIn(mk.slug_of("btc", nxt), e.queued)
        self.assertEqual(e.listing_lags[-1], 0.5)
        # the coin's next unlisted window is asked from now on
        self.assertIn(mk.slug_of("btc", starts[1]), pub.path_asked[-1] + [mk.slug_of("btc", s) for s in e.frontier(starts).values()])

    async def test_fresh_listing_with_fields_missing_is_asked_again_at_once(self):
        e, clock, pub, x = make_engine()
        start = mk.window_starts(clock.t, e.s)[0]  # the coin's next window: asked by the path every poll
        m = pub.list_window("btc", start, created=clock.t - 1)
        tokens = m.pop("clobTokenIds")
        await e.account_once()
        await e.discover()
        slug = mk.slug_of("btc", start)
        self.assertNotIn(slug, e.listings)
        self.assertNotIn(slug, e.skipped)  # young: not set aside for a minute
        m["clobTokenIds"] = tokens
        clock.advance(0.5)
        await e.discover()
        self.assertIn(slug, e.listings)

    async def test_not_accepting_yet_is_retried_quickly(self):
        e, clock, pub, x = make_engine()
        start = T0 + 1800
        # as the direct path answers in the first second of a listing: active false, acceptingOrders null
        m = pub.list_window("btc", start, created=clock.t - 1, accepting=None, active=False)
        pub.not_accepting.add(m["conditionId"])
        await e.account_once()
        await e.discover()
        await drain(e)
        self.assertEqual(x.placed, [])
        slug = mk.slug_of("btc", start)
        self.assertIn(slug, e.timers)  # parked for ARM_RETRY_S
        pub.not_accepting.clear()
        unpark(e)
        await drain(e)
        self.assertEqual(len(x.placed), 2)

    async def test_budget_cap_holds_the_later_windows(self):
        e, clock, pub, x = make_engine(settings(PREOPEN_MAX_OPEN_USD="20"))
        starts = mk.window_starts(clock.t, e.s)
        pub.list_range(["btc"], starts)
        await e.account_once()
        await e.discover()
        await drain(e)
        placed = quotes(e)
        self.assertEqual(len(placed), 2)  # $9.80 each: two fit under $20
        self.assertEqual([q["start"] for q in placed], starts[:2])  # the soonest
        self.assertLessEqual(e.reserved(), D("20"))
        e.sweep()
        await drain(e)
        self.assertEqual(len(quotes(e)), 2)
        self.assertIn("资金", e.budget_note)

    async def test_paper_balance_limits(self):
        e, clock, pub, x = make_engine(balance="15")
        pub.list_range(["btc", "eth"], mk.window_starts(clock.t, e.s))
        await e.account_once()
        await e.discover()
        await drain(e)
        self.assertEqual(len(quotes(e)), 1)

    async def test_hold_without_snapshot_and_during_cooldown(self):
        e, clock, pub, x = make_engine()
        pub.list_window("btc", T0 + 1800)
        await e.discover()
        await drain(e)  # no account snapshot yet
        self.assertEqual(x.placed, [])
        await e.account_once()
        e.set_cooldown("测试", 30)
        unpark(e)
        await drain(e)
        self.assertEqual(x.placed, [])
        clock.advance(31)
        await e.account_once()
        unpark(e)
        await drain(e)
        self.assertEqual(len(x.placed), 2)

    async def test_weekend_and_entry_cutoff(self):
        e, clock, pub, x = make_engine()
        pub.list_window("btc", T0 + 300)  # starts within the 300 s cutoff: never quoted
        await e.account_once()
        await e.discover()
        await drain(e)
        self.assertEqual(x.placed, [])
        self.assertEqual(pub.asked and all(mk.slug_of("btc", T0 + 300) not in a for a in pub.asked), True)


class CancelTest(unittest.IsolatedAsyncioTestCase):
    async def test_cancel_before_start_then_close_and_settle(self):
        e, clock, pub, x = make_engine()
        start = T0 + 1800
        pub.list_window("btc", start)
        await tick(e)
        q = quotes(e)[0]
        ids = [o["id"] for o in q["orders"].values()]
        clock.t = start - 61
        await tick(e)
        self.assertEqual(quotes(e)[0]["phase"], "active")
        clock.t = start - 60  # the cancel time
        await tick(e)
        self.assertEqual(sorted(x.cancel_calls[-1]), sorted(ids))
        q = quotes(e)[0]
        self.assertEqual(q["phase"], "closing")
        self.assertEqual({o["state"] for o in q["orders"].values()}, {"gone"})
        self.assertEqual(q["reason"], "到点撤单")
        self.assertFalse(q["counted"])
        await tick(e, 6)  # the final read
        q = quotes(e)[0]
        self.assertEqual(q["phase"], "settled")  # nothing filled: settled at once
        self.assertEqual(q["pnl"], 0.0)
        self.assertEqual(e.reserved(), D(0))
        self.assertTrue(e.cancel_lags)

    async def test_failed_cancel_is_backed_by_the_market_sweep(self):
        e, clock, pub, x = make_engine()
        start = T0 + 1800
        m = pub.list_window("btc", start)
        await tick(e)
        clock.t = start - 60
        x.fail_cancel = RuntimeError("boom")  # the cancel by id fails...
        await e.cancel_pass()
        self.assertEqual(x.market_cancels, [m["conditionId"]])  # ...the sweep of the market at its cancel time withdraws them
        self.assertEqual({o["state"] for o in quotes(e)[0]["orders"].values()}, {"gone"})
        await e.cancel_pass()
        self.assertEqual(len(x.market_cancels), 1)  # once

    async def test_failed_cancel_sprints_every_two_seconds(self):
        e, clock, pub, x = make_engine()
        start = T0 + 1800
        pub.list_window("btc", start)
        await tick(e)

        async def down(cid):
            raise RuntimeError("sweep down")
        x.cancel_market = down
        clock.t = start - 60
        x.fail_cancel = RuntimeError("boom")
        await tick(e)
        q = quotes(e)[0]
        self.assertEqual({o["state"] for o in q["orders"].values()}, {"open"})
        self.assertEqual({o["retry"] for o in q["orders"].values()}, {clock.t + 2})
        await tick(e, 1)
        self.assertEqual(len(x.cancel_calls), 0)
        await tick(e, 1)
        self.assertEqual(len(x.cancel_calls), 1)
        self.assertEqual({o["state"] for o in quotes(e)[0]["orders"].values()}, {"gone"})

    async def test_backoff_before_the_cancel_time_never_passes_it(self):
        e, clock, pub, x = make_engine(settings(PREOPEN_MARKETS="btc"))
        start = T0 + 1800
        pub.list_window("btc", start)
        await tick(e)
        e.pause()  # closes it long before its cancel time

        async def failing(ids):
            raise RuntimeError("cancel down")
        x.cancel = failing

        async def down(cid):
            raise RuntimeError("sweep down")
        x.cancel_market = down
        for _ in range(8):  # the backoff doubles: 15, 30, 60, 120 s...
            await e.cancel_pass()
            o = quotes(e)[0]["orders"]["up"]
            clock.t = max(clock.t + 1, float(o["retry"]))
        o = quotes(e)[0]["orders"]["up"]
        self.assertLessEqual(float(o["retry"]), start - 60 + 10)  # capped at the cancel time, then sprinting
        self.assertLess(clock.t, start)

    async def test_fill_then_result_and_pnl(self):
        e, clock, pub, x = make_engine()
        start = T0 + 1800
        m = pub.list_window("btc", start)
        await tick(e)
        q = quotes(e)[0]
        x.fill(q["orders"]["up"]["id"], D("4"))
        await tick(e, 10)
        q = quotes(e)[0]
        self.assertEqual(D(q["orders"]["up"]["filled"]), D("4"))
        self.assertTrue(e.fills_pending or any("成交" in n for n in e.notes))
        self.assertEqual(e.reserved(), D("0.49") * 6 + D("4.90"))
        clock.t = start - 60
        await tick(e)
        await tick(e, 6)
        q = quotes(e)[0]
        self.assertEqual(q["phase"], "closed")
        clock.t = start + 300 + 61
        pub.results[m["slug"]] = {**m, "closed": True, "outcomePrices": '["1", "0"]'}
        x.live = True  # the redeem path (the paper exchange answers it)
        await tick(e)
        q = quotes(e)[0]
        self.assertEqual(q["phase"], "settled")
        self.assertEqual(q["result"], "up")
        self.assertAlmostEqual(q["pnl"], 4 - 4 * 0.49)
        self.assertEqual(q["redeem"], "pending")
        await e.redeem_pass()
        self.assertEqual(quotes(e)[0]["redeem"], "done")
        clock.advance(2 * 86400 + 600)  # settled and redeemed: archived two days on
        self.assertEqual(e.ledger.archive_old(clock.t), 1)

    async def test_full_fill_leaves_the_other_side_resting(self):
        e, clock, pub, x = make_engine()
        start = T0 + 1800
        pub.list_window("btc", start)
        await tick(e)
        q = quotes(e)[0]
        x.fill(q["orders"]["down"]["id"], D("10"))  # fully filled: off the book
        await tick(e, 10)
        await tick(e, 6)
        q = quotes(e)[0]
        self.assertEqual(q["orders"]["down"]["state"], "final")
        self.assertEqual(D(q["orders"]["down"]["filled"]), D("10"))
        self.assertEqual(q["orders"]["up"]["state"], "open")
        self.assertEqual(q["phase"], "active")

    async def test_external_cancel_closes_counted_and_requotes(self):
        e, clock, pub, x = make_engine()
        start = T0 + 1800
        pub.list_window("btc", start)
        await tick(e)
        q = quotes(e)[0]
        oid = q["orders"]["up"]["id"]
        row = x.book.pop(oid)  # withdrawn by someone else
        row.status = "CANCELED"
        x.gone[oid] = row
        await tick(e, 10)   # off the snapshot
        await tick(e, 6)    # final read: cancelled with nothing filled -> the record closes, its other side is cancelled
        q = quotes(e)[0]
        self.assertEqual(q["phase"], "closing")
        self.assertTrue(q["counted"])
        await tick(e, 6)    # the other side cancelled
        await tick(e, 6)    # ...and read
        self.assertIn(quotes(e)[0]["phase"], ("closed", "settled"))
        await tick(e, 61)   # COUNTED_REQUOTE_S on: the window is quoted again under attempt 1
        qs = quotes(e, mk.slug_of("btc", start))
        self.assertEqual([q["attempt"] for q in qs], [0, 1])
        self.assertEqual(qs[-1]["phase"], "active")


class RefusalTest(unittest.IsolatedAsyncioTestCase):
    async def test_one_side_refused_cancels_the_other_and_counts(self):
        e, clock, pub, x = make_engine(settings(PREOPEN_MAX_ATTEMPTS="2"))
        start = T0 + 1800
        slug = mk.slug_of("btc", start)
        pub.list_window("btc", start)
        x.reject[f"{slug}#0@0/down"] = ("rejected", "post_only_would_cross")
        await tick(e)
        q = quotes(e)[0]
        self.assertEqual(q["phase"], "closing")
        self.assertTrue(q["counted"])
        self.assertEqual(q["orders"]["down"]["state"], "rejected")
        self.assertEqual(len(x.cancel_calls), 1)  # the side that rested is withdrawn
        await tick(e, 6)
        self.assertIn(quotes(e)[0]["phase"], ("closed", "settled"))
        x.reject[f"{slug}#0@1/up"] = ("rejected", "post_only_would_cross")
        await tick(e, 61)
        await tick(e, 6)
        self.assertEqual([q["attempt"] for q in quotes(e, slug)], [0, 1])
        await tick(e, 61)
        self.assertEqual(len(quotes(e, slug)), 2)  # PREOPEN_MAX_ATTEMPTS=2 reached: no third quote

    async def test_rate_limited_leaves_no_record_and_cools_down(self):
        e, clock, pub, x = make_engine()
        start = T0 + 1800
        slug = mk.slug_of("btc", start)
        pub.list_window("btc", start)
        x.reject[f"{slug}#0@0/up"] = ("not_sent", "rate_limited")
        x.reject[f"{slug}#0@0/down"] = ("not_sent", "rate_limited")
        await tick(e)
        self.assertEqual(quotes(e), [])
        self.assertIn(slug, e.timers)
        unpark(e)
        await tick(e)
        self.assertEqual(len(quotes(e)), 1)
        self.assertEqual(quotes(e)[0]["attempt"], 0)

    async def test_permanent_refusal_is_not_quoted_again(self):
        e, clock, pub, x = make_engine()
        start = T0 + 1800
        slug = mk.slug_of("btc", start)
        pub.list_window("btc", start)
        x.reject[f"{slug}#0@0/up"] = ("rejected", "invalid")
        x.reject[f"{slug}#0@0/down"] = ("rejected", "invalid")
        await tick(e)
        await tick(e, 6)
        await tick(e, 120)
        self.assertEqual(len(quotes(e, slug)), 1)
        self.assertTrue(quotes(e, slug)[0]["permanent"])


class UnknownTest(unittest.IsolatedAsyncioTestCase):
    async def test_unknown_placement_is_found_in_the_open_orders(self):
        e, clock, pub, x = make_engine()
        start = T0 + 1800
        pub.list_window("btc", start)
        await e.account_once()
        await e.discover()
        real_place = x.place

        async def lost_answer(specs):
            await real_place(specs)  # placed, but the answer never came back
            raise TimeoutError("read timeout")
        x.place = lost_answer
        await drain(e)
        q = quotes(e)[0]
        self.assertEqual({o["state"] for o in q["orders"].values()}, {"unknown"})
        await tick(e, 5)
        q = quotes(e)[0]
        self.assertEqual({o["state"] for o in q["orders"].values()}, {"open"})
        self.assertEqual(q["phase"], "closing")  # an unconfirmed side closes the record (no re-signing)
        await tick(e, 1)
        self.assertEqual({o["state"] for o in quotes(e)[0]["orders"].values()}, {"gone"})

    async def test_unknown_at_cancel_time_sweeps_the_market(self):
        e, clock, pub, x = make_engine()
        start = T0 + 1800
        m = pub.list_window("btc", start)

        async def lost(specs):
            raise TimeoutError("read timeout")
        await e.account_once()
        await e.discover()
        x.place = lost
        await drain(e)
        clock.t = start - 60
        await e.cancel_pass()
        self.assertEqual(x.market_cancels, [m["conditionId"]])
        clock.t = start - 30 + 5
        await e.reconcile_pass()
        q = quotes(e)[0]
        self.assertEqual({o["state"] for o in q["orders"].values()}, {"not_found"})
        await e.reconcile_pass()
        self.assertEqual(quotes(e)[0]["phase"], "closed")


class ReviewRegressionTest(unittest.IsolatedAsyncioTestCase):
    async def test_balance_refusal_keeps_no_record_and_holds_every_placement(self):
        e, clock, pub, x = make_engine()
        starts = mk.window_starts(clock.t, e.s)[:3]
        pub.list_range(["btc"], starts)
        first = mk.slug_of("btc", starts[0])
        x.reject[f"{first}#0@0/up"] = ("rejected", "not_enough_balance")
        x.reject[f"{first}#0@0/down"] = ("rejected", "not_enough_balance")
        await e.account_once()
        await e.discover()
        await drain(e)
        self.assertNotIn(first, {q["slug"] for q in quotes(e)})  # nothing rested: no record, no attempt used
        self.assertIn("余额", e.hold())
        placed = len(x.placed)
        for _ in range(10):  # within the minute's hold nothing more is sent, whatever the sweep finds
            await tick(e, 5)
        self.assertEqual(len(x.placed), placed)
        await tick(e, 30)
        self.assertEqual(e.hold(), "")
        self.assertEqual(len(quotes(e, phase="active")), 3)
        self.assertLess(len(e.ledger.rows), 6)

    async def test_maker_fee_market_is_never_quoted(self):
        e, clock, pub, x = make_engine()
        start = T0 + 1800
        pub.list_window("btc", start, feeSchedule={"rate": 0.07, "exponent": 1, "takerOnly": False})
        await tick(e)
        await tick(e, 6)
        self.assertEqual(x.placed, [])
        self.assertIn(mk.slug_of("btc", start), e.listings)

    async def test_market_not_ready_is_rearmed_within_a_fifth_of_a_second(self):
        e, clock, pub, x = make_engine()
        start = T0 + 1800
        slug = mk.slug_of("btc", start)
        pub.list_window("btc", start, created=clock.t - 80)
        x.reject[f"{slug}#0@0/up"] = ("rejected", "market_not_ready")
        x.reject[f"{slug}#0@0/down"] = ("rejected", "market_not_ready")
        await e.account_once()
        await e.discover()
        await drain(e)
        self.assertEqual(quotes(e), [])
        self.assertIn(slug, e.timers)
        self.assertLessEqual(e.timers[slug].when() - asyncio.get_running_loop().time(), 0.25)
        unpark(e)
        await drain(e)
        self.assertEqual(len(quotes(e, phase="active")), 1)

    async def test_fresh_listings_opening_together_respect_the_cap(self):
        e, clock, pub, x = make_engine(settings(PREOPEN_MARKETS="btc,eth,sol,xrp,doge,hype,bnb", PREOPEN_MAX_OPEN_USD="20"))
        start = mk.window_starts(clock.t, e.s)[-1]
        for coin in e.s.coins:
            m = pub.list_window(coin, start, created=clock.t - 60, accepting=None)
            pub.not_accepting.add(m["conditionId"])
        await e.account_once()
        await e.discover()
        await drain(e)
        pub.not_accepting.clear()  # they all open at once
        unpark(e)
        slugs = []
        while not e.queue.empty():
            slugs.append(e.queue.get_nowait()[2])
        for s_ in slugs:
            e.queued.discard(s_)
        await asyncio.gather(*(e.process(s_) for s_ in slugs))  # seven workers side by side
        self.assertLessEqual(e.reserved(), D("20"))
        self.assertEqual(len(quotes(e, phase="active")), 2)

    async def test_an_answer_in_flight_is_not_given_up(self):
        e, clock, pub, x = make_engine()
        start = T0 + 1800
        pub.list_window("btc", start)
        await e.account_once()
        await e.discover()
        gate = asyncio.Event()
        real = x.place

        async def slow(specs):
            await gate.wait()  # the POST takes a long while
            return await real(specs)
        x.place = slow
        slug = mk.slug_of("btc", start)
        e.queue.get_nowait()
        e.queued.discard(slug)
        task = asyncio.create_task(e.process(slug))
        await asyncio.sleep(0)
        for _ in range(20):  # 200 s of reconcile passes while it is in flight
            clock.advance(10)
            await e.account_once()
            await e.reconcile_pass()
        q = quotes(e)[0]
        self.assertEqual({o["state"] for o in q["orders"].values()}, {"placing"})
        gate.set()
        await task
        q = quotes(e)[0]
        self.assertEqual({o["state"] for o in q["orders"].values()}, {"open"})


class HashLookupTest(unittest.IsolatedAsyncioTestCase):
    async def test_unknown_is_found_by_its_hash(self):
        e, clock, pub, x = make_engine()
        start = T0 + 1800
        pub.list_window("btc", start)
        await tick(e)
        q = quotes(e)[0]
        o = q["orders"]["up"]
        oid = o["id"]
        e.by_id.pop(oid)
        o.update(state="unknown", id="", hash=oid)  # as if the answer had been lost (the paper id is the hash)
        e.snapshot = {}
        await e.look_up(q, "up")
        self.assertEqual((o["state"], o["id"]), ("open", oid))
        self.assertEqual(o["note"], "按订单哈希找到")

    async def test_unknown_never_seen_is_given_up_and_the_level_requoted(self):
        e, clock, pub, x = make_engine()
        start = T0 + 1800
        pub.list_window("btc", start)
        await tick(e)
        q = quotes(e)[0]
        o = q["orders"]["up"]
        x.book.pop(o["id"])  # it never rested
        e.by_id.pop(o["id"])
        o.update(state="unknown", id="", hash="0x" + "ee" * 32, sent=clock.t)
        await tick(e, 10)
        await tick(e, 10)
        self.assertEqual(o["state"], "unknown")  # too early to give up
        for _ in range(18):  # given up 120 s after the send, the other side withdrawn and read, then quoted afresh
            await tick(e, 10)
        qs = quotes(e, mk.slug_of("btc", start))
        self.assertEqual(qs[0]["orders"]["up"]["state"], "not_found")
        self.assertFalse(qs[0]["counted"])
        self.assertEqual(qs[-1]["attempt"], 1)
        self.assertEqual(qs[-1]["phase"], "active")


class RestartTest(unittest.IsolatedAsyncioTestCase):
    async def test_takeover_keeps_matching_quotes_and_requotes_changed_ones(self):
        clock = Clock()
        ledger = Ledger(":memory:")
        x = PaperExchange(D("1000"), clock=clock)
        pub = FakePublic(clock)
        e, _, _, _ = make_engine(clock=clock, ledger=ledger, exchange=x, public=pub)
        pub.list_window("btc", T0 + 1800)
        await tick(e)
        self.assertEqual(len(x.placed), 2)
        # same settings: taken over, nothing new placed
        e2, _, _, _ = make_engine(clock=clock, ledger=ledger, exchange=x, public=pub)
        kept, changed = e2.take_over()
        self.assertEqual((kept, changed), (1, 0))
        await tick(e2)
        self.assertEqual(len(x.placed), 2)
        # a new price: withdrawn (free) and quoted again under a new attempt
        e3, _, _, _ = make_engine(settings(PREOPEN_PRICE_CENTS="48"), clock=clock, ledger=ledger, exchange=x, public=pub)
        kept, changed = e3.take_over()
        self.assertEqual((kept, changed), (0, 1))
        await tick(e3)
        await tick(e3, 6)
        await tick(e3, 6)
        qs = quotes(e3)
        self.assertEqual([q["attempt"] for q in qs], [0, 1])
        self.assertFalse(qs[0]["counted"])
        self.assertEqual(D(qs[1]["price"]), D("0.48"))
        self.assertEqual(qs[1]["phase"], "active")

    async def test_mid_send_records_become_unknown(self):
        clock = Clock()
        ledger = Ledger(":memory:")
        e, _, pub, x = make_engine(clock=clock, ledger=ledger)
        pub.list_window("btc", T0 + 1800)
        await tick(e)
        q = quotes(e)[0]
        q["orders"]["up"]["state"] = "placing"
        ledger.save(q)
        e2, _, _, _ = make_engine(clock=clock, ledger=ledger, exchange=x, public=pub)
        e2.take_over()
        self.assertEqual(quotes(e2)[0]["orders"]["up"]["state"], "unknown")


class OperatorTest(unittest.IsolatedAsyncioTestCase):
    async def test_pause_withdraws_and_resume_quotes_again(self):
        e, clock, pub, x = make_engine()
        pub.list_range(["btc"], mk.window_starts(clock.t, e.s)[:3])
        await tick(e)
        self.assertEqual(len(quotes(e, phase="active")), 3)
        n = e.pause()
        self.assertEqual(n, 3)
        await tick(e)
        await tick(e, 6)
        self.assertEqual(len(quotes(e, phase="active")), 0)
        self.assertEqual(e.reserved(), D(0))
        await tick(e, 10)
        self.assertEqual(len(quotes(e, phase="active")), 0)  # paused: nothing new
        e.resume()
        await tick(e, 5)
        self.assertEqual(len(quotes(e, phase="active")), 3)

    async def test_status_and_check_render(self):
        e, clock, pub, x = make_engine()
        pub.list_range(["btc", "eth"], mk.window_starts(clock.t, e.s)[:2])
        await tick(e)
        text = e.status_text()
        self.assertIn("挂着：4 场 8 张单", text)
        self.assertIn("BTC ETH", text)
        self.assertIn("体检", e.check_text())

    async def test_deep_levels_only_near_the_start(self):
        e, clock, pub, x = make_engine(settings(PREOPEN_LEVELS="47x20", PREOPEN_DEEP_AHEAD_MINUTES="20"))
        far, near = T0 + 3000, T0 + 1200
        pub.list_window("btc", far)
        pub.list_window("btc", near)
        await tick(e)
        levels = {(q["start"], q["level"]) for q in quotes(e)}
        self.assertEqual(levels, {(far, 0), (near, 0), (near, 1)})
        clock.t = far - 1200
        await tick(e, 1)
        self.assertIn((far, 1), {(q["start"], q["level"]) for q in quotes(e)})


if __name__ == "__main__":
    unittest.main()
