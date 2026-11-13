"""The engine on a paper book: discovery, placement, the cancel before the start, fills, results, restarts."""
import asyncio
import unittest
from decimal import Decimal as D

from helpers import T0, Clock, FakePublic, drain, make_engine, settings, tick, unpark

from preopen import markets as mk
from preopen.exchange import OrderSpec, PaperExchange
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
        self.assertEqual(first[:2], (0, 0))  # not refused, and priority class 0: fresh listings first
        self.assertEqual(first[2], fresh)
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

    async def test_every_window_is_quoted_on_the_balance_one_window_needs(self):
        e, clock, pub, x = make_engine(balance="15")  # one window takes $9.80: each market is checked on its own
        starts = mk.window_starts(clock.t, e.s)
        pub.list_range(["btc", "eth"], starts)
        await e.account_once()
        await e.discover()
        await drain(e)
        self.assertEqual(len(quotes(e, phase="active")), 2 * len(starts))
        self.assertGreater(e.reserved(), 10 * D("15"))  # all of them together: far more than the balance, as allowed
        await tick(e, 5)
        self.assertEqual(e.budget_note, "")
        self.assertEqual(e.coverage()["counts"]["resting"], 2 * len(starts))

    async def test_budget_cap_holds_the_later_windows(self):
        e, clock, pub, x = make_engine(settings(PREOPEN_MAX_OPEN_USD="20"))  # the bot's orders in all, at most $20
        starts = mk.window_starts(clock.t, e.s)
        pub.list_range(["btc"], starts)
        await e.account_once()
        await e.discover()
        await drain(e)
        placed = quotes(e)
        self.assertEqual(len(placed), 2)  # $9.80 each: two fit under $20
        self.assertEqual([q["start"] for q in placed], starts[:2])  # the soonest
        e.sweep()
        await drain(e)
        self.assertEqual(len(quotes(e)), 2)
        self.assertIn(f"{len(starts) - 2} 场等挂单合计上限", e.budget_note)

    async def test_a_balance_below_one_window_quotes_nothing(self):
        e, clock, pub, x = make_engine(balance="9")
        pub.list_range(["btc", "eth"], mk.window_starts(clock.t, e.s)[:2])
        await e.account_once()
        await e.discover()
        await drain(e)
        self.assertEqual(x.placed, [])
        await tick(e, 5)  # the next sweep says why
        self.assertEqual(x.placed, [])
        self.assertIn("4 场余额不够", e.budget_note)

    async def test_the_cap_takes_the_levels_that_fit_the_nearest_first(self):
        # 49x10 is $9.80 a window, 48x20 another $19.20
        for cap, levels in (("20", [{0}, {0}, set()]), ("30", [{0, 1}, set(), set()])):
            with self.subTest(cap=cap):
                e, clock, pub, x = make_engine(settings(PREOPEN_MAX_OPEN_USD=cap, PREOPEN_LEVELS="48x20"))
                starts = mk.window_starts(clock.t, e.s)[:3]
                pub.list_range(["btc"], starts)
                await tick(e)
                self.assertEqual([{q["level"] for q in quotes(e, mk.slug_of("btc", t))} for t in starts], levels)

    async def test_another_strategys_orders_count_in_their_own_market_only(self):
        e, clock, pub, x = make_engine(balance="15")
        a, b = mk.window_starts(clock.t, e.s)[:2]
        ma = pub.list_window("btc", a)
        pub.list_window("btc", b)
        token = __import__("json").loads(ma["clobTokenIds"])[0]
        await x.place([OrderSpec("website/up", token, D("0.50"), D("20"), a + 30, market=ma["conditionId"])])  # $10 in market a
        await tick(e)
        await tick(e, 5)
        self.assertEqual([q["start"] for q in quotes(e)], [b])  # a has $5 left: not enough; b has all $15
        self.assertIn("1 场余额不够", e.budget_note)

    async def test_fills_that_drain_the_balance_lose_a_markets_orders_until_it_is_back(self):
        e, clock, pub, x = make_engine(balance="15")
        starts = mk.window_starts(clock.t, e.s)[:3]
        pub.list_range(["btc"], starts)
        await tick(e)
        a, b, c = quotes(e)
        x.fill(a["orders"]["up"]["id"], D("10"))  # $4.90 spent: $10.10 left, still above every market's $9.80
        self.assertEqual(len(x.book), 5)
        x.fill(b["orders"]["up"]["id"], D("10"))  # $5.20 left: market c's $9.80 is no longer covered
        self.assertEqual({r.market for r in x.book.values()}, {a["condition_id"], b["condition_id"]})
        await tick(e, 10)
        await tick(e, 6)
        c = quotes(e, c["slug"])[0]
        self.assertIn(c["phase"], ("closed", "settled"))  # cancelled by the exchange: withdrawn, free, not counted
        self.assertFalse(c["counted"])
        self.assertEqual({q["phase"] for q in (quotes(e, a["slug"])[0], quotes(e, b["slug"])[0])}, {"active"})
        self.assertEqual(e.hold(), "")  # no blanket pause: the others keep going
        await tick(e, 10)
        self.assertEqual(len(quotes(e, c["slug"])), 1)  # $5.20 cannot take $9.80: c waits
        self.assertIn("余额不够", e.budget_note)
        x.cash += D("10")  # the winnings redeemed
        await tick(e, 31)
        await tick(e, 5)
        self.assertEqual(quotes(e, c["slug"])[-1]["phase"], "active")  # quoted again, on its own market's room

    async def test_the_websites_cancel_all_pauses_the_bot_and_charges_no_window(self):
        e, clock, pub, x = make_engine()
        starts = mk.window_starts(clock.t, e.s)[:4]
        pub.list_range(["btc"], starts)
        await tick(e)
        self.assertEqual(len(x.book), 8)
        x.cancel_elsewhere()  # 取消所有 on the website: every order of the account, plain CANCELED
        for _ in range(3):
            await tick(e, 6)
        self.assertTrue(e.paused)
        self.assertIn("成批撤掉", e.errors[-1][1])
        self.assertTrue(any("external-cancel" in k for k in e.notified))
        self.assertFalse(any(q.get("counted") for q in quotes(e)))  # no window charged for it
        self.assertEqual(x.book, {})
        e.resume()
        await tick(e, 5)
        await tick(e, 1)
        self.assertEqual({q["start"] for q in quotes(e, phase="active")}, set(starts))

    async def test_one_window_cancelled_elsewhere_counts_and_is_quoted_again(self):
        e, clock, pub, x = make_engine()
        starts = mk.window_starts(clock.t, e.s)[:3]
        pub.list_range(["btc"], starts)
        await tick(e)
        a = quotes(e)[0]
        x.cancel_elsewhere([a["orders"]["up"]["id"]])
        for _ in range(4):
            await tick(e, 6)
        self.assertFalse(e.paused)
        self.assertTrue(quotes(e, a["slug"])[0]["counted"])
        await tick(e, 61)
        await tick(e, 1)
        self.assertEqual(quotes(e, a["slug"])[-1]["phase"], "active")  # quoted again (attempt 1 of the cap)

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

    async def test_failed_cancel_is_backed_by_the_market_sweep_only_on_an_exclusive_key(self):
        e, clock, pub, x = make_engine(settings(PREOPEN_KEY_EXCLUSIVE="on"))
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

    async def test_a_shared_key_is_never_swept_by_market(self):
        e, clock, pub, x = make_engine()  # PREOPEN_KEY_EXCLUSIVE off (the default): another strategy may use the key
        start = T0 + 1800
        m = pub.list_window("btc", start)
        await tick(e)
        other = await x.place([OrderSpec("other/up", quotes(e)[0]["orders"]["up"]["asset"],
                                         D("0.49"), D("10"), start + 30, market=m["conditionId"])])  # same token, price, size
        clock.t = start - 60
        x.fail_cancel = RuntimeError("boom")
        await e.cancel_pass()
        self.assertEqual(x.market_cancels, [])
        await tick(e, 2)  # the cancel by id sprints every 2 s
        self.assertEqual({o["state"] for o in quotes(e)[0]["orders"].values()}, {"gone"})
        self.assertIn(other[0].order_id, x.book)  # the other strategy's identical order is left alone
        for _ in range(5):
            await tick(e, 2)
        self.assertIn(other[0].order_id, x.book)

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

    async def test_a_refusal_about_the_order_counts_against_the_window_up_to_the_cap(self):
        e, clock, pub, x = make_engine()
        start = T0 + 1800
        slug = mk.slug_of("btc", start)
        pub.list_window("btc", start)
        for attempt in range(3):
            for side in ("up", "down"):
                x.reject[f"{slug}#0@{attempt}/{side}"] = ("rejected", "post_only_would_cross")
        await tick(e)
        for _ in range(8):
            await tick(e, 61)
        self.assertEqual([q["attempt"] for q in quotes(e, slug)], [0, 1, 2])  # three tries, then given up on
        self.assertTrue(all(q["counted"] for q in quotes(e, slug)))
        self.assertFalse(any(q.get("permanent") for q in quotes(e, slug)))
        self.assertEqual(e.hold(), "")  # the window's own refusal holds nothing else up
        self.assertIn("失败 3 次", e.coverage()["text"])

    async def test_an_account_refusal_holds_every_window_and_strikes_off_none(self):
        e, clock, pub, x = make_engine()
        starts = mk.window_starts(clock.t, e.s)[:3]
        pub.list_range(["btc"], starts)
        for t in starts:
            for side in ("up", "down"):
                x.reject[f"{mk.slug_of('btc', t)}#0@0/{side}"] = ("rejected", "invalid_expiration")
        await e.account_once()
        await e.discover()
        await drain(e)
        self.assertEqual(len(x.placed), 0)
        self.assertEqual(quotes(e), [])                  # no record, no attempt used
        self.assertIn("交易所拒单（invalid_expiration", e.hold())
        self.assertIn("暂停中", e.coverage()["text"])
        x.reject.clear()
        await tick(e, 31)  # the 30 s hold over: every window placed
        await tick(e, 1)
        self.assertEqual({q["start"] for q in quotes(e, phase="active")}, set(starts))
        self.assertEqual(e.refused_streak, 0)

    async def test_refusals_in_unknown_words_hold_everything_until_one_window_alone_is_refused(self):
        e, clock, pub, x = make_engine()
        a, b = mk.window_starts(clock.t, e.s)[:2]
        ma = pub.list_window("btc", a)
        pub.list_window("btc", b)
        bad = mk.slug_of("btc", a)
        x.refuse_market[ma["conditionId"]] = ("rejected", "unknown")  # every order on that market, every time
        await e.account_once()
        await e.discover()
        await drain(e)
        self.assertIn("交易所拒单（unknown", e.hold())  # unknown words: taken for the account's at first
        self.assertEqual(quotes(e, bad), [])
        await tick(e, 31)
        await tick(e, 1)
        await tick(e, 1)
        # the other window went first and was accepted; the refused one, refused again since: its own
        self.assertEqual(quotes(e, mk.slug_of("btc", b))[0]["phase"], "active")
        (q,) = quotes(e, bad)
        self.assertTrue(q["counted"])
        self.assertIn("只拒这一场", q["reason"])
        self.assertEqual(e.hold(), "")

    async def test_the_same_unknown_words_on_several_windows_charge_none_of_them(self):
        """Like an account limit on open orders: windows beyond it are refused in the same words, while a window gets
        accepted now and then as a slot frees. None of the refused is charged for it."""
        e, clock, pub, x = make_engine()
        a, b, c, d = mk.window_starts(clock.t, e.s)[:4]
        pub.list_window("btc", a)
        mc, md = pub.list_window("btc", c), pub.list_window("btc", d)
        for m in (mc, md):
            x.refuse_market[m["conditionId"]] = ("rejected", "unknown")
        await e.account_once()
        await e.discover()
        await drain(e)
        pub.list_window("btc", b)  # a new window, listed later, accepted between the refusals
        for step in (31, 1, 61, 1, 121, 1, 241, 1):
            await tick(e, step)
        self.assertEqual(quotes(e, mk.slug_of("btc", b))[0]["phase"], "active")
        for t in (c, d):
            self.assertFalse(any(q.get("counted") for q in quotes(e, mk.slug_of("btc", t))))
        self.assertIn("交易所拒单（unknown", e.hold())


class UnknownTest(unittest.IsolatedAsyncioTestCase):
    async def test_unknown_placement_is_found_in_the_open_orders_by_its_hash(self):
        e, clock, pub, x = make_engine()
        start = T0 + 1800
        pub.list_window("btc", start)
        await e.account_once()
        await e.discover()
        real_place = x.place

        async def lost_answer(specs, on_signed=None):
            await real_place(specs, on_signed)  # placed, but the answer never came back
            raise TimeoutError("read timeout")
        x.place = lost_answer
        await drain(e)
        q = quotes(e)[0]
        self.assertEqual({o["state"] for o in q["orders"].values()}, {"unknown"})
        self.assertTrue(all(o["hash"] for o in q["orders"].values()))  # written down before the send
        await tick(e, 5)
        q = quotes(e)[0]
        self.assertEqual({o["state"] for o in q["orders"].values()}, {"open"})
        self.assertEqual({o["id"] for o in q["orders"].values()}, {o["hash"] for o in q["orders"].values()})
        self.assertEqual(q["phase"], "active")  # found in time: it keeps resting (its place in the queue kept)
        self.assertTrue(q.get("placed"))
        await tick(e, 60)
        self.assertEqual(quotes(e)[0]["phase"], "active")
        self.assertEqual(len(quotes(e)), 1)

    async def test_unknown_is_cancelled_by_its_hash_and_settled_from_the_trades_past_its_expiry(self):
        e, clock, pub, x = make_engine()
        start = T0 + 1800
        m = pub.list_window("btc", start)
        hashes = []

        async def lost(specs, on_signed=None):  # signed, then the answer lost: whether it rests is not known
            signed = [(s.ref, "0x" + format(900 + i, "064x"), 900 + i) for i, s in enumerate(specs)]
            hashes.extend(h for _, h, _ in signed)
            on_signed(signed)
            raise TimeoutError("read timeout")
        await e.account_once()
        await e.discover()
        x.place = lost
        await drain(e)
        q = quotes(e)[0]
        self.assertEqual([o["hash"] for o in q["orders"].values()], hashes)
        for _ in range(4):  # never found: withdrawn after the grace, kept unknown (the id is not known to be the hash)
            await tick(e, 10)
        q = quotes(e)[0]
        self.assertEqual(q["phase"], "closing")
        self.assertEqual({o["state"] for o in q["orders"].values()}, {"unknown"})
        self.assertTrue(any(set(c) & set(hashes) for c in x.cancel_calls))  # cancelled by its hash, never by market
        self.assertEqual(x.market_cancels, [])
        clock.t = start - 60
        await tick(e)
        self.assertEqual({o["state"] for o in quotes(e)[0]["orders"].values()}, {"unknown"})  # not given up before its expiry
        clock.t = q["expiration"] + 6
        await tick(e)
        q = quotes(e)[0]
        self.assertEqual({o["state"] for o in q["orders"].values()}, {"not_found"})  # the trades show no fill
        await tick(e)
        self.assertIn(quotes(e)[0]["phase"], ("closed", "settled"))
        self.assertEqual(len(quotes(e)), 1)  # never quoted again meanwhile


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
            slugs.append(e.queue.get_nowait()[-1])
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

        async def slow(specs, on_signed=None):
            await gate.wait()  # the POST takes a long while
            return await real(specs, on_signed)
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
        e.id_is_hash = True  # learned from the first accepted order (live): the id is the hash
        start = T0 + 1800
        pub.list_window("btc", start)
        await tick(e)
        q = quotes(e)[0]
        o = q["orders"]["up"]
        x.book.pop(o["id"])  # it never rested
        e.by_id.pop(o["id"])
        o.update(state="unknown", id="", hash="0x" + "ee" * 32, sent=clock.t)
        e.save(q)
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
