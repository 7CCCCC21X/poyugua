"""The 11-28 audit (on 63a9c20): each finding as it was reported, and what is expected now. A test per finding, named
by its number in the report."""
import json
import sqlite3
import tempfile
import unittest
from decimal import Decimal as D
from pathlib import Path
from unittest import mock

from helpers import T0, Clock, drain, make_engine, settings, tick
from test_export import read
from test_review_basis import rec

import main
from preopen import export, journal
from preopen import markets as mk
from preopen.console import Console
from preopen.exchange import PaperExchange, Row
from preopen.ledger import Ledger

DISK_FULL = sqlite3.OperationalError("database or disk is full")
NOW = T0 + 30 * 86400


def placed(e, x, start=T0 + 1800):
    (q,) = [q for q in e.ledger.rows.values() if q["start"] == start]
    return q


class WriteBeforeSendTest(unittest.IsolatedAsyncioTestCase):
    async def test_1_nothing_is_sent_that_the_ledger_cannot_keep(self):
        e, clock, pub, x = make_engine()
        pub.list_window("btc", T0 + 1800)
        await e.account_once()
        await e.discover()
        with mock.patch.object(e.ledger, "save_many", side_effect=DISK_FULL):
            await drain(e)
            self.assertEqual((len(x.book), len(e.ledger.rows)), (0, 0))   # was: 2 orders sent, in no record at all
            self.assertIn("写不进去", e.hold())
        clock.advance(31)          # room again: a write is tried every 30 s, the hold lifts on its own
        e.hold()
        self.assertEqual(e.write_error, "")
        await tick(e)
        self.assertEqual(len(x.book), 2)

    async def test_1_the_hashes_not_written_nothing_is_sent(self):
        e, clock, pub, x = make_engine()
        pub.list_window("btc", T0 + 1800)
        await e.account_once()
        await e.discover()
        real, calls = e.ledger.save_many, []

        def second_fails(quotes):  # the records' write works; the hashes' (on_signed, before any byte goes) fails
            calls.append(1)
            if len(calls) == 2:
                raise DISK_FULL
            return real(quotes)
        with mock.patch.object(e.ledger, "save_many", side_effect=second_fails):
            await drain(e)
        self.assertEqual(len(x.book), 0)
        self.assertTrue(e.write_error)

    async def test_1_a_record_a_failed_write_did_not_keep_is_still_watched(self):
        e, clock, pub, x = make_engine()
        pub.list_window("btc", T0 + 1800)
        await tick(e)
        q = placed(e, x)
        twin = {**json.loads(json.dumps(q)), "key": q["key"] + "~t"}
        with mock.patch.object(e.ledger, "save_many", side_effect=DISK_FULL):
            e.save(twin)
        self.assertIn(twin["key"], e.ledger.rows)                  # was: indexed, but in no list the loops read
        self.assertIn(twin["key"], {r["key"] for r in e.open_quotes()})
        self.assertIn(twin["key"], e.unsaved)
        e.save()                                                   # the next write that works writes it
        self.assertEqual((e.unsaved, e.write_error), ({}, ""))
        self.assertIsNotNone(e.ledger.conn.execute("SELECT 1 FROM quotes WHERE key = ?", (twin["key"],)).fetchone())


class RebindTest(unittest.IsolatedAsyncioTestCase):
    async def test_2_the_archive_and_its_claims_are_the_other_accounts_after_a_rebind(self):
        path = str(Path(tempfile.mkdtemp()) / "l.sqlite3")
        led = Ledger(path)
        win = rec("btc", T0, 0, "up", {"up": [(100, 10)]}, redeem="manual", redeem_error="没配 Relayer Key")
        loss = rec("btc", T0 + 300, 0, "down", {"up": [(100, 10)]}, redeem="none")
        led.save_many([win, loss])
        led.archive_old(T0 + 3 * 86400)
        self.assertEqual(led.unclaimed_count(), 1)
        led.put("identity", "0xaaa|0xa")
        x = PaperExchange(D("1000"), clock=Clock(T0 + 3 * 86400))
        x.live = True
        x.identity = lambda: "0xbbb|0xb"
        e, *_ = make_engine(ledger=led, exchange=x, clock=x.clock)
        e.blocked = e.check_identity()
        self.assertIn("已改绑", e.rebind())
        self.assertEqual(led.unclaimed_count(), 0)                 # was: 1, claimed by the new account at its restart
        self.assertTrue(all(q.get("foreign") for q in led.archived()))
        self.assertEqual({q["redeem"] for q in led.archived()}, {"none"})
        e2, *_ = make_engine(ledger=led, exchange=x, clock=x.clock)
        self.assertEqual(e2.redeem_again(), 0)
        j = journal.journal(e2.review_reader()(), x.clock(), 60)
        self.assertEqual((j["totals"]["all"]["filled"], j["totals"]["all"]["pnl"]), (0, 0.0))   # was: 2, +0.60
        self.assertIn("已结算盈亏合计 +0.00", main.offline_status(settings(PREOPEN_DB=path, LIVE="on", POLY_PRIVATE_KEY="0x" + "11" * 32)))


class ForeignTest(unittest.TestCase):
    def test_3_another_accounts_records_are_in_no_list(self):
        old = rec("btc", T0, 0, "up", {"up": [(100, 10)]}, foreign=True)
        self.assertEqual(len(journal.detail_table([old])), 1)      # the header alone (was: its row too)
        self.assertEqual(journal.quotes_rows([old])[1], 0)
        self.assertEqual(journal.orders([old], "all", "", NOW)["counts"]["all"], 0)
        e, *_ = make_engine(clock=Clock(NOW))
        e.save(old)
        self.assertEqual(e.report()["fills"], [])
        sheets = read(export.build([old], NOW, 60, None, "全部", refusal_log=[], settings={}, settings_log=[], mode="模拟")[0])
        self.assertEqual((len(sheets["成交明细"]), len(sheets["全部报价"])), (1, 1))


class UnverifiedTest(unittest.IsolatedAsyncioTestCase):
    async def unverified_engine(self):
        """10 shares bought before the cancel; the order then unreadable and the trades too: recorded as 0, unverified."""
        e, clock, pub, x = make_engine()
        m = pub.list_window("btc", T0 + 1800)
        await tick(e)
        q = placed(e, x)
        x.fill(q["orders"]["up"]["id"], D("10"))
        clock.t = T0 + 1800 - e.s.cancel_s
        await tick(e)
        q["orders"]["up"].update(state="gone", filled="0", gone=clock.t, read_at=clock.t)

        async def none(oid):
            return None

        async def fail(cid):
            raise RuntimeError("502 bad gateway")
        x.order, x.fills = none, fail
        for _ in range(7):
            clock.advance(200)
            await e.reconcile_pass()
        self.assertTrue(q["orders"]["up"].get("unverified"))
        self.assertEqual(q["orders"]["up"]["filled"], "0")
        return e, clock, pub, x, m, q

    async def test_4_an_unverified_order_is_verified_by_the_trades_at_settlement(self):
        e, clock, pub, x, m, q = await self.unverified_engine()
        del x.order, x.fills                                       # readable again
        clock.t = T0 + 1800 + 400
        pub.results[m["slug"]] = {**m, "closed": True, "outcomePrices": '["0", "1"]'}   # up lost
        await e.settle_pass()
        o = q["orders"]["up"]
        self.assertEqual((o["filled"], o.get("unverified"), q["phase"]), ("10", False, "settled"))
        self.assertAlmostEqual(q["pnl"], -4.9)                     # was: settled at 0 without a look
        self.assertIn("找到 10 份成交", " ".join(t for _, t in e.errors))

    async def test_4_one_that_stays_unverified_is_said_everywhere(self):
        e, clock, pub, x, m, q = await self.unverified_engine()
        j = journal.journal(e.review_reader()(), clock.t, 60)
        self.assertEqual(j["unverified"]["count"], 1)
        self.assertIn("成交没能核实", journal.review_text([], clock.t, 60, j=j))
        self.assertIn("成交未核实", e.check_text())
        overview = {r[0]: r[1:] for r in read(export.build(e.review_reader(), clock.t, 60, None, "全部", refusal_log=[], settings={},
                                                           settings_log=[], mode="模拟")[0])["总览"] if r}
        self.assertIn("成交未核实", overview)
        self.assertIn("未核实", journal.quotes_rows([q])[0][1][-1])

    async def test_9_shares_the_chain_failed_are_taken_out_at_settlement(self):
        e, clock, pub, x = make_engine()
        m = pub.list_window("btc", T0 + 1800)
        await tick(e)
        q = placed(e, x)
        oid = q["orders"]["up"]["id"]
        x.fill(oid, D("10"))
        clock.t = T0 + 1800 - e.s.cancel_s
        await tick(e)
        await tick(e, 6)
        self.assertEqual(q["orders"]["up"]["filled"], "10")
        x.fill_log.pop(oid)                                        # the trade FAILED on chain: no shares came
        x.failed_log[oid] = D("10")
        clock.t = T0 + 1800 + 400
        pub.results[m["slug"]] = {**m, "closed": True, "outcomePrices": '["1", "0"]'}
        await e.settle_pass()
        self.assertEqual((q["orders"]["up"]["filled"], q["pnl"]), ("0", 0.0))   # was: +5.10 for shares never received
        self.assertIn("链上失败", " ".join(t for _, t in e.errors))


class GhostTest(unittest.IsolatedAsyncioTestCase):
    async def test_5_a_ghost_eaten_whole_is_read_and_restored(self):
        e, clock, pub, x = make_engine()
        m = pub.list_window("btc", T0 + 1800)
        for side in mk.SIDES:
            x.reject[f"{m['slug']}#0@0/{side}"] = ("not_sent", "rate_limited")
        await e.account_once()
        await e.discover()
        await drain(e)
        h = next(h for h, g in e.ghosts.items() if g["side"] == "up")
        x.gone[h] = Row(h, "1", m["conditionId"], "BUY", D("0.49"), D("10"), D("10"), "MATCHED")   # rested, eaten whole
        clock.advance(6)
        await e.ghost_pass(clock.t)                                # its cancel: "not cancelled" (was: dropped unread)
        clock.advance(6)
        await e.ghost_pass(clock.t)
        self.assertEqual(e.ghosts, {})
        (back,) = [q for q in e.ledger.rows.values() if q["orders"]["up"].get("id") == h]
        self.assertEqual((back["orders"]["up"]["filled"], back["phase"]), ("10", "closed"))
        self.assertIn("已补进账本", e.errors[-1][1])
        clock.t = T0 + 1800 + 400
        pub.results[m["slug"]] = {**m, "closed": True, "outcomePrices": '["1", "0"]'}
        await e.settle_pass()
        self.assertEqual(back["phase"], "settled")
        self.assertAlmostEqual(back["pnl"], 10 - 10 * float(back["price"]))

    async def test_5_ghosts_unanswered_at_the_start_are_said(self):
        e, clock, pub, x = make_engine()
        e.ghosts["0xabc"] = {"start": int(clock.t) + 10, "slug": mk.slug_of("btc", int(clock.t) + 10), "side": "up"}
        clock.advance(11)
        await e.ghost_pass(clock.t)
        self.assertIn("到开场还没确认", e.errors[-1][1])


class WholeWindowTest(unittest.TestCase):
    def test_6_a_window_counts_once_all_its_fills_are_settled(self):
        sat = int(mk.dt.datetime(2026, 11, 14, 12, tzinfo=mk.BEIJING).timestamp())
        first = rec("btc", sat, 0, "up", {"up": [(100, 10)]}, price="0.49")
        second = rec("btc", sat, 1, "up", {"down": [(90, 10)]}, price="0.47", phase="closed")
        j = journal.journal([first, second], sat + 500, 60)
        r, we = j["totals"]["range"], j["weekend"]["split"][1]
        self.assertEqual((r["pnl"], r["open"], r["cost"]), (0.0, 1, 0.0))   # was: +5.10 while the window was open
        self.assertEqual(sum(s["pnl"] or 0 for s in j["by_side"][:3]), 0)
        self.assertEqual((we["pnl"], we["ev"], we["diff"], we["settled_filled"]), (0.0, 0.0, 0.0, 0))
        second.update(phase="settled", result="up", pnl=-4.7)
        j = journal.journal([first, second], sat + 500, 60)
        r, we = j["totals"]["range"], j["weekend"]["split"][1]
        self.assertAlmostEqual(r["pnl"], 0.4)
        self.assertAlmostEqual(sum(s["pnl"] or 0 for s in j["by_side"][:3]), 0.4)
        self.assertAlmostEqual(j["model"]["total"]["pnl"], 0.4)
        self.assertAlmostEqual(we["pnl"] - we["ev"], we["diff"])
        self.assertAlmostEqual(sum(p["pnl"] for p in j["by_price"]), 0.4)


class ClaimsAndCardsTest(unittest.IsolatedAsyncioTestCase):
    async def test_7_archived_failed_claims_are_said_with_the_key_configured(self):
        x = PaperExchange(D("1000"), clock=Clock(NOW))
        x.live = True
        e, *_ = make_engine(exchange=x, clock=x.clock)
        e.save(rec("btc", NOW - 3 * 86400, 0, "up", {"up": [(100, 10)]}, redeem="failed"))
        e.ledger.archive_old(NOW)
        line = next(ln for ln in e.check_text().splitlines() if "领取：" in ln)
        self.assertTrue(line.startswith("⚠️"), line)                 # was: ✅ 领取：会自动领取
        self.assertIn("更早的 1 场", line)

    async def test_8_the_cards_follow_a_window_of_yesterday_settled_after_midnight(self):
        midnight = journal.day_start(NOW)
        e, clock, *_ = make_engine(clock=Clock(midnight + 30))
        q = rec("btc", midnight - 300, 0, None, {"up": [(100, 10)]}, phase="closed")
        e.save(q)
        c = Console(e, {"PREOPEN_MARKETS": "btc"}, Path(tempfile.mkdtemp()) / "c.json", {})
        await c.review({"range": "all"})
        q.update(phase="settled", result="up", pnl=5.3)
        e.save(q)
        c._reviews.clear()                                         # the review's own cache gone (a minute on)
        after = await c.review({"range": "all"})
        self.assertEqual(after["totals"]["all"]["pnl"], after["totals"]["range"]["pnl"])   # was: 0.0 against 5.3
        self.assertEqual(after["totals"]["all"]["pnl"], 5.3)


class FillTimesTest(unittest.TestCase):
    def test_9_fill_times_add_up_to_what_was_bought(self):
        q = rec("btc", T0, 0, "up", {"up": [(200, 10)]}, price="0.48")
        q["orders"]["up"]["fills_at"] = [[T0 - 200, "10"], [T0 - 100, "5"]]   # 15 listed, 10 bought
        j = journal.journal([q], T0 + 500, 60)
        self.assertEqual(j["cancel"]["actual"]["shares"], 10.0)    # was: 15
        self.assertEqual(sum(b["shares"] for b in j["by_lead"]), 10.0)
        self.assertEqual(j["totals"]["range"]["shares"], 10.0)

    def test_9_without_the_trades_each_fill_keeps_the_time_it_was_seen(self):
        q = rec("btc", T0, 0, "down", {"up": [(600, 30)]})
        o = q["orders"]["up"]
        o.pop("fills_at")
        o.update(first_fill=T0 - 600, seen=[[T0 - 600, "1"], [T0 - 15, "29"]])
        events, exact = journal.fill_events(q)
        self.assertEqual((events["up"], exact), ([(600, D(1)), (15, D(29))], journal.SEEN))   # was: all 30 at 600 s
        r300 = next(r for r in journal.cancel_review([q], T0 + 500, 300)["rows"] if r["cancel_s"] == 300)
        self.assertEqual(r300["shares"], 1.0)


class ReplayTest(unittest.TestCase):
    def records(self):
        base = T0
        out = []
        for i in range(9):   # cancelled 60 s ahead, confirmed at 58.5: one fill at 59 s came while it was on the way
            t = base + 300 * i
            out.append(rec("btc", t, 0, "down", {"up": [(59, 10)]} if i == 0 else {}, cancel_lead=58.5,
                           reason="到点撤单", closing_at=t - 60))
        t = base + 300 * 9   # cancelled 300 s ahead (an older setting): left out of the comparison, whole
        out.append(rec("eth", t, 0, "up", {"up": [(400, 10)]}, cancel_lead=300.0, reason="到点撤单", closing_at=t - 300))
        return out

    def test_10_the_replay_tables_are_the_compared_windows_and_the_cancel_takes_its_time(self):
        j = journal.journal(self.records(), T0 + 4000, 60)
        c = j["cancel"]
        self.assertEqual(c["excluded"], 1)
        self.assertAlmostEqual(sum(t["pnl"] for t in c["by_fill_time"]), c["actual"]["pnl"])   # was: +0.20 against -4.70
        self.assertAlmostEqual(sum(x["pnl"] for x in c["by_coin"]), c["actual"]["pnl"])
        self.assertAlmostEqual(c["baseline"]["pnl"], c["actual"]["pnl"])   # was: 0.00, the 59 s fill replayed away
        row90 = next(r for r in c["rows"] if r["cancel_s"] == 90)
        self.assertAlmostEqual(row90["delta"], 4.7)
        self.assertIn("撤单确认延迟", " ".join(c["confidence"]["notes"]))

    def test_10_telegrams_coins_are_the_reviews(self):
        recs = self.records() + [rec("sol", T0 + 3000, 0, None, {"down": [(100, 10)]}, phase="closed")]   # waiting
        j = journal.journal(recs, T0 + 4000, 60)
        text = journal.review_text([], T0 + 4000, 60, j=j)
        line = next(ln for ln in text.splitlines() if ln.startswith("【按币种】"))
        self.assertIn("ETH +5.30", line)                          # the review's coins (the page's and the workbook's)
        self.assertIn("BTC -4.70", line)
        self.assertIn("SOL +0.00（1 场成交）", line)               # was: left out (the replay's, which waits for it)
        self.assertEqual({x["coin"] for x in j["by_coin"] if x["filled"]}, {"BTC", "ETH", "SOL"})


class SplitTest(unittest.IsolatedAsyncioTestCase):
    async def test_11_a_split_market_has_no_luck_and_an_even_window_is_said(self):
        q = rec("btc", T0, 0, mk.SPLIT, {"up": [(200, 10)]}, price="0.50")
        q["pnl"] = 0.0
        j = journal.journal([q], T0 + 600, 60)
        self.assertEqual((j["model"]["total"]["sd"], j["model"]["total"]["z"]), (0.0, None))   # was: sd 5.0, z 0.0
        self.assertEqual(j["totals"]["range"]["evens"], 1)
        self.assertIn("平 1", journal.review_text([q], T0 + 600, 60))
        head = journal.daily_table(j)[0]
        self.assertEqual(head[head.index("输") + 1], "平")

    async def test_11_a_result_long_in_coming_is_said(self):
        e, clock, pub, x = make_engine()
        m = pub.list_window("btc", T0 + 1800)
        await tick(e)
        q = placed(e, x)
        x.fill(q["orders"]["up"]["id"], D("10"))
        clock.t = T0 + 1800 - e.s.cancel_s
        await tick(e)
        await tick(e, 6)
        pub.results[m["slug"]] = {**m, "closed": True, "outcomePrices": '["0.3", "0.7"]'}   # not a resolution
        clock.t = T0 + 1800 + 300 + 6 * 3600 + 1
        await e.settle_pass()
        self.assertEqual(q["phase"], "closed")                     # not settled on prices that are no result
        self.assertIn("还没有结算结果", e.errors[-1][1])
        self.assertIn("还没有结算结果", e.check_text())


class SecondReportTest(unittest.IsolatedAsyncioTestCase):
    """The second report on 63a9c20 (the same day): two cases the first fixes did not close."""

    async def test_fills_read_from_the_trades_alone_are_read_again_at_settlement(self):
        """The order's record gone and the trades not listing its fill yet: ended at 0; the shares listed later were
        never read (bought 100 at 47¢, won: +53 recorded as 0)."""
        e, clock, pub, x = make_engine(settings(PREOPEN_SHARES="100"))
        m = pub.list_window("btc", T0 + 1800)
        await tick(e)
        q = placed(e, x)
        oid = q["orders"]["up"]["id"]
        x.fill(oid, D("100"))                                      # eaten whole between two snapshots
        listed = x.fill_log.pop(oid)                               # the account's trades not listing it yet

        async def none(_):
            return None

        async def empty(_):
            return {}
        x.order, x.fills = none, empty
        await tick(e, 6)
        await tick(e, 6)
        clock.t = T0 + 1800 - e.s.cancel_s
        await tick(e)
        await tick(e, 6)
        o = q["orders"]["up"]
        self.assertEqual((o["state"], o["filled"], o.get("by_trades")), ("final", "0", True))
        del x.order, x.fills
        x.fill_log[oid] = listed                                   # listed now
        clock.t = T0 + 1800 + 400
        pub.results[m["slug"]] = {**m, "closed": True, "outcomePrices": '["1", "0"]'}
        await e.settle_pass()
        self.assertEqual((o["filled"], q["phase"]), ("100", "settled"))
        self.assertAlmostEqual(q["pnl"], 100 * (1 - float(q["price"])))   # was: 0.0, settled without a look

    def test_windows_whose_fill_times_are_unknown_do_not_turn_the_recommendation(self):
        """Fills whose times are not known, put at their first sighting, turned the recommended cancel time around
        (the report: 30 s best on the full times, 900 s recommended once some were masked)."""
        recs = []
        for i in range(24):   # timed by the trades: an early fill that won, a late one that lost
            recs.append(rec("btc", T0 + 300 * i, 0, "up", {"up": [(100, 10)], "down": [(20, 10)]}, cancel_lead=10.0))
        for i in range(30):   # a late fill that lost, its time unknown: only first seen, long before
            t = T0 + 300 * (24 + i)
            q = rec("eth", t, 0, "up", {"down": [(20, 10)]}, cancel_lead=10.0)
            q["orders"]["down"].pop("fills_at")
            q["orders"]["down"]["first_fill"] = t - 1000
            recs.append(q)
        c = journal.cancel_review(recs, T0 + 300 * 60, current=10)
        self.assertEqual((c["windows"], c["excluded"]), (24, 30))
        self.assertEqual(c["recommend"], 30)                       # was: 1800
        self.assertIn("30 场的成交时刻读不到", " ".join(c["confidence"]["notes"]))

    def test_mostly_guessed_times_recommend_nothing(self):
        recs = []
        for i in range(24):
            q = rec("btc", T0 + 300 * i, 0, "up", {"up": [(100, 10)], "down": [(20, 10)]}, cancel_lead=10.0)
            for o in q["orders"].values():   # each fill seen by a snapshot (not the trades): approximate times
                o["seen"] = o.pop("fills_at", [])
            recs.append(q)
        c = journal.cancel_review(recs, T0 + 300 * 30, current=10)
        self.assertEqual((c["recommend"], c["confidence"]["level"]), (None, "低"))
        self.assertIn("先不改撤单时间", c["advice"])


if __name__ == "__main__":
    unittest.main()
