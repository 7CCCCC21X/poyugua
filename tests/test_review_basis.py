"""11-21 review of the review: the cancel-time replay takes windows whole and recommends once (web, Telegram, CSV and
Excel alike); rates count started windows; markets sort over all of them; days are never cut; the review reads the
archive by time in a thread instead of holding it; settings changes are compared before and after; a price opens up
into its fills; files go to Telegram with their own type. Synthetic ledgers: 4 windows of several levels, 20 windows
that tie, 501 markets, 501 days."""
import asyncio
import gc
import json
import tempfile
import unittest
from decimal import Decimal as D
from pathlib import Path

import httpx
from helpers import T0, Clock, make_engine, settings

import main
from preopen import journal, telegram
from preopen import markets as mk
from preopen.console import Console
from preopen.ledger import Ledger

ENV = {"PREOPEN_MARKETS": "btc,eth", "PREOPEN_AHEAD_MINUTES": "60"}


def rec(coin, start, level, result, fills, price="0.47", shares=10, cancel_lead=60.0, phase="settled", **extra):
    """A record of one level: fills = {side: [(seconds before the start, shares)]} at the trades' own times."""
    orders = {side: {"state": "final", "id": f"{coin}{start}{level}{side}", "filled": str(sum(sh for _, sh in fills.get(side, []))),
                     "fills_at": [[start - lead, str(sh)] for lead, sh in fills.get(side, [])]} for side in mk.SIDES}
    up, down = (sum(sh for _, sh in fills.get(sd, [])) for sd in mk.SIDES)
    pnl = float((up if result == "up" else down) - D(price) * (up + down)) if phase == "settled" else None
    q = {"key": f"{coin}-{start}#{level}@0", "slug": f"{coin}-updown-5m-{start}", "coin": coin, "start": start, "level": level,
         "attempt": 0, "price": price, "shares": str(shares), "expiration": start + 50, "phase": phase,
         "result": result if phase == "settled" else None, "placed": start - 3000, "listed": start - 3080,
         "cancel_lead": cancel_lead, "orders": orders, "pnl": pnl}
    q.update(extra)
    return q


class ReplayTest(unittest.TestCase):
    def test_four_windows_of_several_levels_are_taken_or_left_whole(self):
        """A window whose deep level was cancelled 63 s ahead and its first level 59 s ahead: replaying 60 s keeps or
        drops the window whole (the old replay kept the first level and dropped the other)."""
        now = T0 + 7200
        records = []
        for i, coin in enumerate(("btc", "eth", "sol", "xrp", "doge")):   # four windows of two levels, and one more
            start = T0 + 300 * i
            records.append(rec(coin, start, 0, "up", {"up": [(100, 10)]}, cancel_lead=59.0))
            records.append(rec(coin, start, 1, "up", {"down": [(100, 20)]}, price="0.40", cancel_lead=63.0 if coin == "doge" else 59.0))
        c = journal.cancel_review(records, now, current=60)
        self.assertEqual(c["base"], 60)                                # four of five rested until 60 s ahead
        self.assertEqual((c["windows"], c["excluded"]), (4, 1))       # doge's deep level left at 63 s: the window out, whole
        self.assertEqual((c["actual"]["filled"], c["actual"]["both"]), (4, 4))   # each window kept: both levels, both sides
        self.assertEqual({r["windows"] for r in c["rows"]}, {4})
        self.assertIn("1 场当时撤得更早，整场没算", c["confidence"]["notes"])

    def test_twenty_windows_that_tie_recommend_nothing(self):
        """Every fill came 300 s ahead: cancelling anywhere from 10 s to 5 minutes ahead gives the same; the current 60 s
        ties with the best and stays (the Telegram button used to offer the earliest of the tie all the same)."""
        now = T0 + 300 * 30
        records = [rec(c, T0 + 300 * i, 0, "up", {"up": [(400, 10)]}, cancel_lead=10.0) for i in range(10) for c in ("btc", "eth")]
        c = journal.cancel_review(records, now, current=60)
        self.assertEqual(c["filled"], 20)
        self.assertEqual((c["best"], c["recommend"]), (60, None))
        self.assertIn("现在的撤单时间（开场前 60 秒）已经是这些记录里最好的", c["advice"])
        self.assertEqual(c["baseline"]["name"], "现在的开场前 60 秒")
        self.assertTrue(all(r["delta"] == 0 for r in c["rows"] if r["cancel_s"] <= 300))
        # the Telegram review offers no button either: the same `recommend`
        e, clock, pub, x = make_engine(settings(**ENV, PREOPEN_CANCEL_SECONDS="60"), clock=Clock(now))
        for q in records:
            e.save(q)
        reply = main.review(e, [])
        self.assertNotIn("set_cancel", json.dumps(reply.markup))

    def test_a_better_time_is_recommended_on_both_sides_the_same(self):
        now = T0 + 300 * 30
        late = [rec(c, T0 + 300 * i, 0, "down", {"up": [(20, 10)]}, cancel_lead=10.0) for i in range(10) for c in ("btc", "eth")]
        early = [rec(c, T0 + 300 * i, 1, "up", {"up": [(400, 10)]}, price="0.40", cancel_lead=10.0) for i in range(10) for c in ("btc", "eth")]
        c = journal.cancel_review(late + early, now, current=10)
        self.assertEqual(c["recommend"], 30)                            # the late fills (20 s ahead) lost: cancel before them
        self.assertEqual(c["baseline"]["cancel_s"], 10)
        self.assertGreater([r for r in c["rows"] if r["cancel_s"] == 30][0]["delta"], 0)
        e, clock, pub, x = make_engine(settings(**ENV, PREOPEN_CANCEL_SECONDS="10", PREOPEN_EXPIRY_SECONDS="5",
                                                PREOPEN_ENTRY_SECONDS="300"), clock=Clock(now))
        for q in late + early:
            e.save(q)
        reply = main.review(e, [])
        self.assertEqual(reply.markup["inline_keyboard"][0][0]["callback_data"], "set_cancel_30")
        self.assertIn("可信度", reply.text)


class CountsTest(unittest.TestCase):
    def test_windows_not_started_stay_out_of_the_fill_rate(self):
        now = T0 + 3600
        started = [rec("btc", T0 + 300 * i, 0, "up", {"up": [(100, 10)]} if i < 2 else {}) for i in range(4)]
        ahead = [rec("eth", now + 3600 + 300 * i, 0, "up", {}, phase="active") for i in range(36)]   # pre-listed, resting
        t = journal.journal(started + ahead, now)["totals"]["all"]
        self.assertEqual((t["quoted"], t["upcoming"], t["filled"], t["fill_rate"]), (4, 36, 2, 50.0))   # not 2/40 = 5%

    def test_another_accounts_records_are_left_out(self):
        now = T0 + 3600
        mine = rec("btc", T0, 0, "up", {"up": [(100, 10)]})
        old = rec("eth", T0, 0, "down", {"up": [(100, 10)]}, foreign=True)
        j = journal.journal([mine, old], now)
        self.assertEqual((j["totals"]["all"]["filled"], j["totals"]["all"]["pnl"]), (1, 5.3))

    def test_501_markets_sort_over_all_of_them(self):
        now = T0 + 300 * 600
        records = [rec("btc", T0 + 300 * i, 0, "up", {"up": [(100, 10)]}) for i in range(501)]
        records[0] = rec("btc", T0, 0, "down", {"up": [(100, 10)]})   # the oldest is the only loss
        j = journal.journal(records, now, market_limit=500, market_sort="loss")
        self.assertEqual((j["market_count"], len(j["by_market"])), (501, 500))
        self.assertEqual((j["by_market"][0]["start"], j["by_market"][0]["pnl"]), (T0, -4.7))
        latest = journal.journal(records, now, market_limit=500)
        self.assertNotIn(T0, [m["start"] for m in latest["by_market"]])   # by time: the latest 500

    def test_501_days_are_all_there(self):
        start0 = int(journal.day_start(T0)) + 36000
        records = [rec("btc", start0 + 86400 * i, 0, "up", {"up": [(100, 10)]}) for i in range(501)]
        now = start0 + 86400 * 501
        j = journal.journal(records, now)
        self.assertEqual(len(j["by_day"]), 501)                         # no longer cut at 90
        self.assertEqual(len(journal.daily_csv(j).strip().splitlines()), 502)
        self.assertAlmostEqual(sum(d["pnl"] for d in j["by_day"]), j["totals"]["all"]["pnl"])


class StreamTest(unittest.IsolatedAsyncioTestCase):
    def ledger(self) -> Ledger:
        led = Ledger(str(Path(tempfile.mkdtemp()) / "l.sqlite3"))
        archived = [rec(c, T0 + 300 * i, lv, "up", {"up": [(100, 10)]} if lv == 0 else {}) for i in range(30) for c in ("btc", "eth") for lv in (0, 1)]
        with led.lock:
            led.conn.executemany("INSERT INTO archive(key, start, data) VALUES (?, ?, ?)", [(q["key"], q["start"], json.dumps(q)) for q in archived])
            led.conn.commit()
        return led

    def test_the_archive_is_read_by_time_a_batch_at_a_time(self):
        led = self.ledger()
        got = list(led.archived_between(T0 + 300 * 5, T0 + 300 * 10, batch=3))
        self.assertEqual(len(got), 5 * 4)
        self.assertEqual([q["start"] for q in got], sorted(q["start"] for q in got))
        newest = list(led.archived_between(float("-inf"), float("inf"), newest_first=True, batch=7))
        self.assertEqual(len(newest), 120)
        self.assertEqual(newest[0]["start"], T0 + 300 * 29)
        self.assertEqual(len({q["key"] for q in newest}), 120)

    async def test_the_reader_merges_the_working_set_once(self):
        led = self.ledger()
        e, clock, pub, x = make_engine(settings(**ENV), clock=Clock(T0 + 300 * 40), ledger=led)
        moving = rec("btc", T0 + 300 * 29, 0, "down", {"up": [(100, 10)]})   # archived and still working (the hour's move)
        e.save(moving)
        e.save(rec("btc", T0 + 300 * 35, 0, "up", {}))
        read = e.review_reader()
        allof = list(read())
        self.assertEqual(len(allof), 121)                           # 120 archived + 1 working; the moving one once
        self.assertEqual(sum(1 for q in allof if q["key"] == moving["key"]), 1)
        self.assertEqual([q["start"] for q in allof], sorted(q["start"] for q in allof))
        self.assertEqual(next(q for q in allof if q["key"] == moving["key"])["result"], "down")   # the working one wins
        self.assertEqual(list(read(newest_first=True))[0]["start"], T0 + 300 * 35)
        moving["result"] = "up"                                     # the loop changes a record after the copy
        self.assertEqual(next(q for q in read() if q["key"] == moving["key"])["result"], "down")

    def test_the_collector_is_quiet_while_reviews_run_and_back_after(self):
        self.assertTrue(gc.isenabled())
        with journal.quiet_gc():
            with journal.quiet_gc():
                self.assertFalse(gc.isenabled())
            self.assertFalse(gc.isenabled())
        self.assertTrue(gc.isenabled())


class ConsoleTest(unittest.IsolatedAsyncioTestCase):
    async def console(self, **env):
        now = T0 + 300 * 40
        e, clock, pub, x = make_engine(settings(**{**ENV, **env}), clock=Clock(now))
        for i in range(30):
            e.save(rec("btc", T0 + 300 * i, 0, "up" if i % 3 else "down", {"up": [(100, 10)]}))
            e.save(rec("btc", T0 + 300 * i, 1, "up", {"down": [(100, 20)]} if i % 5 == 0 else {}, price="0.40"))
        return Console(e, ENV, Path(tempfile.mkdtemp()) / "c.json", {}), e

    async def test_the_review_is_cached_and_a_price_opens_up(self):
        c, e = await self.console()
        first = await c.review({"range": "all"})
        self.assertIs(await c.review({"range": "all"}), first)        # a minute's cache: the page refreshes every minute
        self.assertEqual([p["price"] for p in first["by_price"]], [47.0, 40.0])
        d = await c.price_detail({"range": "all", "price": "40"})
        self.assertEqual(d["count"], 6)                                 # 40¢ filled in windows 0, 5, 10, ...
        self.assertEqual(d["rows"][0]["pnl"], -8.0)                     # down bought, up won: the biggest loss first
        self.assertIn("error", await c.price_detail({"range": "all", "price": "x"}))

    async def test_the_page_applies_the_recommendation_with_the_tied_times(self):
        c, e = await self.console(PREOPEN_CANCEL_SECONDS="60", PREOPEN_ENTRY_SECONDS="300", PREOPEN_EXPIRY_SECONDS="30")
        cached = await c.review({"range": "all"})
        text = await c.action({"action": "set_cancel", "seconds": 260})
        self.assertIn("已改成开场前 260 秒撤单", text)
        self.assertEqual((e.s.cancel_s, e.s.entry_s, e.s.expiry_s), (260, 320, 170))   # one checked step (cancel alone: refused)
        self.assertEqual(e.settings_log[-1]["via"], "网页复盘：开场前 260 秒撤单")
        self.assertIsNot(await c.review({"range": "all"}), cached)     # the cache does not outlive the change
        with self.assertRaises(ValueError):
            await c.action({"action": "set_cancel", "seconds": "x"})

    async def test_settings_changes_are_compared_before_and_after(self):
        c, e = await self.console()
        e.settings_log.append({"at": T0 + 300 * 15, "via": "网页", "changes": [["PREOPEN_CANCEL_SECONDS", "30", "60"]]})
        d = await c.review({"range": "all"})
        (ch,) = d["changes"]
        self.assertEqual((ch["before"]["quoted"], ch["after"]["quoted"]), (15, 15))   # 15 windows each side of it
        self.assertEqual(ch["before_hours"], ch["after_hours"])


class MimeTest(unittest.IsolatedAsyncioTestCase):
    async def test_each_file_goes_with_its_own_type(self):
        seen = []

        async def handler(request):
            seen.append(request.content)
            return httpx.Response(200, json={"ok": True, "result": {}})
        http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        tg = telegram.Telegram("123:abc", "1", http=http)
        await tg._send_document("复盘_近7天.xlsx", b"PK\x03\x04", "")
        await tg._send_document("每日汇总.csv", b"\xef\xbb\xbfa,b", "")
        self.assertIn(b"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", seen[0])
        self.assertIn(b"Content-Type: text/csv", seen[1])
        await http.aclose()


if __name__ == "__main__":
    unittest.main()
