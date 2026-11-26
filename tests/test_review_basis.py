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


class ModelTest(unittest.TestCase):
    """模型 EV 对实际盈亏: the plan (before a window starts either side wins half the time) against what happened, and
    how much of the gap between them luck explains."""

    @staticmethod
    def model(records):
        return journal.journal(records, T0 + 40 * 86400, 60)["model"]

    def test_one_side_alone_is_all_luck_and_a_pair_is_none(self):
        t = self.model([rec("btc", T0, 0, "down", {"up": [(100, 30)]}, shares=30)])["total"]   # 30 up at 47¢, down won
        self.assertEqual((t["windows"], t["shares"], t["cost"], t["ev"], t["pnl"], t["diff"], t["sd"], t["z"]),
                         (1, 30.0, 14.1, 0.9, -14.1, -15.0, 15.0, -1.0))   # worth 50¢ a share; half the stake is one deviation
        m = self.model([rec("btc", T0, 0, "down", {"up": [(100, 30)], "down": [(90, 30)]}, shares=30)])
        t = m["total"]
        self.assertEqual((t["ev"], t["pnl"], t["sd"], t["z"]), (1.8, 1.8, 0.0, None))   # $1 back whatever wins
        self.assertIn("没有运气成分", m["verdict"])
        levels = self.model([rec("btc", T0, 0, "up", {"up": [(100, 10)]}), rec("btc", T0, 1, "up", {"down": [(80, 20)]}, price="0.40")])
        t = levels["total"]   # one window, its levels together: 10 up and 20 down are 10 apart
        self.assertEqual((t["windows"], t["cost"], t["ev"], t["pnl"], t["sd"], t["z"]), (1, 12.7, 2.3, -2.7, 5.0, -1.0))

    def test_the_coins_of_one_start_share_their_luck(self):
        """Two coins of one window agree four times in five (11-20 to 11-25: 81% of 36,288 pairs): 30 up on each is
        nearer one bet of 60 than two of 30."""
        up = {"up": [(100, 30)]}
        same = self.model([rec("btc", T0, 0, "down", up, shares=30), rec("eth", T0, 0, "down", up, shares=30)])
        self.assertEqual(same["total"]["sd"], 27.0)                              # sqrt(0.25 x (0.38 x 1800 + 0.62 x 3600))
        self.assertEqual([(r["name"], r["sd"], r["z"]) for r in same["by_coin"]], [("BTC", 15.0, -1.0), ("ETH", 15.0, -1.0)])
        self.assertEqual(same["total"]["z"], round(-30 / 27, 2))                 # together -1.11, not two times -1
        opposite = self.model([rec("btc", T0, 0, "down", up, shares=30), rec("eth", T0, 0, "down", {"down": [(100, 30)]}, shares=30)])
        self.assertEqual(opposite["total"]["sd"], round((0.25 * 0.38 * 1800) ** 0.5, 4))   # mostly a hedge: 13.08
        apart = self.model([rec("btc", T0, 0, "down", up, shares=30), rec("eth", T0 + 300, 0, "down", up, shares=30)])
        self.assertEqual(apart["total"]["sd"], round(450 ** 0.5, 4))             # another window is another toss: 21.21
        self.assertEqual(same["rho"], journal.COIN_RHO)

    def test_the_curve_steps_by_window_hour_or_day(self):
        won = {"up": [(100, 10)]}
        m = self.model([rec("btc", T0 + 300 * i, 0, "up", won) for i in range(3)])     # ten minutes: a point a window
        self.assertEqual((m["step"], [(p["label"], p["pnl"], p["ev"], p["sd"]) for p in m["points"]]),
                         (300, [(mk.hm(T0), 5.3, 0.3, 5.0), (mk.hm(T0 + 300), 10.6, 0.6, 7.07), (mk.hm(T0 + 600), 15.9, 0.9, 8.66)]))
        m = self.model([rec("btc", T0 + 1500 * i, 0, "up", won) for i in range(60)])   # a day: a point an hour
        self.assertEqual((m["step"], len(m["points"])), (3600, 26))   # 10:30 to 11:05 the next day: 26 hours touched
        self.assertEqual((m["points"][0]["label"], m["points"][0]["pnl"]), (mk.hm(T0 - 1800), 10.6))   # 10:30 and 10:55 in 10:00's
        self.assertEqual(m["points"][-1]["pnl"], round(5.3 * 60, 2))
        m = self.model([rec("btc", T0 + 43200 * i, 0, "up", won) for i in range(20)])  # ten days: a point a (Beijing) day
        self.assertEqual((m["step"], [p["label"] for p in m["points"]][:3], len(m["points"])), (86400, ["11-13", "11-14", "11-15"], 10))
        self.assertEqual(m["points"][-1]["pnl"], m["total"]["pnl"])

    def test_what_is_said_of_the_gap(self):
        lost = [rec("btc", T0 + 300 * i, 0, "down", {"up": [(100, 10)]}) for i in range(40)]
        m = self.model(lost)                                # 40 windows, the side bought never won
        self.assertEqual((m["total"]["pnl"], m["total"]["ev"], m["total"]["z"]), (-188.0, 12.0, round(-200 / 1000 ** 0.5, 2)))
        self.assertIn("实际比模型少 200.00 美元（-6.3σ），超出了运气能解释的范围", m["verdict"])
        self.assertIn("吃单的人比我们知道得多", m["verdict"])
        even = self.model([rec("btc", T0 + 300 * i, 0, "down" if i % 2 else "up", {"up": [(100, 10)]}) for i in range(40)])
        self.assertIn("在运气能解释的范围内", even["verdict"])
        self.assertIn("样本太少", self.model(lost[:10])["verdict"])
        self.assertEqual(self.model([])["verdict"], "还没有已结算的成交")
        waiting = rec("btc", T0 + 300 * 50, 0, "up", {"up": [(100, 10)]}, phase="closed")
        m = self.model(lost + [waiting])
        self.assertEqual((m["waiting"], m["total"]["windows"]), (1, 40))   # a fill not settled yet is in neither line
        self.assertIn("模型对实际：模型 EV +12.00（每边按 50% 胜率），实际 -188.00。", journal.review_text(lost, T0 + 40 * 86400, 60))


class UpdateSpanTest(unittest.IsolatedAsyncioTestCase):
    """上次更新起: the review from the latest settings change or new version of the trading until now."""

    def test_the_span_since_an_update_and_since_a_moment(self):
        now = T0 + 6 * 3600
        ups = [{"at": now - 3600, "kind": "改参数", "text": "网页：挂哪些币", "when": mk.hm(now - 3600)},
               {"at": now - 86400, "kind": "版本更新", "text": "吃满的单按成交算", "when": mk.hm(now - 86400)}]
        span, info = journal.span_from("update", "", "", now, ups)
        self.assertEqual(span, (now - 3600, float("inf")))
        self.assertEqual((info["range"], info["label"], info["to"]), ("update", f"上次更新（{mk.hm(now - 3600)} 改参数）以来", ""))
        self.assertEqual([u["from"] for u in info["updates"]], ["2026-11-13T15:30", "2026-11-12T16:30"])
        span, info = journal.span_from("update", "", "", now, [])           # none kept: everything, and it says so
        self.assertEqual((span, info["label"]), (None, "全部（还没有更新记录）"))
        span, info = journal.span_from("since", info["presets"]["today"]["from"], "", now, ups)   # from a moment, no end
        self.assertEqual((span, info["label"], info["from"]), ((journal.day_start(now), float("inf")), "11-13 00:00 以来", "2026-11-13T00:00"))
        with self.assertRaises(ValueError):
            journal.span_from("since", "", "", now, ups)
        self.assertNotIn("update", info["presets"])
        span, label, rest = journal.span_args(["更新", "btc"], now, ups)    # Telegram's word for it
        self.assertEqual((span, label, rest), ((now - 3600, float("inf")), f"上次更新（{mk.hm(now - 3600)} 改参数）以来", ["btc"]))
        with self.assertRaisesRegex(ValueError, "还没有更新记录"):
            journal.span_args(["更新"], now, [])

    def test_versions_and_settings_changes_are_the_update_points(self):
        from unittest import mock

        from preopen import config
        first = (("2026-11-12 09:00", "第一版"),)
        with mock.patch.object(config, "TRADING_VERSIONS", first):
            e, clock, pub, x = make_engine()
            self.assertEqual([(v["id"], mk.hm(v["at"]), v["note"]) for v in e.version_log], [("2026-11-12 09:00", "11-12 09:00", "第一版")])
            self.assertEqual([(p["kind"], p["when"], p["text"]) for p in e.update_points()], [("版本更新", "11-12 09:00", "第一版")])
        clock.advance(3600)
        with mock.patch.object(config, "TRADING_VERSIONS", first + (("2026-11-13 10:00", "第二版"),)):
            e2, *_ = make_engine(ledger=e.ledger, clock=clock)   # a deploy: the new version is seen starting now
            self.assertEqual([v["at"] for v in e2.version_log][1], float(int(clock.t)))
            e2.settings_log.append({"at": clock.t + 60, "via": "网页", "changes": [["PREOPEN_SHARES", "10", "30"], ["PREOPEN_MARKETS", "btc,eth", "btc"]]})
            self.assertEqual([p["kind"] for p in e2.update_points()], ["版本更新", "版本更新"])   # the change is a minute ahead
            clock.advance(120)
            points = e2.update_points()
            self.assertEqual([(p["kind"], p["text"]) for p in points],
                             [("改参数", "网页：每边份数、挂哪些币"), ("版本更新", "第二版"), ("版本更新", "第一版")])
            e3, *_ = make_engine(ledger=e.ledger, clock=clock)   # a restart on the same version: nothing new
            self.assertEqual(e3.version_log, e2.version_log)
        e4, *_ = make_engine()   # the versions the list names for later than now have not happened yet
        self.assertEqual(e4.update_points(), [])
        self.assertEqual(len(e4.version_log), len(config.TRADING_VERSIONS))

    async def test_the_page_and_telegram_review_since_the_last_update(self):
        now = T0 + 300 * 40
        e, clock, pub, x = make_engine(settings(**ENV), clock=Clock(now))
        for i in range(30):
            e.save(rec("btc", T0 + 300 * i, 0, "up" if i % 3 else "down", {"up": [(100, 10)]}))
        c = Console(e, ENV, Path(tempfile.mkdtemp()) / "c.json", {})
        self.assertEqual((await c.review({"range": "update"}))["span"]["label"], "全部（还没有更新记录）")
        self.assertIn("还没有更新记录", main.review(e, ["更新"]))
        self.assertNotIn("exp_update", str(main.export(e, []).markup))
        changed = T0 + 300 * 20
        e.settings_log.append({"at": changed, "via": "网页", "changes": [["PREOPEN_MARKETS", "btc,eth", "btc"]]})
        d = await c.review({"range": "update"})
        self.assertEqual(d["span"]["label"], f"上次更新（{mk.hm(changed)} 改参数）以来")
        self.assertEqual((d["totals"]["range"]["quoted"], d["model"]["total"]["windows"]), (10, 10))   # windows 20 to 29
        self.assertEqual((d["span"]["updates"][0]["kind"], d["span"]["updates"][0]["text"]), ("改参数", "网页：挂哪些币"))
        since = await c.review({"range": "since", "from": d["span"]["updates"][0]["from"]})
        self.assertEqual((since["totals"]["range"]["quoted"], since["span"]["label"]), (10, f"{mk.hm(changed)} 以来"))
        name, data = await c.xlsx({"range": "update"})
        self.assertTrue(name.startswith("复盘_上次更新") and data[:2] == b"PK")
        reply = main.review(e, ["更新"])
        self.assertIn(f"（上次更新（{mk.hm(changed)} 改参数）以来）", reply.text.splitlines()[0])
        self.assertIn("模型对实际：模型 EV +3.00", reply.text)
        self.assertEqual(reply.markup["inline_keyboard"][-1], [{"text": f"上次更新起（{mk.hm(changed)} 改参数）", "callback_data": "rev_update"}])
        handle = main.commander(settings(**ENV), e, pub, c)
        book = await handle("/exp_update", [])               # 📤 导出 → 上次更新起
        self.assertTrue(book.files[0][0].startswith("复盘_上次更新"))
        self.assertIn("模型EV", (await handle("/export", [])).text)
        self.assertEqual((await handle("/export", ["更新"])).files[0][0], book.files[0][0])


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
