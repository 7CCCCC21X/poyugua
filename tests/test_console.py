"""The console back end: settings saved over the environment (validated, applied live, kept across restarts), the LIVE
switch, the order list, the review and its CSV, the hand withdrawal and the emergency stop."""
import json
import tempfile
import unittest
from decimal import Decimal as D
from pathlib import Path

from helpers import T0, gamma_market, make_engine, settings, tick

from preopen import config, journal, markets as mk
from preopen.console import Console

ENV = {"PREOPEN_MARKETS": "btc,eth", "PREOPEN_AHEAD_MINUTES": "60"}


def quotes(e, slug=None, phase=None):
    return sorted((q for q in e.ledger.rows.values() if (slug is None or q["slug"] == slug) and (phase is None or q["phase"] == phase)),
                  key=lambda q: (q["start"], q["coin"], q["level"], q["attempt"]))


class EffectiveTest(unittest.TestCase):
    def test_saved_values_beat_the_environment_and_bad_ones_are_dropped(self):
        s, kept, notes = config.effective(ENV, {"PREOPEN_SHARES": "20", "PREOPEN_MARKETS": "sol"})
        self.assertEqual((s.first.shares, s.coins, notes), (D("20"), ("sol",), []))
        s, kept, notes = config.effective(ENV, {"PREOPEN_SHARES": "20", "PREOPEN_PRICE_CENTS": "80", "LIVE": "on"})
        self.assertEqual(kept, {"PREOPEN_SHARES": "20"})  # 80¢ is refused, LIVE=on has no key
        self.assertEqual(len(notes), 2)
        self.assertFalse(s.live)
        # values valid only together are kept together
        s, kept, notes = config.effective(ENV, {"PREOPEN_CANCEL_SECONDS": "20", "PREOPEN_EXPIRY_SECONDS": "10"})
        self.assertEqual((s.cancel_s, s.expiry_s, notes), (20, 10, []))

    def test_overrides_file_round_trip(self):
        with tempfile.TemporaryDirectory() as d:
            path = config.control_path(str(Path(d) / "preopen.sqlite3"))
            self.assertEqual(path.name, "preopen.control.json")
            self.assertEqual(config.load_overrides(path), {})
            config.save_overrides(path, {"PREOPEN_SHARES": "20", "POLY_PRIVATE_KEY": "x"})
            self.assertEqual(config.load_overrides(path), {"PREOPEN_SHARES": "20"})  # never a key from the page

    def test_every_form_preset_is_accepted_alone(self):
        for name, form in config.CONTROL_FORMS.items():
            for value in form.get("presets", []) + [o[0] for o in form.get("options", [])]:
                if name == "LIVE" and value == "on":
                    continue
                with self.subTest(name=name, value=value):
                    config.from_env({name: value})


class ConsoleTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.path = Path(self.dir.name) / "preopen.control.json"
        self.e, self.clock, self.pub, self.x = make_engine(settings(**ENV))
        self.sent = []
        self.c = Console(self.e, ENV, self.path, {}, notify=self.sent.append)
        self.pub.list_range(["btc", "eth"], mk.window_starts(self.clock.t, self.e.s)[:2])
        await tick(self.e)

    async def asyncTearDown(self):
        self.dir.cleanup()

    async def test_the_reviews_button_sets_the_cancel_time_and_the_entry_with_it(self):
        import main
        from preopen.telegram import Telegram
        handle = main.commander(self.e.s, self.e, self.pub, self.c)
        self.assertEqual((self.e.s.cancel_s, self.e.s.entry_s), (60, 300))
        text = await handle("/set_cancel_300", [])
        self.assertIn("已改成开场前 5 分撤单", text)
        self.assertIn("距开场不足 6 分不再新挂", text)
        self.assertIn("开场前 210 秒到期", text)                  # an order whose cancel fails still dies early
        self.assertEqual((self.e.s.cancel_s, self.e.s.entry_s, self.e.s.expiry_s), (300, 360, 210))
        saved = config.load_overrides(self.path)                 # kept across restarts
        self.assertEqual((saved["PREOPEN_CANCEL_SECONDS"], saved["PREOPEN_EXPIRY_SECONDS"]), ("300", "210"))
        for _ in range(4):                                       # cancelled, read back, quoted again
            await tick(self.e, 6)
        active = quotes(self.e, phase="active")
        self.assertEqual(len(active), 4)                         # re-placed with the new expiry
        self.assertEqual({q["expiration"] - q["start"] for q in active}, {60 - 210})

        class Tg(Telegram):
            posts: list = []

            async def _post(self, method, **payload):
                self.posts.append((method, payload))
                return True
        tg = Tg("1:x", "7")
        await tg.on_callback({"id": "c1", "data": "set_cancel_120", "from": {"id": 7},
                              "message": {"chat": {"id": 7}, "message_id": 5}}, handle)
        self.assertEqual((self.e.s.cancel_s, self.e.s.entry_s, self.e.s.expiry_s), (120, 360, 115))
        self.assertEqual([m for m, _ in tg.posts], ["answerCallbackQuery", "editMessageReplyMarkup"])  # the review stays
        self.assertIn("已改成开场前 2 分撤单", tg.outbox.get_nowait()[0])

    async def test_a_saved_setting_applies_at_once_and_is_kept(self):
        self.assertEqual(len(quotes(self.e, phase="active")), 4)
        ok, text = await self.c.change({"name": "PREOPEN_SHARES", "value": "20"})
        self.assertTrue(ok, text)
        self.assertIn("10 → 20", text)
        self.assertEqual(self.e.s.first.shares, D("20"))
        self.assertEqual(json.loads(self.path.read_text())["env"], {"PREOPEN_SHARES": "20"})
        self.assertEqual(len(quotes(self.e, phase="closing")), 4)  # no longer what the settings say: withdrawn
        await tick(self.e)
        await tick(self.e, 6)
        await tick(self.e, 6)
        active = quotes(self.e, phase="active")
        self.assertEqual({D(q["shares"]) for q in active}, {D("20")})
        self.assertTrue(self.sent and "网页改了设置" in self.sent[-1])
        items = {i["name"]: i for i in self.c.settings()["items"]}
        self.assertEqual((items["PREOPEN_SHARES"]["value"], items["PREOPEN_SHARES"]["source"]), ("20", "网页"))
        self.assertEqual(items["PREOPEN_MARKETS"]["source"], "环境变量")
        self.assertEqual(items["PREOPEN_PRICE_CENTS"]["source"], "默认")

    async def test_a_setting_that_does_not_touch_the_quotes_keeps_them(self):
        ok, _ = await self.c.change({"name": "PREOPEN_MAX_ATTEMPTS", "value": "5"})
        self.assertTrue(ok)
        self.assertEqual(len(quotes(self.e, phase="active")), 4)  # queue places kept

    async def test_refusals_change_nothing(self):
        for body in ({"name": "PREOPEN_PRICE_CENTS", "value": "80"}, {"name": "PREOPEN_ENTRY_SECONDS", "value": "100"},
                     {"name": "POLY_PRIVATE_KEY", "value": "0xabc"}, {"name": "LIVE", "value": "on"}):
            ok, text = await self.c.change(body)
            self.assertFalse(ok, body)
        self.assertFalse(self.path.exists())
        self.assertEqual(len(quotes(self.e, phase="active")), 4)
        self.assertFalse(self.e.restart)

    async def test_coins_off_and_back_and_reset(self):
        ok, _ = await self.c.change({"name": "PREOPEN_MARKETS", "value": "none"})
        self.assertTrue(ok)
        self.assertEqual(self.e.s.coins, ())
        self.assertEqual(len(quotes(self.e, phase="closing")), 4)
        ok, _ = await self.c.change({"reset": "PREOPEN_MARKETS"})
        self.assertTrue(ok)
        self.assertEqual(self.e.s.coins, ("btc", "eth"))
        ok, _ = await self.c.change({"name": "PREOPEN_SHARES", "value": "20"})
        ok, _ = await self.c.change({"reset_all": True})
        self.assertTrue(ok)
        self.assertEqual(config.load_overrides(self.path), {})
        self.assertEqual(self.e.s.first.shares, D("10"))

    async def test_live_switch_restarts_and_leaving_live_cancels_everything(self):
        c = Console(self.e, {**ENV, "POLY_PRIVATE_KEY": "0x" + "11" * 32}, self.path, {}, notify=self.sent.append)
        ok, text = await c.change({"name": "LIVE", "value": "on"})
        self.assertTrue(ok, text)
        self.assertTrue(self.e.restart and self.e.stop.is_set())
        self.assertEqual(config.load_overrides(self.path), {"LIVE": "on"})
        # a live engine switched off: every order of the key cancelled, then the restart
        e2, clock, pub, x = make_engine(settings(**ENV))
        x.live = True
        e2.s = config.from_env({**ENV, "LIVE": "on", "POLY_PRIVATE_KEY": "0x" + "11" * 32})
        pub.list_range(["btc"], mk.window_starts(clock.t, e2.s)[:1])
        await tick(e2)
        self.assertEqual(len(x.book), 2)
        c2 = Console(e2, {**ENV, "POLY_PRIVATE_KEY": "0x" + "11" * 32}, self.path, {"LIVE": "on"})
        ok, text = await c2.change({"name": "LIVE", "value": "off"})
        self.assertTrue(ok, text)
        self.assertEqual(x.book, {})
        self.assertTrue(e2.restart)

    async def test_hand_withdrawal_and_emergency(self):
        q = quotes(self.e, phase="active")[0]
        text = await self.c.action({"action": "withdraw", "key": q["key"]})
        self.assertIn("不再重挂", text)
        await tick(self.e)
        await tick(self.e, 6)
        await tick(self.e, 10)
        self.assertEqual([p["attempt"] for p in quotes(self.e, q["slug"]) if p["level"] == q["level"]], [0])  # not requoted
        text = await self.c.action({"action": "emergency"})
        self.assertTrue(self.e.paused)
        self.assertEqual(self.x.book, {})
        self.assertIn("撤掉", text)


def settled(coin, start, result, fills, cancel_lead=5.0, price="0.47"):
    """A finished record: fills = {side: [(seconds before the start, shares), ...]} at exact (trade) times."""
    orders = {}
    for side in ("up", "down"):
        log = fills.get(side, [])
        orders[side] = {"state": "final", "id": f"{coin}{start}{side}", "filled": str(sum(sh for _, sh in log)),
                        "fills_at": [[start - lead, str(sh)] for lead, sh in log]}
    return {"key": f"{coin}-{start}#0@0", "slug": f"{coin}-updown-5m-{start}", "coin": coin, "start": start, "level": 0,
            "attempt": 0, "price": price, "shares": "10", "expiration": start + 50, "phase": "settled", "result": result,
            "placed": start - 3600, "cancel_lead": cancel_lead, "orders": orders, "pnl": 0.0}


class CancelReviewTest(unittest.TestCase):
    def setUp(self):
        t = T0
        self.records = [
            settled("btc", t, "up", {"up": [(100, 10)]}),                    # +5.30 while cancelled 100 s or less ahead
            settled("eth", t, "up", {"down": [(8, 10)]}),                     # filled 8 s before: no candidate keeps it
            settled("sol", t, "down", {"up": [(25, 10)], "down": [(12, 10)]}),  # both: +0.60; up alone: -4.70
            settled("xrp", t, "up", {}),                                      # quoted, never filled
        ]
        self.now = T0 + 3600

    def test_each_cancel_time_replays_the_same_windows(self):
        c = journal.cancel_review(self.records, self.now, current=15)
        rows = {r["cancel_s"]: r for r in c["rows"]}
        self.assertEqual(c["base"], 10)                          # every window rested until 5 s before
        self.assertEqual({r["windows"] for r in c["rows"]}, {4})
        self.assertAlmostEqual(rows[10]["pnl"], 5.90)            # btc +5.30, sol both sides +0.60
        self.assertAlmostEqual(rows[15]["pnl"], 0.60)            # sol keeps the up side only: -4.70
        self.assertAlmostEqual(rows[30]["pnl"], 5.30)            # sol no fill
        self.assertAlmostEqual(rows[90]["pnl"], 5.30)
        self.assertAlmostEqual(rows[120]["pnl"], 0.0)            # btc's fill came 100 s before: gone too
        self.assertEqual((rows[10]["filled"], rows[10]["both"]), (2, 1))
        self.assertEqual(c["best"], 10)
        self.assertIsNone(c["recommend"])                        # four windows: too few to recommend a change
        self.assertIn("样本还少", c["advice"])
        self.assertEqual(c["confidence"]["level"], "低")
        quick = {t["bucket"]: t for t in c["by_fill_time"]}["开场前 0～15 秒"]
        self.assertEqual((quick["shares"], quick["won"]), (20.0, 10.0))  # eth down lost, sol down won
        self.assertAlmostEqual(quick["breakeven"], 0.47)

    def test_the_best_is_named_once_there_are_enough(self):
        old = journal.CANCEL_SAMPLE
        journal.CANCEL_SAMPLE = 2
        try:
            c = journal.cancel_review(self.records, self.now, current=15)
        finally:
            journal.CANCEL_SAMPLE = old
        self.assertEqual((c["best"], c["recommend"]), (10, 10))
        self.assertIn("改成开场前 10 秒撤单最好", c["advice"])
        # compared with the current setting replayed on the same windows (15 s: sol keeps only its up side, -4.70),
        # not with what happened under the old setting (actual +1.20)
        self.assertIn("比现在的开场前 15 秒（+0.60）多 5.30", c["advice"])
        self.assertEqual({r["cancel_s"]: r["delta"] for r in c["rows"]}[10], 5.3)

    def test_windows_cancelled_earlier_are_left_out_and_later_cancels_are_not_replayed(self):
        early = [settled("doge", T0 + 300 * i, "up", {"up": [(200, 10)]}, cancel_lead=60) for i in range(9)]
        c = journal.cancel_review(self.records + early, self.now + 3000, current=60)
        self.assertEqual(c["base"], 60)                          # most rested only until 60 s before
        self.assertEqual(min(r["cancel_s"] for r in c["rows"]), 60)
        self.assertEqual(c["excluded"], 0)                       # the 5 s ones rested past 60 s: comparable too
        csv_text = journal.cancel_csv(c)
        self.assertTrue(csv_text.startswith("\ufeff"))
        self.assertIn("开场前几秒撤单", csv_text)

    def test_windows_ended_by_restarts_and_changes_never_push_the_comparison_out(self):
        """11-13 20:02: thousands of records closed hours ahead by restarts and settings changes made the review
        compare only 600 s ("同一批 1115 场 … 另有 4319 场没算"). They would have ended the same under any cancel time."""
        noise = []
        for i in range(200):
            q = settled("bnb", T0 + 300 * i, "up", {})
            q.update(cancel_lead=None, reason="设置不同（价格或份数）", closing_at=T0 + 300 * i - 20000, phase="settled")
            noise.append(q)
        late = [settled(c, T0 + 300 * (i + 1), "down", {"up": [(lead, 10)]}, cancel_lead=14.5)
                for i, (c, lead) in enumerate([("btc", 20), ("eth", 25), ("sol", 40), ("xrp", 50)] * 6)]  # adverse: up lost
        c = journal.cancel_review(noise + late + self.records, T0 + 300 * 300, current=15)
        self.assertEqual(c["base"], 15)                           # the cancel rule's records ended 5-14.5 s ahead
        self.assertEqual(c["excluded"], 0)
        self.assertEqual(c["windows"], 200 + 24 + 4)
        rows = {r["cancel_s"]: r for r in c["rows"]}
        self.assertAlmostEqual(c["actual"]["pnl"], 1.20 - 24 * 4.70, places=2)
        self.assertAlmostEqual(rows[60]["pnl"], 5.30, places=2)  # cancelling 60 s ahead drops every late fill
        self.assertEqual(c["best"], 60)                          # the earliest of the best (90 s ... gives the same)
        self.assertIn("改成开场前 60 秒撤单最好", c["advice"])
        self.assertEqual(c["losses"][0]["bucket"], "15～30 秒")
        text = journal.review_text(noise + late + self.records, T0 + 300 * 300, 15)
        self.assertIn("亏在哪：15～30 秒", text.splitlines()[2])
        self.assertIn("开场前 60 秒～", text)                      # equal rows merged into a range

    def test_429s_at_the_openings_are_named_as_such(self):
        from preopen import refusals
        now = T0 + 1000
        log = [{"at": now - 10 * i, "slug": "btc-updown-5m-1", "coin": "btc", "start": T0 + 3000, "code": "rate_limited",
                "words": "下单被限流（429）", "status": "not_sent", "whole": True, "batch": 14, "http": 429,
                "opened_ago": 1.2 if i < 9 else None, "listed_ago": 76.0} for i in range(12)]
        (g,) = refusals.summary(log, now, 24)
        self.assertIn("9/12 次在新场次开放后 5 秒内（中位 1.2 秒）：开放那一刻同时下单太多", g["evidence"])
        quiet = [dict(x, opened_ago=None) for x in log]
        self.assertNotIn("开放后", refusals.summary(quiet, now, 24)[0]["evidence"])

    def test_snapshot_times_stand_in_and_are_flagged(self):
        q = settled("hype", T0, "up", {})
        q["orders"]["up"].update(filled="10", first_fill=T0 - 40)
        c = journal.cancel_review([q], self.now, current=15)
        self.assertEqual(c["approx"], 1)
        self.assertEqual(journal.fill_times_text(q, "up"), "40.0:10")


class WindowCountTest(unittest.TestCase):
    """11-19 review: quoted windows were counted once per window, fills once per level and attempt: one window filled on
    two levels showed 1 quoted, 2 filled, 200%. And a side's fills all went to its first fill's time."""

    @staticmethod
    def level(coin, start, level, result, fills, price):
        q = settled(coin, start, result, fills, price=price)
        q["key"], q["level"] = f"{coin}-{start}#{level}@0", level
        return q

    def test_a_window_filled_on_two_levels_is_one_filled_window(self):
        t = T0 - 3600
        records = [self.level("btc", t, 0, "up", {"up": [(900, 10)]}, "0.47"),     # 47¢ up 10
                   self.level("btc", t, 1, "up", {"down": [(600, 20)]}, "0.40"),   # 40¢ down 20: a second level, filled
                   self.level("eth", t, 0, "down", {}, "0.47")]                    # quoted, nothing filled
        total = journal.journal(records, T0)["totals"]["all"]
        self.assertEqual((total["quoted"], total["filled"], total["both"], total["fill_rate"]), (2, 1, 1, 50.0))
        # the money still adds up record by record: 10 × 0.47 + 20 × 0.40 paid, 10 won
        self.assertAlmostEqual(total["cost"], 12.7)
        self.assertAlmostEqual(total["pnl"], 10 - 12.7)
        self.assertEqual((total["wins"], total["losses"]), (0, 1))   # the window lost, once

    def test_status_counts_today_by_window(self):
        e, clock, pub, x = make_engine(settings(**ENV))
        start = T0 + 1800
        for level, (up, down) in enumerate((("10", "0"), ("0", "20"))):   # one window, two levels filled
            e.save({"key": f"w#{level}@0", "slug": mk.slug_of("btc", start), "coin": "btc", "start": start, "level": level,
                    "attempt": 0, "price": "0.47", "shares": "20", "phase": "active", "expiration": start,
                    "condition_id": "0xc", "orders": {"up": {"state": "open", "filled": up},
                                                      "down": {"state": "open", "filled": down}}})
        self.assertEqual(e.today(), {"fills": 1, "both": 1, "pnl": 0.0, "settled": 0})

    def test_each_fill_goes_to_its_own_time(self):
        t = T0 - 3600
        q = settled("btc", t, "up", {"up": [(1000, 1), (5, 9)]})       # 1 share 1000 s before the start, 9 shares 5 s before
        lead = {b["bucket"]: (b["fills"], b["shares"]) for b in journal.journal([q], T0)["by_lead"]}
        self.assertEqual(lead, {"5～30 分钟": (1, 1.0), "开场前 1 分钟内": (1, 9.0)})


class TimeFilterTest(unittest.TestCase):
    """11-21: the review by the day's volume, every market's P&L, and a time filter (by the windows' start, Beijing)."""

    def setUp(self):
        self.now = T0 + 6 * 3600                       # 2026-11-13 16:30 Beijing
        today = journal.day_start(self.now)
        self.records = [
            # yesterday 10:00: BTC on two levels, settled (up won: 10 × 0.47 + 20 × 0.40 paid, 10 back)
            WindowCountTest.level("btc", int(today - 86400 + 36000), 0, "up", {"up": [(900, 10)]}, "0.47"),
            WindowCountTest.level("btc", int(today - 86400 + 36000), 1, "up", {"down": [(600, 20)]}, "0.40"),
            # today 09:00: ETH both sides, settled (+0.60); today 15:00: SOL quoted, nothing filled
            settled("eth", int(today + 32400), "down", {"up": [(100, 10)], "down": [(50, 10)]}),
            settled("sol", int(today + 54000), "up", {}),
        ]
        later = settled("xrp", int(today + 61200), "up", {"up": [(300, 10)]})   # today 17:00: filled, not settled yet
        later.update(phase="closed", result=None)
        self.records.append(later)

    def test_moments_and_spans(self):
        now = self.now
        self.assertEqual(journal.parse_when("11-12", now), (journal.day_start(now) - 86400, False))
        self.assertEqual(journal.parse_when("2026-11-13T08:30", now)[1], True)
        self.assertEqual(journal.span_label(journal.preset_span("yesterday", now)), "11-12")
        self.assertEqual(journal.span_label(journal.custom_span("11-11", "11-13", now)), "11-11 ～ 11-13")
        self.assertEqual(journal.span_label(journal.custom_span("11-13 08:00", "11-13 12:00", now)), "11-13 08:00 ～ 11-13 12:00")
        with self.assertRaises(ValueError):
            journal.custom_span("11-13", "11-12", now)
        span, label, rest = journal.span_args(["btc", "11-11~11-12"], now)
        self.assertEqual((label, rest), ("11-11 ～ 11-12", ["btc"]))
        span, info = journal.span_from("7d", "", "", now)
        self.assertEqual((info["label"], info["from"], info["to"]), ("近 7 天", "2026-11-07T00:00", "2026-11-14T00:00"))
        self.assertEqual(journal.span_from("bogus", "", "", now)[0], None)   # anything else: everything

    def test_volume_markets_and_the_filter(self):
        j = journal.journal(self.records, self.now)
        day = {d["day"]: d for d in j["by_day"]}
        today, yesterday = journal.day_of(self.now), journal.day_of(self.now - 86400)
        self.assertEqual((day[yesterday]["shares"], day[yesterday]["volume"], day[yesterday]["pnl"]), (30.0, 12.7, -2.7))
        # today: ETH 20 shares $9.40 settled, XRP 10 shares $4.70 not settled yet (traded, P&L to come)
        self.assertEqual((day[today]["shares"], day[today]["volume"], day[today]["cost"]), (30.0, 14.1, 9.4))
        markets = {m["coin"]: m for m in j["by_market"]}
        self.assertEqual(list(markets), ["XRP", "ETH", "BTC"])          # the latest first; SOL never traded
        self.assertEqual((markets["BTC"]["levels"], markets["BTC"]["up"], markets["BTC"]["down"], markets["BTC"]["pnl"]),
                         ("1、2", 10.0, 20.0, -2.7))
        self.assertEqual((markets["ETH"]["both"], markets["ETH"]["result"], markets["ETH"]["pnl"]), (True, "跌", 0.6))
        self.assertEqual((markets["XRP"]["result"], markets["XRP"]["pnl"]), ("", None))
        # yesterday only: everything below the cards follows it, the cards do not
        y = journal.journal(self.records, self.now, span=journal.preset_span("yesterday", self.now))
        self.assertEqual(y["totals"]["all"], j["totals"]["all"])
        self.assertEqual((y["totals"]["range"]["quoted"], y["totals"]["range"]["pnl"]), (1, -2.7))
        self.assertEqual([m["coin"] for m in y["by_market"]], ["BTC"])
        self.assertEqual([d["day"] for d in y["by_day"]], [yesterday])
        self.assertEqual([c["coin"] for c in y["by_coin"]], ["BTC"])
        self.assertEqual(y["span"], "11-12")
        c = y["cancel"]                                # the replay too, by window: BTC's two levels are one window,
        self.assertEqual((c["windows"], c["actual"]["filled"], c["actual"]["both"]), (1, 1, 1))   # up on one, down on the other
        csv_text = journal.markets_csv(y)
        self.assertIn("BTC,1、2,10,20,是,12.7,涨,-2.7", csv_text)
        daily = journal.daily_csv(j)
        self.assertIn("成交额$", daily.splitlines()[0])

    async def _console(self):
        e, clock, pub, x = make_engine(settings(**ENV))
        clock.t = self.now
        for q in self.records:
            e.save(q)
        return Console(e, ENV, Path(tempfile.mkdtemp()) / "c.json", {})

    def test_the_console_filters_by_the_query(self):
        import asyncio

        async def check():
            c = await self._console()
            d = await c.review({"range": "custom", "from": "11-13 09:00", "to": "11-13 18:00"})
            self.assertEqual([m["coin"] for m in d["by_market"]], ["XRP", "ETH"])
            self.assertEqual(d["span"]["label"], "11-13 09:00 ～ 11-13 18:00")
            self.assertEqual(d["market_more"], 0)
            self.assertIn("error", await c.review({"range": "custom", "from": "11-13", "to": "11-12"}))
            name, text = await c.csv("markets", {"range": "today"})
            self.assertEqual(name, "preopen-markets-今天.csv")
            self.assertNotIn("BTC", text)
            self.assertIn("BTC", (await c.csv("markets", {}))[1])
            with self.assertRaises(ValueError):   # a filter that is not one is said so, not exported as everything
                await c.csv("markets", {"range": "custom", "from": "x", "to": "11-12"})
        asyncio.run(check())


class JournalTest(unittest.IsolatedAsyncioTestCase):
    async def test_orders_tabs_and_review(self):
        e, clock, pub, x = make_engine(settings(**ENV))
        start = T0 + 1800
        m = pub.list_window("btc", start)
        pub.list_window("eth", start)
        await tick(e)
        btc = quotes(e, mk.slug_of("btc", start))[0]
        x.fill(btc["orders"]["up"]["id"], D("10"))   # the whole up side
        x.fill(btc["orders"]["down"]["id"], D("4"))  # part of the down side
        await tick(e, 10)
        view = journal.orders(e.all_records(), "open", "", clock.t)
        self.assertEqual((view["counts"]["open"], view["counts"]["filled"]), (2, 1))
        self.assertEqual(len(journal.orders(e.all_records(), "open", "eth", clock.t)["rows"]), 1)
        clock.t = start - 60
        await tick(e)
        await tick(e, 6)
        clock.t = start + 400
        pub.results[m["slug"]] = {**m, "closed": True, "outcomePrices": '["1", "0"]'}
        await tick(e)
        review = journal.journal(e.all_records(), clock.t)
        day = review["by_day"][0]
        self.assertEqual((day["quoted"], day["filled"], day["both"], day["wins"]), (2, 1, 1, 1))
        self.assertAlmostEqual(day["cost"], 14 * 0.49)
        self.assertAlmostEqual(day["pnl"], 10 - 14 * 0.49)
        self.assertEqual(review["totals"]["all"]["fill_rate"], 50.0)
        # 10 up and 4 down: 4 pairs (+0.08 locked) and 6 up alone (won: +3.06), adding up to the day's +3.14
        self.assertEqual([(r["shares"], r["pnl"]) for r in review["by_side"]], [(4.0, 0.08), (6.0, 3.06), (0.0, 0.0), (0.0, None)])
        self.assertAlmostEqual(sum(r["pnl"] for r in review["by_side"][:3]), day["pnl"])
        self.assertEqual([(b["bucket"], b["fills"]) for b in review["by_lead"]], [("5～30 分钟", 2)])  # seen at the snapshot after the fills
        detail = review["detail"][0]
        self.assertEqual((detail["coin"], detail["result"], detail["up"], detail["down"]), ("BTC", "涨", 10.0, 4.0))
        self.assertIsNotNone(detail["cancel_lead_s"])
        csv_text = journal.detail_csv(e.all_records())
        self.assertTrue(csv_text.startswith("﻿场次"))
        self.assertEqual(len(csv_text.strip().splitlines()), 2)
        self.assertIn(day["day"], journal.daily_csv(review))
        self.assertEqual(journal.orders(e.all_records(), "filled", "", clock.t)["counts"]["filled"], 1)


if __name__ == "__main__":
    unittest.main()


class MainTest(unittest.TestCase):
    def test_saved_settings_at_start_and_the_restart(self):
        import os
        import sys
        from unittest import mock

        import main
        with tempfile.TemporaryDirectory() as d:
            db = str(Path(d) / "preopen.sqlite3")
            config.save_overrides(config.control_path(db), {"PREOPEN_SHARES": "20", "PREOPEN_PRICE_CENTS": "99"})
            seen, execs = {}, []

            async def fake_run(settings, env, kept, dropped):
                seen.update(settings=settings, kept=kept, dropped=dropped)
                return True  # the console asked for a restart

            with mock.patch.object(main, "run", fake_run), mock.patch.object(main.os, "execv", lambda *a: execs.append(a)), \
                    mock.patch.dict(os.environ, {"PREOPEN_DB": db, "PREOPEN_MARKETS": "btc"}, clear=False):
                self.assertEqual(main.main([]), 0)
            self.assertEqual(seen["settings"].first.shares, D("20"))
            self.assertEqual(seen["kept"], {"PREOPEN_SHARES": "20"})   # the 99¢ is dropped, and noted
            self.assertEqual(len(seen["dropped"]), 1)
            self.assertEqual(config.load_overrides(config.control_path(db)), {"PREOPEN_SHARES": "20"})
            self.assertEqual(execs[0][0], sys.executable)
