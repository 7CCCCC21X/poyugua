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
        self.assertEqual({s["side"]: s["shares"] for s in review["by_side"]}, {"涨": 10.0, "跌": 4.0})
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
