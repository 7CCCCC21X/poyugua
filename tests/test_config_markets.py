"""Settings parsing, the window math and the listing checks."""
import json
import unittest
from decimal import Decimal as D

from helpers import T0, gamma_market, settings, token

from preopen import config, markets as mk


class ConfigTest(unittest.TestCase):
    def test_defaults_are_the_seven_coins_paper_mode(self):
        s = config.from_env({})
        self.assertEqual(s.coins, ("btc", "eth", "sol", "xrp", "doge", "hype", "bnb"))
        self.assertFalse(s.live)
        self.assertEqual(s.first, config.Level(D("0.49"), D("10")))
        self.assertEqual((s.cancel_s, s.expiry_s, s.entry_s, s.ahead_s), (60, 30, 300, 86400))
        self.assertEqual(s.window_usd, D("9.80"))

    def test_levels_and_coins(self):
        s = config.from_env({"PREOPEN_MARKETS": "BTC, eth ,sol-5m", "PREOPEN_PRICE_CENTS": "48", "PREOPEN_SHARES": "20",
                             "PREOPEN_LEVELS": "47x30,45.5x40"})
        self.assertEqual(s.coins, ("btc", "eth", "sol"))
        self.assertEqual([(lv.price, lv.shares) for lv in s.levels], [(D("0.48"), D("20")), (D("0.47"), D("30")), (D("0.455"), D("40"))])
        self.assertEqual(config.from_env({"PREOPEN_MARKETS": "none"}).coins, ())
        self.assertEqual(config.from_env({"PREOPEN_MARKETS": "all"}).coins, config.COINS)
        noted = config.from_env({"PREOPEN_MARKETS": "btc,zec"})
        self.assertEqual(noted.coins, ("btc", "zec"))
        self.assertTrue(noted.notes)

    def test_refusals(self):
        bad = [
            {"PREOPEN_PRICE_CENTS": "51"},                      # both sides filled would cost more than $1
            {"PREOPEN_SHARES": "4"},                            # under Polymarket's 5-share minimum
            {"PREOPEN_LEVELS": "49x10"},                        # a deeper level must be cheaper
            {"PREOPEN_LEVELS": "48"},                           # price x shares
            {"PREOPEN_CANCEL_SECONDS": "30", "PREOPEN_EXPIRY_SECONDS": "30"},  # the cancel comes before the expiry
            {"PREOPEN_ENTRY_SECONDS": "120"},                   # GTD needs 3 minutes
            {"LIVE": "on"},                                     # no key
            {"LIVE": "maybe"},
            {"POLY_WALLET": "0x123"},
            {"POLY_API_KEY": "k"},                              # all three or none
            {"PREOPEN_AHEAD_MINUTES": "5"},
        ]
        for env in bad:
            with self.subTest(env=env), self.assertRaises(config.ConfigError):
                config.from_env(env)

    def test_entry_cutoff_follows_the_gtd_minimum(self):
        # expiry 60 s before the start: the stated expiration is the start itself, which must be 3 minutes away when placed
        with self.assertRaises(config.ConfigError):
            config.from_env({"PREOPEN_EXPIRY_SECONDS": "60", "PREOPEN_CANCEL_SECONDS": "90", "PREOPEN_ENTRY_SECONDS": "200"})
        s = config.from_env({"PREOPEN_EXPIRY_SECONDS": "60", "PREOPEN_CANCEL_SECONDS": "90", "PREOPEN_ENTRY_SECONDS": "210"})
        self.assertEqual(s.entry_s, 210)


class MarketsTest(unittest.TestCase):
    def test_slugs(self):
        self.assertEqual(mk.slug_of("btc", 1794536700), "btc-updown-5m-1794536700")
        self.assertEqual(mk.parse_slug("hype-updown-5m-1794536700"), ("hype", 1794536700))
        self.assertIsNone(mk.parse_slug("btc-updown-15m-1794536700"))
        self.assertIsNone(mk.parse_slug("btc-updown-5m-1794536701"))  # not a window boundary

    def test_window_starts(self):
        s = settings(PREOPEN_AHEAD_MINUTES="30")
        starts = mk.window_starts(T0 + 1, s)
        self.assertEqual(starts[0], T0 + 600)          # T0+300 starts within the 300 s cutoff
        self.assertEqual(starts[-1], T0 + 1800)
        self.assertTrue(all(t % 300 == 0 for t in starts))

    def test_weekend_filter(self):
        sat = 1794873600  # 2026-11-17 00:00 UTC is a Tuesday; find a Saturday in Beijing time instead
        import datetime as dt
        t = dt.datetime(2026, 11, 14, 0, 0, tzinfo=mk.BEIJING).timestamp()  # Saturday 00:00 Beijing
        self.assertTrue(mk.on_weekend(int(t)))
        self.assertTrue(mk.on_weekend(int(t) + 2 * 86400 + 7 * 3600))   # Monday 07:00
        self.assertFalse(mk.on_weekend(int(t) + 2 * 86400 + 8 * 3600))  # Monday 08:00
        self.assertFalse(mk.on_weekend(int(t) - 300))                   # Friday 23:55
        s = settings(PREOPEN_WEEKEND="off", PREOPEN_AHEAD_MINUTES="60")
        self.assertEqual(mk.window_starts(t - 1800, s), [int(t) - 1200, int(t) - 900, int(t) - 600, int(t) - 300])
        del sat

    def test_listing_parses_and_maps_outcomes_by_name(self):
        m = gamma_market("btc", T0, outcomes=("Down", "Up"))  # reversed order: the tokens follow the names
        listing, why = mk.parse_listing(m, "btc", T0)
        self.assertEqual(why, "")
        self.assertEqual(listing.up, token("btc", T0, "up"))
        self.assertEqual(listing.down, token("btc", T0, "down"))
        self.assertEqual((listing.tick, listing.min_size), (D("0.01"), D("5")))
        self.assertFalse(listing.maker_fee)

    def test_listing_refusals(self):
        cases = {
            "开始时间不符": gamma_market("btc", T0, eventStartTime="2026-11-13T02:35:00Z"),
            "已关闭": gamma_market("btc", T0, closed=True),
            "结果不是涨/跌": gamma_market("btc", T0, outcomes=("Yes", "No")),
            "缺 conditionId": gamma_market("btc", T0, conditionId=""),
            "没开订单簿": gamma_market("btc", T0, enableOrderBook=False),
        }
        for want, m in cases.items():
            with self.subTest(want=want):
                listing, why = mk.parse_listing(m, "btc", T0)
                self.assertIsNone(listing)
                self.assertIn(want, why)
        listing, why = mk.parse_listing(gamma_market("btc", T0), "eth", T0)
        self.assertIsNone(listing)
        fee = gamma_market("btc", T0, feeSchedule={"rate": 0.07, "takerOnly": False})
        self.assertTrue(mk.parse_listing(fee, "btc", T0)[0].maker_fee)

    def test_v2_market_uses_position_ids(self):
        m = gamma_market("btc", T0, version="v2", positionIds=json.dumps(["111", "222"]))
        listing, _ = mk.parse_listing(m, "btc", T0)
        self.assertEqual((listing.up, listing.down), ("111", "222"))

    def test_level_problem_and_winner(self):
        listing, _ = mk.parse_listing(gamma_market("btc", T0), "btc", T0)
        self.assertEqual(mk.level_problem(listing, D("0.49"), D("10")), "")
        self.assertIn("最小价位", mk.level_problem(listing, D("0.495"), D("10")))
        self.assertIn("最小", mk.level_problem(listing, D("0.49"), D("4")))
        self.assertEqual(mk.winner(gamma_market("btc", T0, closed=True, prices=["0", "1"])), "down")
        self.assertEqual(mk.winner(gamma_market("btc", T0, closed=True, prices=["1", "0"])), "up")
        self.assertIsNone(mk.winner(gamma_market("btc", T0, closed=False, prices=["1", "0"])))
        self.assertIsNone(mk.winner(gamma_market("btc", T0, closed=True, prices=["0.5", "0.5"])))

    def test_iso_and_span(self):
        self.assertEqual(mk.iso_ts("2026-11-13T02:25:00Z"), 1794536700)
        self.assertEqual(mk.iso_ts("2026-11-13 02:31:19+00"), 1794537079)
        self.assertEqual(mk.span(45), "45 秒")
        self.assertEqual(mk.span(3 * 3600 + 120), "3 小时 2 分")


if __name__ == "__main__":
    unittest.main()
