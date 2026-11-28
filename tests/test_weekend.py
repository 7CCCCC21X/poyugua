"""复盘的周末专项: the day filter (全部 / 仅周末 / 仅工作日) and the weekend's comparison (weekdays beside the weekend, the
weekend's four stretches, each weekend), on the page, in Telegram and in the workbook. The weekend is PREOPEN_WEEKEND's
own: 北京时间周六 00:00 ～ 周一 08:00 by the window's start (markets.on_weekend)."""
import datetime as dt
import tempfile
import unittest
from decimal import Decimal as D
from pathlib import Path

from helpers import T0, Clock, make_engine, settings
from test_export import read
from test_review_basis import rec

import main
from preopen import export, journal
from preopen import markets as mk
from preopen.console import Console

ENV = {"PREOPEN_MARKETS": "btc,eth", "PREOPEN_AHEAD_MINUTES": "60"}


def bj(y, m, d, h=0, mi=0) -> int:
    return int(dt.datetime(y, m, d, h, mi, tzinfo=mk.BEIJING).timestamp())


# 2026-11-13 is a Friday; 11-14/15 the weekend, 11-16 Monday; the next weekend 11-21
WINDOWS = [  # (start, coin, result, fills): one level each, 47¢ x 10
    (bj(2026, 11, 13, 12), "btc", "up", {"up": [(100, 10)]}),            # weekday, won   +5.30
    (bj(2026, 11, 13, 23, 55), "eth", "down", {"up": [(100, 10)]}),      # weekday, lost  -4.70
    (bj(2026, 11, 14, 0), "btc", "down", {"up": [(100, 10)]}),           # sat_early, lost -4.70
    (bj(2026, 11, 14, 4, 55), "eth", "up", {"up": [(100, 10)]}),         # sat_early, won  +5.30
    (bj(2026, 11, 14, 5), "btc", "down", {"up": [(100, 10)]}),           # sat_main, lost  -4.70
    (bj(2026, 11, 15, 12), "btc", "up", {"up": [(100, 10)], "down": [(90, 10)]}),   # sun_all, a pair +0.60
    (bj(2026, 11, 16, 7, 55), "eth", "down", {"up": [(100, 10)]}),       # mon_early, lost -4.70
    (bj(2026, 11, 16, 8), "btc", "up", {"up": [(100, 10)]}),             # weekday again, won +5.30
    (bj(2026, 11, 21, 10), "btc", "down", {"down": [(100, 10)]}),        # the next weekend, won +5.30
    (bj(2026, 11, 17, 10), "eth", "up", {}),                             # a weekday quote, nothing filled
]
NOW = bj(2026, 11, 30)


def records():
    return [rec(coin, start, 0, result, fills, price="0.47") for start, coin, result, fills in WINDOWS]


class RuleTest(unittest.TestCase):
    def test_the_weekend_is_saturday_midnight_to_monday_8_beijing(self):
        cases = [((2026, 11, 13, 23, 55), None, "weekday"), ((2026, 11, 14, 0, 0), "sat_early", "weekend"),
                 ((2026, 11, 14, 4, 55), "sat_early", "weekend"), ((2026, 11, 14, 5, 0), "sat_main", "weekend"),
                 ((2026, 11, 15, 23, 55), "sun_all", "weekend"), ((2026, 11, 16, 7, 55), "mon_early", "weekend"),
                 ((2026, 11, 16, 8, 0), None, "weekday")]
        for when, part, day in cases:
            t = bj(*when)
            self.assertEqual(journal.weekend_part(t), part, when)
            self.assertEqual(journal.day_ok(t, day), True, when)
            self.assertEqual(journal.day_ok(t, "weekday" if day == "weekend" else "weekend"), False, when)
            self.assertEqual(mk.on_weekend(t), day == "weekend", when)   # the trading switch's own rule
        self.assertEqual(journal.weekend_of(bj(2026, 11, 16, 7, 55)), "2026-11-14")   # Monday morning: Saturday's weekend
        self.assertEqual(journal.weekend_of(bj(2026, 11, 15, 12)), "2026-11-14")
        self.assertEqual(journal.day_args(["30", "周末", "btc"]), ("weekend", ["30", "btc"]))
        self.assertEqual(journal.day_args(["工作日"]), ("weekday", []))
        self.assertEqual((journal.weekday_label("2026-11-14"), journal.weekday_label("2026-11-16", "weekend"),
                          journal.weekday_label("2026-11-16", "weekday")), ("周六", "周一 00–08 点", "周一 08 点后"))


class ComparisonTest(unittest.TestCase):
    def test_the_parts_add_up(self):
        j = journal.journal(records(), NOW, 60)
        W = j["weekend"]
        weekday, weekend = W["split"]
        self.assertEqual([r["key"] for r in W["parts"]], ["sat_early", "sat_main", "sun_all", "mon_early"])
        r = j["totals"]["range"]
        for k in ("quoted", "filled", "both", "wins", "losses"):
            self.assertEqual(weekday[k] + weekend[k], r[k], k)
            self.assertEqual(sum(p[k] for p in W["parts"]), weekend[k], k)
            self.assertEqual(sum(e[k] for e in W["each"]), weekend[k], k)
        for k in ("pnl", "volume", "cost"):
            self.assertAlmostEqual(weekday[k] + weekend[k], r[k], places=6)
            self.assertAlmostEqual(sum(p[k] for p in W["parts"]), weekend[k], places=6)
        self.assertAlmostEqual(weekday["ev"] + weekend["ev"], j["model"]["total"]["ev"], places=6)
        self.assertEqual((weekday["filled"], weekday["quoted"], weekend["filled"]), (3, 4, 6))
        self.assertAlmostEqual(weekday["pnl"], 5.3 - 4.7 + 5.3, places=6)
        self.assertAlmostEqual(weekend["pnl"], -4.7 + 5.3 - 4.7 + 0.6 - 4.7 + 5.3, places=6)
        self.assertEqual([e["name"] for e in W["each"]], ["11-21～11-23", "11-14～11-16"])   # the latest first
        self.assertEqual((W["each"][1]["from"], W["each"][1]["to"]), ("2026-11-14T00:00", "2026-11-16T08:00"))
        self.assertEqual(W["each"][1]["filled"], 5)                       # Monday 07:55 is the weekend before
        sun = next(p for p in W["parts"] if p["key"] == "sun_all")
        self.assertEqual((sun["both"], sun["z"]), (1, None))              # a pair: no luck in it
        self.assertEqual((weekend["one_win_rate"], weekend["one_breakeven"]), (40.0, 47.0))   # 2 of 5 one-sided won
        self.assertIn("样本太少", W["verdict"])
        self.assertFalse(any(e["ongoing"] for e in W["each"]))
        during = journal.journal(records(), bj(2026, 11, 23, 7, 59), 60)["weekend"]["each"]   # Monday 07:59: not over yet
        self.assertEqual([(e["name"], e["ongoing"]) for e in during], [("11-21～11-23（进行中）", True), ("11-14～11-16", False)])

    def test_the_day_filter_narrows_everything_but_the_comparison(self):
        whole = journal.journal(records(), NOW, 60)
        only = journal.journal(records(), NOW, 60, day_type="weekend")
        r, w = only["totals"]["range"], whole["weekend"]["split"][1]
        self.assertEqual((r["quoted"], r["filled"], r["pnl"]), (w["quoted"], w["filled"], w["pnl"]))
        self.assertEqual(only["weekend"]["split"], whole["weekend"]["split"])   # the comparison stays whole
        self.assertEqual(only["model"]["total"]["ev"], w["ev"])
        self.assertTrue(all(journal.mk.on_weekend(int(m["start"])) for m in only["by_market"]))
        days = {x["day"]: x["weekday"] for x in only["by_day"]}
        self.assertEqual(days["2026-11-16"], "周一 00–08 点")
        weekdays = journal.journal(records(), NOW, 60, day_type="weekday")
        self.assertEqual(weekdays["totals"]["range"]["filled"], 3)
        self.assertEqual(journal.price_markets(records(), NOW, 47, day_type="weekday")["count"], 3)
        self.assertEqual(journal.span_stats(records(), NOW, 0, NOW, day_type="weekend")["filled"], 6)

    def test_telegrams_text(self):
        whole = journal.review_text(records(), NOW, 60)
        self.assertIn("【周末 vs 工作日】", whole)
        self.assertNotIn("【周末的四段】", whole)
        only = journal.review_text([], NOW, 60, "周末", j=journal.journal(records(), NOW, 60, day_type="weekend"))
        self.assertIn("【周末的四段】", only)
        self.assertIn("周六 00–05 点", only)
        self.assertIn("【逐个周末】", only)
        self.assertIn("11-21～11-23", only)

    def test_the_workbook(self):
        data, totals = export.build(records(), NOW, 60, None, "全部", refusal_log=[], settings={}, settings_log=[], mode="模拟")
        sheets = read(data)
        rows = sheets["周末"]
        self.assertEqual(rows[0][0], "结论")
        head = next(i for i, r in enumerate(rows) if r and r[0] == "工作日 vs 周末")
        self.assertEqual(rows[head][1:3], ["已结算盈亏$", "模型 EV$"])
        self.assertEqual([rows[head + 1][0], rows[head + 2][0]], ["工作日", "周末"])
        self.assertEqual(rows[head + 2][9], "6")                          # weekend: 6 windows filled
        overview = {r[0]: r[1:] for r in sheets["总览"] if r}
        self.assertIn("周末表现", overview)
        self.assertIn("周末", {name for name, _ in export.GUIDE})
        data, totals = export.build(records(), NOW, 60, None, "全部_周末", refusal_log=[], settings={}, settings_log=[],
                                    mode="模拟", day_type="weekend")
        sheets = read(data)
        self.assertEqual(totals["filled"], 6)
        self.assertEqual(len(sheets["成交明细"]) - 1, 6)                   # the fills of the weekend only
        self.assertEqual(sheets["周末"][next(i for i, r in enumerate(sheets["周末"]) if r and r[0] == "工作日 vs 周末") + 1][9], "3")


class ConsoleAndTelegramTest(unittest.IsolatedAsyncioTestCase):
    async def engine(self):
        e, clock, pub, x = make_engine(settings(**ENV), clock=Clock(NOW))
        e.save(*records())
        return e, Console(e, ENV, Path(tempfile.mkdtemp()) / "c.json", {})

    async def test_the_page(self):
        e, c = await self.engine()
        whole = await c.review({"range": "all"})
        weekend = await c.review({"range": "all", "day": "weekend"})
        weekdays = await c.review({"range": "all", "day": "weekday"})
        self.assertEqual([d["totals"]["range"]["filled"] for d in (whole, weekend, weekdays)], [9, 6, 3])   # not one cache
        self.assertEqual(weekend["day_type"], "weekend")
        self.assertEqual(whole["totals"]["all"], weekend["totals"]["all"])   # the cards stay the whole ledger's
        self.assertEqual((await c.review({"range": "all", "day": "nonsense"}))["day_type"], "all")
        self.assertEqual((await c.price_detail({"range": "all", "price": "47", "day": "weekend"}))["count"], 6)
        name, data = await c.xlsx({"range": "all", "day": "weekend"})
        self.assertIn("全部_周末", name)
        name, text = await c.csv("detail", {"range": "all", "day": "weekday"})
        self.assertIn("工作日", name)
        self.assertEqual(text.count("\n") - 1, 3)

    async def test_telegram(self):
        e, c = await self.engine()
        reply = main.review(e, ["周末"])
        self.assertIn("（周末）", reply.text.splitlines()[0])
        self.assertIn("【周末的四段】", reply.text)
        self.assertTrue(reply.text.splitlines()[-1].startswith("合计（周末）：报价 6 场，成交 6 场（双边 1）"))   # the weekend's, not the whole's
        self.assertIn("合计（工作日）：报价 4 场，成交 3 场", main.review(e, ["工作日"]).text)
        self.assertFalse(any(b["callback_data"].startswith("set_cancel_") for row in reply.markup["inline_keyboard"] for b in row))
        self.assertIn(["rev_weekend", "rev_weekday"], [[b["callback_data"] for b in row] for row in reply.markup["inline_keyboard"]])
        self.assertIn("（BTC、周末）", main.review(e, ["btc", "周末"]).text.splitlines()[0])
        handle = main.commander(settings(**ENV), e, None, c)
        self.assertIn("（周末）", (await handle("/rev_weekend", [])).text.splitlines()[0])
        self.assertIn("（工作日）", (await handle("/rev_weekday", [])).text.splitlines()[0])
        book = main.export(e, ["30", "周末"])
        self.assertTrue(book.files[0][0].startswith("复盘_最近30天_周末_"))
        self.assertTrue(main.export(e, ["周末"]).files[0][0].startswith("复盘_周末_"))
        csv = main.export(e, ["csv", "工作日"])
        self.assertIn("导出工作日的记录：4 条报价，3 场有成交", csv.text)
        fills = dict(csv.files)[next(n for n, _ in csv.files if n.startswith("成交明细"))].decode("utf-8")
        self.assertEqual(fills.count("\n") - 1, 3)


if __name__ == "__main__":
    unittest.main()
