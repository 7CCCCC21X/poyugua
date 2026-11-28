"""The review as one Excel file (📤 导出, the page's 导出 Excel): the writer, the sheets and what is in them (the prices
quoted, the refusals, the settings and their changes), and the settings log behind the 参数 sheet."""
import io
import tempfile
import unittest
import zipfile
from decimal import Decimal as D
from pathlib import Path
from xml.etree import ElementTree as ET

from helpers import T0, make_engine, settings

from preopen import config, export, journal, xlsx
from preopen import markets as mk
from preopen.console import Console

NS = {"m": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
ENV = {"PREOPEN_MARKETS": "btc,eth", "PREOPEN_AHEAD_MINUTES": "60"}
try:
    import openpyxl
except ImportError:  # the server never needs it: only these tests read the files back with it when it is there
    openpyxl = None


def read(data: bytes) -> dict[str, list[list[str]]]:
    """Every sheet's cells as text, by the standard library alone (the shape any spreadsheet program reads)."""
    z = zipfile.ZipFile(io.BytesIO(data))
    book = ET.fromstring(z.read("xl/workbook.xml"))
    names = [s.get("name") for s in book.find("m:sheets", NS)]
    out = {}
    for i, name in enumerate(names):
        sheet = ET.fromstring(z.read(f"xl/worksheets/sheet{i + 1}.xml"))
        rows = []
        for row in sheet.find("m:sheetData", NS):
            cells: list[str] = []
            for c in row:  # placed by its reference (B2 is the second column): empty cells are not written
                letters = "".join(ch for ch in c.get("r") if ch.isalpha())
                index = 0
                for ch in letters:
                    index = index * 26 + ord(ch) - 64
                cells += [""] * (index - 1 - len(cells))
                v = c.find("m:v", NS)
                t = c.find("m:is/m:t", NS)
                cells.append(v.text if v is not None else (t.text if t is not None else ""))
            rows.append(cells)
        out[name] = rows
    return out


def record(coin, start, level, price, shares, result, fills, phase="settled", **extra):
    """A record of one level: fills = {side: [(seconds before the start, shares)]}."""
    orders = {side: {"state": "final", "id": f"{coin}{start}{level}{side}",
                     "filled": str(sum(sh for _, sh in fills.get(side, []))),
                     "fills_at": [[start - lead, str(sh)] for lead, sh in fills.get(side, [])]} for side in mk.SIDES}
    up, down = (sum(sh for _, sh in fills.get(sd, [])) for sd in mk.SIDES)
    pnl = float((up if result == "up" else down) - D(price) * (up + down)) if phase == "settled" else None
    q = {"key": f"{coin}-{start}#{level}@0", "slug": f"{coin}-updown-5m-{start}", "coin": coin, "start": start, "level": level,
         "attempt": 0, "price": price, "shares": str(shares), "expiration": start + 50, "phase": phase,
         "result": result if phase == "settled" else None, "placed": start - 3000, "listed": start - 3080,
         "cancel_lead": 60.0, "orders": orders, "pnl": pnl}
    q.update(extra)
    return q


class WriterTest(unittest.TestCase):
    def test_cells_names_and_header(self):
        data = xlsx.workbook([xlsx.Sheet("每日", [["日期", "盈亏$", "双边"], ["11-20", -2.5, True], ["11-21", D("3.10"), None]]),
                              xlsx.Sheet("每日", [["a"], ["x\x01<&>"]]), xlsx.Sheet("a/b" + "长" * 40, [["h"]])])
        sheets = read(data)
        self.assertEqual(list(sheets), ["每日", "每日(2)", "a_b" + "长" * 28])   # Excel's rules: unique, ≤31, no / [ ] : * ?
        self.assertEqual(sheets["每日"], [["日期", "盈亏$", "双边"], ["11-20", "-2.5", "是"], ["11-21", "3.10"]])
        self.assertEqual(sheets["每日(2)"][1], ["x<&>"])                  # escaped, the control character dropped
        sheet1 = zipfile.ZipFile(io.BytesIO(data)).read("xl/worksheets/sheet1.xml").decode()
        self.assertIn('state="frozen"', sheet1)                          # the header stays in view
        self.assertIn('<v>-2.5</v>', sheet1)                             # a number, not text
        if openpyxl:
            wb = openpyxl.load_workbook(io.BytesIO(data))
            ws = wb["每日"]
            self.assertEqual(ws["B2"].value, -2.5)
            self.assertTrue(ws["A1"].font.b)
            self.assertEqual(ws.freeze_panes, "A2")


class WorkbookTest(unittest.TestCase):
    def setUp(self):
        self.now = T0 + 6 * 3600
        today = journal.day_start(self.now)
        y = int(today - 86400 + 36000)          # yesterday 10:00
        t = int(today + 32400)                  # today 09:00
        self.records = [
            record("btc", y, 0, "0.47", 10, "up", {"up": [(900, 10)]}),                       # 47¢ up won: +5.30
            record("btc", y, 1, "0.40", 20, "up", {"down": [(600, 20)]}),                     # 40¢ down lost: -8.00
            record("eth", t, 0, "0.47", 10, "down", {"up": [(100, 10)], "down": [(50, 10)]}),  # a pair: +0.60
            record("eth", t, 1, "0.40", 20, "down", {}),                                      # quoted, nothing filled
        ]
        self.log = [{"at": self.now - 600, "slug": "eth-updown-5m-1", "coin": "eth", "start": t, "level": 0, "code": "rate_limited",
                     "words": "Too Many Requests", "status": "not_sent", "http": 429, "batch": 14, "whole": True,
                     "listed_ago": 76.0, "opened_ago": 1.2, "windows": 3},
                    {"at": self.now - 3 * 86400, "slug": "x", "coin": "btc", "start": t - 3 * 86400, "level": 0,
                     "code": "token_not_ready", "words": "invalid token id", "status": "rejected", "http": 400}]
        self.settings = config.current_values(settings(**ENV))
        self.changes = [{"at": self.now - 7200, "via": "网页", "changes": [["PREOPEN_CANCEL_SECONDS", "60", "180"]]}]

    def build(self, span=None, label="全部"):
        data, totals = export.build(self.records, self.now, 60, span, label, refusal_log=self.log, settings=self.settings,
                                    settings_log=self.changes, mode="模拟")
        return read(data), totals

    def test_every_sheet_and_the_guide(self):
        sheets, totals = self.build()
        self.assertEqual(list(sheets), ["总览", "每日", "模型EV", "周末", "市场盈亏", "档位", "成交时刻", "撤单时间", "方向", "速度", "改参对比",
                                        "币种", "时段", "成交明细", "全部报价", "拒单", "参数"])
        overview = {r[0]: r[1:] for r in sheets["总览"] if r}
        self.assertEqual(overview["时间范围（按开场，北京时间）"], ["全部"])
        self.assertEqual(overview["成交场次"], ["2"])
        self.assertEqual(overview["盈亏$（已结算）"], ["-2.1"])
        for name, _ in export.GUIDE:                       # the first sheet says what each sheet is for
            self.assertIn(name, overview)
        self.assertIn("40¢（赢 0% < 保本 40%）", overview["亏钱的价格"][0])
        self.assertEqual((totals["filled"], totals["pnl"]), (2, -2.1))

    def test_the_model_sheet_and_the_versions(self):
        """btc yesterday: 10 up at 47¢ won and 20 down at 40¢ lost (one window: 0.5 x 30 - 12.70 expected, luck half of
        the 10 shares apart); eth today: a pair of 10 at 47¢ (0.60 either way, no luck in it)."""
        data, _ = export.build(self.records, self.now, 60, None, "全部", refusal_log=self.log, settings=self.settings,
                               settings_log=self.changes, mode="模拟",
                               versions=[{"id": "2026-11-12 09:00", "at": self.now - 86400, "note": "吃满的单按成交算"}])
        sheets = read(data)
        rows = sheets["模型EV"]
        self.assertEqual(rows[0][0], "结论")
        self.assertIn("样本太少", rows[0][1])
        head = next(i for i, r in enumerate(rows) if r and r[0] == "币种")
        self.assertEqual(rows[head], journal.MODEL_HEAD)
        self.assertEqual(rows[head + 1], ["BTC", "1", "30", "12.7", "2.3", "-2.7", "-5", "5", "-1"])
        self.assertEqual(rows[head + 2][:8], ["ETH", "1", "20", "9.4", "0.6", "0.6", "0", "0"])   # a pair: no luck, no z
        self.assertEqual(rows[head + 3], ["合计", "2", "50", "22.1", "2.9", "-2.1", "-5", "5", "-1"])
        curve = next(i for i, r in enumerate(rows) if r and r[0].startswith("曲线"))
        self.assertEqual(rows[curve + 1][1:], ["-2.7", "2.3", "-7.7", "12.3"])   # the plan and two of luck's deviations
        self.assertEqual(rows[curve + 2][1:], ["-2.1", "2.9", "-7.1", "12.9"])
        overview = {r[0]: r[1:] for r in sheets["总览"] if r}
        self.assertIn("模型 EV +2.90", overview["模型对实际"][0])
        tail = sheets["参数"]
        at = next(i for i, r in enumerate(tail) if r and r[0].startswith("交易逻辑什么时候更新的"))
        self.assertEqual(tail[at + 1], [mk.hm(self.now - 86400), "吃满的单按成交算"])

    def test_the_prices_quoted(self):
        sheets, _ = self.build()
        head, *rows = sheets["档位"]
        self.assertEqual(head[:6], ["价格¢", "第几档", "报价（已开场）", "还没开场", "有成交", "成交率%"])
        by_price = {r[0]: r for r in rows}
        # 47¢: two quotes, both filled; 30 shares settled, 20 on the winning side: 66.7% against a 47% breakeven
        self.assertEqual(by_price["47"][2:6], ["2", "0", "2", "100"])
        self.assertEqual(by_price["47"][-2:], ["66.7", "47"])
        # 40¢: two quotes, one filled (20 down, which lost)
        self.assertEqual(by_price["40"][2:6], ["2", "0", "1", "50"])
        self.assertEqual((by_price["40"][11], by_price["40"][-2]), ("-8", "0"))

    def test_the_span_refusals_quotes_and_settings(self):
        today = journal.preset_span("today", self.now)
        sheets, totals = self.build(today, "今天")
        self.assertEqual([r[2] for r in sheets["市场盈亏"][1:]], ["ETH"])          # yesterday's BTC left out
        self.assertEqual(totals["quoted"], 1)
        refused = sheets["拒单"]
        self.assertEqual(len(refused), 2)                                         # three days ago is not today
        self.assertEqual(refused[1][1:4], [mk.window_name("eth", int(journal.day_start(self.now) + 32400)), "1",
                                           "下单太快被限流（429）"])
        self.assertEqual(refused[1][-3:], ["3", "76", "1.2"])
        quotes = sheets["全部报价"]
        self.assertEqual(quotes[0][10:12], ["上架后几秒挂上", "开放下单后几秒挂上"])
        self.assertEqual(quotes[1][10], "80")                                     # listed 3080 s, placed 3000 s ahead
        params = sheets["参数"]
        self.assertIn(["开场前几秒撤单", "60", "PREOPEN_CANCEL_SECONDS"], params)
        self.assertIn([mk.hm(self.now - 7200), "网页", "开场前几秒撤单", "60", "180"], params)

    def test_a_long_ledger_keeps_the_latest_quotes(self):
        keep = export.QUOTES_KEPT
        export.QUOTES_KEPT = 2
        try:
            sheets, _ = self.build()
        finally:
            export.QUOTES_KEPT = keep
        self.assertEqual(len(sheets["全部报价"]), 3)
        overview = {r[0]: r[1:] for r in sheets["总览"] if r}
        self.assertIn("只放了最近 2 条（共 4 条）", overview["注意"][0])

    def test_file_names(self):
        self.assertTrue(export.file_name("11-18 ～ 11-20", self.now).startswith("复盘_11-18至11-20_"))
        self.assertTrue(export.file_name("11-20 08:00 ～ 11-20 12:00", self.now).startswith("复盘_11-200800至11-201200_"))


class SettingsLogTest(unittest.IsolatedAsyncioTestCase):
    """Every settings change kept with when and how: the page, Telegram's button, a restart on new variables."""

    async def test_changes_are_kept_and_survive_a_restart(self):
        e, clock, pub, x = make_engine(settings(**ENV))
        self.assertEqual(list(e.settings_log), [])                 # the first start: nothing changed yet
        c = Console(e, ENV, Path(tempfile.mkdtemp()) / "c.json", {})
        ok, _ = await c.change({"name": "PREOPEN_SHARES", "value": "20"})
        self.assertTrue(ok)
        ok, _ = await c.change_many({"PREOPEN_CANCEL_SECONDS": "90", "PREOPEN_ENTRY_SECONDS": "300"}, "开场前 90 秒撤单")
        self.assertTrue(ok)
        log = list(e.settings_log)
        self.assertEqual([(x["via"], x["changes"]) for x in log],
                         [("网页", [["PREOPEN_SHARES", "10", "20"]]),
                          ("Telegram：开场前 90 秒撤单", [["PREOPEN_CANCEL_SECONDS", "60", "90"]])])
        # a restart with another Railway variable: the change is caught at the start
        e2, *_ = make_engine(settings(**{**ENV, "PREOPEN_SHARES": "20", "PREOPEN_CANCEL_SECONDS": "90",
                                         "PREOPEN_PRICE_CENTS": "48"}), ledger=e.ledger)
        last = list(e2.settings_log)[-1]
        self.assertEqual(last["changes"], [["PREOPEN_PRICE_CENTS", "49", "48"]])
        self.assertIn("重启", last["via"])
        self.assertEqual(len(e2.settings_log), 3)
        name, data = await c.xlsx({"range": "today"})
        self.assertTrue(name.startswith("复盘_今天_"))
        self.assertIn("参数", read(data))


if __name__ == "__main__":
    unittest.main()
