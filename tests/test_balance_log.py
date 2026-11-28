"""每天自动记余额: the account recorded at the first account pass of each Beijing day (and at the very first start),
kept in the ledger, said on Telegram and in the check, exported (the workbook's 余额 sheet, the raw export's notes)."""
import json
import unittest

from helpers import T0, Clock, make_engine
from test_export import read
from test_review_basis import rec

import main
from preopen import export, journal

MIDNIGHT = journal.day_start(T0) + 86400   # 2026-11-14 00:00 Beijing


class BalanceLogTest(unittest.IsolatedAsyncioTestCase):
    async def test_once_a_day_kept_and_said(self):
        e, clock, pub, x = make_engine(balance="1000")
        await e.account_once()
        (first,) = e.balance_log
        self.assertEqual((first["day"], first["cash"], first["total"], first["unsettled"], first["unclaimed"]),
                         ("2026-11-13", 1000.0, 1000.0, 0.0, 0.0))                     # the first start: a record at once
        self.assertTrue(any(n.startswith("📒 11-13 记账") for n in e.notes))
        await e.account_once()
        clock.advance(3600)
        await e.account_once()
        self.assertEqual(len(e.balance_log), 1)                                         # once a day
        e.save(rec("btc", T0 + 1800, 0, "up", {"up": [(100, 10)]}, redeem="manual"))    # +5.30 settled, of 11-13 (paper:
                                                                                        # its winnings left to claim)
        clock.t = MIDNIGHT + 5
        await e.account_once()
        second = e.balance_log[-1]
        self.assertEqual((second["day"], second["pnl_day_before"], second["unclaimed"]), ("2026-11-14", 5.3, 10.0))
        self.assertAlmostEqual(second["total"], 1010.0)                                 # the paper cash is not paid out:
        self.assertIn("已结算盈亏 +5.30", e.notes[-1])                                  # the winnings are owed
        self.assertEqual(json.loads(e.ledger.get("balance_log")), e.balance_log)        # kept in the ledger
        e2, *_ = make_engine(ledger=e.ledger, clock=clock, exchange=x)
        await e2.account_once()
        self.assertEqual(e2.balance_log, e.balance_log)                                 # a restart takes none again
        self.assertIn("记账：11-14", e2.check_text())

    async def test_what_is_held_besides_the_cash(self):
        e, clock, pub, x = make_engine(balance="1000", clock=Clock(T0 + 5 * 86400))
        waiting = rec("btc", T0 + 4 * 86400, 0, None, {"up": [(100, 10)]}, phase="closed")      # 10 × 47¢ at cost
        owed = rec("eth", T0 + 4 * 86400, 0, "up", {"up": [(100, 10)]}, redeem="pending")       # pays 10
        claimed = rec("sol", T0 + 4 * 86400, 0, "up", {"up": [(100, 10)]}, redeem="done")
        other = rec("xrp", T0 + 4 * 86400, 0, None, {"up": [(100, 10)]}, phase="closed", foreign=True)
        old = rec("doge", T0, 0, "down", {"down": [(100, 10)]}, redeem="failed")                 # archived, still owed
        e.save(waiting, owed, claimed, other, old)
        e.ledger.archive_old(clock.t)
        self.assertEqual(e.holdings(), {"unsettled": 4.7, "unclaimed": 20.0})

    async def test_none_while_blocked(self):
        e, clock, pub, x = make_engine()
        e.blocked = "账本属于另一个账户"
        await e.account_once()
        self.assertEqual(e.balance_log, [])


class BalanceSheetTest(unittest.TestCase):
    def test_the_sheet_sets_each_change_beside_the_days_pnl(self):
        log = [{"day": "2026-11-13", "at": T0, "mode": "模拟", "cash": 1000.0, "reserved": 9.4, "open_orders": 2,
                "unsettled": 0.0, "unclaimed": 0.0, "total": 1000.0, "pnl_day_before": 0.0},
               {"day": "2026-11-14", "at": MIDNIGHT + 5, "mode": "模拟", "cash": 990.6, "reserved": 0.0, "open_orders": 0,
                "unsettled": 0.0, "unclaimed": 10.0, "total": 1000.6, "pnl_day_before": 0.6}]
        records = [rec("btc", T0 + 600, 0, "up", {"up": [(100, 10)]}), rec("eth", T0 + 900, 0, "down", {"up": [(100, 10)]})]
        data, _ = export.build(records, MIDNIGHT + 60, 60, None, "全部", refusal_log=[], settings={}, settings_log=[],
                               mode="模拟", balances=log)
        rows = read(data)["余额"]
        self.assertEqual(rows[0][:4], ["日期", "记下的时刻（北京时间）", "模式", "现金 pUSD$"])
        self.assertEqual(rows[1][0], "2026-11-13")
        self.assertEqual(rows[1][9:], ["", "0"])                                         # nothing before it to compare
        self.assertEqual(rows[2][0], "2026-11-14")
        self.assertEqual([float(v) for v in rows[2][8:12]], [1000.6, 0.6, 0.6, 0.0])    # the change, the P&L, no gap
        empty, _ = export.build(records, MIDNIGHT + 60, 60, None, "全部", refusal_log=[], settings={}, settings_log=[], mode="模拟")
        self.assertIn("还没有记录", read(empty)["余额"][1][0])

    def test_the_raw_export_carries_them(self):
        e, clock, *_ = make_engine()
        e.balance_log.append({"day": "2026-11-13", "at": T0, "total": 1000.0})
        notes = json.loads(main.export(e, ["账本"]).files[-1][1].decode("utf-8"))
        self.assertEqual(notes["balance_log"], e.balance_log)


if __name__ == "__main__":
    unittest.main()
