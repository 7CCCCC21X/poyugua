"""The audit's files from Telegram: /export 账本 (the ledger's records as stored) and /export 成交 (the account's trades
as the exchange lists them, every status), matched by order id."""
import datetime as dt
import gzip
import json
import types
import unittest
from decimal import Decimal as D
from unittest import mock

from helpers import T0, Clock, make_engine, settings
from test_exchange import live
from test_review_basis import rec

import main
from preopen import export, journal

NOW = T0 + 10 * 86400


def lines(data: bytes) -> list[dict]:
    return [json.loads(x) for x in gzip.decompress(data).decode("utf-8").splitlines()]


def engine_with_records():
    e, clock, *_ = make_engine(clock=Clock(NOW))
    old = [rec("btc", T0 + 300 * i, 0, "up", {"up": [(100, 10)]}) for i in range(3)]   # archived after two days
    e.save(*old)
    e.ledger.archive_old(NOW)
    recent = [rec("eth", NOW - 3600, 0, "down", {"up": [(100, 10)]}),
              rec("sol", NOW - 1800, 0, "up", {}, foreign=True)]                       # another account's, kept raw
    recent[0]["orders"]["up"].update(unverified=True, note="最终成交读不到")
    e.save(*recent)
    e.settings_log.append({"at": NOW - 86400, "via": "网页", "changes": [["PREOPEN_SHARES", "10", "30"]]})
    e.ledger.put("identity", "0xwallet|0xsigner")
    return e, old, recent


class RawLedgerTest(unittest.IsolatedAsyncioTestCase):
    def test_every_record_as_stored_and_the_notes(self):
        e, old, recent = engine_with_records()
        reply = main.export(e, ["账本"])
        (name, data), (notes_name, notes) = reply.files
        self.assertTrue(name.startswith("账本原始记录_全部_") and name.endswith(".jsonl.gz"))
        got = lines(data)
        self.assertEqual([q["key"] for q in got], [q["key"] for q in old + recent])   # the archive and the working set, in order
        self.assertEqual(got[-2], json.loads(json.dumps(recent[0])))                  # every field, as stored
        self.assertTrue(got[-1]["foreign"])
        self.assertIn("5 条记录", reply.text)
        kept = json.loads(notes.decode("utf-8"))
        self.assertEqual(set(kept), {"settings", "settings_log", "trading_versions", "refusals"})
        self.assertNotIn("0xwallet", notes.decode("utf-8") + gzip.decompress(data).decode("utf-8"))
        self.assertTrue(notes_name.startswith("账本附加_"))

    def test_a_time_span_and_long_ledgers_in_parts(self):
        e, old, recent = engine_with_records()
        week = main.export(e, ["账本", "7"])
        self.assertEqual([q["key"] for q in lines(week.files[0][1])], [q["key"] for q in recent])
        with mock.patch.object(export, "RAW_PART_BYTES", 300), mock.patch.object(export, "RAW_PARTS_MAX", 2), \
                mock.patch.object(export, "RAW_FLUSH_EVERY", 1):
            cut = main.export(e, ["账本"])
        parts = [f for f in cut.files if f[0].endswith(".jsonl.gz")]
        self.assertEqual(len(parts), 2)
        self.assertTrue(parts[0][0].endswith("_1.jsonl.gz") and parts[1][0].endswith("_2.jsonl.gz"))
        self.assertIn("只发了最早的", cut.text)
        self.assertEqual(sum(len(lines(d)) for _, d in parts), int(cut.text.split("：")[1].split(" 条")[0]))

    async def test_the_buttons(self):
        e, old, recent = engine_with_records()
        handle = main.commander(settings(), e, None, None)
        menu = await handle("/export", [])
        self.assertIn("交易所成交记录", menu.text)
        reply = await handle("/exp_raw", [])
        self.assertEqual(len(lines(reply.files[0][1])), 5)
        paper = await handle("/exp_trades", [])
        self.assertIn("模拟盘没有交易所的成交记录", paper.text)


def trade(tid: str, status: str, at: dt.datetime, order: str, shares: str) -> types.SimpleNamespace:
    maker = types.SimpleNamespace(order_id=order, owner="api-key-uuid", maker_address="0xMINE", matched_amount=D(shares),
                                  price=D("0.47"), side="BUY")
    other = types.SimpleNamespace(order_id="0xother", owner="their-key", maker_address="0xTHEIRS", matched_amount=D("5"),
                                  price=D("0.47"), side="BUY")
    return types.SimpleNamespace(id=tid, status=status, owner="taker-key", maker_address="0xTAKER", price=D("0.53"),
                                 size=D(shares), matched_at=at, maker_orders=(maker, other), transaction_hash="0xtx" + tid)


class Pages:
    def __init__(self, items, fail_with_bounds=False):
        self.items, self.fail, self.asked = items, fail_with_bounds, []

    def __call__(self, **kw):
        self.asked.append(kw)
        if self.fail and kw:
            raise RuntimeError("400 bad after")
        items = self.items

        async def gen():
            yield types.SimpleNamespace(items=items)
        return gen()


class TradesTest(unittest.IsolatedAsyncioTestCase):
    def trades(self):
        t = dt.datetime.fromtimestamp(T0, dt.timezone.utc)
        return [trade("2", "CONFIRMED", t + dt.timedelta(minutes=5), "0xb", "10"),
                trade("1", "FAILED", t, "0xa", "10"),
                trade("3", "CONFIRMED", t + dt.timedelta(days=2), "0xc", "4.716982")]

    async def test_the_exchanges_trades_without_who_they_belong_to(self):
        x, c = live()
        c.list_account_trades = Pages(self.trades())
        rows = await x.account_trades(T0 - 60, T0 + 86400)
        self.assertEqual(c.list_account_trades.asked, [{"after": str(T0 - 60), "before": str(T0 + 86400)}])
        self.assertEqual([r["id"] for r in rows], ["1", "2"])                         # the oldest first, within the bounds
        self.assertEqual((rows[0]["status"], rows[0]["maker_orders"][0]["matched_amount"]), ("FAILED", "10"))
        text = json.dumps(rows)
        for private in ("api-key-uuid", "0xMINE", "their-key", "0xTHEIRS", "taker-key", "0xTAKER", "owner", "maker_address"):
            self.assertNotIn(private, text)
        self.assertEqual(rows[1]["maker_orders"][0]["order_id"], "0xb")
        c.list_account_trades = Pages(self.trades(), fail_with_bounds=True)          # the bounds refused: all, bounded here
        self.assertEqual([r["id"] for r in await x.account_trades(T0 - 60, T0 + 86400)], ["1", "2"])

    async def test_telegram_sends_them(self):
        e, clock, *_ = make_engine(clock=Clock(T0 + 3 * 86400))
        rows = [{"id": "1", "status": "FAILED"}, {"id": "2", "status": "CONFIRMED"}, {"id": "3", "status": "CONFIRMED"}]
        asked = []

        async def account_trades(after, before):
            asked.append((after, before))
            return rows
        e.x.live, e.x.account_trades = True, account_trades
        reply = await main.commander(settings(), e, None, None)("/export", ["成交", "昨天"])
        (name, data), = reply.files
        self.assertTrue(name.startswith("交易所成交_昨天_") and name.endswith(".jsonl.gz"))
        self.assertEqual(lines(data), rows)
        self.assertIn("CONFIRMED 2、FAILED 1", reply.text)
        lo, hi = journal.span_args(["昨天"], e.now(), [])[0]
        self.assertEqual(asked, [(lo - e.s.ahead_s - 600, hi)])   # the windows of yesterday were quoted up to an hour before
        everything = await main.commander(settings(), e, None, None)("/exp_trades", [])
        self.assertEqual(asked[-1], (None, None))
        self.assertIn("3 笔", everything.text)


if __name__ == "__main__":
    unittest.main()
