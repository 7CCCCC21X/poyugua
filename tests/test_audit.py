"""11-27 audit: each finding confirmed in the code, each with its case. The audit (another review of fa33b1d) listed 17;
every one reproduced. The numbers are the audit's."""
import asyncio
import json
import sqlite3
import tempfile
import unittest
from decimal import Decimal as D
from pathlib import Path
from unittest import mock

import httpx
from helpers import T0, Clock, drain, make_engine, settings, tick

import main
from preopen import config, gamma, journal, web
from preopen import markets as mk
from preopen.console import Console
from preopen.exchange import Cancelled, PaperExchange, Row
from preopen.ledger import Ledger

ENV = {"PREOPEN_MARKETS": "btc,eth", "PREOPEN_AHEAD_MINUTES": "60"}


def quotes(e, slug=None):
    return sorted((q for q in e.ledger.rows.values() if slug is None or q["slug"] == slug),
                  key=lambda q: (q["start"], q["coin"], q["level"], q["attempt"]))


async def settle_win(e, clock, pub, x, side="up", shares="10", result=("1", "0")):
    """One BTC window: `side` bought `shares` at 49¢, cancelled at its time, resolved to `result`."""
    start = mk.window_starts(clock.t, e.s)[0]
    m = pub.list_window("btc", start)
    await tick(e)
    (q,) = quotes(e, m["slug"])
    x.fill(q["orders"][side]["id"], D(shares))
    clock.t = start - e.s.cancel_s
    await tick(e)
    await tick(e, 6)
    clock.t = start + 400
    pub.results[m["slug"]] = {**m, "closed": True, "outcomePrices": json.dumps(list(result))}
    await tick(e)
    await tick(e, 30)
    return q


class StateMachineTest(unittest.IsolatedAsyncioTestCase):
    async def test_1_new_settings_keep_the_other_side_of_a_half_filled_window(self):
        """A tap on the review's 改成开场前 90 秒撤单 changes the orders' expiry, so no resting record fits any more.
        The one whose up side was eaten whole keeps its down side resting: it pairs the fill, and a record with a fill
        is never quoted again. The others are withdrawn and quoted afresh as before."""
        e, clock, pub, x = make_engine()
        pub.list_range(["btc"], mk.window_starts(clock.t, e.s)[:3])
        await tick(e)
        half, untouched, partial = quotes(e)
        x.fill(half["orders"]["up"]["id"], D("10"))     # eaten whole: its down side can only pair it
        x.fill(partial["orders"]["up"]["id"], D("4"))   # partly: its up side may still fill one-sided
        await tick(e, 10)
        await tick(e, 6)
        c = Console(e, ENV, Path(tempfile.mkdtemp()) / "c.json", {})
        ok, text = await c.set_cancel(90, "网页复盘")
        self.assertTrue(ok)
        self.assertEqual((half["phase"], half["orders"]["down"]["state"]), ("active", "open"))
        self.assertIn(half["orders"]["down"]["id"], x.book)
        self.assertIn("留着配对", half["note"])
        self.assertEqual(untouched["phase"], "closing")
        self.assertEqual(partial["phase"], "closing")
        self.assertIn("1 场有成交，另一边留着配对", text)
        clock.t = half["start"] - 90                     # the new cancel time still applies to it
        await tick(e)
        self.assertEqual(half["phase"], "closing")
        self.assertEqual(half["reason"], "到点撤单")
        # a restart on other settings decides the same way
        e2, *_ = make_engine(settings(PREOPEN_PRICE_CENTS="48"), ledger=Ledger(":memory:"), clock=clock)
        self.assertEqual(e2.settings_withdraw(untouched), e2.matches_settings(untouched))

    async def test_2_a_cancel_refused_in_other_words_keeps_the_order_and_its_cancels_going(self):
        e, clock, pub, x = make_engine()
        pub.list_window("btc", T0 + 1800)
        await tick(e)
        (q,) = quotes(e)
        up = q["orders"]["up"]["id"]
        real, refusing = x.cancel, {"on": True}

        async def cancel(ids):
            r = Cancelled()
            for oid in ids:
                if oid == up and refusing["on"]:
                    r.not_canceled[oid] = "matching in progress, retry"
                else:
                    got = await real([oid])
                    r.canceled |= got.canceled
                    r.not_canceled.update(got.not_canceled)
            return r
        x.cancel = cancel
        left = await e.withdraw_all("测试", deadline_s=0.3)
        self.assertEqual(left, 1)                                  # waited for, and said to be left
        self.assertEqual(q["orders"]["up"]["state"], "open")
        self.assertIn(up, x.book)
        self.assertIn("撤单没成功", e.errors[-1][1])
        self.assertGreater(len(x.cancel_calls), 0)
        refusing["on"] = False
        clock.advance(6)
        await e.cancel_pass()
        self.assertNotIn(up, x.book)
        self.assertEqual(q["orders"]["up"]["state"], "gone")
        # words that say the order is off the book: gone at once, read for its fills as before
        self.assertTrue(all(map(__import__("preopen.engine", fromlist=["off_book"]).off_book,
                                ("order already matched", "order can't be found - already canceled or matched", "order not found"))))

    async def test_4_a_lookup_never_undoes_a_cancel_made_while_it_waited(self):
        for answer in ("none", "live"):
            e, clock, pub, x = make_engine()
            e.id_is_hash = True
            m = pub.list_window("btc", T0 + 1800)
            await tick(e)
            (q,) = quotes(e)
            oid = q["orders"]["up"]["id"]
            q["phase"] = "closing"
            q["orders"]["up"].update(state="unknown", id="", hash=oid, sent=clock.t - 200, absent=1)
            e._index(q)
            asked, release = asyncio.Event(), asyncio.Event()
            stale = Row(oid, q["orders"]["up"]["asset"], m["conditionId"], "BUY", D("0.49"), D("10"), D(0), "LIVE")

            async def slow(order_id):
                asked.set()
                await release.wait()
                return None if answer == "none" else stale
            x.order = slow
            lookup = asyncio.create_task(e.look_up(q, "up"))
            await asked.wait()
            await e.cancel_pass()                                  # cancelled by its hash meanwhile
            self.assertEqual(q["orders"]["up"]["state"], "gone")
            release.set()
            await lookup
            self.assertEqual(q["orders"]["up"]["state"], "gone", answer)   # was not_found / open

    async def test_11_a_trades_copy_older_than_the_order_leaving_is_read_afresh(self):
        e, clock, pub, x = make_engine(settings(PREOPEN_LEVELS="48x10"))
        m = pub.list_window("btc", T0 + 1800)
        await tick(e)
        _, deeper = quotes(e)
        await e.trades_of(m["conditionId"])                        # a copy kept for another order
        clock.advance(3)
        x.fill(deeper["orders"]["up"]["id"], D("10"))
        deeper["orders"]["up"].update(state="gone", gone=clock.t, read_at=clock.t + 5)
        clock.advance(5)

        async def no_order(oid):
            return None
        x.order = no_order
        await e.read_final(deeper, "up")
        self.assertEqual((deeper["orders"]["up"]["state"], D(deeper["orders"]["up"]["filled"])), ("final", D("10")))   # was 0

    async def test_12_ghosts_outlive_a_restart_and_one_that_rested_is_read_for_fills(self):
        e, clock, pub, x = make_engine()
        pub.list_window("btc", T0 + 1800)
        x.reject["btc-updown-5m-%d#0@0/up" % (T0 + 1800)] = ("not_sent", "rate_limited")
        x.reject["btc-updown-5m-%d#0@0/down" % (T0 + 1800)] = ("not_sent", "rate_limited")
        await e.account_once()
        await e.discover()
        await drain(e)                                             # placed; the cancel pass not run yet
        self.assertEqual(len(e.ghosts), 2)
        self.assertEqual({g["side"] for g in e.ghosts.values()}, {"up", "down"})
        e2, *_ = make_engine(ledger=e.ledger, clock=clock, exchange=x)
        self.assertEqual(e2.ghosts, e.ghosts)                      # kept in the ledger
        h = next(h for h, g in e2.ghosts.items() if g["side"] == "up")
        x.book[h] = Row(h, "1", "0xc", "BUY", D("0.49"), D("10"), D("3"), "LIVE")   # it rested after all, 3 bought
        clock.advance(6)
        await e2.ghost_pass(clock.t)
        self.assertEqual(e2.ghosts, {})
        self.assertEqual(json.loads(e2.ledger.get("ghosts")), {})
        self.assertIn("成交 3 份，已补进账本", e2.errors[-1][1])   # audit 11-28: back in the ledger (was: said only)
        self.assertIn(mk.window_name("btc", T0 + 1800) + " 涨方", e2.errors[-1][1])
        (back,) = [q for q in e2.ledger.rows.values() if q["orders"]["up"].get("id") == h]
        self.assertEqual((back["phase"], back["orders"]["up"]["filled"], back["orders"]["down"]["state"]), ("closed", "3", "not_sent"))


class ExchangeAnswersTest(unittest.IsolatedAsyncioTestCase):
    async def test_9_only_a_plain_cancel_counts_as_one_made_elsewhere(self):
        e, clock, pub, x = make_engine()
        pub.list_range(["btc"], mk.window_starts(clock.t, e.s)[:3])
        await tick(e)
        for q in quotes(e):
            oid = q["orders"]["up"]["id"]
            row = x.book.pop(oid)
            row.status = "EXPIRED"
            x.gone[oid] = row
        await tick(e, 10)
        await tick(e, 6)
        self.assertFalse(e.paused)                                 # was: paused, everything withdrawn
        self.assertTrue(all(not q.get("counted") for q in quotes(e)))
        self.assertIn("到期", " ".join(text for _, text in e.errors))
        self.assertEqual(e.clock_at, 0.0)                          # the clock is checked again at once

    async def test_13_one_refused_request_is_one_refusal(self):
        e, clock, pub, x = make_engine(settings(PREOPEN_PRICE_CENTS="49", PREOPEN_LEVELS="48x10,47x10,46x10,45x10"))
        m = pub.list_window("btc", T0 + 1800)
        x.refuse_market[m["conditionId"]] = ("rejected", "invalid_expiration")
        await tick(e)
        self.assertEqual((e.refused_streak, round(e.refused_until - clock.t)), (1, 30))   # was 5 and 480
        clock.advance(31)
        await tick(e)
        self.assertEqual((e.refused_streak, round(e.refused_until - clock.t)), (2, 60))   # refused again: doubled

    async def test_10_a_fee_schedule_that_does_not_say_is_not_quoted(self):
        e, clock, pub, x = make_engine()
        start = T0 + 1800
        pub.list_window("btc", start, feeSchedule={"rate": 0.07, "exponent": 1})       # takerOnly missing
        pub.list_window("eth", start, makerBaseFee=1000, takerBaseFee=1000)           # as every real market: taker-only
        await tick(e)
        self.assertEqual({q["coin"] for q in quotes(e)}, {"eth"})
        self.assertIn("feeSchedule.takerOnly 缺失", " ".join(text for _, text in e.errors))
        self.assertEqual(e.why_not(e.listings[mk.slug_of("btc", start)]), "没说挂单收不收手续费")
        listing, _ = mk.parse_listing(pub.listed[mk.slug_of("btc", start)] | {"feeSchedule": {"takerOnly": False}}, "btc", start)
        self.assertTrue(listing.maker_fee)

    async def test_10_a_market_resolved_50_50_settles(self):
        x = PaperExchange(D("1000"), clock=Clock())
        x.live = True
        e, clock, pub, x = make_engine(exchange=x, clock=x.clock)
        q = await settle_win(e, clock, pub, x, result=("0.5", "0.5"))
        self.assertEqual((q["phase"], q["result"], q["pnl"], q["redeem"]), ("settled", "split", 0.1, "pending"))   # 10 x 0.5 - 4.90
        self.assertEqual(e.exposure(), D(0))
        review = journal.journal(e.all_records(), clock.t, 60)
        self.assertEqual(review["totals"]["all"]["pnl"], 0.1)
        self.assertEqual(review["by_market"][0]["result"], "五五")
        self.assertEqual(review["model"]["total"]["pnl"], 0.1)
        self.assertIn("按 50/50 结算", " ".join(text for _, text in e.errors))

    async def test_15_a_v2_allowance_below_one_window_holds_v2_alone(self):
        e, clock, pub, x = make_engine()
        x.v2_allowance = e.s.window_usd - D("0.01")
        self.assertIn("不够一场", e.v2_blocked())
        x.v2_allowance = e.s.window_usd
        self.assertEqual(e.v2_blocked(), "")

    async def test_the_clock_is_checked_again_and_odd_readings_are_not_believed(self):
        e, clock, pub, x = make_engine()
        for server, skew in ((clock.t + 5, 5.0), (clock.t + 0.4, 0.0), (clock.t + 9 * 3600, 0.0)):
            async def server_time(value=server):
                return value
            pub.server_time = server_time
            await e.sync_clock()
            self.assertEqual(round(e.skew, 1), skew)
        self.assertIn("不像真的", e.errors[-1][1])


class RecordsTest(unittest.IsolatedAsyncioTestCase):
    async def test_3_winnings_not_claimed_are_archived_and_claimed_when_the_key_comes(self):
        x = PaperExchange(D("1000"), clock=Clock())
        x.live = True
        x.redeem_problem = lambda: "没配 Relayer Key"
        e, clock, pub, x = make_engine(exchange=x, clock=x.clock)
        q = await settle_win(e, clock, pub, x)
        self.assertEqual((q["redeem"], bool(q.get("redeem_error"))), ("manual", True))
        clock.advance(2 * 86400 + 600)
        self.assertEqual(e.ledger.archive_old(clock.t), 1)        # the working set stays short...
        self.assertEqual(e.ledger.unclaimed_count(), 1)           # ...and the win is not forgotten
        self.assertIn("更早的 1 场已归档", e.check_text())
        x.redeem_problem = lambda: ""                              # the key put in, the process restarted
        e2, *_ = make_engine(ledger=e.ledger, clock=clock, exchange=x)
        self.assertEqual(e2.redeem_again(), 1)                     # was 0
        self.assertEqual((e2.ledger.rows[q["key"]]["redeem"], e2.ledger.unclaimed_count()), ("retry", 0))
        await e2.redeem_pass()
        self.assertEqual(e2.ledger.rows[q["key"]]["redeem"], "done")

    async def test_7_a_write_that_fails_rolls_back_and_holds_new_orders(self):
        with tempfile.TemporaryDirectory() as d:
            led = Ledger(str(Path(d) / "t.sqlite3"))
            led.save({"key": "k", "slug": "btc-updown-5m-1000", "start": 1000, "phase": "settled", "redeem": "done",
                      "orders": {"up": {"state": "final"}, "down": {"state": "final"}}})
            led.conn.execute("CREATE TRIGGER no_delete BEFORE DELETE ON quotes BEGIN SELECT RAISE(ABORT, 'disk'); END;")
            led.conn.commit()
            with self.assertRaises(sqlite3.Error):
                led.archive_old(1000 + 3 * 86400)
            self.assertFalse(led.conn.in_transaction)
            led.put("x", "1")                                      # an unrelated write commits nothing half-done
            counts = [led.conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0] for t in ("quotes", "archive")]
            self.assertEqual(counts, [1, 0])                       # was [1, 1]: in both tables
            led.close()
        e, clock, pub, x = make_engine()
        pub.list_window("btc", T0 + 1800)
        await tick(e)
        (q,) = quotes(e)
        real = e.ledger.save_many
        e.ledger.save_many = mock.Mock(side_effect=sqlite3.OperationalError("database or disk is full"))
        q["note"] = "changed"
        e.save(q)                                                  # the loops go on...
        self.assertIn("账本写不进去", e.hold())                     # ...but nothing new is placed
        self.assertIn(q["key"], e.unsaved)
        self.assertIn("账本：写不进去", e.check_text())
        e.ledger.save_many = real
        e.save()                                                   # the next write that works writes them too
        self.assertEqual((e.hold(), e.unsaved), ("", {}))

    async def test_8_another_accounts_records_count_nowhere_here(self):
        x = PaperExchange(D("1000"), clock=Clock())
        x.live = True
        e, clock, pub, x = make_engine(exchange=x, clock=x.clock)
        lost = await settle_win(e, clock, pub, x, result=("0", "1"))   # -4.90, settled under the old account
        later = mk.window_starts(clock.t, e.s)[0]
        m2 = pub.list_window("btc", later)
        await tick(e)
        (open_one,) = quotes(e, m2["slug"])
        x.fill(open_one["orders"]["up"]["id"], D("10"))
        await tick(e, 10)
        await tick(e, 6)
        e.ledger.put("identity", "0xold|0xoldsigner")
        e.blocked = e.check_identity()
        e.rebind()
        e2, *_ = make_engine(settings(PREOPEN_MAX_DAILY_LOSS_USD="4"), ledger=e.ledger, clock=clock, exchange=x, public=pub)
        self.assertTrue(lost["foreign"])                           # was: only the open ones were marked
        self.assertEqual((e2.day_pnl(), e2.risk_hold(), e2.exposure()), (D(0), "", D(0)))   # was -4.90, held
        clock.t = later + 400
        pub.results[m2["slug"]] = {**m2, "closed": True, "outcomePrices": '["1", "0"]'}
        await tick(e2)
        await tick(e2, 30)
        self.assertEqual((open_one["phase"], open_one["redeem"]), ("settled", "none"))   # was pending: claimed here
        self.assertEqual(e2.today()["pnl"], 0.0)
        self.assertEqual(journal.journal(e2.all_records(), clock.t)["totals"]["today"]["pnl"], 0.0)

    async def test_14_what_is_kept_per_window_is_let_go(self):
        e, clock, pub, x = make_engine()
        old = mk.slug_of("btc", int(clock.t) - 7200)
        e.swept["0xold"], e.swept["0xnew"] = clock.t - 2 * 86400, clock.t
        e.refused_at[old], e.token_refusals[old], e.token_refusals[mk.slug_of("btc", T0 + 1800)] = clock.t - 2 * 86400, 3, 1
        e.prune_memory()
        self.assertEqual((set(e.swept), e.refused_at, set(e.token_refusals)), ({"0xnew"}, {}, {mk.slug_of("btc", T0 + 1800)}))
        size = e.ledger.size()
        self.assertIsNone(size)                                    # in memory: nothing to say about the disk

    def test_16_review_days_and_the_offline_status(self):
        for words in (["-5"], ["0"], ["1000000"], ["nan"], ["inf"]):
            with self.assertRaisesRegex(ValueError, "天数"):
                journal.span_args(words, T0)
        self.assertEqual(journal.span_args(["0.5"], T0)[0], (T0 - 43200, float("inf")))
        self.assertIn("天数要在", main.review(make_engine()[0], ["-5"]))
        with tempfile.TemporaryDirectory() as d:
            s = settings(PREOPEN_DB=str(Path(d) / "s.sqlite3"))
            led = Ledger(s.ledger_path)
            rec = {"key": "a", "slug": "btc-updown-5m-1000", "start": 1000, "phase": "settled", "pnl": -2.5,
                   "orders": {"up": {"state": "final"}, "down": {"state": "final"}}}
            led.save(rec)
            led.save({**rec, "key": "b", "start": 2000, "pnl": 1.0})
            led.archive_old(10 * 86400)
            led.save({**rec, "key": "c", "start": 9 * 86400, "pnl": 4.0})
            led.close()
            text = main.offline_status(s)
            self.assertIn("已结算盈亏合计 +2.50 美元（近两天 +4.00，归档的 -1.50", text)


class ConsoleTest(unittest.IsolatedAsyncioTestCase):
    async def test_5_wrong_keys_in_a_header_are_limited_like_logins_and_by_source(self):
        class App:
            def report(self):
                return {"ok": True}
        s = web.WebServer(0, App(), web_key="correct-horse-battery")

        async def no_wait(_):
            return None
        a, b = {"x-forwarded-for": "6.6.6.6, 1.1.1.1"}, {"x-forwarded-for": "2.2.2.2"}
        with mock.patch.object(web, "LOGIN_FAILS_MAX", 3), mock.patch.object(web.asyncio, "sleep", no_wait):
            for i in range(3):
                self.assertEqual((await s.respond("GET", "/api/status", {**a, "authorization": f"Bearer guess{i}"}, b""))[0], 401)
            right = {**a, "authorization": "Bearer correct-horse-battery"}
            self.assertEqual((await s.respond("GET", "/api/status", right, b""))[0], 429)    # that source: locked
            self.assertEqual((await s.respond("POST", "/api/login", a, b'{"key": "correct-horse-battery"}'))[0], 429)
            ok = {**b, "authorization": "Bearer correct-horse-battery"}
            self.assertEqual((await s.respond("GET", "/api/status", ok, b""))[0], 200)       # another one: not
        self.assertEqual(list(s.fails), ["1.1.1.1"])                # the proxy's last word, not the client's

    async def test_5_requests_are_bounded(self):
        class App:
            def report(self):
                return {"ok": True}
        s = web.WebServer(0, App(), web_key="k" * 20, host="127.0.0.1")
        await s.start()
        port = s.server.sockets[0].getsockname()[1]

        async def ask(raw: bytes) -> bytes:
            r, w = await asyncio.open_connection("127.0.0.1", port)
            w.write(raw)
            await w.drain()
            out = await r.read()
            w.close()
            return out.split(b"\r\n", 1)[0]
        many = b"GET /health HTTP/1.1\r\n" + b"".join(f"X-H-{i}: a\r\n".encode() for i in range(100)) + b"\r\n"
        self.assertEqual(await ask(many), b"HTTP/1.1 431 Request Header Fields Too Large")   # was 200
        self.assertEqual(await ask(b"GET /health HTTP/1.1\r\nX-Big: " + b"a" * 20000 + b"\r\n\r\n"), b"HTTP/1.1 400 Bad Request")
        self.assertEqual(await ask(b"GET /health HTTP/1.1\r\n\r\n"), b"HTTP/1.1 200 OK")
        s.busy = web.CONNECTIONS_MAX
        self.assertEqual(await ask(b"GET /health HTTP/1.1\r\n\r\n"), b"HTTP/1.1 503 Service Unavailable")
        s.busy = 0
        await s.close()

    async def test_6_leaving_live_trading_stops_first_and_does_not_switch_with_orders_left(self):
        env = {**ENV, "LIVE": "on", "POLY_PRIVATE_KEY": "0x" + "11" * 32, "PREOPEN_KEY_EXCLUSIVE": "on"}
        x = PaperExchange(D("1000"), clock=Clock())
        x.live = True
        e, clock, pub, x = make_engine(config.from_env(env), exchange=x, clock=x.clock)
        pub.list_window("btc", T0 + 1800)
        await tick(e)
        seen = {}
        real = x.cancel_all

        async def cancel_all():
            seen["stopping"] = e.stopping
            return await real()
        x.cancel_all = cancel_all

        async def stuck(reason, deadline_s=60.0):
            e.stopping = e.stopping or reason
            return 2                                               # two orders not confirmed cancelled
        e.withdraw_all = stuck
        path = Path(tempfile.mkdtemp()) / "c.json"
        c = Console(e, env, path, {})
        ok, text = await c.change({"name": "LIVE", "value": "off"})
        self.assertEqual(seen["stopping"], "切换到模拟盘")          # nothing new placed while cancelling (was "")
        self.assertFalse(ok)
        self.assertIn("没切换：还有 2 张", text)
        self.assertFalse(e.restart)
        self.assertEqual((e.paused, e.stopping), (True, ""))
        self.assertEqual(config.load_overrides(path), {})          # LIVE stays on after a restart
        e.withdraw_all = lambda reason, deadline_s=60.0: asyncio.sleep(0, 0)
        ok, text = await c.change({"name": "LIVE", "value": "off"})
        self.assertTrue(ok and e.restart)
        self.assertEqual(config.load_overrides(path), {"LIVE": "off"})

    def test_17_secrets_stay_out_of_reprs_logs_and_cut_values(self):
        s = config.from_env({**ENV, "POLY_PRIVATE_KEY": "0x" + "ab" * 32, "TELEGRAM_BOT_TOKEN": "123:SECRET", "WEB_KEY": "w" * 20})
        self.assertNotIn("abab", repr(s))
        self.assertNotIn("SECRET", repr(s))
        from preopen.telegram import Telegram
        tg = Telegram.__new__(Telegram)
        tg.token = "123:SECRET"
        self.assertEqual(tg.scrub(RuntimeError("POST https://api.telegram.org/bot123:SECRET/getUpdates failed")),
                         "POST https://api.telegram.org/bot<TELEGRAM_BOT_TOKEN>/getUpdates failed")
        with tempfile.TemporaryDirectory() as d, mock.patch.dict("os.environ", {}, clear=True):
            (Path(d) / ".env").write_text('WEB_KEY="pa #ss" # a comment\nOTHER=plain # comment\n', encoding="utf-8")
            config.load_dotenv(str(Path(d) / ".env"))
            import os
            self.assertEqual((os.environ["WEB_KEY"], os.environ["OTHER"]), ("pa #ss", "plain"))


class PublicApiTest(unittest.IsolatedAsyncioTestCase):
    async def test_one_failed_request_does_not_throw_away_the_others(self):
        def handler(request: httpx.Request) -> httpx.Response:
            if request.url.params.get("version") == "v2":
                return httpx.Response(500)
            return httpx.Response(200, json=[{"slug": s, "version": "v1"} for s in request.url.params.get_list("slug")])
        api = gamma.PublicApi(client=httpx.AsyncClient(transport=httpx.MockTransport(handler)))
        with mock.patch.object(gamma, "RETRIES", 0, create=True):
            found = await api.listings([f"btc-updown-5m-{T0 + 300 * i}" for i in range(3)])
        self.assertEqual(len(found), 3)                            # the v1 ones, read; the v2 request failed
        await api.close()


if __name__ == "__main__":
    unittest.main()
