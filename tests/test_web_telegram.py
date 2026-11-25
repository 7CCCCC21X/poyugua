"""The web console (sign-in, routes, a real socket round trip) and the Telegram buttons (keyboard, menu, the pause
confirmation)."""
from decimal import Decimal as D
import asyncio
import hashlib
import hmac
import json
import time
import unittest
import urllib.parse

from helpers import make_engine, settings, tick

import main
from preopen import journal, markets as mk
from preopen import web
from preopen.telegram import BUTTONS, CONFIRM_PAUSE, Reply, Telegram

TOKEN = "123456:TEST-token"
ADMIN = "777"


def init_data(user_id: str = ADMIN, token: str = TOKEN, auth_date: int | None = None, extra: dict | None = None) -> str:
    fields = {"auth_date": str(auth_date or int(time.time())), "query_id": "AAE", "user": json.dumps({"id": int(user_id), "first_name": "x"})}
    fields.update(extra or {})
    check = "\n".join(f"{k}={v}" for k, v in sorted(fields.items()))
    secret = hmac.new(b"WebAppData", token.encode(), hashlib.sha256).digest()
    fields["hash"] = hmac.new(secret, check.encode(), hashlib.sha256).hexdigest()
    return urllib.parse.urlencode(fields)


class InitDataTest(unittest.TestCase):
    def test_valid_and_invalid(self):
        self.assertTrue(web.check_init_data(init_data(), TOKEN, ADMIN))
        self.assertTrue(web.check_init_data(init_data(extra={"signature": "abc"}), TOKEN, ADMIN))
        self.assertFalse(web.check_init_data(init_data(user_id="778"), TOKEN, ADMIN))           # someone else
        self.assertFalse(web.check_init_data(init_data(token="999:other"), TOKEN, ADMIN))        # another bot's
        self.assertFalse(web.check_init_data(init_data(auth_date=int(time.time()) - 8 * 86400), TOKEN, ADMIN))
        tampered = init_data().replace("first_name", "first_nam3")
        self.assertFalse(web.check_init_data(tampered, TOKEN, ADMIN))
        self.assertFalse(web.check_init_data("", TOKEN, ADMIN))
        self.assertFalse(web.check_init_data(init_data(), TOKEN, ""))
        self.assertFalse(web.check_init_data(init_data(), TOKEN, ()))
        admins = ("555", ADMIN)  # TELEGRAM_ADMIN_IDS: any of them
        self.assertTrue(web.check_init_data(init_data(), TOKEN, admins))
        self.assertTrue(web.check_init_data(init_data("555"), TOKEN, admins))
        self.assertFalse(web.check_init_data(init_data("556"), TOKEN, admins))


class FakeApp:
    ACTIONS = ("pause",)

    def __init__(self):
        self.calls = []

    def report(self):
        return {"ok": 1, "mode": "模拟"}

    def settings(self):
        return {"items": []}

    async def orders(self, tab, coin, limit):
        return {"tab": tab, "coin": coin, "limit": limit}

    async def review(self, query=None):
        self.calls.append(("review", dict(query or {})))
        return {"error": "看不懂的时间：x"} if (query or {}).get("from") == "x" else {"totals": {}}

    async def price_detail(self, query=None):
        self.calls.append(("price", dict(query or {})))
        return {"price": 49.0, "rows": []}

    async def xlsx(self, query=None):
        self.calls.append(("xlsx", dict(query or {})))
        if (query or {}).get("from") == "x":
            raise ValueError("看不懂的时间：x")
        return "复盘_今天_20261121-0930.xlsx", b"PK\x03\x04 workbook"

    async def csv(self, kind, query=None):
        self.calls.append(("csv", kind, dict(query or {})))
        return f"{kind}.csv", "\ufeffa,b\n1,2\n"

    async def change(self, body):
        self.calls.append(("change", body))
        return body.get("name") == "PREOPEN_SHARES", "saved"

    async def action(self, body):
        self.calls.append(("action", body["action"]))
        return "paused"


class WebServerTest(unittest.IsolatedAsyncioTestCase):
    def server(self, key: str = "correct-horse-battery"):
        self.app = FakeApp()
        return web.WebServer(0, self.app, web_key=key, bot_token=TOKEN, admin_id=ADMIN)

    async def test_routes_and_sign_in(self):
        s = self.server()
        status, ctype, body, _ = await s.respond("GET", "/", {}, b"")
        self.assertEqual(status, 200)
        self.assertIn(b"telegram-web-app.js", body)
        self.assertEqual((await s.respond("GET", "/health", {}, b""))[0], 200)
        self.assertEqual((await s.respond("GET", "/api/status", {}, b""))[0], 401)
        self.assertEqual((await s.respond("POST", "/api/login", {}, b'{"key": "nope"}'))[0], 401)
        status, _, _, extra = await s.respond("POST", "/api/login", {}, b'{"key": "correct-horse-battery"}')
        self.assertEqual(status, 200)
        cookie = dict(extra)["Set-Cookie"].split(";")[0]
        self.assertIn("HttpOnly", dict(extra)["Set-Cookie"])
        status, _, body, _ = await s.respond("GET", "/api/status", {"cookie": cookie}, b"")
        self.assertEqual((status, json.loads(body)["ok"]), (200, 1))
        # the page knows its version and the status says the running one: a page left open across a deploy reloads
        page = (await s.respond("GET", "/", {}, b""))[2].decode()
        version = json.loads(body)["page"]
        self.assertRegex(version, r"^[0-9a-f]{12}$")
        self.assertIn(f'const PAGE_VERSION = "{version}";', page)
        self.assertNotIn("__PAGE_VERSION__", page)
        self.assertEqual((await s.respond("GET", "/api/status", {"x-telegram-init-data": init_data()}, b""))[0], 200)
        self.assertEqual((await s.respond("GET", "/api/status", {"x-telegram-init-data": init_data("5")}, b""))[0], 401)
        self.assertEqual((await s.respond("GET", "/api/status", {"authorization": "Bearer correct-horse-battery"}, b""))[0], 200)

    async def test_actions_need_sign_in_and_json(self):
        s = self.server()
        auth = {"x-telegram-init-data": init_data()}
        self.assertEqual((await s.respond("POST", "/api/action", {}, b'{"action": "pause"}'))[0], 401)
        self.assertEqual((await s.respond("POST", "/api/action", auth, b'{"action": "pause"}'))[0], 400)  # not JSON-typed
        auth["content-type"] = "application/json"
        self.assertEqual((await s.respond("POST", "/api/action", auth, b'{"action": "rm -rf"}'))[0], 400)
        status, _, body, _ = await s.respond("POST", "/api/action", auth, b'{"action": "pause"}')
        self.assertEqual((status, json.loads(body)["message"], self.app.calls), (200, "paused", [("action", "pause")]))

    async def test_settings_orders_journal_routes(self):
        s = self.server()
        auth = {"x-telegram-init-data": init_data(), "content-type": "application/json"}
        self.assertEqual((await s.respond("GET", "/api/settings", {}, b""))[0], 401)
        self.assertEqual((await s.respond("GET", "/api/settings", auth, b""))[0], 200)
        status, _, body, _ = await s.respond("POST", "/api/settings", auth, b'{"name": "PREOPEN_SHARES", "value": "20"}')
        self.assertEqual((status, json.loads(body)["ok"]), (200, True))
        status, _, body, _ = await s.respond("POST", "/api/settings", auth, b'{"name": "PREOPEN_PRICE_CENTS", "value": "99"}')
        self.assertEqual((status, json.loads(body)["ok"]), (400, False))
        status, _, body, _ = await s.respond("GET", "/api/orders?tab=filled&coin=btc&limit=5", auth, b"")
        self.assertEqual(json.loads(body), {"tab": "filled", "coin": "btc", "limit": 5})
        self.assertEqual((await s.respond("GET", "/api/journal", auth, b""))[0], 200)
        status, _, body, _ = await s.respond("GET", "/api/journal?range=custom&from=x&to=11-20", auth, b"")
        self.assertEqual((status, json.loads(body)["error"]), (400, "看不懂的时间：x"))   # the filter's words, said
        status, ctype, body, extra = await s.respond("GET", "/api/journal.csv?kind=daily&range=yesterday", auth, b"")
        self.assertEqual((status, ctype.split(";")[0]), (200, "text/csv"))
        self.assertIn("daily.csv", dict(extra)["Content-Disposition"])
        self.assertEqual(s.app.calls[-1], ("csv", "daily", {"kind": "daily", "range": "yesterday"}))  # the CSV follows the filter
        status, ctype, body, extra = await s.respond("GET", "/api/journal.xlsx?range=today", auth, b"")
        self.assertEqual((status, body[:2]), (200, b"PK"))
        self.assertIn("spreadsheetml", ctype)
        self.assertIn("filename*=UTF-8''%E5%A4%8D%E7%9B%98", dict(extra)["Content-Disposition"])   # 复盘_… for every browser
        self.assertEqual(s.app.calls[-1], ("xlsx", {"range": "today"}))
        for page in ("/", "/control", "/journal"):
            self.assertEqual((await s.respond("GET", page, {}, b""))[0], 200)

    async def test_without_a_key_only_telegram_signs_in(self):
        s = self.server(key="")
        self.assertEqual((await s.respond("POST", "/api/login", {}, b'{"key": ""}'))[0], 401)
        self.assertEqual((await s.respond("GET", "/api/status", {"x-telegram-init-data": init_data()}, b""))[0], 200)

    async def test_a_real_socket_round_trip(self):
        s = self.server()
        s.port, s.host = 0, "127.0.0.1"
        await s.start()
        port = s.server.sockets[0].getsockname()[1]

        async def request(raw: bytes) -> bytes:
            reader, writer = await asyncio.open_connection("127.0.0.1", port)
            writer.write(raw)
            await writer.drain()
            data = await reader.read()
            writer.close()
            return data
        page = await request(b"GET / HTTP/1.1\r\nHost: x\r\n\r\n")
        self.assertTrue(page.startswith(b"HTTP/1.1 200 OK"))
        self.assertIn(b"Content-Type: text/html", page)
        body = b'{"key": "correct-horse-battery"}'
        login = await request(b"POST /api/login HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\nContent-Length: "
                              + str(len(body)).encode() + b"\r\n\r\n" + body)
        self.assertIn(b"Set-Cookie: preopen_session=", login)
        self.assertTrue((await request(b"garbage\r\n\r\n")).startswith(b"HTTP/1.1 400"))
        await s.close()

    async def test_the_engine_report_is_json(self):
        e, clock, pub, x = make_engine()
        pub.list_range(["btc", "eth"], mk.window_starts(clock.t, e.s)[:2])
        await tick(e)
        report = json.loads(json.dumps(e.report(), ensure_ascii=False))
        self.assertEqual(report["windows"], 4)
        self.assertEqual(len(report["upcoming"]), 4)
        self.assertEqual(report["upcoming"][0]["up"], "open")


class FakeTelegram(Telegram):
    def __init__(self, **kw):
        super().__init__(TOKEN, ADMIN, **kw)
        self.posts = []
        self.documents = []

    async def _post(self, method, **payload):
        self.posts.append((method, payload))
        return True

    async def _send_document(self, name, data, caption):
        self.documents.append((name, data, caption))


class TelegramTest(unittest.IsolatedAsyncioTestCase):
    async def test_buttons_map_to_commands_and_bring_the_keyboard(self):
        tg = FakeTelegram()
        seen = []

        async def handle(cmd, args):
            seen.append(cmd)
            return "ok"
        for text, cmd in BUTTONS.items():
            await tg.on_message({"chat": {"id": int(ADMIN)}, "from": {"id": int(ADMIN)}, "text": text}, handle)
        self.assertEqual(seen, list(BUTTONS.values()))
        await tg.on_message({"chat": {"id": 1}, "from": {"id": 1}, "text": "📊 状态"}, handle)  # not the admin: ignored
        self.assertEqual(len(seen), len(BUTTONS))
        text, markup, _ = tg.outbox.get_nowait()
        self.assertEqual(markup["keyboard"][0][0]["text"], "📊 状态")

    async def test_pause_asks_then_confirms_by_callback(self):
        e, clock, pub, x = make_engine()
        pub.list_range(["btc"], mk.window_starts(clock.t, e.s)[:2])
        await tick(e)
        handle = main.commander(settings(), e, pub)
        reply = await handle("/pause", [])
        self.assertIsInstance(reply, Reply)
        self.assertEqual(reply.markup["inline_keyboard"][0][0]["callback_data"], CONFIRM_PAUSE)
        self.assertFalse(e.paused)
        tg = FakeTelegram()
        await tg.on_callback({"id": "cb1", "data": CONFIRM_PAUSE, "from": {"id": int(ADMIN)},
                              "message": {"chat": {"id": int(ADMIN)}, "message_id": 9}}, handle)
        self.assertTrue(e.paused)
        methods = [m for m, _ in tg.posts]
        self.assertEqual(methods, ["answerCallbackQuery", "editMessageText"])
        self.assertIn("已暂停", tg.posts[1][1]["text"])
        e.resume()
        await tg.on_callback({"id": "cb2", "data": "cancel", "from": {"id": int(ADMIN)},
                              "message": {"chat": {"id": int(ADMIN)}, "message_id": 10}}, handle)
        self.assertFalse(e.paused)
        self.assertEqual(tg.posts[-1][1]["text"], "已取消")

    async def test_menu_button_opens_the_console_when_there_is_a_domain(self):
        tg = FakeTelegram(web_url="https://bot.up.railway.app")
        await tg.setup()
        self.assertEqual(tg.posts[0][0], "setMyCommands")
        self.assertEqual(tg.posts[1][1]["menu_button"]["web_app"]["url"], "https://bot.up.railway.app")
        plain = FakeTelegram()
        await plain.setup()
        self.assertEqual(plain.posts[1][1]["menu_button"], {"type": "commands"})

    async def test_review_and_export_send_the_records_as_files(self):
        e, clock, pub, x = make_engine()
        starts = mk.window_starts(clock.t, e.s)[:2]
        ma = pub.list_window("btc", starts[0])
        pub.list_window("btc", starts[1])
        await tick(e)
        q = next(q for q in e.ledger.rows.values() if q["start"] == starts[0])
        x.fill(q["orders"]["up"]["id"], D("10"))
        clock.t = starts[0] + 400
        pub.results[ma["slug"]] = {**ma, "closed": True, "outcomePrices": '["1", "0"]'}
        for _ in range(4):
            await tick(e, 30)
        handle = main.commander(settings(), e, pub)

        async def say(cmd, args):
            reply = await handle(cmd, args)
            return reply.text if isinstance(reply, Reply) else reply
        review = await handle("/review", [])
        self.assertTrue(review.text.splitlines()[1].startswith("结论："))  # the conclusion first
        self.assertIn("【如果提前撤单】", review.text)
        self.assertIn("什么时候成交的", review.text)
        buttons = [b["callback_data"] for row in review.markup["inline_keyboard"] for b in row]
        self.assertEqual(buttons[-5:], ["rev_today", "rev_yesterday", "rev_7", "rev_30", "rev_all"])   # another time, a tap away
        self.assertIn("（BTC、最近 7 天）", await say("/review", ["btc", "7"]))
        self.assertIn("报价 0 场", await say("/review", ["eth"]))  # only ETH: nothing quoted
        self.assertIn("看不懂：abc", await say("/review", ["abc"]))
        text = await say("/review", [])
        self.assertIn("【每日】", text)
        self.assertIn("【按市场】1 场有成交", text)
        self.assertIn("赚最多：BTC", text)
        self.assertIn("成交额 $4.90", text)
        day = journal.day_of(starts[0])[5:]
        self.assertIn(f"合计（{day}）：报价 2 场，成交 1 场", await say("/review", [day]))
        self.assertIn("合计（昨天）：报价 0 场", await say("/review", ["昨天"]))
        self.assertIn("合计（昨天）：报价 0 场", await say("/rev_yesterday", []))   # the button
        self.assertIn("没有这个时间：13-40", await say("/review", ["13-40"]))
        # /export alone: the time as buttons; a time: one Excel file
        choose = await handle("/export", [])
        self.assertEqual([b["callback_data"] for row in choose.markup["inline_keyboard"] for b in row],
                         ["exp_today", "exp_yesterday", "exp_7", "exp_30", "exp_all"])
        self.assertEqual(choose.files, [])
        book = await handle("/exp_all", [])
        (name, data), = book.files
        self.assertTrue(name.startswith("复盘_全部_") and name.endswith(".xlsx"))
        self.assertEqual(data[:2], b"PK")
        self.assertIn("1 场有成交，成交额 $4.90，已结算盈亏 +5.10 美元", book.text)
        self.assertTrue((await handle("/export", ["7"])).files[0][0].startswith("复盘_最近7天_"))
        # the CSV files, as before: /export csv [time]
        reply = await handle("/export", ["csv"])
        self.assertEqual([n.split("_")[0] for n, _ in reply.files], ["市场盈亏", "每日汇总", "成交明细", "全部报价", "撤单时间复盘"])
        markets = dict(reply.files)[next(n for n, _ in reply.files if n.startswith("市场盈亏"))].decode("utf-8")
        self.assertIn("BTC", markets)
        self.assertIn(",涨,5.1", markets)                          # up won: 10 shares paid $4.90
        self.assertIn("看不懂：abc", (await handle("/export", ["abc"])).text)
        self.assertIn("导出昨天的记录：0 条报价", (await handle("/export", ["csv", "昨天"])).text)
        fills = dict(reply.files)[next(n for n, _ in reply.files if n.startswith("成交明细"))].decode("utf-8")
        self.assertTrue(fills.startswith("\ufeff"))
        self.assertIn("涨成交时刻(开场前秒:份)", fills)
        self.assertIn(",是,", fills)  # the fill's time is the trade's own
        # on Telegram: 📤 导出 asks for the time; a tap sends the file after the message, the question stays
        tg = FakeTelegram()
        await tg.on_message({"chat": {"id": int(ADMIN)}, "from": {"id": int(ADMIN)}, "text": "📤 导出"}, handle)
        asked = tg.outbox.get_nowait()
        self.assertIn("导出哪段时间", asked[0])
        self.assertEqual(asked[1]["inline_keyboard"][0][0]["callback_data"], "exp_today")
        await tg.on_callback({"id": "c", "data": "exp_7", "from": {"id": int(ADMIN)},
                              "message": {"chat": {"id": int(ADMIN)}, "message_id": 5}}, handle)
        self.assertEqual([m_ for m_, _ in tg.posts], ["answerCallbackQuery", "editMessageReplyMarkup"])
        task = asyncio.create_task(tg.sender())
        for _ in range(100):
            await asyncio.sleep(0.05)
            if tg.documents:
                break
        task.cancel()
        self.assertEqual(len(tg.documents), 1)
        self.assertTrue(tg.documents[0][0].endswith(".xlsx"))
        self.assertEqual(tg.posts[2][0], "sendMessage")  # the text first, then the file

    async def test_rejects_explains_the_refusals_and_checks_them_live(self):
        e, clock, pub, x = make_engine()
        start = mk.window_starts(clock.t, e.s)[0]
        m = pub.list_window("btc", start)
        handle = main.commander(settings(), e, pub)
        self.assertIn("没有被拒的单", await handle("/rejects", []))
        x.refuse_market[m["conditionId"]] = ("rejected", "token_not_ready")
        await tick(e)
        await tick(e, 2)
        reply = await handle("/rejects", [])
        self.assertIsInstance(reply, Reply)
        self.assertIn("交易所撮合引擎还不认这个代币", reply.text)
        self.assertIn("机器人：只把这一场过一会儿再挂（旧市场半秒；V2 市场 2 秒", reply.text)
        self.assertEqual(reply.markup["inline_keyboard"][0][0]["callback_data"], "chk_rejects")
        self.assertIn("最近 1 小时拒单", e.check_text())
        self.assertEqual(e.hold(), "")
        tg = FakeTelegram()
        await tg.on_callback({"id": "c", "data": "chk_rejects", "from": {"id": int(ADMIN)},
                              "message": {"chat": {"id": int(ADMIN)}, "message_id": 3}}, handle)
        self.assertEqual([m_ for m_, _ in tg.posts], ["answerCallbackQuery", "editMessageReplyMarkup"])  # the report stays
        check = tg.outbox.get_nowait()[0]
        self.assertIn("🔬 现场检测", check)
        self.assertIn("交易所接受下单 ✅", check)
        self.assertIn("/rejects", main.HELP) if hasattr(main, "HELP") else None
        self.assertIn("🧾 拒单", BUTTONS)

    async def test_web_command(self):
        e, clock, pub, x = make_engine()
        no_domain = await main.commander(settings(), e, pub)("/web", [])
        self.assertIn("Generate Domain", no_domain)
        s = settings(WEB_KEY="correct-horse-battery", RAILWAY_PUBLIC_DOMAIN="bot.up.railway.app", PORT="8080")
        self.assertEqual((s.web_url, s.web_port), ("https://bot.up.railway.app", 8080))
        reply = await main.commander(s, e, pub)("/web", [])
        self.assertEqual(reply.markup["inline_keyboard"][0][0]["web_app"]["url"], "https://bot.up.railway.app")


if __name__ == "__main__":
    unittest.main()
