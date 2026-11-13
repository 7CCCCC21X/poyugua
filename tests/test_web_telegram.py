"""The web console (sign-in, routes, a real socket round trip) and the Telegram buttons (keyboard, menu, the pause
confirmation)."""
import asyncio
import hashlib
import hmac
import json
import time
import unittest
import urllib.parse

from helpers import make_engine, settings, tick

import main
from preopen import markets as mk
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


class WebServerTest(unittest.IsolatedAsyncioTestCase):
    def server(self, key: str = "correct-horse-battery"):
        self.calls = []

        async def pause():
            self.calls.append("pause")
            return "paused"
        return web.WebServer(0, lambda: {"ok": 1, "mode": "模拟"}, {"pause": pause}, web_key=key, bot_token=TOKEN, admin_id=ADMIN)

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
        self.assertEqual((status, json.loads(body)["message"], self.calls), (200, "paused", ["pause"]))

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

    async def _post(self, method, **payload):
        self.posts.append((method, payload))
        return True


class TelegramTest(unittest.IsolatedAsyncioTestCase):
    async def test_buttons_map_to_commands_and_bring_the_keyboard(self):
        tg = FakeTelegram()
        seen = []

        async def handle(cmd, args):
            seen.append(cmd)
            return "ok"
        for text, cmd in BUTTONS.items():
            await tg.on_message({"chat": {"id": int(ADMIN)}, "text": text}, handle)
        self.assertEqual(seen, list(BUTTONS.values()))
        await tg.on_message({"chat": {"id": 1}, "text": "📊 状态"}, handle)  # not the admin: ignored
        self.assertEqual(len(seen), len(BUTTONS))
        text, markup = tg.outbox.get_nowait()
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
        await tg.on_callback({"id": "cb1", "data": CONFIRM_PAUSE, "message": {"chat": {"id": int(ADMIN)}, "message_id": 9}}, handle)
        self.assertTrue(e.paused)
        methods = [m for m, _ in tg.posts]
        self.assertEqual(methods, ["answerCallbackQuery", "editMessageText"])
        self.assertIn("已暂停", tg.posts[1][1]["text"])
        e.resume()
        await tg.on_callback({"id": "cb2", "data": "cancel", "message": {"chat": {"id": int(ADMIN)}, "message_id": 10}}, handle)
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
