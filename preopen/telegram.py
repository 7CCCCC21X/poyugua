"""Optional Telegram link: notifications to the admin chat, commands and buttons from it.

Buttons:
    the menu button (left of the input)   opens the web console inside Telegram (a Mini App, signed in by
                                          Telegram) when the service has a public domain; the command list otherwise
    the keyboard under the input          📊 状态  🩺 体检 / 🔎 探测  🌐 网页 / ⏸ 暂停  ▶️ 恢复
    ⏸ 暂停                               asks first (an inline 确认 / 取消), then withdraws every resting order
Commands (only from TELEGRAM_CHAT_ID): /status /check /probe [h] /web /pause /resume /help
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Any, Awaitable, Callable

import httpx

LOG = logging.getLogger(__name__)
API = "https://api.telegram.org"
MAX_LEN = 3900

BUTTONS = {  # keyboard button text -> command
    "📊 状态": "/status",
    "🩺 体检": "/check",
    "🔎 探测": "/probe",
    "🌐 网页": "/web",
    "⏸ 暂停": "/pause",
    "▶️ 恢复": "/resume",
}
COMMANDS = [("status", "状态"), ("check", "体检"), ("probe", "只读探测接下来的场次"), ("web", "打开网页控制台"),
            ("pause", "暂停并撤掉全部挂单"), ("resume", "恢复挂单"), ("help", "说明和按钮")]
CONFIRM_PAUSE = "pause!"  # callback data of the inline 确认 button: the handler is asked "/pause!"
CANCEL = "cancel"

HELP = ("按钮：📊 状态、🩺 体检、🔎 探测、🌐 网页、⏸ 暂停（会先确认）、▶️ 恢复；左下角菜单按钮打开网页控制台。\n"
        "命令：/status 状态 /check 体检 /probe [小时] 只读探测 /web 网页 /pause 暂停并撤单 /resume 恢复 /help 本说明")


@dataclass
class Reply:
    text: str
    markup: dict | None = None    # reply_markup; None = the keyboard


Handler = Callable[[str, list[str]], Awaitable["Reply | str"]]


def confirm_markup(action: str, yes: str) -> dict:
    return {"inline_keyboard": [[{"text": yes, "callback_data": action}, {"text": "取消", "callback_data": CANCEL}]]}


def web_markup(url: str) -> dict:
    return {"inline_keyboard": [[{"text": "打开控制台", "web_app": {"url": url}}]]}


class Telegram:
    def __init__(self, token: str, chat_id: str, http: httpx.AsyncClient | None = None, web_url: str = ""):
        self.token, self.chat_id = token, str(chat_id).strip()
        self.web_url = web_url
        self.http = http or httpx.AsyncClient(timeout=40)
        self.outbox: asyncio.Queue[tuple[str, dict | None]] = asyncio.Queue(maxsize=500)
        self.offset = 0
        self.keyboard_sent = False

    def keyboard(self) -> dict:
        return {"keyboard": [[{"text": "📊 状态"}, {"text": "🩺 体检"}],
                             [{"text": "🔎 探测"}, {"text": "🌐 网页"}],
                             [{"text": "⏸ 暂停"}, {"text": "▶️ 恢复"}]],
                "resize_keyboard": True, "is_persistent": True}

    def send(self, text: str, key: str = "", markup: dict | None = None) -> None:
        """Queue a message (callable from any coroutine, never blocks). The first message of a process brings the
        keyboard along, so a restart refreshes it."""
        if markup is None and not self.keyboard_sent:
            markup, self.keyboard_sent = self.keyboard(), True
        try:
            self.outbox.put_nowait((text[:MAX_LEN], markup))
        except asyncio.QueueFull:
            LOG.warning("Telegram 发件队列满，丢弃：%s", text[:80])

    async def _post(self, method: str, **payload: Any) -> Any:
        r = await self.http.post(f"{API}/bot{self.token}/{method}", json=payload)
        data = r.json()
        if not data.get("ok"):
            raise RuntimeError(f"Telegram {method}: {data.get('description')}")
        return data.get("result")

    def chat(self) -> int | str:
        return int(self.chat_id) if self.chat_id.lstrip("-").isdigit() else self.chat_id

    async def setup(self) -> None:
        """The command list (the menu) and the menu button: the web console when there is a public https address."""
        try:
            await self._post("setMyCommands", commands=[{"command": c, "description": d} for c, d in COMMANDS])
            button = ({"type": "web_app", "text": "控制台", "web_app": {"url": self.web_url}}
                      if self.web_url.startswith("https://") else {"type": "commands"})
            await self._post("setChatMenuButton", chat_id=self.chat(), menu_button=button)
        except Exception as error:  # the buttons are a convenience: the bot runs without them
            LOG.warning("Telegram 菜单设置失败：%s", error)

    async def sender(self) -> None:
        while True:
            text, markup = await self.outbox.get()
            payload: dict[str, Any] = {"chat_id": self.chat(), "text": text, "disable_web_page_preview": True}
            if markup:
                payload["reply_markup"] = markup
            for attempt in range(3):
                try:
                    await self._post("sendMessage", **payload)
                    break
                except Exception as error:
                    LOG.warning("Telegram 发送失败（第 %d 次）：%s", attempt + 1, error)
                    await asyncio.sleep(3 * (attempt + 1))
            await asyncio.sleep(1.0)

    async def on_message(self, msg: dict, handle: Handler) -> None:
        if str((msg.get("chat") or {}).get("id")) != self.chat_id:
            return
        text = str(msg.get("text") or "").strip()
        if text in BUTTONS:
            text = BUTTONS[text]
        if not text.startswith("/"):
            return
        parts = text.split()
        cmd = parts[0].split("@")[0].lower()
        try:
            reply = await handle(cmd, parts[1:])
        except Exception as error:
            LOG.exception("命令 %s 出错", cmd)
            reply = f"命令出错：{error}"
        if isinstance(reply, str):
            reply = Reply(reply)
        if reply and reply.text:
            self.send(reply.text, markup=reply.markup or self.keyboard())

    async def on_callback(self, cb: dict, handle: Handler) -> None:
        msg = cb.get("message") or {}
        mine = str((msg.get("chat") or {}).get("id")) == self.chat_id
        data = str(cb.get("data") or "")
        result = "只接受管理员的操作"
        if mine:
            if data == CANCEL:
                result = "已取消"
            else:
                try:
                    reply = await handle("/" + data, [])
                    result = reply.text if isinstance(reply, Reply) else str(reply)
                except Exception as error:
                    LOG.exception("按钮 %s 出错", data)
                    result = f"操作出错：{error}"
        try:
            await self._post("answerCallbackQuery", callback_query_id=cb.get("id"), text=result[:190])
            if mine and msg.get("message_id"):
                await self._post("editMessageText", chat_id=self.chat(), message_id=msg["message_id"], text=result[:MAX_LEN])
        except Exception as error:
            LOG.warning("Telegram 按钮应答失败：%s", error)

    async def commands(self, handle: Handler) -> None:
        while True:
            try:
                updates = await self._post("getUpdates", offset=self.offset, timeout=30,
                                           allowed_updates=["message", "callback_query"])
            except Exception as error:
                LOG.warning("Telegram 收消息失败：%s", error)
                await asyncio.sleep(10)
                continue
            for u in updates or []:
                self.offset = max(self.offset, int(u.get("update_id", 0)) + 1)
                if u.get("callback_query"):
                    await self.on_callback(u["callback_query"], handle)
                elif u.get("message"):
                    await self.on_message(u["message"], handle)

    async def run(self, handle: Handler) -> None:
        await self.setup()
        await asyncio.gather(self.sender(), self.commands(handle))

    async def close(self) -> None:
        await self.http.aclose()
