"""Optional Telegram link: notifications to the admin chat, and a few commands from it.

Commands (only from TELEGRAM_CHAT_ID):
    /status      the summary (also /preopen)
    /check       the health check
    /probe [h]   read-only: the next windows' listings and books (h hours ahead, default 1)
    /pause       stop placing and withdraw every resting order of the bot
    /resume      place again
    /help
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any, Awaitable, Callable

import httpx

LOG = logging.getLogger(__name__)
API = "https://api.telegram.org"
MAX_LEN = 3900


class Telegram:
    def __init__(self, token: str, chat_id: str, http: httpx.AsyncClient | None = None):
        self.token, self.chat_id = token, str(chat_id)
        self.http = http or httpx.AsyncClient(timeout=40)
        self.outbox: asyncio.Queue[str] = asyncio.Queue(maxsize=500)
        self.offset = 0

    def send(self, text: str, key: str = "") -> None:
        """Queue a message (callable from any coroutine, never blocks)."""
        try:
            self.outbox.put_nowait(text[:MAX_LEN])
        except asyncio.QueueFull:
            LOG.warning("Telegram 发件队列满，丢弃：%s", text[:80])

    async def _post(self, method: str, **payload: Any) -> Any:
        r = await self.http.post(f"{API}/bot{self.token}/{method}", json=payload)
        data = r.json()
        if not data.get("ok"):
            raise RuntimeError(f"Telegram {method}: {data.get('description')}")
        return data.get("result")

    async def sender(self) -> None:
        while True:
            text = await self.outbox.get()
            for attempt in range(3):
                try:
                    await self._post("sendMessage", chat_id=self.chat_id, text=text, disable_web_page_preview=True)
                    break
                except Exception as error:
                    LOG.warning("Telegram 发送失败（第 %d 次）：%s", attempt + 1, error)
                    await asyncio.sleep(3 * (attempt + 1))
            await asyncio.sleep(1.0)

    async def commands(self, handle: Callable[[str, list[str]], Awaitable[str]]) -> None:
        while True:
            try:
                updates = await self._post("getUpdates", offset=self.offset, timeout=30, allowed_updates=["message"])
            except Exception as error:
                LOG.warning("Telegram 收消息失败：%s", error)
                await asyncio.sleep(10)
                continue
            for u in updates or []:
                self.offset = max(self.offset, int(u.get("update_id", 0)) + 1)
                msg = u.get("message") or {}
                if str((msg.get("chat") or {}).get("id")) != self.chat_id:
                    continue
                text = str(msg.get("text") or "").strip()
                if not text.startswith("/"):
                    continue
                parts = text.split()
                cmd = parts[0].split("@")[0].lower()
                try:
                    reply = await handle(cmd, parts[1:])
                except Exception as error:
                    LOG.exception("命令 %s 出错", cmd)
                    reply = f"命令出错：{error}"
                if reply:
                    self.send(reply)

    async def run(self, handle: Callable[[str, list[str]], Awaitable[str]]) -> None:
        await asyncio.gather(self.sender(), self.commands(handle))

    async def close(self) -> None:
        await self.http.aclose()


HELP = ("/status 状态（也可 /preopen）\n/check 体检\n/probe [小时] 只读探测接下来的场次\n"
        "/pause 暂停并撤掉本程序的全部挂单\n/resume 恢复挂单\n/help 本说明")
