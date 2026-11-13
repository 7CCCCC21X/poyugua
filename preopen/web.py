"""The web console, served by the bot itself (no extra dependency): one page with the status, the resting orders, the
fills and the problems, and pause / resume.

    GET  /             the page (no data in it; the data comes from /api/status)
    GET  /health       "ok", no sign-in
    POST /api/login    {"key": WEB_KEY} -> a session cookie
    GET  /api/status   the engine's report (signed in)
    POST /api/action   {"action": "pause" | "resume" | "emergency" | "withdraw" | "rebind"} (signed in)

Signing in: inside Telegram the page is a Mini App (the bot's menu button), and the request carries Telegram's signed
initData, checked with the bot token and the admin's user id: no password. In a browser, WEB_KEY once (a cookie keeps
the session for 30 days). Railway gives the service a domain (Settings -> Networking -> Generate Domain) and the PORT
it routes to; the server listens there.
"""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import time
import urllib.parse
from typing import Any

LOG = logging.getLogger(__name__)
COOKIE = "preopen_session"
SESSION_DAYS = 30
INIT_DATA_MAX_AGE_S = 7 * 86400
LOGIN_FAILS_MAX = 20                # failed logins in LOGIN_WINDOW_S before /api/login refuses everything for a while
LOGIN_WINDOW_S = 600
MAX_BODY = 16 * 1024
REASONS = {200: "OK", 204: "No Content", 400: "Bad Request", 401: "Unauthorized", 403: "Forbidden", 404: "Not Found",
           405: "Method Not Allowed", 413: "Payload Too Large", 429: "Too Many Requests", 500: "Internal Server Error"}


def session_token(web_key: str) -> str:
    return hmac.new(web_key.encode(), b"preopen-web-session", hashlib.sha256).hexdigest()


def check_init_data(init_data: str, bot_token: str, admins: Any, now: float | None = None) -> bool:
    """Telegram Mini App initData: signed with the bot token (HMAC-SHA256, key = HMAC("WebAppData", token)), fresh,
    and from an admin (a user id in TELEGRAM_ADMIN_IDS; by default TELEGRAM_CHAT_ID, a private chat's user)."""
    if not init_data or not bot_token:
        return False
    try:
        pairs = urllib.parse.parse_qsl(init_data, keep_blank_values=True, strict_parsing=True)
    except ValueError:
        return False
    fields = dict(pairs)
    received = fields.pop("hash", "")
    if not received:
        return False
    secret = hmac.new(b"WebAppData", bot_token.encode(), hashlib.sha256).digest()
    candidates = [fields, {k: v for k, v in fields.items() if k != "signature"}]  # with or without the newer signature field
    if not any(hmac.compare_digest(hmac.new(secret, "\n".join(f"{k}={v}" for k, v in sorted(c.items())).encode(),
                                            hashlib.sha256).hexdigest(), received) for c in candidates):
        return False
    try:
        auth_date = int(fields.get("auth_date", "0"))
        user = json.loads(fields.get("user", "{}"))
    except (ValueError, TypeError):
        return False
    if (now or time.time()) - auth_date > INIT_DATA_MAX_AGE_S:
        return False
    allowed = {str(a).strip() for a in ([admins] if isinstance(admins, (str, int)) else admins or ()) if str(a).strip()}
    return str(user.get("id", "")) in allowed


class WebServer:
    """`app` is the console back end (console.Console): report(), settings(), orders(tab, coin, limit), review(),
    csv(kind), and the coroutines change(body) and action(body)."""

    def __init__(self, port: int, app: Any, web_key: str = "", bot_token: str = "", admin_id: str = "", host: str = "0.0.0.0"):
        self.port, self.host = port, host
        self.app = app
        self.web_key, self.bot_token, self.admin_id = web_key, bot_token, admin_id
        self.server: asyncio.base_events.Server | None = None
        self.fails: list[float] = []

    @property
    def sign_in_ready(self) -> bool:
        return bool(self.web_key) or bool(self.bot_token and self.admin_id)

    async def start(self) -> None:
        self.server = await asyncio.start_server(self._serve, self.host, self.port)
        LOG.info("网页控制台在端口 %d%s", self.port, "" if self.sign_in_ready else "（没设 WEB_KEY 也没设 Telegram：只能看到登录页）")

    async def close(self) -> None:
        if self.server is not None:
            self.server.close()
            await self.server.wait_closed()

    def authorized(self, headers: dict[str, str]) -> bool:
        if check_init_data(headers.get("x-telegram-init-data", ""), self.bot_token, self.admin_id):
            return True
        if not self.web_key:
            return False
        token = session_token(self.web_key)
        cookies = dict(part.strip().split("=", 1) for part in headers.get("cookie", "").split(";") if "=" in part)
        if hmac.compare_digest(cookies.get(COOKIE, ""), token):
            return True
        bearer = headers.get("authorization", "")
        return bearer.startswith("Bearer ") and hmac.compare_digest(bearer[7:].strip(), self.web_key)

    @staticmethod
    def _json(status: int, data: Any, extra: list[tuple[str, str]] | None = None) -> tuple[int, str, bytes, list[tuple[str, str]]]:
        return status, "application/json; charset=utf-8", json.dumps(data, ensure_ascii=False, default=str).encode(), extra or []

    async def respond(self, method: str, target: str, headers: dict[str, str], body: bytes) -> tuple[int, str, bytes, list[tuple[str, str]]]:
        parts = urllib.parse.urlsplit(target)
        path, query = parts.path, dict(urllib.parse.parse_qsl(parts.query))
        if path == "/health":
            return 200, "text/plain; charset=utf-8", b"ok", []
        if path in ("/", "/index.html", "/control", "/journal"):
            if method != "GET":
                return 405, "text/plain", b"", []
            return 200, "text/html; charset=utf-8", PAGE_HTML, []
        if path == "/api/login":
            if method != "POST":
                return 405, "text/plain", b"", []
            now = time.time()
            self.fails = [t for t in self.fails if now - t < LOGIN_WINDOW_S]
            if len(self.fails) >= LOGIN_FAILS_MAX:
                return self._json(429, {"error": "too many attempts"})
            try:
                key = str(json.loads(body or b"{}").get("key", ""))
            except (ValueError, AttributeError):
                key = ""
            if not self.web_key or not hmac.compare_digest(key, self.web_key):
                self.fails.append(now)
                await asyncio.sleep(1.0)
                return self._json(401, {"error": "wrong key"})
            cookie = (f"{COOKIE}={session_token(self.web_key)}; Path=/; Max-Age={SESSION_DAYS * 86400}; HttpOnly; Secure; "
                      "SameSite=Lax")
            return self._json(200, {"ok": True}, [("Set-Cookie", cookie)])
        if not path.startswith("/api/"):
            return 404, "text/plain", b"not found", []
        if not self.authorized(headers):
            return self._json(401, {"error": "sign in", "web_key": bool(self.web_key)})
        if method == "GET":
            if path == "/api/status":  # with the page's version: a page left open across a deploy reloads itself
                return self._json(200, {**self.app.report(), "page": PAGE_VERSION})
            if path == "/api/settings":
                return self._json(200, self.app.settings())
            if path == "/api/orders":
                try:
                    limit = int(query.get("limit") or 300)
                except ValueError:
                    limit = 300
                return self._json(200, self.app.orders(query.get("tab", "open"), query.get("coin", ""), limit))
            if path == "/api/journal":
                return self._json(200, self.app.review())
            if path == "/api/journal.csv":
                name, text = self.app.csv(query.get("kind", "detail"))
                return 200, "text/csv; charset=utf-8", text.encode("utf-8"), [("Content-Disposition", f'attachment; filename="{name}"')]
            return 404, "text/plain", b"not found", []
        if method != "POST":
            return 405, "text/plain", b"", []
        if "application/json" not in headers.get("content-type", ""):
            return self._json(400, {"error": "json only"})
        try:
            data = json.loads(body or b"{}")
            if not isinstance(data, dict):
                raise ValueError
        except ValueError:
            return self._json(400, {"error": "bad json"})
        if path == "/api/settings":
            ok, message = await self.app.change(data)
            return self._json(200 if ok else 400, {"ok": ok, "message": message, "settings": self.app.settings()})
        if path == "/api/action":
            if str(data.get("action") or "") not in getattr(self.app, "ACTIONS", ()):
                return self._json(400, {"error": "unknown action"})
            return self._json(200, {"message": await self.app.action(data)})
        return 404, "text/plain", b"not found", []

    async def _serve(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        status, ctype, payload, extra = 500, "text/plain", b"", []
        try:
            line = await asyncio.wait_for(reader.readline(), 15)
            parts = line.decode("latin-1").split()
            if len(parts) < 2:
                raise ValueError("bad request line")
            method, target = parts[0].upper(), parts[1]
            headers: dict[str, str] = {}
            while True:
                raw = await asyncio.wait_for(reader.readline(), 15)
                if raw in (b"\r\n", b"\n", b""):
                    break
                name, _, value = raw.decode("latin-1").partition(":")
                headers[name.strip().lower()] = value.strip()
            length = int(headers.get("content-length") or 0)
            if length > MAX_BODY:
                status, ctype, payload = 413, "text/plain", b"too large"
            else:
                body = await asyncio.wait_for(reader.readexactly(length), 15) if length else b""
                status, ctype, payload, extra = await self.respond(method, target, headers, body)
        except (asyncio.TimeoutError, asyncio.IncompleteReadError, ConnectionError, ValueError):
            status, ctype, payload = 400, "text/plain", b"bad request"
        except Exception:
            LOG.exception("网页请求出错")
            status, ctype, payload = 500, "text/plain", b"error"
        head = [f"HTTP/1.1 {status} {REASONS.get(status, 'OK')}", f"Content-Type: {ctype}", f"Content-Length: {len(payload)}",
                "Cache-Control: no-store", "Connection: close", "X-Content-Type-Options: nosniff",
                "Referrer-Policy: no-referrer"] + [f"{k}: {v}" for k, v in extra]
        try:
            writer.write(("\r\n".join(head) + "\r\n\r\n").encode("utf-8") + payload)
            await writer.drain()
        except (ConnectionError, RuntimeError):
            pass
        finally:
            writer.close()


PAGE = r"""<!doctype html>
<html lang="zh">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
<title>预挂控制台</title>
<script src="https://telegram.org/js/telegram-web-app.js"></script>
<style>
:root { --bg:#f6f7f9; --card:#fff; --text:#16181d; --muted:#6b7280; --line:#e5e7eb; --accent:#2563eb; --good:#15803d; --bad:#b91c1c; --warn:#b45309; --chip:#eef2ff; }
@media (prefers-color-scheme: dark) { :root { --bg:#0f1115; --card:#181b21; --text:#e8eaee; --muted:#9aa1ad; --line:#2a2f38; --accent:#5b8cff; --good:#4ade80; --bad:#f87171; --warn:#fbbf24; --chip:#1f2a44; } }
* { box-sizing:border-box; }
[hidden] { display:none !important; }
body { margin:0; background:var(--bg); color:var(--text); font:14px/1.5 -apple-system,BlinkMacSystemFont,"PingFang SC","Microsoft YaHei",sans-serif; }
main { max-width:1080px; margin:0 auto; padding:0 16px 24px; }
.top { position:sticky; top:0; z-index:5; background:var(--bg); padding:10px 0 6px; border-bottom:1px solid var(--line); margin-bottom:12px; }
.title { display:flex; flex-wrap:wrap; align-items:center; gap:6px; }
h1 { font-size:17px; margin:0 6px 0 0; }
.badge { display:inline-block; padding:1px 8px; border-radius:999px; font-size:12px; background:var(--line); color:var(--text); }
.badge.live { background:var(--bad); color:#fff; } .badge.paper { background:var(--accent); color:#fff; } .badge.pause { background:var(--warn); color:#fff; } .badge.run { background:var(--good); color:#fff; }
.bar { display:flex; flex-wrap:wrap; gap:4px 14px; font-size:12px; color:var(--muted); margin-top:4px; } .bar b { color:var(--text); font-weight:600; }
nav { display:flex; gap:6px; margin-top:8px; overflow-x:auto; }
nav a { padding:6px 14px; border-radius:999px; text-decoration:none; color:var(--text); background:var(--card); border:1px solid var(--line); white-space:nowrap; }
nav a.on { background:var(--accent); color:#fff; border-color:var(--accent); }
.ops { display:flex; flex-wrap:wrap; gap:8px; margin-bottom:12px; }
.cards { display:grid; grid-template-columns:repeat(auto-fit,minmax(135px,1fr)); gap:10px; margin-bottom:12px; }
.card { background:var(--card); border:1px solid var(--line); border-radius:12px; padding:12px; }
.card .k { color:var(--muted); font-size:12px; } .card .v { font-size:20px; font-weight:600; margin-top:2px; } .card .s { color:var(--muted); font-size:12px; }
section { background:var(--card); border:1px solid var(--line); border-radius:12px; padding:12px; margin-bottom:12px; }
section h2 { font-size:15px; margin:0 0 8px; display:flex; justify-content:space-between; align-items:center; gap:8px; flex-wrap:wrap; }
.scroll { overflow-x:auto; }
table { width:100%; border-collapse:collapse; font-size:13px; white-space:nowrap; }
th, td { text-align:left; padding:6px 8px; border-bottom:1px solid var(--line); }
th { color:var(--muted); font-weight:500; }
.good { color:var(--good); } .bad { color:var(--bad); } .warn { color:var(--warn); } .muted { color:var(--muted); } .small { font-size:12px; }
button { font:inherit; border:0; border-radius:10px; padding:8px 14px; cursor:pointer; background:var(--accent); color:#fff; }
button.secondary { background:var(--line); color:var(--text); } button.danger { background:var(--bad); } button.small { padding:3px 10px; font-size:12px; border-radius:8px; }
button:disabled { opacity:.5; cursor:default; }
.notice { padding:10px 12px; border-radius:10px; margin-bottom:12px; background:var(--card); border:1px solid var(--warn); }
.kv { display:grid; grid-template-columns:max-content 1fr; gap:4px 12px; font-size:13px; } .kv div:nth-child(odd) { color:var(--muted); }
.grid2 { display:grid; grid-template-columns:repeat(auto-fit,minmax(300px,1fr)); gap:12px; }
.setting { border:1px solid var(--line); border-radius:12px; padding:12px; background:var(--card); }
.setting .head { display:flex; justify-content:space-between; gap:8px; align-items:baseline; flex-wrap:wrap; }
.setting .name { font-weight:600; } .setting .val { font-weight:600; color:var(--accent); }
.setting .help { color:var(--muted); font-size:12px; margin:4px 0 8px; }
.chips { display:flex; flex-wrap:wrap; gap:6px; margin-bottom:6px; }
.chip { padding:4px 10px; border-radius:999px; border:1px solid var(--line); background:var(--card); color:var(--text); font-size:13px; cursor:pointer; }
.chip.on { background:var(--accent); color:#fff; border-color:var(--accent); }
.custom { display:flex; gap:6px; } .custom input { flex:1; min-width:0; padding:6px 8px; border-radius:8px; border:1px solid var(--line); background:var(--bg); color:var(--text); font:inherit; }
.src { font-size:11px; padding:0 6px; border-radius:6px; background:var(--chip); color:var(--muted); }
.src.web { background:var(--accent); color:#fff; }
a.link { color:var(--accent); cursor:pointer; font-size:12px; }
.toast { position:fixed; left:50%; bottom:20px; transform:translateX(-50%); max-width:92vw; padding:10px 14px; border-radius:10px; background:var(--text); color:var(--bg); font-size:13px; z-index:9; box-shadow:0 4px 20px rgba(0,0,0,.2); }
.toast.err { background:var(--bad); color:#fff; }
details summary { cursor:pointer; color:var(--muted); margin:8px 0; }
select { font:inherit; padding:5px 8px; border-radius:8px; border:1px solid var(--line); background:var(--card); color:var(--text); }
.orow { padding:8px 0; border-bottom:1px solid var(--line); font-size:13px; }
.orow .l1 { display:flex; flex-wrap:wrap; align-items:center; gap:4px 10px; } .orow .l1 button { margin-left:auto; }
.orow .l2 { margin-top:2px; color:var(--text); }
#login { max-width:360px; margin:12vh auto; } #login input { width:100%; padding:10px; border-radius:10px; border:1px solid var(--line); background:var(--card); color:var(--text); font:inherit; margin:8px 0; }
</style>
</head>
<body>
<main>
  <div id="login" hidden>
    <section>
      <h2>预挂控制台</h2>
      <p class="muted" id="loginHint">输入 Railway 里设置的 WEB_KEY（或从 Telegram 机器人的菜单按钮打开，免密码）。</p>
      <input id="key" type="password" placeholder="WEB_KEY" autocomplete="current-password">
      <button id="loginBtn">登录</button> <span id="loginMsg" class="bad"></span>
    </section>
  </div>
  <div id="app" hidden>
    <div class="top">
      <div class="title"><h1>Polymarket 5 分钟涨跌预挂</h1><span id="mode" class="badge"></span><span id="runstate" class="badge"></span></div>
      <div class="bar" id="bar"></div>
      <nav id="nav"><a data-tab="overview">概览</a><a data-tab="settings">参数</a><a data-tab="orders">订单</a><a data-tab="journal">复盘</a></nav>
    </div>
    <div class="ops"><button id="pauseBtn" class="secondary">⏸ 暂停并撤单</button><button id="resumeBtn">▶️ 恢复挂单</button><button id="emergencyBtn" class="danger">🛑 紧急停止</button></div>

    <div id="tab-overview">
      <div id="blocked" class="notice" hidden><span id="blockedText"></span> <button id="rebindBtn" class="danger">改绑当前账户</button></div>
      <div id="hold" class="notice" hidden></div>
      <div class="cards" id="cards"></div>
      <div class="grid2">
        <section><h2>运行情况</h2><div class="kv" id="info"></div></section>
        <section><h2>待处理</h2><div id="pending"></div></section>
      </div>
      <section><h2>挂着的单（开场最近的 40 档）<a class="link" data-goto="orders">全部订单 →</a></h2><div class="scroll"><table id="upcoming"></table></div></section>
      <section><h2>有成交的场次<a class="link" data-goto="journal">复盘 →</a></h2><div class="scroll"><table id="fills"></table></div></section>
      <section><h2>最近的问题</h2><div id="errors" class="muted"></div></section>
    </div>

    <div id="tab-settings" hidden>
      <div class="notice">点选项或保存后<b>立即生效</b>，并保存下来（重启后仍按网页上的设置运行）；网页上的值高于 Railway 变量。和新设置一致的挂单原样保留（排队位置不丢），不一致的撤掉后按新设置重挂。私钥、API Key、Token、钱包只能在 Railway 变量里改。</div>
      <div id="settingsBody"></div>
      <p><button id="resetAll" class="secondary">清除全部网页设置（回到 Railway 变量）</button></p>
    </div>

    <div id="tab-orders" hidden>
      <section>
        <h2><span class="chips" id="orderTabs"></span><select id="coinFilter"><option value="">全部币种</option></select></h2>
        <div class="scroll"><table id="orderTable"></table></div><div id="orderList" hidden></div>
        <p class="muted small" id="orderMore"></p>
      </section>
    </div>

    <div id="tab-journal" hidden>
      <div class="cards" id="jTotals"></div>
      <div class="ops"><button class="secondary" id="csvDetail">导出成交明细 CSV</button><button class="secondary" id="csvDaily">导出每日汇总 CSV</button><button class="secondary" id="csvQuotes">导出全部报价 CSV</button><button class="secondary" id="csvCancel">导出撤单时间复盘 CSV</button></div>
      <section><h2>开场前几秒撤单最好</h2><p id="cAdvice"></p><div class="scroll"><table id="cRows"></table></div>
        <p class="muted small" id="cNote">把每一场按「如果提前 X 秒撤单」重算：X 秒之后才成交的单当作没成交。同一批场次比较，所以各行能直接比。只能算比当时撤得更早的情况。</p></section>
      <section><h2>什么时候成交的（按成交时刻）</h2><div class="scroll"><table id="cTimes"></table></div>
        <p class="muted small">买的那一边最后赢的比例低于保本线（=买价），这个时段的成交就是亏的：撤单时间应该提前到它之前。</p></section>
      <section><h2>按日期（北京时间，按开场日）</h2><div class="scroll"><table id="jDay"></table></div></section>
      <div class="grid2">
        <section><h2>按币种</h2><div class="scroll"><table id="jCoin"></table></div></section>
        <section><h2>按方向</h2><div class="scroll"><table id="jSide"></table></div><p class="muted small">成交了哪一边、那一边赢了多少：两边成交的那部分是锁定利润，只成交一边的是 49¢ 买五五开</p></section>
      </div>
      <section><h2>按开场时段（北京时间几点）</h2><div class="scroll"><table id="jHour"></table></div></section>
      <section><h2>成交明细（最近 200 场）</h2><div class="scroll"><table id="jDetail"></table></div><p class="muted small" id="jMore"></p></section>
    </div>
    <p class="muted small" id="stamp"></p>
  </div>
</main>
<div id="toast" class="toast" hidden></div>
<script>
const PAGE_VERSION = "__PAGE_VERSION__";
const tg = window.Telegram && window.Telegram.WebApp;
if (tg) { tg.ready(); tg.expand(); }
const initData = tg ? tg.initData : "";
const native = !!(tg && tg.isVersionAtLeast && tg.isVersionAtLeast("6.2"));
const $ = (id) => document.getElementById(id);
const esc = (s) => String(s ?? "").replace(/[&<>"']/g, (c) => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));
const money = (v) => v == null ? "—" : (v < 0 ? "-$" : "$") + Math.abs(Number(v)).toFixed(2);
const signed = (v) => v == null ? "—" : `<span class="${v >= 0 ? "good" : "bad"}">${v >= 0 ? "+" : "-"}$${Math.abs(Number(v)).toFixed(2)}</span>`;
const pct = (v) => v == null ? "—" : Number(v).toFixed(1) + "%";
const secs = (v) => v == null ? "—" : (v >= 3600 ? (v / 3600).toFixed(1) + " 小时" : v >= 60 ? Math.round(v / 60) + " 分" : Math.round(v) + " 秒");
const store = { get(k, d) { try { return localStorage.getItem(k) ?? d; } catch (e) { return d; } }, set(k, v) { try { localStorage.setItem(k, v); } catch (e) {} } };
const ask = (q) => new Promise((ok) => native ? tg.showConfirm(q, ok) : ok(window.confirm(q)));
let toastTimer = null;
function toast(text, err) { const t = $("toast"); t.textContent = text; t.className = "toast" + (err ? " err" : ""); t.hidden = false; clearTimeout(toastTimer); toastTimer = setTimeout(() => t.hidden = true, err ? 7000 : 4000); }
async function api(path, opts = {}) {
  const headers = Object.assign({"Content-Type": "application/json"}, opts.headers || {});
  if (initData) headers["X-Telegram-Init-Data"] = initData;
  const ctl = new AbortController(); const timer = setTimeout(() => ctl.abort(), 15000);
  try {
    const r = await fetch(path, Object.assign({credentials: "same-origin", signal: ctl.signal}, opts, {headers}));
    if (r.status === 401) { showLogin(await r.json().catch(() => ({}))); throw new Error("sign in"); }
    return {status: r.status, data: path.endsWith(".csv") || path.includes(".csv?") ? await r.text() : await r.json().catch(() => ({}))};
  } finally { clearTimeout(timer); }
}
function showLogin(info) {
  $("app").hidden = true; $("login").hidden = false;
  if (initData) $("loginHint").textContent = "Telegram 验证没通过：确认 TELEGRAM_CHAT_ID 是你自己的用户 ID；或输入 WEB_KEY。";
  else if (info && info.web_key === false) $("loginHint").textContent = "还没设置 WEB_KEY：在 Railway 的 Variables 里加上 WEB_KEY（至少 12 位）后重新部署。";
}
$("loginBtn").onclick = async () => {
  const r = await fetch("/api/login", {method: "POST", credentials: "same-origin", headers: {"Content-Type": "application/json"}, body: JSON.stringify({key: $("key").value})});
  if (r.status === 200) { $("loginMsg").textContent = ""; $("login").hidden = true; $("app").hidden = false; go(state.tab); }
  else $("loginMsg").textContent = r.status === 429 ? "尝试太多，稍后再试" : "WEB_KEY 不对";
};
$("key").addEventListener("keydown", (e) => { if (e.key === "Enter") $("loginBtn").click(); });

const state = { tab: (location.hash || "").slice(1) || store.get("tab", "overview"), ordersTab: store.get("ordersTab", "open"), coin: store.get("coin", ""), report: null };
const TABS = ["overview", "settings", "orders", "journal"];
let timer = null;
function go(tab) {
  if (!TABS.includes(tab)) tab = "overview";
  state.tab = tab; store.set("tab", tab); if (location.hash !== "#" + tab) history.replaceState(null, "", "#" + tab);
  for (const t of TABS) $("tab-" + t).hidden = t !== tab;
  for (const a of document.querySelectorAll("nav a")) a.className = a.dataset.tab === tab ? "on" : "";
  clearTimeout(timer); tick(true);
}
document.querySelectorAll("nav a").forEach((a) => a.onclick = () => go(a.dataset.tab));
document.querySelectorAll("[data-goto]").forEach((a) => a.onclick = () => go(a.dataset.goto));
async function tick(first) {
  try {
    const st = await api("/api/status");
    if (st.status === 200 && st.data.page && st.data.page !== PAGE_VERSION) { location.reload(); return; }  // a new version deployed
    if (st.status === 200) { state.report = st.data; renderTop(st.data); if (state.tab === "overview") renderOverview(st.data); }
    if (state.tab === "settings" && first) await loadSettings();
    if (state.tab === "orders") await loadOrders();
    if (state.tab === "journal" && (first || Date.now() - (state.jAt || 0) > 60000)) await loadJournal();
    $("app").hidden = false; $("login").hidden = true;
  } catch (e) { if (String(e.message) !== "sign in") $("stamp").textContent = "连接失败，稍后重试"; else return; }
  timer = setTimeout(() => tick(false), state.tab === "overview" ? 5000 : 10000);
}

function renderTop(d) {
  $("mode").textContent = d.mode; $("mode").className = "badge " + (d.live ? "live" : "paper");
  $("runstate").textContent = d.paused ? "已暂停" : "运行中"; $("runstate").className = "badge " + (d.paused ? "pause" : "run");
  const t = d.today || {};
  $("bar").innerHTML = [`挂着 <b>${d.windows}</b> 场 <b>${d.resting}</b> 张`, `余额 <b>${money(d.balance)}</b>`,
    `每场占用 ${money(d.window_usd)}` + (d.buffer != null && d.buffer < 0 ? ` <span class="warn">余额不够一场</span>` : ""), `今日盈亏 ${signed(t.pnl || 0)}`, d.unknown ? `<span class="warn">状态不明 ${d.unknown}</span>` : "", `更新 ${esc(d.now_text)}`]
    .filter(Boolean).map((x) => `<span>${x}</span>`).join("");
  $("pauseBtn").disabled = d.paused; $("resumeBtn").disabled = !d.paused;
  $("stamp").textContent = "北京时间 " + d.now_text;
}
function card(k, v, s) { return `<div class="card"><div class="k">${esc(k)}</div><div class="v">${v}</div><div class="s">${s || ""}</div></div>`; }
const REDEEM = {pending:"待领取", retry:"重试中", done:"已领取", failed:"领取失败", manual:"需手动领取", none:"—", "":""};
const STATE = {open:"挂着", placing:"发送中", unknown:"状态不明", gone:"已撤/核对中", final:"已结束", rejected:"被拒", not_sent:"没发出", not_found:"没挂上"};
function renderOverview(d) {
  $("blocked").hidden = !d.blocked; $("blockedText").textContent = d.blocked ? "⛔ " + d.blocked : "";
  const hold = d.blocked ? "" : (d.hold || d.cooldown);
  $("hold").hidden = !hold && !d.budget_note; $("hold").textContent = [hold && ("暂不新挂：" + hold), d.budget_note && ("资金：" + d.budget_note)].filter(Boolean).join("　");
  const t = d.today || {};
  $("cards").innerHTML = [
    card("挂着", `${d.windows} 场`, `${d.resting} 张单，合计 ${money(d.reserved)}` + (d.unknown ? `，<span class="warn">${d.unknown} 张状态不明</span>` : "")),
    card("余额", money(d.balance), `每场占用 ${money(d.window_usd)}，各场不共用` + (d.buffer == null ? "" :
      d.buffer < 0 ? `<br><span class="warn">不够一场，挂不了</span>` : `<br>成交缓冲 ${money(d.buffer)}`) +
      (d.settings.max_open ? `<br>挂单合计上限 ${money(d.settings.max_open)}` : "")),
    card("今天成交", `${t.fills || 0} 场`, `双边 ${t.both || 0}，已结算 ${t.settled || 0}`),
    card("今天盈亏", signed(t.pnl || 0), "已结算部分"),
    card("开放→挂上", d.lags.open == null ? "—" : d.lags.open + " 秒", "中位（市场约在创建后 75～90 秒开放）"),
    card("撤单确认", d.lags.cancel == null ? "—" : "开场前 " + d.lags.cancel + " 秒", "中位"),
    card("持仓最坏亏损", money(d.exposure), (d.limits.exposure ? `上限 ${money(d.limits.exposure)}` : "未设上限") +
      (d.limits.daily_loss ? `；日亏损上限 ${money(d.limits.daily_loss)}` : "") + (d.risk ? `<br><span class="warn">${esc(d.risk)}</span>` : "")),
  ].join("");
  const s = d.settings;
  const nxt = d.next_listing ? `${d.next_listing.start}（预计 ${d.next_listing.expected} 发布）` : "范围内都已发布";
  $("info").innerHTML = [
    ["币种", d.coins.join(" ") || "无"], ["每边", d.levels + `（每场占用 ${money(d.window_usd)}，各场分别核对余额、不共用）`],
    ["范围内", d.coverage ? d.coverage.text : "—"],
    ["时间", `开场前 ${s.ahead_min} 分钟内的场次；开场前 ${s.cancel_s} 秒撤单、${s.expiry_s} 秒到期、${s.entry_s} 秒内不新挂` + (s.weekend ? "" : "；周末不挂")],
    ["已发布到", d.listed_until || "—"], ["下一场", nxt], ["账户快照", d.snapshot_age == null ? "还没读到" : d.snapshot_age + " 秒前"],
    ["队列", `排队 ${d.queue}，等候 ${d.waiting}，正在挂 ${d.placing}`],
    ["余额", d.balance_age == null ? "还没读到" : d.balance_age + " 秒前读到"],
    ["撤单范围", d.key_exclusive ? "整个 API Key（PREOPEN_KEY_EXCLUSIVE=on）" : "只撤本程序签的单"],
    ["订单号=哈希", d.id_is_hash == null ? "未知" : (d.id_is_hash ? "是" : "否")], ["限流档位", d.tier || "未知"], ["发布提前量", d.lead_h + " 小时"],
  ].map(([k, v]) => `<div>${esc(k)}</div><div>${esc(v)}</div>`).join("");
  const pend = (d.pending || []).map((p) => `<div>⚠️ ${esc(p.what)}：${esc(p.window)} ${esc(p.side)}（${esc(p.since)}）<span class="muted small">${esc(p.note)}</span></div>`);
  if (d.unmanaged) pend.push(`<div>账户里另有 ${d.unmanaged} 张本程序没记录的挂单（${money(d.unmanaged_usd)}），只占用它们各自那一场的余额</div>`);
  if (d.cooldown) pend.push(`<div>接口冷却：${esc(d.cooldown)}</div>`);
  $("pending").innerHTML = pend.length ? pend.join("") : '<span class="muted">无</span>';
  $("upcoming").innerHTML = "<tr><th>场次</th><th>还有</th><th>档</th><th>价格×份数</th><th>涨</th><th>跌</th></tr>" + (d.upcoming.map((q) =>
    `<tr><td>${esc(q.coin)} ${esc(q.when)}</td><td>${esc(q.in)}</td><td>${q.level + 1}</td><td>${q.price}¢×${q.shares}</td>` +
    `<td>${esc(STATE[q.up] || q.up)}${q.up_filled ? ` <b class="good">成交 ${q.up_filled}</b>` : ""}</td>` +
    `<td>${esc(STATE[q.down] || q.down)}${q.down_filled ? ` <b class="good">成交 ${q.down_filled}</b>` : ""}</td></tr>`).join("") ||
    '<tr><td colspan="6" class="muted">没有挂着的单</td></tr>');
  $("fills").innerHTML = "<tr><th>场次</th><th>价格</th><th>涨成交</th><th>跌成交</th><th>结果</th><th>盈亏</th><th>领取</th></tr>" + (d.fills.map((q) =>
    `<tr><td>${esc(q.coin)} ${esc(q.when)}</td><td>${q.price}¢</td><td>${q.up || ""}</td><td>${q.down || ""}</td><td>${esc(q.result || (q.phase === "settled" ? "" : "待结算"))}</td>` +
    `<td>${q.pnl == null ? "" : signed(q.pnl)}</td><td title="${esc(q.redeem_error || "")}">${esc(REDEEM[q.redeem] ?? q.redeem)}` +
    (q.redeem_error ? `<div class="muted small">${esc(q.redeem_error.slice(0, 60))}…</div>` : "") + `</td></tr>`).join("") || '<tr><td colspan="7" class="muted">还没有成交</td></tr>');
  $("errors").innerHTML = d.errors.length ? d.errors.map((e) => `<div>${esc(e.when)} ${esc(e.text)}</div>`).join("") : "无";
}

// ---- 参数
const GROUPS = [["运行", "运行"], ["预挂", "预挂策略"], ["资金", "资金与风控"], ["高级", "高级参数（很少需要改）"]];
let settingsData = null;
async function loadSettings() { const r = await api("/api/settings"); if (r.status === 200) { settingsData = r.data; renderSettings(); } }
function optionLabel(form, v) {
  if (form.kind === "number" && form.zero && Number(v) === 0) return `0（${form.zero}）`;
  if (form.kind === "text" && v === "" && form.empty) return form.empty;
  const o = (form.options || []).find((x) => x[0] === v); return o ? o[1] : (v === "" ? "（空）" : v + (form.unit ? " " + form.unit : ""));
}
function renderSettings() {
  const d = settingsData; let html = "";
  for (const [g, title] of GROUPS) {
    const items = d.items.filter((it) => it.form.group === g); if (!items.length) continue;
    const body = '<div class="grid2">' + items.map(settingCard).join("") + "</div>";
    html += g === "高级" ? `<details><summary>${title}</summary>${body}</details>` : `<section><h2>${title}</h2>${body}</section>`;
  }
  $("settingsBody").innerHTML = html;
  $("resetAll").disabled = !d.overrides;
  for (const el of document.querySelectorAll("[data-set]")) el.onclick = () => saveSetting(el.dataset.set, el.dataset.value);
  for (const el of document.querySelectorAll("[data-save]")) el.onclick = () => saveSetting(el.dataset.save, $("in-" + el.dataset.save).value);
  for (const el of document.querySelectorAll("[data-reset]")) el.onclick = () => change({reset: el.dataset.reset}, "恢复成 Railway 变量的值？");
  for (const el of document.querySelectorAll("[data-coin]")) el.onclick = () => toggleCoin(el.dataset.coin);
}
function settingCard(it) {
  const f = it.form, v = it.value; let ctl = "";
  if (f.kind === "multi") {
    const on = coinsOf(v, f);
    ctl = '<div class="chips">' + f.options.map(([k, label]) => `<span class="chip ${on.includes(k) ? "on" : ""}" data-coin="${k}">${esc(label)}</span>`).join("") + "</div>" +
      `<div class="chips"><span class="chip" data-set="PREOPEN_MARKETS" data-value="all">一键全开</span><span class="chip" data-set="PREOPEN_MARKETS" data-value="none">一键全关</span></div>`;
  } else if (f.kind === "choice") {
    ctl = '<div class="chips">' + f.options.map(([k, label]) => `<span class="chip ${k === v ? "on" : ""}" data-set="${it.name}" data-value="${esc(k)}">${esc(label)}</span>`).join("") + "</div>";
  } else {
    ctl = '<div class="chips">' + (f.presets || []).map((p) => `<span class="chip ${String(p) === String(v) ? "on" : ""}" data-set="${it.name}" data-value="${esc(p)}">${esc(optionLabel(f, p))}</span>`).join("") + "</div>" +
      `<div class="custom"><input id="in-${it.name}" value="${esc(v)}" placeholder="${f.kind === "text" ? "如 48x20,47x40" : "自定义"}"><button class="small" data-save="${it.name}">保存</button></div>`;
  }
  const src = `<span class="src ${it.source === "网页" ? "web" : ""}">${esc(it.source)}</span>`;
  const reset = it.source === "网页" ? ` · <a class="link" data-reset="${it.name}">恢复环境变量（${esc(optionLabel(f, it.env_value))}）</a>` : "";
  return `<div class="setting"><div class="head"><span class="name">${esc(it.label)}</span><span><span class="val">${esc(optionLabel(f, v))}</span> ${src}</span></div>` +
    `<div class="help">${esc(it.help)} <span class="muted">（${esc(it.name)}）</span>${reset}</div>${ctl}</div>`;
}
function coinsOf(v, f) { const all = f.options.map((o) => o[0]); if (v === "all" || v === "") return all; if (v === "none") return []; return v.split(",").map((x) => x.trim()).filter(Boolean); }
function toggleCoin(c) {
  const it = settingsData.items.find((x) => x.name === "PREOPEN_MARKETS"); const all = it.form.options.map((o) => o[0]);
  let on = coinsOf(it.value, it.form); on = on.includes(c) ? on.filter((x) => x !== c) : all.filter((x) => on.includes(x) || x === c);
  saveSetting("PREOPEN_MARKETS", on.length === all.length ? "all" : on.length ? on.join(",") : "none");
}
async function saveSetting(name, value) {
  if (name === "LIVE") {
    const q = value === "on" ? "切到真实下单？程序会自动重启，之后用真钱挂单。" : "切到模拟盘？会先撤掉本程序的全部真实挂单（PREOPEN_KEY_EXCLUSIVE=on 时撤掉这个 API Key 的全部挂单），然后程序自动重启。";
    if (!(await ask(q))) return;
  }
  await change({name, value});
}
async function change(body, question) {
  if (question && !(await ask(question))) return;
  const r = await api("/api/settings", {method: "POST", body: JSON.stringify(body)});
  toast(r.data.message || "完成", !r.data.ok);
  if (r.data.settings) { settingsData = r.data.settings; renderSettings(); }
}
$("resetAll").onclick = () => change({reset_all: true}, "清除全部网页设置、回到 Railway 变量？");

// ---- 订单
const OTABS = [["open", "挂着"], ["filled", "有成交"], ["closed", "已撤无成交"], ["all", "全部"]];
async function loadOrders() {
  const r = await api(`/api/orders?tab=${state.ordersTab}&coin=${encodeURIComponent(state.coin)}`);
  if (r.status !== 200) return; const d = r.data;
  $("orderTabs").innerHTML = OTABS.map(([k, label]) => `<span class="chip ${k === d.tab ? "on" : ""}" data-otab="${k}">${label} ${d.counts[k] ?? 0}</span>`).join("");
  for (const el of document.querySelectorAll("[data-otab]")) el.onclick = () => { state.ordersTab = el.dataset.otab; store.set("ordersTab", state.ordersTab); loadOrders(); };
  if ($("coinFilter").options.length <= 1 && state.report) { for (const c of state.report.coins) $("coinFilter").insertAdjacentHTML("beforeend", `<option value="${esc(c.toLowerCase())}">${esc(c)}</option>`); $("coinFilter").value = state.coin; }
  const narrow = window.matchMedia("(max-width: 640px)").matches;
  $("orderTable").hidden = narrow; $("orderList").hidden = !narrow;
  const fillTxt = (side, state, n) => `${side} ${esc(state)}${n ? ` <b class="good">成交 ${n}</b>` : ""}`;
  $("orderList").innerHTML = d.rows.map((q) =>
    `<div class="orow"><div class="l1"><b>${esc(q.coin)} ${esc(q.when)}</b><span class="muted">${esc(q.in)}</span><span>第${q.level}档 ${q.price}¢×${q.shares}</span><span class="muted">第${q.attempt}次</span>` +
    `${q.can_withdraw ? `<button class="small secondary" data-wd="${esc(q.key)}">撤</button>` : ""}</div>` +
    `<div class="l2">${fillTxt("涨", q.up, q.up_filled)} · ${fillTxt("跌", q.down, q.down_filled)} · ${esc(q.phase_text)}` +
    `${q.result ? " · 结果" + esc(q.result) : q.reason ? ` · <span class="muted">${esc(q.reason)}</span>` : ""}${q.pnl == null ? "" : " · " + signed(q.pnl)}</div></div>`).join("") ||
    '<p class="muted">没有</p>';
  $("orderTable").innerHTML = "<tr><th>场次</th><th>还有</th><th>档</th><th>价格×份数</th><th>第几次</th><th>涨</th><th>跌</th><th>状态</th><th>原因 / 结果</th><th>盈亏</th><th></th></tr>" + (d.rows.map((q) =>
    `<tr><td>${esc(q.coin)} ${esc(q.when)}</td><td>${esc(q.in)}</td><td>${q.level}</td><td>${q.price}¢×${q.shares}</td><td>${q.attempt}</td>` +
    `<td>${esc(q.up)}${q.up_filled ? ` <b class="good">${q.up_filled}</b>` : ""}</td><td>${esc(q.down)}${q.down_filled ? ` <b class="good">${q.down_filled}</b>` : ""}</td>` +
    `<td>${esc(q.phase_text)}</td><td class="small">${esc(q.result ? "结果" + q.result : q.reason)}</td><td>${q.pnl == null ? "" : signed(q.pnl)}</td>` +
    `<td>${q.can_withdraw ? `<button class="small secondary" data-wd="${esc(q.key)}">撤</button>` : ""}</td></tr>`).join("") || '<tr><td colspan="11" class="muted">没有</td></tr>');
  $("orderMore").textContent = d.more ? `还有 ${d.more} 条没显示` : "";
  for (const el of document.querySelectorAll("[data-wd]")) el.onclick = () => act("withdraw", "撤掉这一场这一档？之后不再重挂。", {key: el.dataset.wd});
}
$("coinFilter").onchange = () => { state.coin = $("coinFilter").value; store.set("coin", state.coin); loadOrders(); };

// ---- 复盘
async function loadJournal() {
  const r = await api("/api/journal"); if (r.status !== 200) return; const d = r.data; state.jAt = Date.now();
  const T = d.totals, names = {today: "今天", "7d": "近 7 天", all: "累计"};
  $("jTotals").innerHTML = Object.keys(names).map((k) => card(names[k], signed(T[k].pnl),
    `报价 ${T[k].quoted} 场，成交 ${T[k].filled}（${pct(T[k].fill_rate)}），双边 ${T[k].both}；赢 ${T[k].wins} 输 ${T[k].losses}` + (T[k].open ? `，待结算 ${T[k].open}` : "") + `；成本 ${money(T[k].cost)}，收益率 ${pct(T[k].roi)}`)).join("");
  const row = (label, x) => `<tr><td>${esc(label)}</td><td>${x.quoted ?? ""}</td><td>${x.filled}</td><td>${pct(x.fill_rate)}</td><td>${x.both}</td><td>${x.wins}/${x.losses}</td><td>${money(x.cost)}</td><td>${signed(x.pnl)}</td><td>${pct(x.roi)}</td></tr>`;
  const head = "<tr><th></th><th>报价场次</th><th>成交场次</th><th>成交率</th><th>双边</th><th>赢/输</th><th>成本</th><th>盈亏</th><th>收益率</th></tr>";
  $("jDay").innerHTML = head.replace("<th></th>", "<th>日期</th>") + (d.by_day.map((x) => row(x.day, x)).join("") || '<tr><td colspan="9" class="muted">还没有记录</td></tr>');
  $("jCoin").innerHTML = head.replace("<th></th>", "<th>币种</th>") + d.by_coin.map((x) => row(x.coin, x)).join("");
  $("jHour").innerHTML = head.replace("<th></th>", "<th>开场</th>") + d.by_hour.map((x) => row(String(x.hour).padStart(2, "0") + ":00", x)).join("");
  $("jSide").innerHTML = "<tr><th>方向</th><th>成交份数</th><th>成本</th><th>赢回</th><th>盈亏</th></tr>" + d.by_side.map((x) => `<tr><td>${x.side}</td><td>${x.shares}</td><td>${money(x.cost)}</td><td>${money(x.won)}</td><td>${signed(x.pnl)}</td></tr>`).join("");
  const C = d.cancel || {rows: [], by_fill_time: []};
  $("cAdvice").textContent = "结论：" + (C.advice || "—");
  $("cRows").innerHTML = "<tr><th>开场前几秒撤</th><th>有成交的场次</th><th>双边</th><th>成交份数</th><th>成本</th><th>盈亏</th><th></th></tr>" + (C.rows.map((x) =>
    `<tr><td>${x.cancel_s} 秒</td><td>${x.filled} / ${x.windows}</td><td>${x.both}</td><td>${x.shares}</td><td>${money(x.cost)}</td><td>${signed(x.pnl)}</td><td>${x.cancel_s === C.current ? "现在" : ""}${x.cancel_s === C.best ? " ★最好" : ""}</td></tr>`).join("") || '<tr><td colspan="7" class="muted">还没有结束的场次</td></tr>');
  $("cNote").textContent = C.rows.length ? `把每一场按「如果开场前 X 秒撤单」重算（X 秒之后的成交当作没发生），同一批 ${C.windows - C.excluded} 场比较。比开场前 ${C.base} 秒更晚撤算不出来：那时单子已经撤了。${C.excluded ? `另有 ${C.excluded} 场当时撤得更早，没算。` : ""}${C.approx ? `${C.approx} 场的成交时刻是快照看到的时间（偏晚几秒）。` : ""}` : "";
  $("cTimes").innerHTML = "<tr><th>成交时距开场</th><th>份数</th><th>成本</th><th>盈亏</th><th>那一边赢的比例</th><th>保本线</th></tr>" + (C.by_fill_time.map((x) =>
    `<tr><td>${esc(x.bucket)}</td><td>${x.shares}</td><td>${money(x.cost)}</td><td>${signed(x.pnl)}</td><td class="${x.win_rate >= x.breakeven ? "good" : "bad"}">${(x.win_rate * 100).toFixed(0)}%</td><td>${(x.breakeven * 100).toFixed(0)}%</td></tr>`).join("") || '<tr><td colspan="6" class="muted">还没有成交</td></tr>');
  $("jDetail").innerHTML = "<tr><th>场次</th><th>档</th><th>价格</th><th>涨</th><th>跌</th><th>成本</th><th>结果</th><th>盈亏</th><th>挂上时距开场</th><th>撤单确认</th><th>首次成交距开场</th><th>第几次</th></tr>" + (d.detail.map((x) =>
    `<tr><td>${esc(x.coin)} ${esc(x.when)}</td><td>${x.level}</td><td>${x.price}¢</td><td>${x.up || ""}</td><td>${x.down || ""}</td><td>${money(x.cost)}</td><td>${esc(x.result)}</td><td>${x.pnl == null ? "" : signed(x.pnl)}</td>` +
    `<td>${secs(x.placed_lead_s)}</td><td>${x.cancel_lead_s == null ? "—" : "开场前 " + secs(x.cancel_lead_s)}</td><td>${secs(x.first_fill_lead_s)}</td><td>${x.attempt}</td></tr>`).join("") || '<tr><td colspan="12" class="muted">还没有成交</td></tr>');
  $("jMore").textContent = d.detail_more ? `另有 ${d.detail_more} 场在 CSV 里` : "";
}
async function download(kind) {
  const r = await api("/api/journal.csv?kind=" + kind); if (r.status !== 200) return toast("导出失败", true);
  const url = URL.createObjectURL(new Blob([r.data], {type: "text/csv;charset=utf-8"}));
  const a = document.createElement("a"); a.href = url; a.download = {daily: "preopen-daily.csv", quotes: "preopen-quotes.csv", cancel: "preopen-cancel-review.csv"}[kind] || "preopen-fills.csv"; document.body.appendChild(a); a.click(); a.remove();
  setTimeout(() => URL.revokeObjectURL(url), 5000);
  if (initData) toast("Telegram 里可能下载不了：发 /export 让机器人把文件发给你");
}
$("csvDetail").onclick = () => download("detail");
$("csvDaily").onclick = () => download("daily");
$("csvQuotes").onclick = () => download("quotes");
$("csvCancel").onclick = () => download("cancel");

// ---- 操作
async function act(action, question, extra) {
  if (!(await ask(question))) return;
  const r = await api("/api/action", {method: "POST", body: JSON.stringify(Object.assign({action}, extra || {}))});
  toast(r.data.message || r.data.error || "完成", r.status !== 200); clearTimeout(timer); tick(false);
}
$("pauseBtn").onclick = () => act("pause", "暂停新挂，并撤掉本程序的全部挂单？");
$("resumeBtn").onclick = () => act("resume", "恢复挂单？");
$("emergencyBtn").onclick = () => act("emergency", "紧急停止：暂停，并马上撤掉本程序的全部挂单（PREOPEN_KEY_EXCLUSIVE=on 时一次撤光这个 API Key 的全部挂单）？");
$("rebindBtn").onclick = () => act("rebind", "确定把账本改绑到当前账户吗？旧账户的记录不再接管（它们的挂单请先到 Polymarket 撤掉，否则到期才失效），程序会重启。");
window.addEventListener("hashchange", () => go((location.hash || "").slice(1)));
go(state.tab);
</script>
</body>
</html>
"""

PAGE_VERSION = hashlib.sha256(PAGE.encode()).hexdigest()[:12]
PAGE_HTML = PAGE.replace("__PAGE_VERSION__", PAGE_VERSION).encode()
