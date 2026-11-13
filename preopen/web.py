"""The web console, served by the bot itself (no extra dependency): one page with the status, the resting orders, the
fills and the problems, and pause / resume.

    GET  /             the page (no data in it; the data comes from /api/status)
    GET  /health       "ok", no sign-in
    POST /api/login    {"key": WEB_KEY} -> a session cookie
    GET  /api/status   the engine's report (signed in)
    POST /api/action   {"action": "pause" | "resume"} (signed in)

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
from typing import Any, Awaitable, Callable

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


def check_init_data(init_data: str, bot_token: str, admin_id: str, now: float | None = None) -> bool:
    """Telegram Mini App initData: signed with the bot token (HMAC-SHA256, key = HMAC("WebAppData", token)), fresh,
    and from the admin (the user id equals TELEGRAM_CHAT_ID, which for a private chat is the user's id)."""
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
    return str(user.get("id", "")) == str(admin_id).strip() and bool(str(admin_id).strip())


class WebServer:
    def __init__(self, port: int, report: Callable[[], dict], actions: dict[str, Callable[[], Awaitable[str]]],
                 web_key: str = "", bot_token: str = "", admin_id: str = "", host: str = "0.0.0.0"):
        self.port, self.host = port, host
        self.report, self.actions = report, actions
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

    async def respond(self, method: str, target: str, headers: dict[str, str], body: bytes) -> tuple[int, str, bytes, list[tuple[str, str]]]:
        path = urllib.parse.urlsplit(target).path
        if path == "/health":
            return 200, "text/plain; charset=utf-8", b"ok", []
        if path in ("/", "/index.html"):
            if method != "GET":
                return 405, "text/plain", b"", []
            return 200, "text/html; charset=utf-8", PAGE.encode(), []
        if path == "/api/login":
            if method != "POST":
                return 405, "text/plain", b"", []
            now = time.time()
            self.fails = [t for t in self.fails if now - t < LOGIN_WINDOW_S]
            if len(self.fails) >= LOGIN_FAILS_MAX:
                return 429, "application/json", b'{"error": "too many attempts"}', []
            try:
                key = str(json.loads(body or b"{}").get("key", ""))
            except (ValueError, AttributeError):
                key = ""
            if not self.web_key or not hmac.compare_digest(key, self.web_key):
                self.fails.append(now)
                await asyncio.sleep(1.0)
                return 401, "application/json", b'{"error": "wrong key"}', []
            cookie = (f"{COOKIE}={session_token(self.web_key)}; Path=/; Max-Age={SESSION_DAYS * 86400}; HttpOnly; Secure; "
                      "SameSite=Lax")
            return 200, "application/json", b'{"ok": true}', [("Set-Cookie", cookie)]
        if path == "/api/status":
            if not self.authorized(headers):
                return 401, "application/json", json.dumps({"error": "sign in", "web_key": bool(self.web_key)}).encode(), []
            return 200, "application/json", json.dumps(self.report(), ensure_ascii=False, default=str).encode(), []
        if path == "/api/action":
            if method != "POST":
                return 405, "text/plain", b"", []
            if not self.authorized(headers):
                return 401, "application/json", b'{"error": "sign in"}', []
            if "application/json" not in headers.get("content-type", ""):
                return 400, "application/json", b'{"error": "json only"}', []
            try:
                action = str(json.loads(body or b"{}").get("action", ""))
            except (ValueError, AttributeError):
                action = ""
            if action not in self.actions:
                return 400, "application/json", b'{"error": "unknown action"}', []
            message = await self.actions[action]()
            return 200, "application/json", json.dumps({"message": message}, ensure_ascii=False).encode(), []
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
            writer.write(("\r\n".join(head) + "\r\n\r\n").encode("latin-1") + payload)
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
:root { --bg:#f6f7f9; --card:#fff; --text:#16181d; --muted:#6b7280; --line:#e5e7eb; --accent:#2563eb; --good:#15803d; --bad:#b91c1c; --warn:#b45309; }
@media (prefers-color-scheme: dark) { :root { --bg:#0f1115; --card:#181b21; --text:#e8eaee; --muted:#9aa1ad; --line:#2a2f38; --accent:#5b8cff; --good:#4ade80; --bad:#f87171; --warn:#fbbf24; } }
* { box-sizing:border-box; }
[hidden] { display:none !important; }
body { margin:0; background:var(--bg); color:var(--text); font:14px/1.5 -apple-system,BlinkMacSystemFont,"PingFang SC","Microsoft YaHei",sans-serif; }
main { max-width:960px; margin:0 auto; padding:16px; }
h1 { font-size:18px; margin:0; }
header { display:flex; flex-wrap:wrap; align-items:center; gap:8px; justify-content:space-between; margin-bottom:12px; }
.badge { display:inline-block; padding:2px 8px; border-radius:999px; font-size:12px; background:var(--line); color:var(--text); margin-left:6px; }
.badge.live { background:var(--bad); color:#fff; } .badge.paper { background:var(--accent); color:#fff; } .badge.pause { background:var(--warn); color:#fff; }
.cards { display:grid; grid-template-columns:repeat(auto-fit,minmax(150px,1fr)); gap:10px; margin-bottom:12px; }
.card { background:var(--card); border:1px solid var(--line); border-radius:12px; padding:12px; }
.card .k { color:var(--muted); font-size:12px; } .card .v { font-size:20px; font-weight:600; margin-top:2px; } .card .s { color:var(--muted); font-size:12px; }
section { background:var(--card); border:1px solid var(--line); border-radius:12px; padding:12px; margin-bottom:12px; }
section h2 { font-size:15px; margin:0 0 8px; }
.scroll { overflow-x:auto; }
table { width:100%; border-collapse:collapse; font-size:13px; white-space:nowrap; }
th, td { text-align:left; padding:6px 8px; border-bottom:1px solid var(--line); }
th { color:var(--muted); font-weight:500; }
.good { color:var(--good); } .bad { color:var(--bad); } .warn { color:var(--warn); } .muted { color:var(--muted); }
button { font:inherit; border:0; border-radius:10px; padding:8px 14px; cursor:pointer; background:var(--accent); color:#fff; }
button.secondary { background:var(--line); color:var(--text); } button.danger { background:var(--bad); }
.notice { padding:10px 12px; border-radius:10px; margin-bottom:12px; background:var(--card); border:1px solid var(--warn); }
.kv { display:grid; grid-template-columns:max-content 1fr; gap:4px 12px; font-size:13px; } .kv div:nth-child(odd) { color:var(--muted); }
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
    <header>
      <h1>Polymarket 5 分钟涨跌预挂 <span id="mode" class="badge"></span><span id="paused" class="badge pause" hidden>已暂停</span></h1>
      <div><button id="pauseBtn" class="danger">暂停并撤单</button> <button id="resumeBtn">恢复挂单</button></div>
    </header>
    <div id="hold" class="notice" hidden></div>
    <div class="cards" id="cards"></div>
    <section><h2>运行情况</h2><div class="kv" id="info"></div></section>
    <section><h2>挂着的单（开场最近的 40 档）</h2><div class="scroll"><table id="upcoming"></table></div></section>
    <section><h2>有成交的场次</h2><div class="scroll"><table id="fills"></table></div></section>
    <section><h2>最近的问题</h2><div id="errors" class="muted"></div></section>
    <p class="muted" id="stamp"></p>
  </div>
</main>
<script>
const tg = window.Telegram && window.Telegram.WebApp;
if (tg) { tg.ready(); tg.expand(); }
const initData = tg ? tg.initData : "";
const $ = (id) => document.getElementById(id);
const esc = (s) => String(s ?? "").replace(/[&<>"']/g, (c) => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));
const money = (v) => v == null ? "—" : "$" + Number(v).toFixed(2);
const STATE = {open:"挂着", placing:"发送中", unknown:"状态不明", gone:"已撤/核对中", final:"已结束", rejected:"被拒", not_sent:"没发出", not_found:"没挂上"};
async function api(path, opts = {}) {
  const headers = Object.assign({"Content-Type": "application/json"}, opts.headers || {});
  if (initData) headers["X-Telegram-Init-Data"] = initData;
  const r = await fetch(path, Object.assign({credentials: "same-origin"}, opts, {headers}));
  return {status: r.status, data: await r.json().catch(() => ({}))};
}
function card(k, v, s) { return `<div class="card"><div class="k">${esc(k)}</div><div class="v">${v}</div><div class="s">${s || ""}</div></div>`; }
function render(d) {
  $("login").hidden = true; $("app").hidden = false;
  $("mode").textContent = d.mode; $("mode").className = "badge " + (d.live ? "live" : "paper");
  $("paused").hidden = !d.paused;
  const hold = d.hold || d.cooldown;
  $("hold").hidden = !hold && !d.budget_note; $("hold").textContent = [hold && ("暂不新挂：" + hold), d.budget_note && ("资金：" + d.budget_note)].filter(Boolean).join("　");
  const t = d.today || {};
  $("cards").innerHTML = [
    card("挂着", `${d.windows} 场`, `${d.resting} 张单` + (d.unknown ? `，<span class="warn">${d.unknown} 张状态不明</span>` : "")),
    card("预留 / 余额", money(d.reserved), "余额 " + money(d.balance) + (d.settings.max_open ? `，上限 ${money(d.settings.max_open)}` : "")),
    card("今天成交", `${t.fills || 0} 场`, `双边 ${t.both || 0}，已结算 ${t.settled || 0}`),
    card("今天盈亏", `<span class="${(t.pnl || 0) >= 0 ? "good" : "bad"}">${(t.pnl || 0) >= 0 ? "+" : ""}${Number(t.pnl || 0).toFixed(2)}</span>`, "美元，已结算部分"),
    card("开放→挂上", d.lags.open == null ? "—" : d.lags.open + " 秒", "中位"),
    card("撤单确认", d.lags.cancel == null ? "—" : "开场前 " + d.lags.cancel + " 秒", "中位"),
  ].join("");
  const s = d.settings;
  const nxt = d.next_listing ? `${d.next_listing.start}（预计 ${d.next_listing.expected} 发布）` : "范围内都已发布";
  $("info").innerHTML = [
    ["币种", d.coins.join(" ") || "无"], ["每边", d.levels + `（每场预留 ${money(d.window_usd)}）`],
    ["时间", `开场前 ${s.ahead_min} 分钟内的场次；开场前 ${s.cancel_s} 秒撤单、${s.expiry_s} 秒到期、${s.entry_s} 秒内不新挂` + (s.weekend ? "" : "；周末不挂")],
    ["已发布到", d.listed_until || "—"], ["下一场", nxt],
    ["账户快照", d.snapshot_age == null ? "还没读到" : d.snapshot_age + " 秒前"],
    ["队列", `排队 ${d.queue}，等候 ${d.waiting}，正在挂 ${d.placing}`],
    ["其他挂单", d.unmanaged ? `${d.unmanaged} 张本程序没记录的挂单（${money(d.unmanaged_usd)}）` : "无"],
    ["订单号=哈希", d.id_is_hash == null ? "未知" : (d.id_is_hash ? "是" : "否")], ["限流档位", d.tier || "未知"],
  ].map(([k, v]) => `<div>${esc(k)}</div><div>${esc(v)}</div>`).join("");
  $("upcoming").innerHTML = "<tr><th>场次</th><th>还有</th><th>档</th><th>价格×份数</th><th>涨</th><th>跌</th></tr>" + (d.upcoming.map((q) =>
    `<tr><td>${esc(q.coin)} ${esc(q.when)}</td><td>${esc(q.in)}</td><td>${q.level + 1}</td><td>${q.price}¢×${q.shares}</td>` +
    `<td>${esc(STATE[q.up] || q.up)}${q.up_filled ? ` <b class="good">成交 ${q.up_filled}</b>` : ""}</td>` +
    `<td>${esc(STATE[q.down] || q.down)}${q.down_filled ? ` <b class="good">成交 ${q.down_filled}</b>` : ""}</td></tr>`).join("") ||
    '<tr><td colspan="6" class="muted">没有挂着的单</td></tr>');
  $("fills").innerHTML = "<tr><th>场次</th><th>价格</th><th>涨成交</th><th>跌成交</th><th>结果</th><th>盈亏</th><th>领取</th></tr>" + (d.fills.map((q) =>
    `<tr><td>${esc(q.coin)} ${esc(q.when)}</td><td>${q.price}¢</td><td>${q.up || ""}</td><td>${q.down || ""}</td><td>${esc(q.result || (q.phase === "settled" ? "" : "待结算"))}</td>` +
    `<td class="${(q.pnl || 0) >= 0 ? "good" : "bad"}">${q.pnl == null ? "" : ((q.pnl >= 0 ? "+" : "") + Number(q.pnl).toFixed(2))}</td><td>${esc(q.redeem)}</td></tr>`).join("") ||
    '<tr><td colspan="7" class="muted">还没有成交</td></tr>');
  $("errors").innerHTML = d.errors.length ? d.errors.map((e) => `<div>${esc(e.when)} ${esc(e.text)}</div>`).join("") : "无";
  $("stamp").textContent = "更新于 " + d.now_text + "（北京时间），每 5 秒刷新";
}
let timer = null;
async function refresh() {
  try {
    const r = await api("/api/status");
    if (r.status === 401) { showLogin(r.data); return; }
    if (r.status === 200) render(r.data);
  } catch (e) { $("stamp").textContent = "连接失败，稍后重试"; }
  timer = setTimeout(refresh, 5000);
}
function showLogin(info) {
  $("app").hidden = true; $("login").hidden = false;
  if (initData) $("loginHint").textContent = "Telegram 验证没通过：确认 TELEGRAM_CHAT_ID 是你自己的用户 ID；或输入 WEB_KEY。";
  if (info && info.web_key === false && !initData) $("loginHint").textContent = "还没设置 WEB_KEY：在 Railway 的 Variables 里加上 WEB_KEY 后重新部署。";
}
$("loginBtn").onclick = async () => {
  const r = await api("/api/login", {method: "POST", body: JSON.stringify({key: $("key").value})});
  if (r.status === 200) { $("loginMsg").textContent = ""; clearTimeout(timer); refresh(); } else { $("loginMsg").textContent = r.status === 429 ? "尝试太多，稍后再试" : "WEB_KEY 不对"; }
};
$("key").addEventListener("keydown", (e) => { if (e.key === "Enter") $("loginBtn").click(); });
const native = tg && tg.isVersionAtLeast && tg.isVersionAtLeast("6.2");
const ask = (q) => new Promise((ok) => native ? tg.showConfirm(q, ok) : ok(window.confirm(q)));
const tell = (t) => native ? tg.showAlert(t) : window.alert(t);
async function act(action, question) {
  if (!(await ask(question))) return;
  const r = await api("/api/action", {method: "POST", body: JSON.stringify({action})});
  tell(r.data.message || r.data.error || "完成"); clearTimeout(timer); refresh();
}
$("pauseBtn").onclick = () => act("pause", "暂停新挂，并撤掉本程序的全部挂单？");
$("resumeBtn").onclick = () => act("resume", "恢复挂单？");
refresh();
</script>
</body>
</html>
"""
