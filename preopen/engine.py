"""The controller: find each window the moment it is listed, quote it post-only on both sides, withdraw before the start.

Loops (all in one asyncio process):
    discovery   every coin's next listing by the direct path (fast around the expected moment), a full pass a minute
    workers     place the queued windows: the freshest listings first (armed until the CLOB opens), then the soonest
    scan        a full pass over the range's unlisted windows a minute (its own loop: a slow pass never delays the next)
    cancel      every second: the windows at their cancel time (and any closing record) withdrawn by id, an order of
                unknown state by its hash; failed cancels sprint every 2 s to the expiry
    reconcile   orders off the book get their final record read; unknown placements are looked for by their hash
    account     the open orders and the balance: fills, orders gone from the book, the budget
    settle      the fills of a closed window valued by the official result; the winning side redeemed

A window's level is a record (a "quote", ledger.py) with an order per side. The orders are BUY, post-only, GTD:
they expire on their own `expiry_s` before the start (the backstop of the cancel), so a stopped process leaves nothing
on the book past the start; the next process takes the resting orders over.

Every order is written down twice before it is sent: the record (state "placing"), then, once signed, its hash (the
CLOB's order id). An order whose answer was lost, or whose process stopped mid-send, is therefore never guessed at: it is
looked for by its hash, cancelled by it, and given up only on the CLOB's word (no such order) or, past its signed
expiry, with its fills read from the account's trades. Only the orders the bot itself signed are ever cancelled, unless
PREOPEN_KEY_EXCLUSIVE=on says the API key is the bot's alone (then a window's market is also swept at its cancel time).
"""
from __future__ import annotations

import asyncio
import bisect
import collections
import contextlib
import heapq
import json
import logging
import re
import sqlite3
import time
from dataclasses import dataclass
from decimal import Decimal as D
from typing import Any, Callable, Iterator

from . import config
from . import markets as mk
from . import refusals
from .config import GTD_THRESHOLD_S, Settings
from .exchange import (MATCHED_STATES, WINDOW_CODES, Cancelled, Exchange, OrderSpec, Placed, Row, Throttled, refusal_code,
                       retryable, unrecognised)
from .gamma import PublicThrottled
from .ledger import ENDED, MAY_REST, NEVER_PLACED, Ledger, quote_key, unclaimed

LOG = logging.getLogger(__name__)

FULL_SCAN_S = 60          # a full pass over the range's unlisted windows this often (between them: each coin's next one)
SKIP_RETRY_S = 60         # a listed window that is no candidate is read again this often
FAST_BEFORE_S = 15        # poll fast from this long before a listing's expected moment...
FAST_AFTER_S = 180        # ...to this long after (a listing can come minutes late: 11-13 02:52)
LEADS_KEPT = 5            # a coin's listings are expected by the median lead of its latest this many: a V2 market is
                          # created 23 h 59 min before its start, a CTF one 23 h 53 min. One median over every coin, as
                          # it was, had the other kind's listings found by the slow poll: 7 s after their creation, when
                          # a V2 market opens after 9.4 (11-25, on a paper run beside a probe)
HOLD_RETRY_S = 5          # a window held back (pause, cooldown, stale snapshot) is tried again this long on
BUDGET_RETRY_S = 30       # ...held back by the budget
ARM_RETRY_S = 0.2         # a fresh listing the CLOB does not accept yet is asked again this often from the moment it is
ARM_FAST_UNTIL_S = 120    # found, to this age. Measured 11-25 over seven listings: V2 markets are opened in batches ten
                          # seconds apart, a market by the first batch 9.4 s or more after its creation: 9.4-10.2 s for
                          # most, the coins of a round together; 19.5 s for one created a moment late (07:01: DOGE and
                          # HYPE, ten seconds after SOL, XRP and BNB). Polymarket's new_market push comes 2.7 s after
                          # the opening and the first resting order of anyone 6-7 s after it. A CTF market opens after
                          # 70-94 s (11-13, 11-25), the push 0.35 s later. (Once a second until +55 s, as it was for
                          # the CTF markets, found a V2 opening half a second late; on a paper run beside a probe the
                          # five coins' opening is now seen 0.15-0.5 s after it, the round trip included.)
ARM_SLOW_S = 1.0          # ...past that age once a second,
ARM_FRESH_S = 300         # ...and from this age every HOLD_RETRY_S
ARM_IDLE_S = 900          # still not accepting this long after its creation (V2 markets may be created a day before their
ARM_IDLE_RETRY_S = 30     # trading starts, docs 11-22): asked every 30 s, and ARM_IDLE_RATE a second over all such
ARM_IDLE_RATE = 2.0       # listings, so hundreds of them never flood the CLOB (a 429 there would stall every opening)
ERROR_RETRY_S = 30        # a placement that raised
FREE_REQUOTE_S = 3        # a withdrawn level is quoted again this long after its close (not its own failure)...
COUNTED_REQUOTE_S = 60    # ...or after its own failure
SPRINT_S = 2              # past the cancel time an open order is asked to be cancelled this often
RETRY_S, RETRY_MAX_S = 15, 120   # before it: a failed cancel or read retried after this, doubling to the cap
FINAL_DELAY_S = 5         # an order off the book has its final record read this long after it left
GIVE_UP_S = 120           # an unknown placement the CLOB says it has no order for (its id being the hash) this long
                          # after the send never rested
LOOKUP_EVERY_S = 5        # an unknown placement is looked for by its hash this often
UNKNOWN_GRACE_S = 30      # a resting record with an order still unknown this long after it became so is withdrawn
EXPIRED_MARGIN_S = 5      # past its stated expiration (and this) an order rests nowhere: its fills are final
TRADES_TTL_S = 10         # the account's trades of a market, read once for every order that needs them this often
UNVERIFIED_AFTER_S = 1800 # past the expiration, the trades unreadable this long: recorded unverified (and said so)
SEEN_MARGIN_S = 3         # an order accepted this long before a snapshot began should be in it
LEDGER_FREE_WARN = 500 * 2**20   # the check warns when the ledger's disk has less than this left
CLOCK_EVERY_S = 3600      # the clock checked against the CLOB's this often
CLOCK_BELIEVED_S = 6 * 3600   # a difference larger than this is a bad reading, not a clock
EXTERNAL_STATES = ("CANCELED", "CANCELLED")   # an order's end that somebody's cancel made (the website, the key elsewhere)
OFF_BOOK_WORDS = ("already", "not found", "can't be found", "cannot be found", "does not exist", "doesn't exist",
                  "no such order", "expired")   # a cancel refused in these words: the order is off the book (filled,
                                                # cancelled, gone); in any other words it may still rest
SWEEP_EVERY_S = 5         # a hash not answered for (an unknown order, a send judged refused) is cancelled again this often
WITHDRAW_PAUSE_S = 1.0    # a withdrawal (the exit, a switch to paper) asks again this often (or after the server's wait)
PLACING_DRAIN_S = 10      # at the exit a placement in flight is given this long for its answer before the workers stop
BALANCE_EVERY_S = 30      # the balance is read this often...
BALANCE_STALE_S = 3 * BALANCE_EVERY_S + 15   # ...and no new order is placed on a balance older than this
SETTLE_AFTER_S = 60       # a window's result is looked for this long after its end
RESULT_RETRY_S = 30
REDEEM_RETRY_S = 600
FILL_LOG_TRIES = 3        # a window's trades read for the fills' exact times this many passes, then settled without them
REDEEM_TRIES = 6
SETTINGS_KEPT = 200       # settings changes kept (when, how, what from and to), for the review's 参数 sheet
V2_HOLD_S = 600           # a V2 market refused for the balance or allowance with ExchangeV3's not known granted: V2 waits
V2_REFRESH_S = 300        # ...and the CLOB is asked to read the allowances afresh this often meanwhile
V2_NOTE_S = 1800          # V2 markets left unquoted for the approval: told on Telegram this often
V2_HELP = ("账户的 pUSD 还没授权给 Polymarket Protocol V2 的交易所（ExchangeV3），V2 市场的单会被拒。到 Polymarket 网页或 App "
           "打开一个 V2 市场（比如 SOL 的 5 分钟涨跌场），按提示完成授权；授权后几分钟内"
           "机器人自己开始挂 V2 市场。旧的（CTF）市场照常挂，不受影响")
STALE_EXTRA_S = 15
BALANCE_HOLD_S = 60       # the CLOB refused or cancelled an order for the balance: no new order this long
RECONCILE_BUDGET_S = 20   # a reconcile pass is cut off after this (its reads are retried next pass)
ERRORS_KEPT = 30
UNSURE = ("unknown", "placing", "prepared")   # sent (or maybe sent) and not answered for: looked for by the hash
REFUSE_HOLD_S = 30        # the CLOB refused orders for a reason not the window's own: no new order this long, doubling
REFUSE_HOLD_MAX_S = 600   # to this while it keeps refusing (an accepted order resets it); no window is struck off for it
ACCOUNT_REFUSALS = {"invalid_expiration", "banned", "closed_only", "batch_too_large", "invalid_payload", "auth"}
REFUSAL_HELP = {          # what an account-wide refusal means for the operator, said with it
    "auth": "签名私钥和 API Key 不是同一个账户：Railway 里的 POLY_PRIVATE_KEY 和 POLY_API_KEY / POLY_API_SECRET / "
            "POLY_API_PASSPHRASE 要属于同一个账户（最简单是删掉 POLY_API_ 这三项，程序会用私钥自己生成）",
    "banned": "这个地址被 Polymarket 禁止交易",
    "closed_only": "这个地址只能平仓、不能新挂单",
    "invalid_expiration": "交易所说到期时间无效（检查撤单/到期秒数的设置）",
    "invalid_payload": "交易所说订单格式不对",
    "batch_too_large": "一次发的单超过了交易所的上限",
}
DEEP_AFTER_OPEN_S = 30    # a market just opened: only its nearest level goes at once (the race for the queue is at the
                          # best price); the deeper ones this long after the opening, so the opening burst stays small
                          # (11-17: seven coins x three levels x two sides = 42 orders in one instant, then 429s)
TOKEN_RETRY_S = 0.5       # a market whose tokens the matching engine does not know yet ("invalid token id", common in
TOKEN_FAST_TRIES = 40     # the seconds after a market opens): its window alone is tried again this often, this many times (20 s);
TOKEN_SLOW_RETRY_S = 10   # then this often, and the listing is read again from Gamma once (its tokens may have changed)
OPEN_SETTLE_S = 0.6       # a market just seen accepting: its first order goes this long after (a CTF market),
OPEN_SETTLE_V2_S = 3.0    # ...or this long (a V2 one). An order sent before the matching engine knows the token is not
TOKEN_RETRY_V2_S = 2.0    # just refused ("invalid token id"): what follows is refused too, for seconds. Live 11-25:
TOKEN_FAST_TRIES_V2 = 10  #  15:26  V2, asked every 0.2 s and sent at once: SOL, BNB, HYPE refused 40 times in a row, 20 s
                          #         and more (a try every half second);
                          #  16:07  CTF, sent at once: BTC "invalid token id", then 429 after 429 (Retry-After 1-2 s each)
                          #         until 8.2 s after its opening; ETH, opened 4 s after BTC, refused with it until the
                          #         same moment: the refusals are the account's, not the market's;
                          #  16:06, 16:11  V2, sent 2.5 s after the opening: 429 (1 s) for three of four, resting at once
                          #         for one; sent 3.5 s after: resting, seven of seven.
                          # The engine knows a token about when Polymarket pushes new_market: 0.35 s after a CTF market's
                          # opening, 2.7 s after a V2 one's (11-25, a listener beside a probe); on a CTF book the first
                          # order of anyone rested 0.38 s after the opening and the crowd's from 1.25 s, on a V2 book
                          # 6-7 s after. So the first order waits out the push with a margin, and a V2 token refused all
                          # the same is tried again after two seconds, ten times, then the slow way (a CTF one: half a
                          # second, forty times, as before).
OPENINGS_KEPT = 40        # the latest fresh windows' openings kept: when each order request went and what it was answered
OPENING_WATCH_S = 90      # ...for this long after the opening was seen
WAIT_MARGIN_S = 0.05      # a window refused with a wait (a 429's Retry-After) is tried again this long after it ends
TWINS_ALERT = 6           # this many of the bot's own kind of orders on the account, not the bot's: another copy runs
REFUSALS_KEPT = 300       # the latest refusals kept for /rejects (also across restarts)
REFUSALS_SAVE_S = 10      # ...written to the ledger at most this often
EXTERNAL_MASS = 3         # the bot's orders cancelled elsewhere (plain CANCELED: the website's 取消所有, another program
EXTERNAL_WINDOW_S = 60    # with the key) in this many windows within this long: taken as "stop", and the bot pauses
PAUSE_LOOKBACK_S = 120    # a pause kept without its reason (an older version): the records closed this long before it
EXTERNAL_WHY = "本程序的挂单在别处被成批撤掉"   # the guard's pause, as 📊, 🩺 and the page word it
PAIR_DUST = D("0.01")     # of an order's shares: what an order eaten whole may end short of its size (the CLOB takes each
                          # fill off it rounded up to 0.01 share: 30 shares in seven fills ended at 29.98). The other
                          # side's whole order still pairs it.


def dnum(value: Any) -> D:
    return D(str(value or 0))


def off_book(said: str) -> bool:
    """Whether the CLOB's reason for not cancelling an order says the order is off the book (already matched,
    already cancelled, not found): then there is nothing to cancel. In other words it may still rest."""
    text = str(said or "").lower()
    return any(w in text for w in OFF_BOOK_WORDS)


@dataclass
class Hold:
    reason: str
    retry: float | None   # None: not parked; the sweep (every poll) queues the window again when it fits


class Engine:
    def __init__(self, settings: Settings, exchange: Exchange, public: Any, ledger: Ledger,
                 notify: Callable[[str, str], None] | None = None, clock: Callable[[], float] = time.time):
        self.s = settings
        self.x = exchange
        self.pub = public
        self.ledger = ledger
        self.notify_cb = notify
        self.clock = clock
        self.skew = 0.0
        self.listings: dict[str, mk.Listing] = {}
        self.skipped: dict[str, tuple[float, str]] = {}
        self.queue: asyncio.PriorityQueue = asyncio.PriorityQueue()
        self.queued: set[str] = set()
        self.placing: set[str] = set()
        self.timers: dict[str, asyncio.TimerHandle] = {}
        self.by_slug: dict[str, set[str]] = collections.defaultdict(set)
        self.by_id: dict[str, tuple[str, str]] = {}
        self.open_keys: set[str] = set()     # records not settled yet (the loops walk these, not the whole ledger)
        self.paused = ledger.get("paused") == "1"
        self.pause_note: dict[str, Any] = self.read_pause_note() if self.paused else {}   # when, why, which windows
        self.id_is_hash: bool | None = {"1": True, "0": False}.get(ledger.get("id_is_hash"))
        self.cooldown_until = 0.0
        self.cooldown_reason = ""
        self.snapshot: dict[str, Row] = {}
        self.snapshot_at = 0.0
        self.snapshot_took = 0.0
        self.unmanaged: list[Row] = []
        self.balance: D | None = None
        self.balance_at = 0.0
        self.full_scan_at = 0.0
        self.sweep_at = 0.0
        self.swept: dict[str, float] = {}
        self.lead = float(settings.publish_lead_s)   # the coins' leads' median: for the reports, and a coin never seen
        self.leads: dict[str, collections.deque] = {}   # coin -> start - createdAt of its latest listings
        self.listing_lags: collections.deque = collections.deque(maxlen=50)   # seen - createdAt of fresh listings
        self.place_lags: collections.deque = collections.deque(maxlen=50)     # placed - createdAt
        self.open_lags: collections.deque = collections.deque(maxlen=50)      # placed - when the CLOB was first seen accepting
        self.cancel_lags: collections.deque = collections.deque(maxlen=50)    # seconds before the start the cancel was confirmed
        self.errors: collections.deque = collections.deque(maxlen=ERRORS_KEPT)
        self.fills_pending: list[str] = []
        self.hold_note = ""
        self.scan_note = ""
        self.full_note = ""
        self.stop = asyncio.Event()
        self.started_at = 0.0
        self.notified: dict[str, float] = {}
        self.redeem_at = 0.0
        self.counters = collections.Counter()
        self.version = 0                      # bumped on every record change: reserved() is cached by it
        self._reserved: tuple[int, D] = (-1, D(0))
        self._memo: dict[str, tuple[Any, Any]] = {}   # per_version(): name -> ((version, extra), value)
        self.budget_note = ""
        self.balance_hold_until = 0.0         # the CLOB said the balance was short: no new order until then
        self.refused_until = 0.0              # the CLOB refused orders for the account's sake, not the window's: no new
        self.refused_reason = ""              # order until then (doubling while it keeps refusing)
        self.refused_code = ""
        self.refused_streak = 0
        self.refused_at: dict[str, float] = {}  # slug -> when its orders were last refused that way
        self.accepted_at = 0.0                # when the CLOB last accepted an order (ends a refusal streak)
        self.last_refusal: tuple[float, str, str] | None = None   # (when, code, words) of the latest order refused
        self.token_refusals: dict[str, int] = {}  # slug -> "invalid token id" answers in a row
        self.refusal_log: collections.deque = collections.deque(maxlen=REFUSALS_KEPT)  # every refusal, for /rejects
        try:
            self.refusal_log.extend(json.loads(ledger.get("refusals") or "[]"))
        except ValueError:
            pass
        self.refusals_saved_at = 0.0
        self.recent_refusals: collections.deque = collections.deque(maxlen=500)  # (when, slug, code) of those refusals
        self.external: collections.deque = collections.deque(maxlen=200)         # (when, slug) of cancels made elsewhere
        self.restart = False                  # set by the console (a LIVE switch): main() starts the process again
        self.attempted: dict[str, tuple[str, int]] = {}  # condition id -> (slug, start) of every market an order was sent to
        self.strays: set[str] = set()         # order ids that came back after their record was judged: cancelled
        self.by_hash: dict[str, tuple[str, str]] = {}    # order hash -> (record key, side)
        self.ghosts: dict[str, dict] = self.read_ghosts()   # hash -> {start, slug, side} of an order judged refused
                                              # before processing: cancelled by its hash all the same (a misread refusal
                                              # never leaves an order unmanaged); kept in the ledger across a restart
        self.ghost_at = 0.0
        self.hedges_kept = 0                  # half-filled records new settings left resting (their other side pairs)
        self.unsaved: dict[str, dict] = {}    # records a failed write did not keep: written with the next write
        self.write_error = ""                 # why the last write failed ("" while writes work): no new order meanwhile
        self.clock_at = 0.0                   # when the clock was last checked against the CLOB's
        self.trades: dict[str, tuple[float, dict[str, D]]] = {}   # condition id -> (read at, order id -> shares bought)
        self.blocked = ""                     # the ledger belongs to another account: nothing is taken over or placed
        self.stopping = ""                    # a withdrawal began (the exit, a switch to paper): nothing new is placed
        self.v2_hold_until = 0.0              # V2 markets wait (their approval seems missing) until then
        self.idle_ask_at = 0.0                # the next CLOB "accepting?" read of a long-idle listing not before then
        self.openings: dict[str, dict] = {}   # slug -> a fresh window's opening: its order requests and their answers
        self.v2_refreshed = 0.0               # when the CLOB was last asked to read the allowances afresh
        self.cancel_wait_until = 0.0          # a cancel refused before processing (429, a restart): asked again after this
        self._risk: tuple[Any, str] = (None, "")
        self.risk_note = ""
        self.settings_log: collections.deque = collections.deque(maxlen=SETTINGS_KEPT)
        try:
            self.settings_log.extend(json.loads(ledger.get("settings_log") or "[]"))
        except ValueError:
            pass
        self.note_settings(settings, "重启后（Railway 变量或网页设置）")
        self.version_log: list[dict[str, Any]] = self.read_versions()
        for q in ledger.rows.values():
            self._index(q)

    # ------------------------------------------------------------------ basics
    def now(self) -> float:
        return self.clock() + self.skew

    def notify(self, text: str, key: str = "", every_s: float = 600) -> None:
        LOG.info("通知：%s", text)
        if key:
            last = self.notified.get(key, 0.0)
            if self.now() - last < every_s:
                return
            self.notified[key] = self.now()
        if self.notify_cb:
            try:
                self.notify_cb(text, key)
            except Exception as error:  # a notification never stops the bot
                LOG.warning("通知发送失败：%s", error)

    def problem(self, text: str, key: str = "") -> None:
        LOG.warning(text)
        self.errors.append((self.now(), text))
        if key:
            self.notify(f"⚠️ {text}", key)

    def _index(self, q: dict) -> None:
        self.by_slug[q["slug"]].add(q["key"])
        for side, o in (q.get("orders") or {}).items():
            if o.get("id"):
                self.by_id[o["id"]] = (q["key"], side)
            if o.get("hash"):
                self.by_hash[o["hash"]] = (q["key"], side)
        if q.get("phase") != "settled":
            self.open_keys.add(q["key"])
        else:
            self.open_keys.discard(q["key"])

    def save(self, *quotes: dict) -> None:
        """Records to the ledger. A write that fails (a full disk, a broken file) does not stop the loops, which go on
        from the records in memory: the records are written with the next write that works, and until then nothing
        new is placed (hold): an order the ledger cannot keep would be lost to a restart (audit 11-27)."""
        batch = {q["key"]: q for q in (*self.unsaved.values(), *quotes)}
        try:
            self.ledger.save_many(list(batch.values()))
        except (sqlite3.Error, OSError) as error:
            self.unsaved.update(batch)
            if not self.write_error:
                LOG.error("账本写不进去：%s", error)
            self.write_error = str(error)[:200] or type(error).__name__
            self.problem(f"账本写不进去（{self.write_error}）：暂停新挂，撤单照常；磁盘满了就清理或扩容 /data", key="ledger-write")
        else:
            if self.write_error:
                self.notify("✅ 账本又能写了，恢复新挂")
            self.unsaved.clear()
            self.write_error = ""
        self.version += 1
        for q in quotes:
            self._index(q)

    def forget(self, q: dict) -> None:
        """Drop a record that never placed anything."""
        self.ledger.delete(q["key"])
        self.version += 1
        self.by_slug[q["slug"]].discard(q["key"])
        self.open_keys.discard(q["key"])
        for o in q["orders"].values():
            if self.by_hash.get(o.get("hash") or "", ("",))[0] == q["key"]:
                del self.by_hash[o["hash"]]

    def read_ghosts(self) -> dict[str, dict]:
        try:
            kept = json.loads(self.ledger.get("ghosts") or "{}")
        except ValueError:
            kept = {}
        return {h: g for h, g in kept.items() if isinstance(g, dict) and g.get("start")} if isinstance(kept, dict) else {}

    def keep_ghosts(self) -> None:
        self.ledger.put("ghosts", json.dumps(self.ghosts, separators=(",", ":")))

    def ref_of(self, oid: str) -> tuple[str, str] | None:
        """The record and side of an order id (or of the hash of an order whose id is not known)."""
        return self.by_id.get(oid) or self.by_hash.get(oid)

    def learn_id_is_hash(self, same: bool) -> None:
        if self.id_is_hash is None or self.id_is_hash != same:
            self.id_is_hash = same
            self.ledger.put("id_is_hash", "1" if same else "0")
            LOG.info("订单号%s订单哈希", "等于" if same else "不等于")

    def quotes_of(self, slug: str) -> list[dict]:
        return [self.ledger.rows[k] for k in self.by_slug.get(slug, ()) if k in self.ledger.rows]

    def open_quotes(self) -> list[dict]:
        return [self.ledger.rows[k] for k in list(self.open_keys) if k in self.ledger.rows]

    def current(self, slug: str, level: int) -> dict | None:
        qs = [q for q in self.quotes_of(slug) if int(q["level"]) == level]
        return max(qs, key=lambda q: int(q["attempt"])) if qs else None

    def failures(self, slug: str, level: int) -> int:
        return sum(1 for q in self.quotes_of(slug) if int(q["level"]) == level and q.get("counted"))

    def expiration(self, start: int) -> int:
        """The stated GTD expiration: the order lives until 60 s before it, i.e. until expiry_s before the start."""
        return int(start - self.s.expiry_s + GTD_THRESHOLD_S)

    # ------------------------------------------------------------------ gates
    def hold(self) -> str:
        """Why no new order may be placed now ("" when one may). The open orders and the balance are each checked for
        freshness: neither an old list of orders nor an old balance is placed against."""
        now = self.now()
        if self.blocked:
            return self.blocked
        if self.stopping:
            return f"正在撤单停止（{self.stopping}）"
        if self.write_error:
            return f"账本写不进去（{self.write_error}），暂停新挂"
        if self.paused:
            return f"已暂停（{self.pause_text()}；/resume 恢复）"
        if risk := self.risk_hold():
            return risk
        if now < self.cooldown_until:
            return f"接口冷却中（{self.cooldown_reason}，还有 {self.cooldown_until - now:.0f} 秒）"
        if now < self.balance_hold_until:
            return f"交易所说余额不够，{self.balance_hold_until - now:.0f} 秒后再试"
        if now < self.refused_until:
            help_ = REFUSAL_HELP.get(self.refused_code, "")
            return (f"交易所拒单：{help_}，{self.refused_until - now:.0f} 秒后再试（交易所原话见下）" if help_
                    else f"交易所拒单（{self.refused_reason}），{self.refused_until - now:.0f} 秒后再试")
        if not self.snapshot_at:
            return "账户快照还没读到"
        if now - self.snapshot_at > self.stale_after():
            return f"账户快照过期（{now - self.snapshot_at:.0f} 秒前）"
        if self.balance is None or not self.balance_at:
            return "余额还没读到"
        if now - self.balance_at > BALANCE_STALE_S:
            return f"余额过期（{now - self.balance_at:.0f} 秒前读到的，读余额一直失败）"
        return ""

    def per_version(self, name: str, work: Callable[[], Any], extra: Any = None) -> Any:
        """work()'s value, worked out once per version of the records (every change of a record bumps it, as reserved()
        relies on) and per `extra` (whatever else it depends on: the day). The page, 📊 and 🩺 ask for the same
        summaries every few seconds; worked out anew each time, a page's refresh held the event loop 52 ms at 2000
        windows x 3 levels (11-25). The value is shared: the callers only read it."""
        key = (self.version, extra)
        hit = self._memo.get(name)
        if hit is None or hit[0] != key:
            hit = self._memo[name] = (key, work())
        return hit[1]

    def day(self) -> Any:
        return mk.dt.datetime.fromtimestamp(self.now(), mk.BEIJING).date()

    # ------------------------------------------------------------------ risk
    def exposure(self) -> D:
        """The worst case of the positions not settled yet: per window (its levels together), what its fills cost less
        the smaller side's shares, since a pair of opposite shares pays $1 whichever side wins. One-sided fills are the
        whole of it; a window filled evenly on both sides adds nothing."""
        return self.per_version("exposure", self._exposure)

    def _exposure(self) -> D:
        windows: dict[str, list[D]] = {}
        for q in self.open_quotes():
            if q.get("foreign"):  # another account's (改绑): its positions are not this account's risk
                continue
            f = {side: dnum(o.get("filled")) for side, o in q["orders"].items()}
            if not any(f.values()):
                continue
            w = windows.setdefault(q["slug"], [D(0), D(0), D(0)])
            w[0] += sum(f.values(), D(0)) * dnum(q["price"])
            w[1] += f.get("up", D(0))
            w[2] += f.get("down", D(0))
        return sum((max(D(0), cost - min(up, down)) for cost, up, down in windows.values()), D(0))

    def day_pnl(self) -> D:
        """Today's (Beijing time) settled profit and loss, by the windows' start."""
        return self.per_version("day_pnl", self._day_pnl, self.day())

    def _day_pnl(self) -> D:
        day = self.day()
        total = D(0)
        for q in self.ledger.rows.values():
            if q.get("phase") == "settled" and q.get("pnl") is not None and not q.get("foreign") and \
                    mk.dt.datetime.fromtimestamp(q["start"], mk.BEIJING).date() == day:
                total += D(str(q["pnl"]))
        return total

    def risk_hold(self) -> str:
        """The risk limits: today's loss (PREOPEN_MAX_DAILY_LOSS_USD, lifted at Beijing midnight) and the worst case of
        the unsettled positions (PREOPEN_MAX_EXPOSURE_USD, lifted as they settle). Cached by the records' version."""
        if not (self.s.max_daily_loss_usd > 0 or self.s.max_exposure_usd > 0):
            return ""
        key = (self.version, mk.dt.datetime.fromtimestamp(self.now(), mk.BEIJING).date(), self.s.max_daily_loss_usd,
               self.s.max_exposure_usd)
        if self._risk[0] == key:
            return self._risk[1]
        text = ""
        if self.s.max_daily_loss_usd > 0:
            pnl = self.day_pnl()
            if pnl <= -self.s.max_daily_loss_usd:
                text = (f"今天已亏 ${-pnl:.2f}，到了日亏损上限 PREOPEN_MAX_DAILY_LOSS_USD={self.s.max_daily_loss_usd}："
                        "今天不再新挂（北京时间零点后自动恢复）")
        if not text and self.s.max_exposure_usd > 0:
            worst = self.exposure()
            if worst >= self.s.max_exposure_usd:
                text = (f"未结算持仓的最坏亏损 ${worst:.2f}，到了上限 PREOPEN_MAX_EXPOSURE_USD={self.s.max_exposure_usd}："
                        "不再新挂，结算后自动恢复")
        self._risk = (key, text)
        return text

    def risk_pass(self) -> int:
        """A risk limit reached: no resting order may add to the worst case. The records risk_withdrawals names are
        withdrawn (both sides: an order cannot be made smaller). Returns how many."""
        why = self.risk_hold()
        if why != self.risk_note:
            if why:
                self.problem(f"风控：{why}", key="risk")
            elif self.risk_note:
                self.notify("✅ 风控解除，恢复挂单")
            self.risk_note = why
        if not why:
            return 0
        n = 0
        for slug in {q["slug"] for q in self.open_quotes() if q["phase"] == "active"}:
            for q in self.risk_withdrawals(slug):
                self.close(q, "风控：" + why.split("：")[0], free=True)
                n += 1
        return n

    def risk_withdrawals(self, slug: str) -> list[dict]:
        """The active records of a window that could add to its worst case, its levels together (the fills of records
        already closed count too). Nothing filled, or both sides alike: every one (any fill would be one-sided). One
        side ahead: a record still resting on that side (its remainder could only add one-sided shares); and on the
        side behind, nearest level first, a resting order once the remainders kept would exceed the gap: each share
        bought within it pairs one already bought, one more would be one-sided the other way. 11-19 review: a record
        with a fill used to keep both sides, its filled side's remainder free to fill on."""
        records = [q for q in self.quotes_of(slug) if q["phase"] != "settled"]
        got = {side: sum((dnum(q["orders"][side].get("filled")) for q in records), D(0)) for side in mk.SIDES}
        active = sorted((q for q in records if q["phase"] == "active"), key=lambda q: (int(q["level"]), int(q["attempt"])))
        if got["up"] == got["down"]:
            return active
        ahead = "up" if got["up"] > got["down"] else "down"
        behind = "down" if ahead == "up" else "up"
        room = abs(got["up"] - got["down"])
        out = []
        for q in active:
            mine, other = q["orders"][ahead], q["orders"][behind]
            if mine["state"] in MAY_REST:
                out.append(q)
            elif other["state"] in MAY_REST:
                left = dnum(q["shares"]) - dnum(other.get("filled"))
                if left <= room + dnum(q["shares"]) * PAIR_DUST:  # the side ahead, eaten whole, is a hair short of it
                    room = max(D(0), room - left)
                else:
                    out.append(q)
        return out

    def v2_blocked(self) -> str:
        """Why a Polymarket Protocol V2 market is not quoted now ("" when it may be): ExchangeV3 known not to be allowed
        any pUSD, or a V2 refusal for the balance or allowance that the allowance did not explain. Granted, it is quoted
        at once (the balance read every 30 s brings the allowance)."""
        allowance = getattr(self.x, "v2_allowance", None)
        need = self.s.window_usd
        if allowance is not None and allowance >= need:
            self.v2_hold_until = 0.0
            return ""
        if allowance is not None and allowance > 0:  # a limited approval used up (audit 11-27: any amount above 0 was
            # taken as granted, and the refusals that followed held every market, V1 too)
            return f"V2 市场的授权额度只剩 ${allowance:.2f}，不够一场的 ${need:.2f}：" + V2_HELP
        if allowance is not None or self.now() < self.v2_hold_until:
            return "V2 市场还没授权：" + V2_HELP
        return ""

    def v2_warn(self, text: str) -> None:
        """V2 markets left unquoted for the approval: said now and then (Telegram, the errors), not at every retry."""
        if self.now() - self.notified.get("v2-approval", float("-inf")) >= V2_NOTE_S:
            self.problem(text, key="v2-approval")

    def stale_after(self) -> float:
        return 3 * max(self.s.snapshot_s, self.snapshot_took) + STALE_EXTRA_S

    def hold_balance(self, text: str) -> None:
        """The CLOB refused or cancelled an order for the balance: every placement waits a minute and the balance is
        read again (the local budget does not see orders placed elsewhere, e.g. on the website, nor the allowance)."""
        self.balance_hold_until = self.now() + BALANCE_HOLD_S
        self.balance_at = 0.0
        self.problem(text, key="balance")

    def log_refusal(self, listing: mk.Listing, q: dict, r: Placed, at: float) -> None:
        """A window's orders refused: kept with the exchange's words and the circumstances (how long after the listing
        and the opening, how many orders the request held), for /rejects to tell the real cause. A refusal of the whole
        request before processing (429, a restart) is one entry however many windows it caught."""
        last = self.refusal_log[-1] if self.refusal_log else None
        if r.wait > 0 and last and last.get("code") == r.code and at - float(last["at"]) <= 2:
            last["windows"] = int(last.get("windows") or 1) + 1
            return
        self.refusal_log.append({
            "at": round(at, 2), "slug": q["slug"], "coin": q["coin"], "start": q["start"], "level": int(q["level"]),
            "code": r.code, "words": (r.message or "")[:300], "status": r.status, "http": r.http, "batch": r.batch,
            "whole": r.whole, "listed_ago": round(at - listing.created, 1) if listing.created else None,
            "opened_ago": round(at - listing.opened, 2) if listing.opened else None,
            "up": listing.up, "down": listing.down, "cid": listing.condition_id})
        if at - self.refusals_saved_at >= REFUSALS_SAVE_S:
            self.save_refusals()

    def save_refusals(self) -> None:
        self.refusals_saved_at = self.now()
        try:
            self.ledger.put("refusals", json.dumps(list(self.refusal_log)[-200:], ensure_ascii=False))
        except Exception as error:  # the log is a convenience
            LOG.debug("拒单记录保存失败：%s", error)

    def refuse(self, code: str, message: str) -> None:
        """The CLOB refused orders for a reason that is not the window's own (an exchange rule, the account, words not
        recognised): every new placement waits, 30 s doubling to 10 min while it keeps refusing. No window is struck off
        for it: each is placed again once the refusals stop. Counted once per refusal: the answers to orders sent before
        the hold began (the other levels and windows of the same request) are that same refusal (audit 11-27: one
        request of five levels doubled the wait four times over, to 8 minutes)."""
        if self.now() >= self.refused_until:
            self.refused_streak += 1
        wait = min(REFUSE_HOLD_MAX_S, REFUSE_HOLD_S * 2 ** min(self.refused_streak - 1, 6))
        self.refused_until = max(self.refused_until, self.now() + wait)
        self.refused_reason = f"{code}：{message[:120]}" if message else code
        self.refused_code = code
        help_ = REFUSAL_HELP.get(code, "")
        self.problem(f"交易所拒单（{self.refused_reason}）" + (f"。{help_}" if help_ else "")
                     + f"。所有新挂暂停 {wait:.0f} 秒后再试（连续第 {self.refused_streak} 次），场次不作废", key=f"refused-{code}")

    def hold_retry(self) -> float | None:
        """When a window held back is tried again: as the cooldown ends when that is what holds it (the exchange said
        how long: a 429's Retry-After is a second or two), else HOLD_RETRY_S on; not at all while paused (the sweep
        queues it on resume). Live 11-25: the first order request of every opening is answered 429 with Retry-After
        1-2 s; tried again five seconds on, the orders rested 8-10 s after the opening."""
        if self.paused:
            return None
        left = self.cooldown_until - self.now()
        return min(HOLD_RETRY_S, left + WAIT_MARGIN_S) if left > 0 else HOLD_RETRY_S

    def note_opening(self, listing: mk.Listing, sent: float, done: float, outcome: str) -> None:
        """A fresh window's order request, for /check's 最近开盘: sent and answered this long after its opening was
        seen, and what the answer was. Only until its first level rests, or OPENING_WATCH_S on."""
        if not listing.opened or done - listing.opened > OPENING_WATCH_S:
            return
        o = self.openings.get(listing.slug)
        if o is None:
            while len(self.openings) >= OPENINGS_KEPT:
                self.openings.pop(next(iter(self.openings)))
            o = self.openings[listing.slug] = {"coin": listing.coin, "start": listing.start, "version": listing.version,
                                               "opened": listing.opened, "tries": [], "rested": None}
        if o["rested"] is None and len(o["tries"]) < 30:
            o["tries"].append((round(sent - listing.opened, 1), round(done - listing.opened, 1), outcome))
            if outcome == "挂上":
                o["rested"] = round(done - listing.opened, 1)

    def openings_text(self, limit: int = 10) -> list[str]:
        """The latest openings, newest first: "SOL 11-26 16:00 V2：2.6→429 等2秒；4.7→挂上(4.9)" (seconds after the
        opening was seen: sent→answer; a resting one with when it was answered)."""
        lines = []
        for o in sorted(self.openings.values(), key=lambda o: -o["opened"])[:limit]:
            tries = "；".join(f"{sent:g}→{what}" + (f"({done:g})" if what == "挂上" else "") for sent, done, what in o["tries"])
            lines.append(f"  {mk.window_name(o['coin'], o['start'])} {'V2' if o['version'] == 'v2' else '旧'}"
                         f"（{mk.hms(o['opened'])} 开放）：{tries or '还没发单'}")
        return lines

    def set_cooldown(self, reason: str, seconds: float) -> None:
        """A refusal before processing (429, a restart, a timeout): every placement waits. Said once per cooldown: one
        refused request answers for every window in it, and each would say it again."""
        now = self.now()
        fresh = now >= self.cooldown_until
        until = now + max(1.0, min(float(seconds), 600.0))
        if until > self.cooldown_until:
            self.cooldown_until, self.cooldown_reason = until, reason
        if fresh:
            self.problem(f"接口冷却 {seconds:.0f} 秒：{reason}", key="cooldown")

    def cancel_throttled(self, reason: str, seconds: float) -> None:
        """A cancel refused before processing (429, a restart): the cooldown, and a withdrawal asks again only after it."""
        self.set_cooldown(reason, seconds)
        self.cancel_wait_until = max(self.cancel_wait_until, self.now() + max(0.0, float(seconds)))

    def reserved(self) -> D:
        """The bot's own resting money: every order that may rest, at its price, for what is not filled."""
        if self._reserved[0] == self.version:
            return self._reserved[1]
        total = D(0)
        for q in self.open_quotes():
            for o in q["orders"].values():
                if o["state"] in MAY_REST:
                    total += dnum(q["price"]) * max(D(0), dnum(q["shares"]) - dnum(o.get("filled")))
        self._reserved = (self.version, total)
        return total

    def unmanaged_usd(self, market: str | None = None) -> D:
        """The open BUYs on the account that are not the bot's (the website, another strategy): one market's, or all."""
        return sum((r.price * max(D(0), r.size - r.matched) for r in self.unmanaged
                    if r.side == "BUY" and (market is None or r.market == market)), D(0))

    def market_rests(self, slug: str) -> D:
        """The bot's own resting money in one market (a window): its levels, and orders of an earlier attempt that are
        not confirmed gone yet."""
        return sum((dnum(q["price"]) * max(D(0), dnum(q["shares"]) - dnum(o.get("filled")))
                    for q in self.quotes_of(slug) for o in q["orders"].values() if o["state"] in MAY_REST), D(0))

    def market_room(self, listing: mk.Listing) -> D:
        """What more one market (window) may take. Polymarket checks each market's BUY orders against the balance on its
        own: a market's open BUYs, the new one included, may add up to the balance, and other markets' orders do not
        count (11-13: three $49 orders in three markets rest together on a $75.10 balance). Funds are not shared, so
        every window can be quoted with the balance one window needs."""
        if self.balance is None:
            return D(0)
        return self.balance - self.market_rests(listing.slug) - self.unmanaged_usd(listing.condition_id)

    def cap_room(self) -> D:
        """PREOPEN_MAX_OPEN_USD: the bot's own resting orders in all at most this (the exchange does not limit the sum,
        which can be many times the balance); unlimited when 0."""
        return self.s.max_open_usd - self.reserved() if self.s.max_open_usd > 0 else D("Infinity")

    def fit(self, levels: list[int], room: D) -> list[int]:
        """The levels that fit in `room`, the nearest first (a level's two sides together); a deeper one that does not
        fit waits for money."""
        out: list[int] = []
        for i in levels:
            need = 2 * self.s.levels[i].usd
            if need > room:
                break
            out.append(i)
            room -= need
        return out

    def need_of(self, levels: list[int]) -> D:
        return sum((2 * self.s.levels[i].usd for i in levels), D(0))

    def short_of(self, listing: mk.Listing, levels: list[int], cap: D) -> str:
        """Why not even the first of these levels fits: "cap" (PREOPEN_MAX_OPEN_USD reached), "balance" (this market's
        room), or "" when it fits."""
        if not levels or self.fit(levels, min(self.market_room(listing), cap)):
            return ""
        return "cap" if cap < 2 * self.s.levels[levels[0]].usd else "balance"

    def budget_problem(self, listing: mk.Listing, levels: list[int]) -> str:
        """Why not even the first of these levels fits ("" when it does)."""
        if self.balance is None:
            return "还没读到余额"
        why = self.short_of(listing, levels, self.cap_room())
        if not why:
            return ""
        need = 2 * self.s.levels[levels[0]].usd
        if why == "cap":
            return f"本程序挂单合计到了上限 PREOPEN_MAX_OPEN_USD=${self.s.max_open_usd}（已挂 ${self.reserved():.2f}，再挂一场要 ${need:.2f}）"
        mine, others = self.market_rests(listing.slug), self.unmanaged_usd(listing.condition_id)
        return (f"余额不够这一场：余额 ${self.balance:.2f}" + (f"，这一场已挂 ${mine:.2f}" if mine else "")
                + (f"，这一场别的挂单 ${others:.2f}" if others else "") + f"，要 ${need:.2f}（各场分别核对余额，不共用）")

    # ------------------------------------------------------------------ discovery
    def frontier(self, starts: list[int]) -> dict[str, int]:
        """Each coin's soonest window in range that is not listed yet (what Polymarket lists next)."""
        out = {}
        for coin in self.s.coins:
            for t in starts:
                slug = mk.slug_of(coin, t)
                if slug not in self.listings and slug not in self.skipped:
                    out[coin] = t
                    break
        return out

    def lead_of(self, coin: str) -> float:
        """How long before its start this coin's windows are listed: the median over its own latest listings."""
        leads = self.leads.get(coin)
        return sorted(leads)[len(leads) // 2] if leads else self.lead

    def next_listing(self, front: dict[str, int]) -> tuple[int, float] | None:
        """The listing expected next among each coin's next window: (its start, when its coin's listings come)."""
        if not front:
            return None
        when, start = min((t - self.lead_of(coin), t) for coin, t in front.items())
        return start, when

    def lead_text(self) -> str:
        """23.98 小时, or the span when the coins differ (V2 and CTF markets are listed six minutes apart)."""
        hours = sorted(self.lead_of(coin) / 3600 for coin in self.leads) or [self.lead / 3600]
        return f"{hours[0]:.2f} 小时" if hours[-1] - hours[0] < 0.01 else f"{hours[0]:.2f}～{hours[-1]:.2f} 小时，各币不同"

    def coming_text(self, front: dict[str, int]) -> str:
        coming = self.next_listing(front)
        return f"下一场 {mk.hm(coming[0])} 预计 {mk.hms(coming[1])} 发布" if coming else "范围内都已发布"

    def poll_wait(self) -> float:
        now = self.now()
        starts = mk.window_starts(now, self.s)
        for coin, t in self.frontier(starts).items():
            expected = t - self.lead_of(coin)
            if expected - FAST_BEFORE_S <= now <= expected + FAST_AFTER_S:
                return self.s.fast_poll_s
        return self.s.poll_s

    async def discover(self) -> None:
        """Both scans in turn (the loops run them apart: scan_loop and discovery_loop)."""
        await self.scan_full()
        await self.scan_front()

    async def scan_full(self) -> None:
        """A full pass over the range's unlisted windows a minute: the batched GET /markets?slug= query, fine for
        windows listed long ago (it lags a new listing by ~88 s). Its own loop: a slow pass never holds the next up."""
        now = self.now()
        if now - self.full_scan_at < FULL_SCAN_S:
            return
        self.full_scan_at = now
        starts = mk.window_starts(now, self.s)
        self.prune(starts)
        ask = [mk.slug_of(c, t) for c in self.s.coins for t in starts if self.unlisted(mk.slug_of(c, t), now)]
        if ask:
            got = await self.pub.listings(ask)
            found = self.take(ask, got)
            self.full_note = f"{mk.hms(now)} 全量扫描 {len(ask)} 场，新发现 {found} 场"

    async def scan_front(self) -> None:
        """Every poll, each coin's next window by the direct path GET /markets/slug/<slug>, which answers within a
        second of the listing (the batched query lags ~88 s, and the crowd with it); then the sweep."""
        now = self.now()
        starts = mk.window_starts(now, self.s)
        front = [mk.slug_of(c, t) for c, t in self.frontier(starts).items()]
        if front:
            got = await self.pub.markets(front)
            found = self.take(front, got)
            self.scan_note = f"{mk.hms(now)} 问了下一场 {len(front)} 个，新发现 {found} 场"
        if self.leads:
            ordered = sorted(self.lead_of(coin) for coin in self.leads)
            self.lead = ordered[len(ordered) // 2]
        if now - self.sweep_at >= self.s.poll_s:
            self.sweep_at = now
            self.sweep()

    def unlisted(self, slug: str, now: float) -> bool:
        return slug not in self.listings and not (slug in self.skipped and now - self.skipped[slug][0] < SKIP_RETRY_S)

    def take(self, asked: list[str], found: dict[str, dict]) -> int:
        """The listings just read: checked, kept and queued (the fresh ones first). A fresh listing whose fields are not
        all there yet is asked again at the next poll, not set aside for a minute."""
        seen = self.now()
        new = 0
        for slug in asked:
            m = found.get(slug)
            if m is None or slug in self.listings:
                continue
            coin, start = mk.parse_slug(slug)
            listing, why = mk.parse_listing(m, coin, start, seen)
            if listing is None:
                created = mk.iso_ts(m.get("createdAt"))
                if not (created and seen - created < 120 and ("缺" in why)):
                    self.skipped[slug] = (seen, why)
                continue
            new += 1
            self.skipped.pop(slug, None)
            self.listings[slug] = listing  # kept (the coin's next window moves on); wanted() refuses a maker-fee market
            if listing.created:
                self.leads.setdefault(coin, collections.deque(maxlen=LEADS_KEPT)).append(start - listing.created)
                if seen - listing.created < 120:
                    self.listing_lags.append(seen - listing.created)
            if listing.maker_fee:
                self.problem(f"{mk.window_name(coin, start)} 挂单方也收手续费（feeSchedule.takerOnly=false）：不挂", key="maker-fee")
                continue
            if listing.fee_unknown:
                self.problem(f"{mk.window_name(coin, start)} 的手续费设置没写挂单收不收费（feeSchedule.takerOnly 缺失）："
                             "先不挂，交易所写清楚了自动恢复", key="fee-unknown")
                continue
            self.enqueue(slug)
        return new

    def prune(self, starts: list[int]) -> None:
        """Forget the listings and skips of windows that left the range (their records stay in the ledger)."""
        first = starts[0] if starts else int(self.now())
        for slug in [s for s, l in self.listings.items() if l.start < first - 3600]:
            del self.listings[slug]
        for slug in [s for s in self.skipped if (mk.parse_slug(s) or ("", 0))[1] < first]:
            del self.skipped[slug]

    def prune_memory(self) -> None:
        """What the bot remembers per market or window, for windows long gone (audit 11-27: these only ever grew, a
        couple of thousand entries a day): markets swept, windows refused, "invalid token id" counts."""
        now = self.now()
        self.swept = {cid: at for cid, at in self.swept.items() if now - at < 86400}
        self.refused_at = {slug: at for slug, at in self.refused_at.items() if now - at < 86400}
        self.token_refusals = {slug: n for slug, n in self.token_refusals.items()
                               if (mk.parse_slug(slug) or ("", 0))[1] > now - 3600}

    def sweep(self) -> None:
        """Every listed window in range with a level to quote and not queued is queued (a hold that cleared, a level
        withdrawn, a deeper level now near, money back), the soonest first. Each market is checked against the balance
        on its own (funds are not shared between markets); PREOPEN_MAX_OPEN_USD, when set, caps the bot's orders in
        all, the soonest windows taking it first."""
        if self.hold():
            self.budget_note = ""  # a hold is reported on its own
            return
        cap = self.cap_room()
        for slug in self.queued | self.placing:  # windows on their way: their share of the cap is spoken for
            if slug in self.listings:
                listing = self.listings[slug]
                cap -= self.need_of(self.fit(self.wanted(listing), min(self.market_room(listing), cap)))
        short: collections.Counter = collections.Counter()
        for t in mk.window_starts(self.now(), self.s):
            for coin in self.s.coins:
                slug = mk.slug_of(coin, t)
                if slug not in self.listings or slug in self.queued or slug in self.placing or slug in self.timers:
                    continue
                listing = self.listings[slug]
                wanted = self.wanted(listing)
                if not wanted:  # every level rests (nearly every window, nearly every sweep): its room is not worked
                    continue    # out (11-25: more than half of a sweep's 37 ms at 2000 windows x 3 levels)
                levels = self.fit(wanted, min(self.market_room(listing), cap))
                if not levels:
                    short[self.short_of(listing, wanted, cap) or "balance"] += 1
                    continue
                cap -= self.need_of(levels)
                self.enqueue(slug)
        parts = []
        if short["cap"]:
            parts.append(f"{short['cap']} 场等挂单合计上限 PREOPEN_MAX_OPEN_USD=${self.s.max_open_usd}（开场最近的先挂）")
        if short["balance"]:
            parts.append(f"{short['balance']} 场余额不够（余额 ${self.balance or 0:.2f}，一场要 ${2 * self.s.first.usd:.2f}；"
                         f"各场分别核对余额，不共用）")
        self.budget_note = "；".join(parts)

    def priority(self, slug: str) -> tuple[int, int, int]:
        """Fresh listings first (the queue's front), then the soonest; a window refused since the CLOB last accepted an
        order goes after the others, so that another window tries first (a refusal that is the window's own is then told
        apart from the account's)."""
        listing = self.listings[slug]
        fresh = listing.created is not None and self.now() - listing.created < ARM_FRESH_S
        refused = self.refused_at.get(slug)
        later = 1 if refused is not None and refused >= self.accepted_at else 0
        return (later, 0 if fresh else 1, listing.start)

    def enqueue(self, slug: str) -> None:
        self.timers.pop(slug, None)
        if slug in self.queued or slug in self.placing or slug not in self.listings:
            return
        self.queued.add(slug)
        self.queue.put_nowait((*self.priority(slug), slug))

    def park(self, slug: str, delay: float | None) -> None:
        if slug in self.timers or delay is None:
            return
        loop = asyncio.get_running_loop()
        self.timers[slug] = loop.call_later(max(0.05, delay), self.enqueue, slug)

    def wanted(self, listing: mk.Listing) -> list[int]:
        """The levels of a window to quote now: none past the new-order cutoff; not one already live or being
        withdrawn, one at its failures cap, or one refused for good; the deeper ones only near the start."""
        now = self.now()
        if now > listing.start - self.s.entry_s or listing.start > now + self.s.ahead_s:
            return []
        if listing.coin not in self.s.coins or (not self.s.weekend and mk.on_weekend(listing.start)):
            return []
        if listing.maker_fee or listing.fee_unknown:  # makers would pay a fee on every fill (or might: not said), the
            return []                                  # pair's 2¢ margin gone; not quoted
        out = []
        for i, lv in enumerate(self.s.levels):
            if i and self.s.deep_ahead_s and listing.start - now > self.s.deep_ahead_s:
                break
            if mk.level_problem(listing, lv.price, lv.shares):
                continue
            cur = self.current(listing.slug, i)
            if cur is not None and not self.requotable(cur):
                continue
            if self.failures(listing.slug, i) >= self.s.max_attempts:
                continue
            out.append(i)
        return out

    def requotable(self, q: dict) -> bool:
        if q["phase"] in ("active", "closing"):
            return False
        if any(dnum(o.get("filled")) > 0 for o in q["orders"].values()):
            return False  # a window with a fill keeps its record: not quoted again
        if q.get("permanent"):
            return False
        wait = COUNTED_REQUOTE_S if q.get("counted") else FREE_REQUOTE_S
        return self.now() >= float(q.get("closed_at") or 0) + wait

    # ------------------------------------------------------------------ placement
    async def worker(self) -> None:
        while not self.stop.is_set():
            *_, slug = await self.queue.get()
            self.queued.discard(slug)
            await self.process(slug)

    async def process(self, slug: str) -> Hold | None:
        """One queued window: placed, or held (parked for a retry, or left to the sweep)."""
        if slug in self.placing:
            return None
        self.placing.add(slug)
        try:
            hold = await self.place_window(slug)
        except Exception as error:
            LOG.exception("挂单出错 %s", slug)
            self.problem(f"挂单出错 {slug}：{error}", key="place-error")
            hold = Hold(str(error), ERROR_RETRY_S)
        finally:
            self.placing.discard(slug)
        if hold is not None:
            self.hold_note = hold.reason
            self.park(slug, hold.retry)
        return hold

    async def place_window(self, slug: str) -> Hold | None:
        listing = self.listings.get(slug)
        if listing is None:
            return None
        levels = self.wanted(listing)
        if not levels:
            return None
        if why := self.hold():
            return Hold(why, self.hold_retry())
        if listing.version == "v2" and (why := self.v2_blocked()):  # V2 alone waits; the CTF markets go on
            self.v2_warn(f"{mk.window_name(listing.coin, listing.start)} 是 V2 市场，没挂：{V2_HELP}")
            return Hold(why, HOLD_RETRY_S * 12)
        if not listing.accepting:
            age = self.now() - (listing.created or listing.seen)
            if age >= ARM_IDLE_S:  # long listed and still shut: read within one rate over all such listings
                if self.now() < self.idle_ask_at:
                    return Hold("市场还没开放下单", ARM_IDLE_RETRY_S)
                self.idle_ask_at = self.now() + 1 / ARM_IDLE_RATE
            try:
                accepting = await self.pub.accepting(listing.condition_id)
            except PublicThrottled as t:
                return Hold(f"公共接口限流（{t}）", t.seconds)
            except Exception as error:
                LOG.debug("accepting %s: %s", slug, error)
                accepting = None
            if not accepting:
                retry = (ARM_IDLE_RETRY_S if age >= ARM_IDLE_S else HOLD_RETRY_S if age >= ARM_FRESH_S
                         else ARM_RETRY_S if age < ARM_FAST_UNTIL_S else ARM_SLOW_S)
                return Hold("市场还没开放下单", retry)
            listing.accepting = True
            listing.opened = self.now()
            levels = self.wanted(listing)  # judged again after the await
            if not levels:
                return None
            if why := self.hold():
                return Hold(why, self.hold_retry())
        settle = listing.opened + (OPEN_SETTLE_V2_S if listing.version == "v2" else OPEN_SETTLE_S) - self.now()
        if listing.opened and settle > 0:
            return Hold("市场刚开放：等撮合引擎认它的代币再发第一张单", settle)
        opened_ago = self.now() - listing.opened if listing.opened else None
        if opened_ago is not None and opened_ago < DEEP_AFTER_OPEN_S and len(self.s.levels) > 1:
            if levels[0] != 0:  # the nearest level rests: the deeper ones once the opening's rush is over
                return Hold(f"深档在市场开放 {DEEP_AFTER_OPEN_S} 秒后再挂（开放那一刻只抢第一档）", DEEP_AFTER_OPEN_S - opened_ago)
            levels = levels[:1]
        # from here to the records' save nothing awaits: the budget check and the reservation are one step (the seven
        # coins' fresh listings open together and must not all pass against the same reservation)
        fitting = self.fit(levels, min(self.market_room(listing), self.cap_room()))
        if not fitting:
            return Hold(self.budget_problem(listing, levels), None)
        if len(fitting) < len(levels):
            LOG.info("%s 余额只放得下前 %d 档（共要挂 %d 档）", slug, len(fitting), len(levels))
        levels = fitting
        now = self.now()
        if now > listing.start - self.s.entry_s:
            return None
        quotes, specs = [], []
        for i in levels:
            lv = self.s.levels[i]
            prev = self.current(slug, i)
            attempt = int(prev["attempt"]) + 1 if prev else 0
            q = {
                "key": quote_key(slug, i, attempt), "slug": slug, "coin": listing.coin, "start": listing.start, "level": i,
                "attempt": attempt, "price": str(lv.price), "shares": str(lv.shares), "expiration": self.expiration(listing.start),
                "condition_id": listing.condition_id, "market_id": listing.market_id, "neg_risk": listing.neg_risk,
                "version": listing.version, "listed": listing.created, "seen": listing.seen, "created": now, "phase": "active", "reason": "",
                "orders": {side: {"asset": listing.asset(side), "state": "placing", "id": "", "hash": "", "salt": 0,
                                  "filled": "0", "error": "", "sent": now} for side in mk.SIDES},
            }
            quotes.append(q)
            for side in mk.SIDES:
                specs.append(OrderSpec(ref=f"{q['key']}/{side}", asset=listing.asset(side), price=lv.price, size=lv.shares,
                                       expiration=q["expiration"], neg_risk=listing.neg_risk, market=listing.condition_id))
        self.save(*quotes)  # durable before anything is signed
        self.attempted[listing.condition_id] = (slug, listing.start)
        self.x.prime(listing)
        refs = {f"{q['key']}/{side}": (q, side) for q in quotes for side in mk.SIDES}
        written = False

        def on_signed(hashes: list[tuple[str, str, int]]) -> None:
            """Signed, not sent yet: the hashes (the orders' ids to be) are durable before the first byte goes out."""
            nonlocal written
            touched: dict[str, dict] = {}
            for ref, h, salt in hashes:
                if ref in refs:
                    q, side = refs[ref]
                    q["orders"][side].update(hash=h, salt=int(salt))
                    touched[q["key"]] = q
            self.save(*touched.values())
            written = True
        try:
            results = await self.x.place(specs, on_signed=on_signed)
        except Exception as error:  # the adapter raised: sent or not is known by whether the hashes were written
            LOG.exception("下单请求异常 %s", slug)
            status = "unknown" if written else "not_sent"
            results = [Placed(spec.ref, status, code="exception", message=str(error)) for spec in specs]
        by_ref = {r.ref: r for r in results}
        done = self.now()
        hold: Hold | None = None
        first = [by_ref.get(f"{q['key']}/{side}") for q in quotes if int(q["level"]) == 0 for side in mk.SIDES]
        if first and all(first):  # the nearest level's request, for the opening's timeline
            bad = next((r for r in first if r.status != "open"), None)
            self.note_opening(listing, now, done, "挂上" if bad is None else
                              f"429 等{bad.wait:g}秒" if bad.code == "rate_limited" else
                              {"token_not_ready": "不认代币", "market_not_ready": "没开放", "not_enough_balance": "余额不足"}.get(
                                  bad.code, bad.code or bad.status) + (f" 等{bad.wait:g}秒" if bad.wait > 0 else ""))
        waits = [r.wait for r in results if r.wait > 0]
        token_counted = False   # "invalid token id": once a request, however many levels were in it
        for q in quotes:
            for side in mk.SIDES:
                r = by_ref.get(f"{q['key']}/{side}") or Placed("", "unknown", code="missing", message="没有应答")
                self.apply_placed(q, side, r, done)
                if r.status == "open":
                    self.accepted_at, self.refused_streak = done, 0
                    self.token_refusals.pop(slug, None)
                elif r.status in ("rejected", "not_sent") and r.code:
                    self.last_refusal = (done, r.code, r.message)
                if r.wait > 0:
                    self.set_cooldown(f"{r.code}：{r.message[:120]}", r.wait)
                if r.code == "market_not_ready":
                    listing.accepting = False
                    hold = Hold("市场还没开放下单", ARM_RETRY_S)
                if r.code == "token_not_ready" and not token_counted:  # once a request: its orders were sent together
                    token_counted = True
                    tries = self.token_refusals[slug] = self.token_refusals.get(slug, 0) + 1
                    again, fast = ((TOKEN_RETRY_V2_S, TOKEN_FAST_TRIES_V2) if listing.version == "v2"
                                   else (TOKEN_RETRY_S, TOKEN_FAST_TRIES))
                    hold = Hold(f"交易所撮合引擎还不认这个代币（刚开放的市场常见），这一场 {again:g} 秒后再挂",
                                again if tries < fast else TOKEN_SLOW_RETRY_S)
                    if tries == fast:  # its tokens may have changed: the listing read afresh
                        self.listings.pop(slug, None)
                        self.problem(f"{mk.window_name(listing.coin, listing.start)} 连续 {tries} 次 invalid token id："
                                     f"重新读一遍这一场的市场信息再挂", key="token-refresh")
                if r.code == "not_enough_balance" and listing.version == "v2" and \
                        not (getattr(self.x, "v2_allowance", None) or 0) >= self.s.window_usd:
                    # a V2 market refused while ExchangeV3's allowance is not known granted: the V2 approval, most likely
                    # (11-22: the CTF approvals do not cover V2). Only the V2 markets wait; the others go on
                    self.v2_hold_until = max(self.v2_hold_until, self.now() + V2_HOLD_S)  # the allowances read afresh
                    # at the next account pass (account_once); the balance's age untouched, or every placement waits
                    hold = Hold("V2 市场还没授权：" + V2_HELP, V2_HOLD_S)
                    self.v2_warn(f"交易所拒了 V2 市场 {mk.window_name(listing.coin, listing.start)} 的单（余额或授权不足）："
                                 + V2_HELP)
                elif r.code == "not_enough_balance":
                    hold = Hold("余额或授权不足（交易所拒单）", None)
                    self.hold_balance("交易所拒单：余额或授权不足（到 Polymarket 核对 pUSD 余额和交易授权）")
            states = {o["state"] for o in q["orders"].values()}
            codes = [o.get("code", "") for o in q["orders"].values() if o["state"] != "open"]
            account = [c for c in codes if c in ACCOUNT_REFUSALS or unrecognised(c)]
            refused = next((by_ref.get(f"{q['key']}/{side}") for side in mk.SIDES
                            if (by_ref.get(f"{q['key']}/{side}") or Placed("", "")).status in ("rejected", "not_sent")
                            and (by_ref.get(f"{q['key']}/{side}") or Placed("", "")).code), None)
            if refused is not None:
                self.log_refusal(listing, q, refused, done)
            if states <= {"not_sent", "rejected"} and all(retryable(c) for c in codes):
                ghosted = False
                prev = self.refused_at.get(slug)
                words = next((c for c in codes if unrecognised(c)), "")
                if words and prev is not None and self.accepted_at > prev and not any(
                        when > prev and other != slug and code == words for when, other, code in self.recent_refusals):
                    # refused again in words not recognised, while other windows' orders were accepted since its last
                    # refusal and none of them was refused so: the window's own (counted, as a refusal about its order)
                    self.refused_at.pop(slug, None)
                    self.close(q, "交易所只拒这一场：" + "；".join(f"{mk.SIDES_CN[s]} {o.get('code', '')} {o['error'][:80]}".strip()
                                                         for s, o in q["orders"].items()), free=False, save=False)
                    self.save(q)
                    self.problem(f"{mk.window_name(q['coin'], q['start'])} 被交易所单独拒单（{codes[0]}），这一场计一次失败",
                                 key="refused-window")
                    continue
                # nothing rests and nothing was the window's own doing (not ready yet, the balance, a rate limit, an
                # exchange rule, words not recognised): no record, no attempt used; the window is placed afresh
                # (re-armed at once when the market was not ready). A send judged refused before processing is
                # cancelled by its hash all the same, should the refusal have been misread: nothing the bot signed is
                # ever left on the book unmanaged.
                for gside, o in q["orders"].items():
                    if o["state"] == "not_sent" and o.get("hash"):
                        self.ghosts[o["hash"]] = {"start": int(q["start"]), "slug": q["slug"], "side": gside,
                                                  "price": str(q["price"]), "shares": str(q["shares"])}
                        ghosted = True
                if ghosted:
                    self.keep_ghosts()
                self.forget(q)
                if account:  # the account's refusal: everything waits (escalating), nothing is struck off
                    self.refused_at[slug] = done
                    self.recent_refusals.append((done, slug, account[0]))
                    self.refuse(account[0], next((o["error"] for o in q["orders"].values() if o.get("code") == account[0]), ""))
                    hold = hold or Hold(f"交易所拒单（{account[0]}）", None)
                hold = hold or Hold(next((o["error"] for o in q["orders"].values() if o["error"]), "没发出"),
                                    max(waits) + WAIT_MARGIN_S if waits else HOLD_RETRY_S)  # the exchange said how long
                continue
            if states == {"open"}:
                self.rested(q, listing, done)
            elif states <= {"open", "unknown"}:
                pass  # the lost answers are looked for by their hashes; withdrawn if not found within UNKNOWN_GRACE_S
            else:
                codes = [o.get("code", "") for o in q["orders"].values() if o["state"] != "open"]
                self.close(q, "单边没挂上：" + "；".join(f"{mk.SIDES_CN[s]} {o['state']} {o.get('code', '')} {o['error'][:80]}".strip()
                                                    for s, o in q["orders"].items() if o["state"] != "open"),
                           free=all(retryable(c) for c in codes), save=False)
                if account:
                    self.refused_at[slug] = done
                    self.recent_refusals.append((done, slug, account[0]))
                    self.refuse(account[0], next((o["error"] for o in q["orders"].values() if o.get("code") == account[0]), ""))
            self.save(q)
            for side, o in q["orders"].items():
                if o["state"] in ("rejected", "unknown") and not retryable(str(o.get("code") or "")):
                    self.problem(f"{mk.window_name(q['coin'], q['start'])} {mk.SIDES_CN[side]} {o['state']}：{o.get('code')} {o['error'][:120]}",
                                 key=f"reject-{o.get('code')}")
        return hold

    def rested(self, q: dict, listing: mk.Listing | None, at: float) -> None:
        """Both sides of a record are on the book: when, and how long after the listing and the opening."""
        if q.get("placed"):
            return
        q["placed"] = at
        if q.get("listed") and at - float(q["listed"]) < ARM_FRESH_S:  # a fresh listing (not a backlog's)
            self.place_lags.append(at - float(q["listed"]))
        if listing is not None and listing.opened and at - listing.opened < 60:
            q["opened"] = listing.opened
            self.open_lags.append(at - listing.opened)

    def apply_placed(self, q: dict, side: str, r: Placed, at: float) -> None:
        o = q["orders"][side]
        if o["state"] != "placing":  # judged meanwhile (cannot happen while its window is in `placing`): never revived
            if r.status == "open" and r.order_id:
                self.strays.add(r.order_id)
                self.problem(f"{q['key']} {side} 的应答来得太晚，已撤掉它挂上的单 {r.order_id}", key="stray")
            return
        o.update(hash=r.hash or o.get("hash") or "", salt=r.salt or o.get("salt") or 0, code=r.code, error=r.message[:300], sent=at)
        if o["hash"]:
            self.by_hash[o["hash"]] = (q["key"], side)
        if r.status == "open":
            o.update(state="open", id=r.order_id, filled=str(r.matched))
            self.by_id[r.order_id] = (q["key"], side)
            if r.hash and self.x.live:
                self.learn_id_is_hash(r.order_id.lower() == r.hash.lower())
        elif r.status in ("rejected", "not_sent"):
            o["state"] = r.status
        else:  # unknown (or an answer this code does not know): looked for by its hash
            o.update(state="unknown", unknown_at=at)

    # ------------------------------------------------------------------ withdrawal
    def close(self, q: dict, reason: str, free: bool, save: bool = True) -> None:
        if q["phase"] not in ("active",) or self.blocked:  # another account's records are left as they are
            return
        q["phase"] = "closing"
        q["reason"] = reason
        q["closing_at"] = self.now()
        q["counted"] = not free
        if save:
            self.save(q)

    def close_all(self, reason: str) -> int:
        if self.blocked:
            return 0
        n = 0
        for q in self.open_quotes():
            if q["phase"] == "active":
                self.close(q, reason, free=True)
                n += 1
        return n

    async def cancel_pass(self, sweep_all: bool = False) -> None:
        """Withdraw what is due: each record at its cancel time (or paused) closes; its open orders are cancelled by id
        and an order of unknown state by its hash (an order the bot lost track of is still its own, and cannot outlive
        the start), the soonest windows first. Only orders the bot signed are touched; with PREOPEN_KEY_EXCLUSIVE=on
        each window's market is also swept once at its cancel time (cancel-market-orders: every order of the key)."""
        if self.blocked:
            return
        now = self.now()
        self.risk_pass()
        due: list[tuple[dict, str]] = []
        sweep: dict[str, dict] = {}
        exclusive = self.s.key_exclusive
        for q in self.open_quotes():
            at_cancel = now >= q["start"] - self.s.cancel_s
            if q["phase"] == "active":
                if at_cancel:
                    self.close(q, "到点撤单", free=True)
                elif self.paused:
                    self.close(q, "暂停", free=True)
                else:
                    continue
            if q["phase"] not in ("closing", "closed"):
                continue
            if q["phase"] == "closing":
                for side, o in q["orders"].items():
                    if now < float(o.get("retry") or 0):
                        continue
                    if o["state"] == "open":
                        due.append((q, side))
                    elif o["state"] in UNSURE and q["slug"] not in self.placing and now < float(q["expiration"]) + EXPIRED_MARGIN_S:
                        if o.get("hash") and self.id_is_hash is not False:
                            due.append((q, side))
                        elif exclusive and (at_cancel or sweep_all) and now < q["start"]:
                            sweep[q["condition_id"]] = q
            if exclusive and at_cancel and now < q["start"] and not q.get("swept"):
                sweep.setdefault(q["condition_id"], q)
        if exclusive:
            for cid, (slug, start) in list(self.attempted.items()):  # a market an order was sent to, record or not
                if now >= start:
                    del self.attempted[cid]
                elif now >= start - self.s.cancel_s and not any(p.get("swept") for p in self.quotes_of(slug)):
                    sweep.setdefault(cid, {"slug": slug, "condition_id": cid})
        else:
            self.attempted.clear()
        if self.strays:
            ids, self.strays = list(self.strays), set()
            try:
                r = await self.x.cancel(ids)
                self.strays |= {i for i in ids if i not in r.canceled and i not in r.not_canceled}
            except Exception as error:
                self.strays |= set(ids)
                LOG.warning("撤迟到的单失败：%s", error)
        await self.ghost_pass(now)
        if due:
            due.sort(key=lambda d: d[0]["start"])
            await self.cancel_orders(due)
        for cid, q in sweep.items():
            if now - self.swept.get(cid, 0.0) < SWEEP_EVERY_S:
                continue
            self.swept[cid] = now
            try:
                r = await self.x.cancel_market(cid)
            except Throttled as t:
                self.cancel_throttled(t.reason, t.seconds)
                continue
            except Exception as error:
                self.problem(f"按市场撤单失败 {q['slug']}：{error}", key="sweep-error")
                continue
            same = [p for p in self.quotes_of(q["slug"]) if p["phase"] != "settled"]
            for p in same:
                p["swept"] = True
            if same:
                self.save(*same)
            else:
                self.attempted.pop(cid, None)  # no record left to carry the flag: swept once is enough
            if r.canceled:
                LOG.info("%s 按市场撤单：撤掉 %d 张", q["slug"], len(r.canceled))
            self.mark_cancelled(r)

    async def ghost_pass(self, now: float) -> None:
        """The hashes of sends judged refused before processing, cancelled all the same until one answer says what became
        of them (cancelled: the refusal was misread and the order rested; not cancelled: it never did). One that rested
        is read for fills: its record was dropped, so a fill of it is in no record, and that is said (audit 11-27)."""
        expired = [h for h, g in self.ghosts.items() if now >= float(g.get("start") or 0)]
        for h in expired:
            del self.ghosts[h]
        if expired:
            self.keep_ghosts()
        if not self.ghosts or now < self.ghost_at:
            return
        self.ghost_at = now + SWEEP_EVERY_S
        ids = list(self.ghosts)
        try:
            r = await self.x.cancel(ids)
        except Throttled as t:
            self.cancel_throttled(t.reason, t.seconds)
            return
        except Exception as error:
            LOG.warning("按哈希撤被拒的单失败：%s", error)
            return
        if r.throttle:
            self.cancel_throttled(*r.throttle)
        rested = {h: self.ghosts[h] for h in ids if h in r.canceled and h in self.ghosts}
        for h in ids:  # answered (cancelled, not cancelled, or left out of an answered chunk: the CLOB has no such
            if h not in r.unanswered:  # order); a chunk refused or failed is asked again (11-19 review)
                self.ghosts.pop(h, None)
        self.keep_ghosts()
        if rested:
            told = []
            for h, g in rested.items():
                try:
                    row = await self.x.order(h)
                except Exception:
                    row = None
                coin, start = mk.parse_slug(str(g.get("slug") or "")) or ("", 0)
                name = f"{mk.window_name(coin, start)} {mk.SIDES_CN.get(str(g.get('side')), '')}方" if coin else h[:12]
                got = row.matched if row is not None else None
                told.append(f"{name}（{'成交 ' + mk.fmt(got) + ' 份，这部分没记进账本' if got else '没有成交' if got is not None else '成交读不到'}）")
            self.problem(f"有 {len(rested)} 张判为没发出的单其实挂上了，已按哈希撤掉：" + "；".join(told)
                         + ("。有成交的请到 Polymarket 核对" if any("成交 " in t for t in told) else ""), key="ghost")

    async def cancel_orders(self, due: list[tuple[dict, str]]) -> None:
        """Cancel by id (an order of unknown state by its hash)."""
        ids = [q["orders"][side]["id"] or q["orders"][side]["hash"] for q, side in due]
        now = self.now()
        try:
            r = await self.x.cancel(ids)
        except Throttled as t:
            self.cancel_throttled(t.reason, t.seconds)
            self.retry_later(due, now, t.seconds)
            return
        except Exception as error:
            self.problem(f"撤单失败（{len(ids)} 张）：{error}", key="cancel-error")
            self.retry_later(due, now)
            return
        if r.throttle:
            self.cancel_throttled(*r.throttle)
        touched = self.mark_cancelled(r)
        missing = [(q, side) for (q, side), oid in zip(due, ids) if oid not in r.canceled and oid not in r.not_canceled]
        if missing:
            self.retry_later(missing, now, r.throttle[1] if r.throttle else 0.0)
            if r.error:
                self.problem(f"撤单只完成一部分：{len(missing)} 张没收到答复（{r.error[:120]}），稍后重撤", key="cancel-error")
        for q, side in due:
            if q["key"] not in touched:
                self.save(q)

    def mark_cancelled(self, r: Cancelled) -> set[str]:
        """Orders the CLOB cancelled, or would not cancel (already matched, already gone): off the book, their final
        record read soon (sooner past the cancel time: one that is somehow still live is cancelled again). An order of
        unknown state cancelled by its hash did rest: from now on it is an order like the others. One the CLOB would
        not cancel stays unknown: never there, or already filled or cancelled, its lookup by hash tells which."""
        now = self.now()
        touched: dict[str, dict] = {}
        for oid in list(r.canceled) + list(r.not_canceled):
            ref = self.ref_of(oid)
            if not ref or ref[0] not in self.ledger.rows:
                continue
            q = self.ledger.rows[ref[0]]
            o = q["orders"][ref[1]]
            late = now >= q["start"] - self.s.cancel_s
            said = r.not_canceled.get(oid)
            if o["state"] == "open" and said is not None and not off_book(said):
                # not cancelled, and not said to be off the book: it may well rest. It stays open: cancelled again soon,
                # waited for by a withdrawal, its money still counted (audit 11-27: such an answer was read as gone, so
                # the cancels stopped, and a switch to paper trading or an exit left the order resting unwatched)
                o.update(retry=now + (SPRINT_S if late else SWEEP_EVERY_S), cancel_note=said[:200])
                self.problem(f"{mk.window_name(q['coin'], q['start'])} {mk.SIDES_CN[ref[1]]}方撤单没成功（交易所：{said[:100]}），继续撤",
                             key="not-canceled")
            elif o["state"] == "open":
                o.update(state="gone", gone=now, read_at=now + (1 if late and said is not None else FINAL_DELAY_S),
                         note=said if said is not None else "已撤", read_tries=0)
                if q["phase"] == "closing" and q["reason"] == "到点撤单" and oid in r.canceled:
                    self.cancel_lags.append(q["start"] - now)
                    q["cancel_lead"] = round(q["start"] - now, 2)
            elif o["state"] in UNSURE:
                if oid in r.canceled:
                    o.update(state="gone", id=oid, gone=now, read_at=now + FINAL_DELAY_S, note="状态不明的单按订单哈希撤掉（它挂上了）")
                    self.by_id[oid] = (q["key"], ref[1])
                    self.learn_id_is_hash(oid == o.get("hash"))
                else:  # asked again (past the cancel time every SWEEP_EVERY_S; before it backing off to the cancel time)
                    tries = int(o.get("hash_tries") or 0) + 1
                    cancel_at = q["start"] - self.s.cancel_s
                    wait = SWEEP_EVERY_S if late else min(RETRY_MAX_S, SWEEP_EVERY_S * 2 ** (tries - 1))
                    o.update(hash_tries=tries, retry=now + wait if late else min(now + wait, max(cancel_at, now + SWEEP_EVERY_S)),
                             lookup_at=0, cancel_note=r.not_canceled.get(oid, ""), hash_answered=now)
            else:
                continue
            touched[q["key"]] = q
        if touched:
            self.save(*touched.values())
            if r.canceled:  # money freed: the next windows are placed now, not at the next sweep (with a short cancel
                self.sweep()  # time the window after can be within the new-order cutoff a few seconds later)
        return set(touched)

    def retry_later(self, due: list[tuple[dict, str]], now: float, wait: float = 0.0) -> None:
        """A failed cancel is tried again: past the cancel time every SPRINT_S (or the server's wait, at most 10 s);
        before it with a doubling backoff that never reaches past the cancel time."""
        for q, side in due:
            o = q["orders"][side]
            cancel_at = q["start"] - self.s.cancel_s
            if now >= cancel_at:
                o["retry"] = now + max(SPRINT_S, min(wait, 10.0))
            else:
                o["tries"] = int(o.get("tries") or 0) + 1
                o["retry"] = min(now + max(wait, min(RETRY_MAX_S, RETRY_S * 2 ** (o["tries"] - 1))), cancel_at)
        self.save(*{q["key"]: q for q, _ in due}.values())

    # ------------------------------------------------------------------ reconciliation
    async def reconcile_pass(self) -> None:
        """Orders off the book get their final record read; unknown placements are looked for by their hash; a resting
        record with an order still unknown after UNKNOWN_GRACE_S is withdrawn; a record whose orders all ended closes."""
        now = self.now()
        reads: list[tuple[dict, str]] = []
        lookups: list[tuple[dict, str]] = []
        for q in self.open_quotes():
            if q["slug"] in self.placing:
                continue  # its POST is in flight: its orders are judged by the answer, not looked for
            for side, o in q["orders"].items():
                read_at = float(o.get("read_at") or float(o.get("gone") or 0) + FINAL_DELAY_S)
                if o["state"] == "gone" and now >= read_at and now >= float(o.get("retry") or 0):
                    reads.append((q, side))
                elif o["state"] in UNSURE and now >= float(o.get("sent") or 0) + SEEN_MARGIN_S:
                    if not self.claim(q, side) and now >= float(o.get("lookup_at") or 0):
                        lookups.append((q, side))
        gate = asyncio.Semaphore(4)

        async def one(q: dict, side: str) -> None:
            async with gate:
                await self.read_final(q, side)

        async def look(q: dict, side: str) -> None:
            async with gate:
                await self.look_up(q, side)
        if reads or lookups:
            reads.sort(key=lambda d: d[0]["start"])
            try:
                await asyncio.wait_for(asyncio.gather(*(one(q, side) for q, side in reads[:60]),
                                                      *(look(q, side) for q, side in lookups[:20])), RECONCILE_BUDGET_S)
            except asyncio.TimeoutError:
                LOG.warning("核对超过 %d 秒，余下的下一轮再读", RECONCILE_BUDGET_S)
        now = self.now()
        for q in self.open_quotes():
            if q["phase"] == "active" and q["slug"] not in self.placing and any(
                    o["state"] in UNSURE and now >= float(o.get("unknown_at") or o.get("sent") or 0) + UNKNOWN_GRACE_S
                    for o in q["orders"].values()):
                # not found on the book in time: withdrawn (its other side with it); the unknown one is cancelled by its
                # hash and the level quoted again only once it is known to rest nowhere
                self.close(q, "有一边状态不明（找不到），撤掉", free=True)
            if q["phase"] in ("closing", "active") and all(o["state"] in ENDED for o in q["orders"].values()):
                if not q.get("reason"):
                    q["reason"] = "挂单都已结束"
                q["phase"] = "closed"
                q["closed_at"] = now
                self.save(q)

    def claim(self, q: dict, side: str) -> bool:
        """An unknown placement found in the open orders. By its hash, exactly, whenever it has one and the CLOB's id
        is (or may be) the hash. Only an order without a hash, or one whose id is known not to be its hash, is matched
        by its look (its token, BUY, its price and size, sent since it was: no record's order) — and then only when a
        single order fits: two or more may be another strategy's, and none is claimed."""
        o = q["orders"][side]
        h = o.get("hash") or ""
        if h and self.id_is_hash is not False:
            row = self.snapshot.get(h)
            if row is not None:
                self.adopt(q, side, row, "按订单哈希在挂单列表里认领", resting=True)  # in the open orders: resting
                return True
            return False
        sent = float(o.get("sent") or 0)
        fits = [row for row in self.snapshot.values()
                if self.ref_of(row.id) in (None, (q["key"], side))   # not another record's order
                and row.asset == o["asset"] and row.side == "BUY" and row.price == dnum(q["price"])
                and row.size == dnum(q["shares"]) and (row.created is None or row.created >= sent - 30)]
        if len(fits) == 1:
            self.adopt(q, side, fits[0], "按挂单列表认领（唯一一张同规格的单）", resting=True)
            return True
        if len(fits) > 1:
            o["claim_note"] = f"挂单列表里有 {len(fits)} 张同规格的单，不自动认领"
        return False

    def adopt(self, q: dict, side: str, row: Row, note: str, resting: bool | None = None) -> None:
        o = q["orders"][side]
        live = row.resting if resting is None else resting
        o.update(state="open" if live else "gone", id=row.id, note=note, gone=self.now(), read_at=self.now(), retry=0)
        self.by_id[row.id] = (q["key"], side)
        if o.get("hash"):
            self.learn_id_is_hash(row.id.lower() == o["hash"].lower())
        self.note_fill(q, side, row.matched)
        if q["phase"] == "active" and all(p["state"] == "open" for p in q["orders"].values()):
            self.rested(q, self.listings.get(q["slug"]), self.now())
        LOG.info("认领状态未知的单 %s %s -> %s（%s）", q["key"], side, row.id, note)
        self.save(q)

    async def look_up(self, q: dict, side: str) -> None:
        """An order of unknown state (its answer lost, its process stopped mid-send) looked for by its hash, the CLOB's
        order id: found, it is adopted (resting or not, with its fills). It is given up as never placed only on the
        CLOB's word: its hash answered "no such order" GIVE_UP_S after the send, the id being known to be the hash.
        Short of that it stays unknown — cancelled by its hash, its level not quoted again — until its signed expiry
        has passed; then it rests nowhere, and what it bought is read from the account's trades."""
        o = q["orders"][side]
        now = self.now()
        early = now < float(o.get("sent") or 0) + GIVE_UP_S
        o["lookup_at"] = now + (LOOKUP_EVERY_S if early else 60)  # past the first minutes, a minute apart (to the expiry)
        h = o.get("hash") or ""
        if h and self.id_is_hash is not False:
            try:
                row = await self.x.order(h)
            except Exception as error:  # no answer is no evidence: nothing concluded, asked again
                LOG.debug("按哈希查单失败 %s：%s", h, error)
                self.save(q)
                return
            if o["state"] not in UNSURE:  # moved on while the answer came (cancelled by its hash meanwhile): what was
                return                    # asked is answered by that; a stale "no such order" or LIVE row must not undo it
            if row is not None:
                self.adopt(q, side, row, "按订单哈希找到")
                return
            o["absent"] = int(o.get("absent") or 0) + 1
            if self.id_is_hash and o["absent"] >= 2 and now >= float(o.get("sent") or 0) + GIVE_UP_S:
                self.never_placed(q, side, "交易所查无此订单哈希（订单号就是哈希），确认没挂上")
                return
        if now < float(q["expiration"]) + EXPIRED_MARGIN_S:
            self.save(q)
            return
        await self.unknown_expired(q, side)

    def never_placed(self, q: dict, side: str, note: str) -> None:
        o = q["orders"][side]
        o.update(state="not_found", note=note)
        if q["phase"] == "active":  # the other side rests alone: withdrawn, the level quoted afresh
            self.close(q, f"{mk.SIDES_CN[side]}方没挂上", free=True, save=False)
        self.save(q)

    async def trades_of(self, condition_id: str, since: float = 0.0) -> dict[str, D]:
        """The account's fills on a market (order id -> shares bought), read once for every order that asks soon; read
        afresh when the copy kept was read before `since` (the moment the asking order left the book: a fill of it came
        before then, and a copy older than that may not have it — audit 11-27, a fill missed so)."""
        at, fills = self.trades.get(condition_id, (0.0, {}))
        if self.now() - at < TRADES_TTL_S and at >= since:
            return fills
        fills = await self.x.fills(condition_id)
        self.trades[condition_id] = (self.now(), fills)
        for cid in [c for c, (t, _) in self.trades.items() if self.now() - t > 600]:
            del self.trades[cid]
        return fills

    async def unknown_expired(self, q: dict, side: str) -> None:
        """An unknown order past its signed expiry: it rests nowhere. Its fills are read from the account's trades by
        its hash (its id); without a usable hash they cannot be told from another strategy's, so it is recorded as
        unverified and said so. Trades that cannot be read are asked again, for UNVERIFIED_AFTER_S at most."""
        o = q["orders"][side]
        now = self.now()
        h = o.get("hash") or ""
        if not h or self.id_is_hash is False:
            o.update(state="not_found", unverified=True, note="没有可用的订单号：成交无法核实，请到 Polymarket 核对这一场")
            self.problem(f"{mk.window_name(q['coin'], q['start'])} {mk.SIDES_CN[side]}方的单状态不明、成交无法核实，请到 Polymarket 核对",
                         key="unverified")
            self.save(q)
            return
        try:
            fills = await self.trades_of(q["condition_id"], since=float(q["expiration"]))
        except Exception as error:
            if now >= float(q["expiration"]) + UNVERIFIED_AFTER_S:
                o.update(state="not_found", unverified=True, note=f"成交记录一直读不到（{error}）：按没有成交记下，请到 Polymarket 核对")
                self.problem(f"{mk.window_name(q['coin'], q['start'])} {mk.SIDES_CN[side]}方的成交读不到，请到 Polymarket 核对",
                             key="unverified")
            else:
                LOG.debug("读成交失败 %s：%s", q["condition_id"], error)
            self.save(q)
            return
        if o["state"] not in UNSURE:  # moved on while the trades were read
            return
        got = fills.get(h, D(0))
        if got > 0:
            o.update(id=h)
            self.by_id[h] = (q["key"], side)
            self.learn_id_is_hash(True)
            self.note_fill(q, side, got, at=float(q["start"]) - self.s.expiry_s)  # at the latest: when it expired
            o.update(state="final", final_at=now, note="状态不明，到期后按成交记录核实：有成交")
        elif self.id_is_hash:
            o.update(state="not_found", note="状态不明，到期后按成交记录核实：没有成交")
        else:  # whether the CLOB's id is the hash was never confirmed on this account: no trade under it proves little
            o.update(state="not_found", unverified=True, note="到期后按订单哈希没找到成交（订单号是否等于哈希还没确认），请核对")
            self.problem(f"{mk.window_name(q['coin'], q['start'])} {mk.SIDES_CN[side]}方的单状态不明，按哈希没找到成交，请到 Polymarket 核对",
                         key="unverified")
        self.save(q)

    async def read_final(self, q: dict, side: str) -> None:
        o = q["orders"][side]
        try:
            row = await self.x.order(o["id"])
        except Exception as error:  # a count of its own: a cancel's failures before it (tries) do not lengthen this wait
            o["read_tries"] = int(o.get("read_tries") or 0) + 1
            o["retry"] = self.now() + min(RETRY_MAX_S, RETRY_S * 2 ** (o["read_tries"] - 1))
            self.save(q)
            LOG.debug("读单失败 %s：%s", o["id"], error)
            return
        if o["state"] != "gone":  # moved on while the answer came
            return
        if row is None:  # the CLOB has no record of it now: what it bought is in the trades
            try:
                fills = await self.trades_of(q["condition_id"], since=float(o.get("gone") or 0))
            except Exception as error:
                o["read_tries"] = int(o.get("read_tries") or 0) + 1
                if o["read_tries"] < 6:
                    o["retry"] = self.now() + min(RETRY_MAX_S, RETRY_S * 2 ** (o["read_tries"] - 1))
                    self.save(q)
                    return
                o.update(state="final", final_at=self.now(), unverified=True,
                         note=(o.get("note") or "") + f"；最终记录和成交记录都读不到（{error}），按已知成交")
                self.problem(f"{mk.window_name(q['coin'], q['start'])} {mk.SIDES_CN[side]}方的最终成交读不到，按已知的记，请核对", key="unverified")
                self.save(q)
                return
            self.note_fill(q, side, fills.get(o["id"], D(0)), at=float(o.get("gone") or self.now()))
            o.update(state="final", final_at=self.now(), note=(o.get("note") or "") + "；最终记录读不到，按成交记录")
            self.save(q)
            return
        self.note_fill(q, side, row.matched, at=float(o.get("gone") or self.now()))
        if row.resting:  # still on the book: the cancel did not take (or the order never left)
            o.update(state="open", retry=0)
            self.save(q)
            return
        o.update(state="final", final_at=self.now(), status=row.status)
        if row.matched_out:
            # Eaten whole, a hair under its size or not (Row.matched_out): a fill like any other. The record stays as it
            # is, its other side resting to pair it. Until 11-25 one short of its size was taken for an order cancelled
            # elsewhere: the other side was withdrawn, and three such windows in a minute paused the bot (11-25 16:31:
            # BTC, ETH and SOL bought 29.99, 29.98 and 29.98 of 30 within 52 seconds).
            pass
        elif q["phase"] == "active" and row.matched < row.size and self.now() < q["start"] - self.s.cancel_s:
            state = row.state
            if state == "CANCELED_NOT_ENOUGH_BALANCE":  # fills elsewhere took the balance below this market's orders
                self.close(q, f"{mk.SIDES_CN[side]}方挂单因余额不足被交易所撤掉", free=True, save=False)
                try:  # the balance as it is now: each window then waits for its own market's room, nothing else waits
                    self.balance, self.balance_at = await self.x.balance(), self.now()
                except Exception:
                    self.balance_at = 0.0  # unreadable: nothing is placed until it is read again
                self.problem("余额降到了一场的挂单合计以下，交易所撤掉了这些场次的挂单；余额回来后（结算、领取）自动重挂",
                             key="balance-cancel")
            elif state not in EXTERNAL_STATES:  # the exchange's own doing (the market resolved or vanished, the order
                # timed out, expired, was rejected or invalidated): not the window's failure, nor anybody's cancel. Only a
                # plain CANCELED is somebody's (audit 11-27: EXPIRED and REJECTED were counted, three of them paused)
                self.close(q, f"{mk.SIDES_CN[side]}方挂单被交易所撤掉（{row.status}）", free=True, save=False)
                if state == "EXPIRED":  # it expired before the cancel time: this clock is behind the CLOB's
                    self.clock_at = 0.0  # checked again at once (the settle loop)
                    self.problem(f"{mk.window_name(q['coin'], q['start'])} 的单在撤单时间之前就到期了：本机时钟可能比交易所慢，"
                                 "马上重新对时", key="early-expiry")
            else:  # cancelled by someone else (the website, another program with the key)
                now = self.now()
                self.external.append((now, q["slug"]))
                if len({s for t, s in self.external if t > now - EXTERNAL_WINDOW_S}) >= EXTERNAL_MASS:
                    # many windows at once (the website's 取消所有): a stop. Paused, said so; no window charged for it
                    self.close(q, f"{mk.SIDES_CN[side]}方挂单被别处成批撤掉（{row.status}）", free=True, save=False)
                    for other in {s for t, s in self.external if t > now - EXTERNAL_WINDOW_S} - {q["slug"]}:
                        for r in self.quotes_of(other):  # the first ones of the batch were charged before it was one
                            if r.get("counted") and "挂单被撤（" in str(r.get("reason") or "") and \
                                    float(r.get("closing_at") or 0) > now - EXTERNAL_WINDOW_S:
                                r["counted"] = False
                                r["reason"] = str(r["reason"]).replace("挂单被撤（", "挂单被别处成批撤掉（")
                                self.save(r)
                    if not self.paused:
                        batch = [r for s in {s for t, s in self.external if t > now - EXTERNAL_WINDOW_S}
                                 for r in self.quotes_of(s) if "被别处成批撤掉" in str(r.get("reason") or "")]
                        detail = self.external_detail(batch)
                        self.pause(EXTERNAL_WHY, detail)
                        self.problem(f"{EXTERNAL_WHY}（{detail}）：网页上的「取消所有」、用同一个 Key 的别的程序，"
                                     "或者交易所撤的。已暂停新挂并撤掉其余挂单；要继续请在控制台点「恢复挂单」或发 /resume",
                                     key="external-cancel")
                else:  # one window: counted, quoted again up to the cap
                    self.close(q, f"{mk.SIDES_CN[side]}方挂单被撤（{row.status}）", free=False, save=False)
        self.save(q)

    def note_fill(self, q: dict, side: str, matched: D, at: float | None = None) -> None:
        """A fill seen (`at`: when it was first seen; an order gone from the book was filled by that snapshot)."""
        o = q["orders"][side]
        before = dnum(o.get("filled"))
        if matched > before:
            o["filled"] = str(matched)
            if before <= 0:
                o["first_fill"] = at if at is not None else self.now()
            ahead = q["start"] - self.now()
            self.fills_pending.append(f"{mk.window_name(q['coin'], q['start'])} {mk.SIDES_CN[side]} +{mk.fmt(matched - before)} 份"
                                      f" @{mk.fmt(dnum(q['price']) * 100)}¢（开场前 {mk.span(ahead)}）")
            self.counters["fills"] += 1

    # ------------------------------------------------------------------ account
    async def account_once(self) -> None:
        """The open orders, then the balance (every BALANCE_EVERY_S). Each keeps its own time: hold() refuses to place
        on either one grown old, so a balance that keeps failing to read stops the placing on its own."""
        began = self.now()
        rows = await self.x.open_orders()
        self.snapshot_took = self.now() - began
        self.apply_snapshot(rows, began)
        held_v2 = bool(self.v2_blocked()) and self.now() - self.v2_refreshed >= V2_REFRESH_S
        if held_v2 and hasattr(self.x, "refresh_allowance"):  # an approval made on the website: the CLOB reads it afresh
            self.v2_refreshed = self.now()
            await self.x.refresh_allowance()
        if self.now() - self.balance_at >= BALANCE_EVERY_S or self.balance is None or held_v2:
            self.balance = await self.x.balance()
            self.balance_at = self.now()

    def apply_snapshot(self, rows: list[Row], began: float) -> None:
        self.snapshot = {r.id: r for r in rows}
        mine = set()
        changed: dict[str, dict] = {}
        for q in self.open_quotes():
            for side, o in q["orders"].items():
                if o["state"] != "open" or not o.get("id"):
                    continue
                row = self.snapshot.get(o["id"])
                if row is not None:
                    mine.add(row.id)
                    if row.matched > dnum(o.get("filled")):
                        self.note_fill(q, side, row.matched)
                        changed[q["key"]] = q
                elif float(o.get("sent") or 0) < began - SEEN_MARGIN_S:
                    o.update(state="gone", gone=self.now(), note="挂单列表里没有了（成交、被撤或到期）")
                    changed[q["key"]] = q
        self.unmanaged = [r for r in rows if r.id not in self.by_id and r.id not in self.by_hash]
        self.snapshot_at = self.now()
        twins = self.twins()
        if twins >= TWINS_ALERT:
            self.problem(f"账户里有 {twins} 张和本程序一模一样、但不是本程序下的挂单：同一个账户很可能还有另一个机器人在跑"
                         "（Railway 上的另一个服务、电脑上本地运行的）。两个一起跑会重复挂单、互相抢下单限速，请只留一个", key="twins")
        if changed:
            self.save(*changed.values())

    # ------------------------------------------------------------------ results
    async def log_fill_times(self, q: dict, cache: dict[str, Any]) -> bool:
        """Each filled side's fills with the moment they matched (the account's trades): the cancel-time review replays
        the window from them. Seen-at times from the snapshots stand in when the trades cannot be read."""
        cid = q["condition_id"]
        if cid not in cache:
            try:
                cache[cid] = await self.x.trade_log(cid)
            except Exception as error:
                LOG.debug("成交时刻 %s：%s", q["slug"], error)
                cache[cid] = None
        log = cache[cid]
        if log is None:
            q["fill_log_tries"] = int(q.get("fill_log_tries") or 0) + 1
            return False
        for o in q["orders"].values():
            if dnum(o.get("filled")) > 0 and o.get("id") in log:
                o["fills_at"] = [[round(ts, 3), str(shares)] for ts, shares in sorted(log[o["id"]]) if ts > 0]
        q["fills_logged"] = True
        return True

    async def settle_pass(self) -> None:
        now = self.now()
        results: dict[str, str | None] = {}
        logs: dict[str, Any] = {}
        for q in self.open_quotes():
            if q["phase"] != "closed":
                continue
            filled = {side: dnum(o.get("filled")) for side, o in q["orders"].items()}
            if not any(filled.values()):
                q["phase"] = "settled"
                q["pnl"] = 0.0
                self.save(q)
                continue
            if now < q["start"] + mk.PERIOD_S + SETTLE_AFTER_S or now < float(q.get("result_retry") or 0):
                continue
            if not q.get("fills_logged") and int(q.get("fill_log_tries") or 0) < FILL_LOG_TRIES:
                if not await self.log_fill_times(q, logs):
                    self.save(q)
                    continue
            slug = q["slug"]
            if slug not in results:
                try:
                    m = await self.pub.market(slug)
                    results[slug] = (mk.winner(m) or (mk.SPLIT if mk.split(m) else None)) if m else None
                except Exception as error:
                    LOG.debug("结果 %s：%s", slug, error)
                    results[slug] = None
            win = results[slug]
            if win is None:
                q["result_retry"] = now + RESULT_RETRY_S
                self.save(q)
                continue
            price = dnum(q["price"])
            cost = sum((v * price for v in filled.values()), D(0))
            payout = sum((v * mk.paid(win, side) for side, v in filled.items()), D(0))
            foreign = bool(q.get("foreign"))   # another account's (改绑): its result for the record; not claimed from here
            auto = self.s.auto_redeem and self.x.live and not foreign
            why = self.x.redeem_problem() if auto and payout > 0 else ""
            q.update(phase="settled", result=win, pnl=float(payout - cost), settled_at=now,
                     redeem="none" if foreign or payout <= 0 else "pending" if auto and not why else "manual")
            if why:  # nothing tried six times over an hour: said once, claimed after a restart that brings the key
                q["redeem_error"] = why
                won = f"赢了 {mk.fmt(filled[win])} 份" if win in mk.SIDES else f"按 50/50 结算，可领回 {mk.fmt(payout)} 美元"
                self.problem(f"{mk.window_name(q['coin'], q['start'])} {won}，{why}", key="redeem-key")
            self.save(q)
            if foreign:
                continue
            if win == mk.SPLIT:
                self.problem(f"{mk.window_name(q['coin'], q['start'])} 按 50/50 结算（没有输赢），两边每份都按 50¢ 算", key="split")
            self.notify(f"结算 {mk.window_name(q['coin'], q['start'])}：结果{mk.RESULT_CN[win]}，"
                        f"成交 涨 {mk.fmt(filled['up'])} / 跌 {mk.fmt(filled['down'])}，盈亏 {q['pnl']:+.2f} 美元")

    def redeem_again(self) -> int:
        """At start: a claim given up on (six failures), or left for the website for want of a Relayer key, is tried
        again when this process can claim (a restart is when a new key comes in)."""
        if not (self.s.auto_redeem and self.x.live) or self.x.redeem_problem():
            return 0
        for q in self.ledger.restore_unclaimed():  # archived after two days, still this bot's to claim (audit 11-27)
            self._index(q)
        again = [q for q in self.ledger.rows.values() if unclaimed(q)]
        for q in again:
            q.update(redeem="retry", redeem_tries=0, redeem_retry=0, redeem_error="")
        if again:
            self.save(*again)
            LOG.info("重新领取 %d 条", len(again))
        return len(again)

    async def redeem_pass(self) -> None:
        if not (self.s.auto_redeem and self.x.live):
            return
        now = self.now()
        if now - self.redeem_at < 20:
            return
        for q in list(self.ledger.rows.values()):
            if q.get("redeem") not in ("pending", "retry") or now < float(q.get("redeem_retry") or 0) or q.get("foreign"):
                continue
            self.redeem_at = now
            same = [p for p in self.quotes_of(q["slug"]) if p.get("redeem") in ("pending", "retry")]
            try:
                tx = await self.x.redeem(q["condition_id"])
            except Exception as error:
                tries = int(q.get("redeem_tries") or 0) + 1
                for p in same:
                    p.update(redeem="retry" if tries < REDEEM_TRIES else "failed", redeem_tries=tries,
                             redeem_retry=now + REDEEM_RETRY_S, redeem_error=str(error)[:200])
                self.save(*same)
                self.problem(f"领取 {q['slug']} 失败（第 {tries} 次）：{error}", key="redeem")
                return
            for p in same:
                p.update(redeem="done", redeem_tx=tx, redeemed_at=now)
            self.save(*same)
            LOG.info("已领取 %s：%s", q["slug"], tx)
            return

    # ------------------------------------------------------------------ takeover
    def matches_settings(self, q: dict) -> str:
        """Why a resting record no longer fits the settings ("" when it does)."""
        level = int(q["level"])
        if q["coin"] not in self.s.coins:
            return "设置不同（不再挂这个币）"
        if level >= len(self.s.levels):
            return "设置不同（档位减少）"
        lv = self.s.levels[level]
        if dnum(q["price"]) != lv.price or dnum(q["shares"]) != lv.shares:
            return "设置不同（价格或份数）"
        if int(q["expiration"]) != self.expiration(q["start"]):
            return "设置不同（到期时间）"
        if q["start"] > self.now() + self.s.ahead_s:
            return "设置不同（超出提前范围）"
        if not self.s.weekend and mk.on_weekend(q["start"]):
            return "设置不同（周末不挂）"
        return ""

    def settings_withdraw(self, q: dict) -> str:
        """Why new settings withdraw a resting record ("" when it stays): one that no longer fits them, unless it has a
        fill and nothing of it could add one-sided shares (risk_withdrawals: what rests of it can only pair what was
        bought). A record with a fill is never quoted again, so withdrawing its other side would leave the fill
        one-sided for good (audit 11-27: a tap on the review's 改成开场前 X 秒撤单 did that to every half-filled
        window). Such a record keeps resting as it is, until the cancel time."""
        why = self.matches_settings(q)
        if why and any(dnum(o.get("filled")) > 0 for o in q["orders"].values()) \
                and q["key"] not in {r["key"] for r in self.risk_withdrawals(q["slug"])}:
            q["note"] = f"{why}，但这一档有成交，另一边留着配对，到撤单时间照常撤"
            self.hedges_kept += 1
            return ""
        return why

    def take_over(self) -> tuple[int, int]:
        kept = changed = 0
        now = self.now()
        self.revive()
        for q in self.open_quotes():
            for o in q["orders"].values():
                if o["state"] in ("placing", "prepared"):
                    # the previous process stopped mid-send: it may rest; looked for by its hash (written before the send)
                    o.update(state="unknown", unknown_at=now, note="上次进程在发送中停止")
                elif o["state"] == "unknown":
                    o["unknown_at"] = now   # a fresh grace to be found in
            if q["phase"] == "active":
                why = self.settings_withdraw(q)
                if why:
                    self.close(q, why, free=True, save=False)
                    changed += 1
                else:
                    kept += 1
            self.save(q)
        return kept, changed

    def note_settings(self, new: Settings, via: str) -> None:
        """A settings change kept with when and how it came (the page, Telegram's button, a restart on new variables):
        the review sets results before and after it side by side. Only the values the page shows (no key, no token)."""
        values = config.current_values(new)
        try:
            before = json.loads(self.ledger.get("settings_now") or "null")
        except ValueError:
            before = None
        if isinstance(before, dict):
            changes = [[k, str(before.get(k, "")), v] for k, v in values.items() if str(before.get(k, "")) != v]
            if changes:
                self.settings_log.append({"at": round(self.now(), 1), "via": via, "changes": changes})
                self.ledger.put("settings_log", json.dumps(list(self.settings_log), ensure_ascii=False))
        if before != values:
            self.ledger.put("settings_now", json.dumps(values, ensure_ascii=False))

    def read_versions(self) -> list[dict[str, Any]]:
        """The versions of the trading code (config.TRADING_VERSIONS) this ledger has run under, each with when it began
        here, kept the first time it is seen: that moment for a new one; for the ones already out when the log began,
        the time the list names (when they went out: nobody wrote down when they first ran here)."""
        try:
            log = json.loads(self.ledger.get("version_log") or "[]")
        except ValueError:
            log = []
        log = [e for e in log if isinstance(e, dict) and e.get("id") and e.get("at")] if isinstance(log, list) else []
        known = {e["id"] for e in log}
        fresh = [(when, note) for when, note in config.TRADING_VERSIONS if when not in known]
        if not fresh:
            return log
        newest, begun = config.TRADING_VERSIONS[-1][0], bool(log)
        for when, note in fresh:
            went_out = mk.dt.datetime.strptime(when, "%Y-%m-%d %H:%M").replace(tzinfo=mk.BEIJING).timestamp()
            first_seen = self.now() if begun and when == newest else went_out   # a new one, seen starting right now
            log.append({"id": when, "at": float(int(max(first_seen, went_out))), "note": note})
        log.sort(key=lambda e: float(e["at"]))
        self.ledger.put("version_log", json.dumps(log, ensure_ascii=False))
        return log

    def update_points(self, limit: int = 12) -> list[dict[str, Any]]:
        """The moments the bot was changed, the latest first: a settings change (how, and which settings) or a new version
        of the trading code (what changed). The review's 上次更新起 starts at the first one; the page lists the others to
        start from instead."""
        labels = {k: label for k, label, _ in config.CONTROL_KEYS}
        points = [{"at": float(e["at"]), "kind": "版本更新", "text": str(e.get("note") or "")} for e in self.version_log]
        for e in self.settings_log:
            if e.get("changes"):
                names = "、".join(dict.fromkeys(labels.get(c[0], c[0]) for c in e["changes"]))
                points.append({"at": float(e["at"]), "kind": "改参数", "text": f"{e.get('via') or ''}：{names}".lstrip("：")})
        now = self.now()
        points = sorted((p for p in points if p["at"] <= now), key=lambda p: -p["at"])[:limit]
        return [{**p, "when": mk.hm(p["at"])} for p in points]

    def apply_settings(self, new: Settings, via: str = "网页") -> int:
        """New settings in force at once (the web console): a resting record that no longer fits them is withdrawn and
        quoted again under the new ones; one that fits keeps its place in the queue. Returns the withdrawn count."""
        self.note_settings(new, via)
        self.s = new
        if hasattr(self.x, "settings"):
            self.x.settings = new
        for name, rate, burst in (("orders", new.order_rate, new.order_burst), ("cancels", new.cancel_rate, new.cancel_burst)):
            bucket = getattr(self.x, name, None)
            if name == "orders" and hasattr(self.x, "set_order_limits"):
                self.x.set_order_limits(rate, burst)  # kept under the account's tier
            elif bucket is not None and hasattr(bucket, "rate"):
                bucket.rate, bucket.burst = rate, burst
        if not self.x.live and hasattr(self.x, "cash"):
            self.x.cash = new.paper_balance
            self.balance = new.paper_balance
        changed = 0
        self.hedges_kept = 0
        for q in self.open_quotes():
            if q["phase"] == "active" and (why := self.settings_withdraw(q)):
                self.close(q, why, free=True)
                changed += 1
        self.full_scan_at = self.sweep_at = 0.0  # new coins, a wider range: looked for at the next poll
        self.version += 1
        return changed

    def withdraw(self, key: str) -> str:
        """One window level withdrawn by hand (the console's 撤): not quoted again."""
        q = self.ledger.rows.get(key)
        if q is None:
            return "没有这条记录"
        if q["phase"] != "active":
            return f"这一场已经是 {q['phase']}，不用撤"
        self.close(q, "网页手动撤单", free=True, save=False)
        q["permanent"] = q["manual"] = True
        self.save(q)
        return f"{mk.window_name(q['coin'], q['start'])} 第 {int(q['level']) + 1} 档在撤单，不再重挂"

    async def emergency(self) -> str:
        """Pause, and cancel at once every order the bot signed (by id, an unknown one by its hash); with
        PREOPEN_KEY_EXCLUSIVE=on also every open order of the API key. The cancel loop keeps at whatever is left."""
        n = self.pause("网页上点了 🛑 紧急停止")
        extra = ""
        if self.s.key_exclusive:
            try:
                r = await self.x.cancel_all()
                self.mark_cancelled(r)
                extra = f"，按 API Key 一次撤掉 {len(r.canceled)} 张"
            except Exception as error:
                self.problem(f"紧急停止：按 API Key 一次撤光失败（{error}），按记录逐张撤", key="emergency")
                extra = f"，按 API Key 一次撤光失败（{error}）"
        try:
            await self.cancel_pass(sweep_all=True)
        except Exception as error:
            self.problem(f"紧急停止：撤单出错（{error}），撤单循环继续撤", key="emergency")
        left = sum(1 for q in self.open_quotes() for o in q["orders"].values() if o["state"] in MAY_REST)
        text = f"已暂停，{n} 场在撤单{extra}" + (f"；还有 {left} 张在撤/核对中，撤单循环继续" if left else "；本程序的挂单都已撤掉")
        self.notify(f"🛑 紧急停止：{text}")
        return text

    # ------------------------------------------------------------------ operator
    def drop_unsent(self, why: str) -> int:
        """Orders signed and waiting for the sender, not one byte sent: answered not sent (a stop or a pause is not
        followed by new orders reaching the book; audit 11-27)."""
        drop = getattr(self.x, "drop_unsent", None)
        return drop(why) if callable(drop) else 0

    def pause(self, why: str = "手动暂停", detail: str = "") -> int:
        """Stop placing and withdraw everything; when and why kept (across a restart too) for 📊, 🩺 and the page."""
        self.drop_unsent("已暂停")
        self.paused = True
        self.ledger.put("paused", "1")
        self.pause_note = {"at": self.now(), "why": why, "detail": detail}
        self.ledger.put("pause_note", json.dumps(self.pause_note, ensure_ascii=False))
        LOG.warning("暂停：%s %s", why, detail)
        return self.close_all("暂停")

    def pause_text(self) -> str:
        """Since when and why: "11-23 03:12 起：本程序的挂单在别处被成批撤掉"."""
        at = float(self.pause_note.get("at") or 0)
        return (f"{mk.hm(at)} 起：" if at else "") + str(self.pause_note.get("why") or "原因没有记下")

    def pause_detail(self) -> str:
        """Which windows and the exchange's word for it, as the pause kept them. A pause made by the versions up to
        11-25 for orders the exchange had ended as MATCHED says so: those were fills, nothing was cancelled elsewhere."""
        detail = str(self.pause_note.get("detail") or "")
        said = re.search(r"交易所状态 ([A-Za-z_/]+)", detail)
        words = {w.upper().removeprefix("ORDER_STATUS_") for w in said.group(1).split("/")} if said else set()
        if words and words <= set(MATCHED_STATES) and self.pause_note.get("why") == EXTERNAL_WHY:
            detail += "。这次是误报：MATCHED 是成交，不是撤单（旧版本把吃满的单当成了被别处撤掉，已修复），点 ▶️ 恢复即可"
        return detail

    def read_pause_note(self) -> dict[str, Any]:
        """The pause kept by pause(); one kept by an older version, which kept no reason, read from its records."""
        try:
            note = json.loads(self.ledger.get("pause_note") or "{}")
        except ValueError:
            note = {}
        return note if isinstance(note, dict) and note.get("why") else self.pause_from_records()

    def pause_from_records(self) -> dict[str, Any]:
        """When and why from the records a pause closed: it closes every active record as 暂停 at once, and the guard's
        pause follows a batch closed as 被别处成批撤掉 (the website's 取消所有, another program, the exchange)."""
        rows = list(self.ledger.rows.values())
        paused = [float(q.get("closing_at") or 0) for q in rows if q.get("reason") == "暂停"]
        if not paused:
            return {"at": 0.0, "why": "原因没有记下（升级前暂停的，暂停时没有挂着的单）", "detail": ""}
        at = max(paused)
        batch = [q for q in rows if "被别处成批撤掉" in str(q.get("reason") or "")
                 and at - PAUSE_LOOKBACK_S <= float(q.get("closing_at") or 0) <= at + 1]
        if batch:
            return {"at": at, "why": EXTERNAL_WHY, "detail": self.external_detail(batch)}
        return {"at": at, "why": "手动暂停（Telegram 或网页上的 ⏸ 暂停，或网页上的 🛑 紧急停止）", "detail": ""}

    @staticmethod
    def external_detail(records: list[dict]) -> str:
        """Which windows' orders went and the exchange's word for it: "SOL 11-24 03:10、XRP 11-24 03:10，共 2 场；
        交易所状态 CANCELED；V2 市场"."""
        windows = sorted({(int(q["start"]), str(q["coin"])) for q in records})
        statuses = sorted({m.group(1) for q in records if (m := re.search(r"成批撤掉（([^）]*)）", str(q.get("reason") or "")))})
        versions = sorted({str(q["version"]).upper() for q in records if q.get("version")})
        names = "、".join(mk.window_name(c, t) for t, c in windows[:4])
        return (names + (f" 等 {len(windows)} 场" if len(windows) > 4 else f"，共 {len(windows)} 场")
                + (f"；交易所状态 {'/'.join(statuses)}" if statuses else "") + (f"；{'/'.join(versions)} 市场" if versions else ""))

    def resume(self) -> None:
        self.paused = False
        self.ledger.put("paused", "0")
        self.pause_note = {}
        self.ledger.put("pause_note", "")
        for slug in list(self.timers):
            self.timers.pop(slug).cancel()
            self.enqueue(slug)
        self.sweep()

    async def withdraw_all(self, reason: str, deadline_s: float = 60.0) -> int:
        """Stop placing, close every active record and keep cancelling until the CLOB has answered for everything that
        may rest (withdraw_waits), or the deadline: a cancel that failed or was refused is asked again, after the wait
        the server asked for. Returns how many orders may still rest."""
        if self.blocked:
            return 0
        self.stopping = self.stopping or reason
        self.drop_unsent(reason)
        self.close_all(reason)
        begun = self.now()
        end = time.monotonic() + deadline_s
        while True:
            for q in self.open_quotes():
                for o in q["orders"].values():
                    o.pop("retry", None)
            try:
                await self.cancel_pass(sweep_all=True)
            except Exception as error:  # one failed pass does not end the withdrawal
                LOG.warning("撤单出错（%s），继续撤", error)
            left = time.monotonic() < end and self.withdraw_waits(begun)
            if not left:
                break
            pause = max(WITHDRAW_PAUSE_S, min(self.cancel_wait_until - self.now(), 10.0))
            await asyncio.sleep(max(0.0, min(pause, end - time.monotonic())))
        return sum(1 for q in self.open_quotes() for o in q["orders"].values() if o["state"] in MAY_REST)

    def withdraw_waits(self, begun: float) -> int:
        """The orders a withdrawal begun at `begun` still waits for: an open one (its cancel not answered); one whose
        placement is in flight (its answer says what rests); one of unknown state the CLOB has not answered for since
        `begun` (by its hash; with PREOPEN_KEY_EXCLUSIVE=on, without a usable hash, until its market is swept). An
        order past its expiration rests nowhere. 11-19 review: only open orders were waited for, so an unknown one
        whose first cancel failed or was refused was not asked again."""
        now = self.now()
        n = 0
        for q in self.open_quotes():
            live = now < float(q["expiration"]) + EXPIRED_MARGIN_S
            for o in q["orders"].values():
                if o["state"] == "open":
                    n += 1
                elif o["state"] in UNSURE and live:
                    if q["slug"] in self.placing:
                        n += 1
                    elif o.get("hash") and self.id_is_hash is not False:
                        n += float(o.get("hash_answered") or 0) < begun
                    elif self.s.key_exclusive and now < q["start"]:
                        n += not q.get("swept")
        return n

    async def wind_down(self, tasks: list[asyncio.Task]) -> int:
        """The exit, in order: nothing new is placed (hold() says stopping); the loops stop; a placement in flight gets
        its answer before the workers stop (cut off mid-send, its orders could rest with no answer read); then, with
        PREOPEN_CANCEL_ON_EXIT=on, everything is withdrawn. Returns how many orders may still rest."""
        self.stopping = self.stopping or "程序退出"
        for h in self.timers.values():
            h.cancel()
        workers = [t for t in tasks if t.get_name().startswith("worker")]
        loops = [t for t in tasks if t not in workers]
        for t in loops:
            t.cancel()
        await asyncio.gather(*loops, return_exceptions=True)
        await self.let_placements_finish(PLACING_DRAIN_S)
        for t in workers:
            t.cancel()
        await asyncio.gather(*workers, return_exceptions=True)
        for h in self.timers.values():
            h.cancel()
        resting = sum(1 for q in self.open_quotes() for o in q["orders"].values() if o["state"] in MAY_REST)
        if self.s.cancel_on_exit:
            resting = await self.withdraw_all("程序退出", deadline_s=30)
            LOG.info("退出撤单完成，可能还挂着 %d 张", resting)
        return resting

    async def let_placements_finish(self, seconds: float) -> None:
        """The exit: a placement in flight gets its answer (what rests, under which id) before the workers stop; cut off
        mid-send, its orders could rest with no answer read. Nothing new starts meanwhile (hold() says stopping)."""
        end = time.monotonic() + seconds
        while self.placing and time.monotonic() < end:
            await asyncio.sleep(0.05)
        if self.placing:
            LOG.warning("退出：%d 场的下单还没回应，不再等", len(self.placing))

    # ------------------------------------------------------------------ loops
    async def sleep(self, seconds: float) -> None:
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(self.stop.wait(), timeout=max(0.0, seconds))

    async def discovery_loop(self) -> None:
        """Each coin's next window by the direct path (fast around the expected listing), and the sweep."""
        while not self.stop.is_set():
            try:
                await self.scan_front()
            except PublicThrottled as t:
                self.problem(f"公共接口限流：{t}", key="public-429")
                await self.sleep(t.seconds)
            except Exception as error:
                self.problem(f"扫描出错：{error}", key="scan-error")
                await self.sleep(5)
            await self.sleep(self.poll_wait())

    async def scan_loop(self) -> None:
        """The full pass a minute, apart from the fast poll: a slow or failing pass never delays the next listing."""
        while not self.stop.is_set():
            try:
                await self.scan_full()
            except PublicThrottled as t:
                self.problem(f"公共接口限流：{t}", key="public-429")
                await self.sleep(t.seconds)
            except Exception as error:
                self.full_scan_at = self.now() - FULL_SCAN_S + 10  # tried again in 10 s
                self.problem(f"全量扫描出错：{error}", key="full-scan-error")
            await self.sleep(5)

    async def cancel_loop(self) -> None:
        """Alone in its task: a slow reconcile (its reads time out after 15 s) never holds a cancel back."""
        while not self.stop.is_set():
            try:
                await self.cancel_pass()
            except Exception as error:
                LOG.exception("撤单循环出错")
                self.problem(f"撤单循环出错：{error}", key="cancel-loop")
            await self.sleep(1.0)

    async def reconcile_loop(self) -> None:
        while not self.stop.is_set():
            try:
                await self.reconcile_pass()
                if self.fills_pending:
                    lines, self.fills_pending = self.fills_pending, []
                    self.notify("成交：\n" + "\n".join(lines[:30]) + (f"\n……共 {len(lines)} 笔" if len(lines) > 30 else ""))
            except Exception as error:
                LOG.exception("核对循环出错")
                self.problem(f"核对循环出错：{error}", key="reconcile-loop")
            await self.sleep(1.0)

    async def account_loop(self) -> None:
        while not self.stop.is_set():
            began = self.now()
            try:
                await self.account_once()
                wait = max(self.s.snapshot_s, 2 * self.snapshot_took) - (self.now() - began)
            except Exception as error:
                self.problem(f"读账户失败：{error}", key="account-error")
                wait = 10
            await self.sleep(wait)

    async def settle_loop(self) -> None:
        archive_at = 0.0
        self.clock_at = self.now()   # checked at the start (run)
        while not self.stop.is_set():
            try:
                if self.now() - self.clock_at > CLOCK_EVERY_S:
                    self.clock_at = self.now()
                    await self.sync_clock()
                await self.settle_pass()
                await self.redeem_pass()
                if self.now() - archive_at > 3600:
                    archive_at = self.now()
                    self.prune_memory()
                    n = self.ledger.archive_old(self.now())
                    if n:
                        self.version += 1  # records left the working set: what is worked out per version is again
                        for key in [k for k in self.open_keys if k not in self.ledger.rows]:
                            self.open_keys.discard(key)
                        self.rebuild_index()
                        LOG.info("归档 %d 条旧记录", n)
            except Exception as error:
                LOG.exception("结算循环出错")
                self.problem(f"结算循环出错：{error}", key="settle-loop")
            await self.sleep(10)

    def rebuild_index(self) -> None:
        self.by_slug.clear()
        self.by_id.clear()
        self.by_hash.clear()
        self.open_keys.clear()
        for q in self.ledger.rows.values():
            self._index(q)

    async def sync_clock(self) -> None:
        """This machine's clock against the CLOB's: at the start, every hour, and at once after an order expired before
        its cancel time (audit 11-27: checked at the start only). More than a second apart, the CLOB's time is used; a
        reading hours off is not believed."""
        try:
            server = await self.pub.server_time()
        except Exception:
            server = None
        if not server:
            return
        skew = server - self.clock()
        if abs(skew) > CLOCK_BELIEVED_S:
            self.problem(f"交易所报的时间和本机差 {skew:+.0f} 秒，不像真的，没有采用", key="clock-odd")
            return
        new = skew if abs(skew) > 1.0 else 0.0
        if abs(new - self.skew) > 1.0:
            self.problem(f"本机时钟和 Polymarket 差 {skew:+.1f} 秒，按交易所时间算", key="clock")
        self.skew = new

    def check_identity(self) -> str:
        """The ledger is bound to the account its orders belong to (the wallet and the signer, whose API key alone can
        cancel them). Another account's records are not taken over: their orders could be neither cancelled nor read,
        and their windows would be quoted twice. Returns why the bot is blocked ("" when it is not)."""
        current = self.x.identity()
        saved = self.ledger.get("identity")
        if not saved:
            self.ledger.put("identity", current)
            return ""
        if saved == current:
            return ""

        def short(ident: str) -> str:
            wallet, _, signer = ident.partition("|")
            return f"钱包 {wallet[:8]}…{wallet[-4:]}" + (f"／签名 {signer[:8]}…{signer[-4:]}" if signer else "") if wallet else ident
        return (f"账本属于另一个账户（{short(saved)}），现在用的是 {short(current)}：没有接管旧记录，也不挂新单。"
                "换回原来的私钥/钱包；或者先到 Polymarket 撤掉旧账户的挂单，再在网页上点「改绑当前账户」")

    def rebind(self) -> str:
        """The console's 改绑当前账户: the other account's open records are closed as they stand (their orders left to
        the other account: they expire on their own), the ledger is bound to this account, and the process restarts."""
        if not self.blocked:
            return "账本和当前账户一致，不用改绑"
        now = self.now()
        old = self.open_quotes()
        for q in old:
            for o in q["orders"].values():
                if o["state"] not in ENDED:
                    o.update(state="final", final_at=now, unverified=True, note=(o.get("note") or "") + "；旧账户的单，没有接管")
            if q["phase"] in ("active", "closing"):
                q.update(phase="closed", closed_at=now, reason=(q.get("reason") or "") + "（旧账户的记录）")
        n = len(old)
        # every record of the working set is the other account's, settled ones too: none counts in this account's
        # day, risk or claims (audit 11-27: the settled ones were not marked, and their loss held the new account)
        everything = list(self.ledger.rows.values())
        for q in everything:
            q["foreign"] = True
            if q.get("redeem") in ("pending", "retry", "failed", "manual"):
                q["redeem"] = "none"   # its winnings are the other account's to claim
        self.save(*everything)
        self.ledger.put("identity", self.x.identity())
        self.notify(f"🛠 账本已改绑当前账户：{n} 条旧账户的记录不再接管，程序正在重启")
        self.restart = True
        self.stop.set()
        return f"已改绑当前账户：{n} 条旧记录不再接管，程序正在重启"

    async def run(self) -> None:
        await self.x.start()
        self.started_at = self.now()
        await self.sync_clock()
        self.blocked = self.check_identity()
        if self.blocked:  # nothing is taken over, read, cancelled or placed: the console and Telegram say why
            self.problem(self.blocked, key="identity")
            self.notify(f"⛔ {self.blocked}")
            await self.stop.wait()
            return
        kept, changed = self.take_over()
        self.redeem_again()
        if kept or changed:
            LOG.info("接管 %d 场，%d 场设置不同将撤掉重报", kept, changed)
        try:
            await self.account_once()
        except Exception as error:
            self.problem(f"首次读账户失败：{error}")
        tasks = [asyncio.create_task(self.discovery_loop(), name="discovery"),
                 asyncio.create_task(self.scan_loop(), name="scan"),
                 asyncio.create_task(self.cancel_loop(), name="cancel"),
                 asyncio.create_task(self.reconcile_loop(), name="reconcile"),
                 asyncio.create_task(self.account_loop(), name="account"),
                 asyncio.create_task(self.settle_loop(), name="settle")]
        tasks += [asyncio.create_task(self.worker(), name=f"worker{i}") for i in range(self.s.workers)]
        self.notify(f"预挂机器人启动：{self.s.mode()}，{len(self.s.coins)} 个币种，接管 {kept} 场" + (f"，{changed} 场设置不同将重报" if changed else ""))
        try:
            await self.stop.wait()
        finally:
            resting = await self.wind_down(tasks)
            self.notify(f"预挂机器人已停止：{resting} 张挂单留在簿上（各自在开场前 {self.s.expiry_s} 秒到期，重启后接管）"
                        if resting and not self.s.cancel_on_exit else f"预挂机器人已停止" + (f"，还可能挂着 {resting} 张" if resting else ""))

    # ------------------------------------------------------------------ reports
    def coverage(self) -> dict[str, Any]:
        """The windows in range now, each by what is happening to it: what the console shows as 范围内 (why a window is
        not quoted is never left to guess)."""
        now = self.now()
        held = self.hold()
        counts: collections.Counter = collections.Counter()
        stopped: collections.Counter = collections.Counter()
        for t in mk.window_starts(now, self.s):
            for coin in self.s.coins:
                slug = mk.slug_of(coin, t)
                listing = self.listings.get(slug)
                if listing is None:
                    counts["unlisted"] += 1
                    continue
                qs = self.quotes_of(slug)
                if any(q["phase"] == "active" and any(o["state"] == "open" for o in q["orders"].values()) for q in qs):
                    counts["resting"] += 1
                elif any(q["phase"] == "active" for q in qs):  # being sent, or its answer lost: not on the book yet
                    counts["queued"] += 1
                elif any(q["phase"] == "closing" for q in qs):
                    counts["closing"] += 1
                elif not self.wanted(listing):
                    counts["stopped"] += 1
                    stopped[self.why_not(listing)] += 1
                elif slug in self.queued or slug in self.placing:
                    counts["queued"] += 1
                elif slug in self.timers:
                    counts["waiting"] += 1
                else:
                    counts["held" if held else "budget"] += 1
        names = (("resting", "挂着"), ("closing", "撤单中"), ("queued", "排队/正在挂"), ("waiting", "等开放/重试"),
                 ("budget", "资金不够"), ("held", "暂停中"), ("unlisted", "还没发布"), ("stopped", "不挂"))
        parts = [f"{label} {counts[k]}" for k, label in names if counts[k]]
        if stopped:
            parts[-1] += "（" + "，".join(f"{why} {n}" for why, n in stopped.most_common(4)) + "）"
        total = sum(counts.values())
        return {"total": total, "counts": dict(counts), "stopped": stopped.most_common(6),
                "text": f"{total} 场：" + "，".join(parts) if total else "范围内没有场次"}

    def why_not(self, listing: mk.Listing) -> str:
        """Why a listed window in range has no level to quote (wanted() said none)."""
        if listing.maker_fee:
            return "收挂单手续费"
        if listing.fee_unknown:
            return "没说挂单收不收手续费"
        reasons = []
        for i, lv in enumerate(self.s.levels):
            if i and self.s.deep_ahead_s and listing.start - self.now() > self.s.deep_ahead_s:
                break
            if problem := mk.level_problem(listing, lv.price, lv.shares):
                reasons.append(problem)
                continue
            cur = self.current(listing.slug, i)
            if cur is not None and any(dnum(o.get("filled")) > 0 for o in cur["orders"].values()):
                reasons.append("有成交")
            elif cur is not None and cur.get("manual"):
                reasons.append("手动撤了")
            elif cur is not None and cur.get("permanent"):
                reasons.append("被拒停挂")
            elif self.failures(listing.slug, i) >= self.s.max_attempts:
                reasons.append(f"失败 {self.s.max_attempts} 次")
            elif cur is not None:
                reasons.append("刚撤，稍后重挂")
        return reasons[0] if reasons else "其他"

    def revive(self) -> int:
        """Windows given up on for something that was not their own doing (a refusal the earlier rules took as final:
        a 400 for the whole batch, words not recognised; an order the exchange invalidated; orders cancelled elsewhere,
        e.g. the website's 取消所有, three times): quoted again while their entry time is ahead. A manual withdrawal, a
        window with a fill and a refusal about the order itself stay as they are."""
        now = self.now()
        revived = []
        for q in self.ledger.rows.values():
            if q["phase"] not in ("closed", "settled") or q.get("manual") or q.get("reason") == "网页手动撤单":
                continue
            if not (q.get("permanent") or q.get("counted")) or now > q["start"] - self.s.entry_s:
                continue
            orders = q["orders"].values()
            if any(dnum(o.get("filled")) > 0 for o in orders):
                continue
            codes = [refusal_code(o.get("error") or "", str(o.get("code") or "")) for o in orders
                     if o["state"] in ("rejected", "not_sent")]
            invalidated = any(str(o.get("status") or "").upper() == "INVALID" for o in orders)
            elsewhere = "挂单被撤（" in str(q.get("reason") or "")  # cancelled elsewhere (e.g. the website's 取消所有)
            if not (codes or invalidated or elsewhere) or any(c in WINDOW_CODES for c in codes):
                continue
            q["permanent"] = q["counted"] = False
            q["revived"] = now
            revived.append(q)
        if revived:
            self.save(*revived)
            self.problem(f"恢复了 {len({q['slug'] for q in revived})} 场之前因交易所拒单（不是这一场自己的问题）被停挂的场次，会重新挂",
                         key="revived")
        return len(revived)

    def twins(self) -> int:
        """Open orders on the account the bot did not place yet quotes like it (its prices, sizes and markets): another
        copy of the bot on the same account, doubling the orders and the rate."""
        prices = {lv.price for lv in self.s.levels}
        sizes = {lv.shares for lv in self.s.levels}
        tokens = {t for listing in self.listings.values() for t in (listing.up, listing.down)}
        return sum(1 for r in self.unmanaged if r.side == "BUY" and r.asset in tokens and r.price in prices and r.size in sizes)

    def counts(self) -> dict[str, Any]:
        """resting: orders the CLOB accepted and holds; sending: signed, the answer not back yet (not resting yet)."""
        return self.per_version("counts", self._counts)

    def _counts(self) -> dict[str, Any]:
        active = [q for q in self.open_quotes() if q["phase"] == "active"]
        states = [o["state"] for q in self.open_quotes() for o in q["orders"].values()]
        windows = {q["slug"] for q in active if any(o["state"] == "open" for o in q["orders"].values())}
        return {"active": len(active), "windows": len(windows), "resting": states.count("open"),
                "sending": states.count("placing") + states.count("prepared"), "unknown": states.count("unknown"),
                "closing": sum(1 for q in self.open_quotes() if q["phase"] == "closing")}

    def today(self) -> dict[str, Any]:
        """Today's (Beijing time) windows with a fill, by window (its levels and attempts together): how many, how many
        bought both sides, how many settled, and the settled P&L."""
        return self.per_version("today", self._today, self.day())

    def _today(self) -> dict[str, Any]:
        day = self.day()
        windows: dict[str, dict[str, Any]] = {}
        pnl = 0.0
        for q in self.ledger.rows.values():
            if q.get("foreign") or mk.dt.datetime.fromtimestamp(q["start"], mk.BEIJING).date() != day:
                continue
            f = {s: dnum(o.get("filled")) for s, o in q["orders"].items()}
            if not any(f.values()):
                continue  # withdrawn with nothing filled: not a trade
            w = windows.setdefault(q["slug"], {"sides": set(), "settled": True})
            w["sides"] |= {s for s, v in f.items() if v > 0}
            if q.get("phase") == "settled":
                pnl += float(q.get("pnl") or 0)
            else:
                w["settled"] = False
        return {"fills": len(windows), "both": sum(1 for w in windows.values() if len(w["sides"]) == len(mk.SIDES)),
                "pnl": pnl, "settled": sum(1 for w in windows.values() if w["settled"])}

    def review_reader(self) -> Callable[..., Iterator[dict]]:
        """For a review worked out in another thread. Here, on the event loop: the working records copied (a record and
        its orders: the loops change them in place). Returned: read(lo, hi, newest_first=False), the records of the
        windows starting from lo to hi in start order, the archived ones read from the database a batch at a time
        (11-21: a 60-day archive read whole took 0.7 GB and 3 s on the event loop; it is never held now)."""
        working = sorted(({**q, "orders": {side: dict(o) for side, o in (q.get("orders") or {}).items()}}
                          for q in self.ledger.rows.values()), key=lambda q: q["start"])
        keys = {q["key"] for q in working}
        starts = [q["start"] for q in working]
        ledger = self.ledger

        def read(lo: float = float("-inf"), hi: float = float("inf"), newest_first: bool = False) -> Iterator[dict]:
            part = working[bisect.bisect_left(starts, lo):bisect.bisect_left(starts, hi)]
            archived = (q for q in ledger.archived_between(lo, hi, newest_first) if q["key"] not in keys)
            return heapq.merge(archived, reversed(part) if newest_first else part, key=lambda q: q["start"], reverse=newest_first)
        return read

    def all_records(self) -> list[dict]:
        """The working records and the archived ones, all in memory (the tests; a review reads by review_reader)."""
        return list(self.ledger.rows.values()) + list(self.ledger.archived())

    def pending(self) -> list[dict[str, Any]]:
        """What wants a look (the console's 待处理): orders of unknown state, withdrawals running late."""
        now = self.now()
        out = []
        for q in self.open_quotes():
            for side, o in q["orders"].items():
                if o["state"] == "unknown":
                    out.append({"what": "状态不明的单", "window": mk.window_name(q["coin"], q["start"]), "side": mk.SIDES_CN[side],
                                "since": mk.span(now - float(o.get("sent") or now)), "note": o.get("note") or o.get("error") or ""})
            if q["phase"] == "closing" and now > q["start"] - self.s.cancel_s + 60 and now < q["start"]:
                out.append({"what": "撤单还没确认", "window": mk.window_name(q["coin"], q["start"]), "side": "",
                            "since": mk.span(now - float(q.get("closing_at") or now)), "note": q.get("reason") or ""})
        return out[:30]

    def report(self) -> dict[str, Any]:
        """Everything the web console shows, as plain JSON-able values (times also pre-formatted in Beijing time)."""
        now = self.now()
        c = self.counts()
        t = self.today()

        def median(values: collections.deque) -> float | None:
            ordered = sorted(values)
            return round(ordered[len(ordered) // 2], 2) if ordered else None

        def f(value: Any) -> float:
            return float(dnum(value))
        starts = mk.window_starts(now, self.s)
        front = self.frontier(starts)
        listed_until = max((l.start for l in self.listings.values()), default=0)
        coming = self.next_listing(front)
        rate = self.x.rate_status() if hasattr(self.x, "rate_status") else None
        active = self.per_version("soonest", lambda: heapq.nsmallest(
            40, (q for q in self.open_quotes() if q["phase"] in ("active", "closing")),
            key=lambda q: (q["start"], q["coin"], q["level"])))
        upcoming = [{
            "coin": mk.name_of(q["coin"]), "start": q["start"], "when": mk.hm(q["start"]), "in": mk.span(q["start"] - now),
            "level": int(q["level"]), "price": f(q["price"]) * 100, "shares": f(q["shares"]), "phase": q["phase"],
            "up": q["orders"]["up"]["state"], "down": q["orders"]["down"]["state"],
            "up_filled": f(q["orders"]["up"].get("filled")), "down_filled": f(q["orders"]["down"].get("filled")),
        } for q in active[:40]]
        filled = self.per_version("filled", lambda: heapq.nlargest(
            30, (q for q in self.ledger.rows.values() if any(dnum(o.get("filled")) > 0 for o in q["orders"].values())),
            key=lambda q: q["start"]))
        fills = [{
            "coin": mk.name_of(q["coin"]), "start": q["start"], "when": mk.hm(q["start"]), "level": int(q["level"]),
            "price": f(q["price"]) * 100, "up": f(q["orders"]["up"].get("filled")), "down": f(q["orders"]["down"].get("filled")),
            "phase": q["phase"], "result": mk.RESULT_CN.get(str(q.get("result") or ""), ""), "pnl": q.get("pnl"),
            "redeem": q.get("redeem") or "", "redeem_error": q.get("redeem_error") or "",
        } for q in filled[:30]]
        return {
            "now": now, "now_text": mk.hms(now), "mode": self.s.mode(), "live": bool(self.x.live), "paused": self.paused,
            "coins": [mk.name_of(x) for x in self.s.coins], "levels": self.s.levels_text(), "window_usd": float(self.s.window_usd),
            "settings": {"ahead_min": self.s.ahead_s // 60, "cancel_s": self.s.cancel_s, "expiry_s": self.s.expiry_s,
                         "entry_s": self.s.entry_s, "weekend": self.s.weekend, "max_open": float(self.s.max_open_usd)},
            "reserved": float(self.reserved()), "balance": float(self.balance) if self.balance is not None else None,
            "buffer": float(self.balance - self.s.window_usd) if self.balance is not None else None,
            "refused": self.refused_reason if now < self.refused_until else "",
            "coverage": self.coverage(),
            "rate": rate, "rate_note": refusals.rate_note(rate), "twins": self.twins(),
            "refusals": [{k: g[k] for k in ("code", "title", "count", "last_text", "words", "evidence", "action")}
                         for g in refusals.summary(self.refusal_log, now, 24)[:6]],
            "unmanaged": len(self.unmanaged), "unmanaged_usd": float(self.unmanaged_usd()),
            "windows": c["windows"], "resting": c["resting"], "unknown": c["unknown"], "closing": c["closing"],
            "listed_until": mk.hm(listed_until) if listed_until else "",
            "next_listing": {"start": mk.hm(coming[0]), "expected": mk.hms(coming[1])} if coming else None,
            "lags": {"open": median(self.open_lags), "place": median(self.place_lags), "cancel": median(self.cancel_lags),
                     "listing": median(self.listing_lags)},
            "today": t, "hold": self.hold(), "budget_note": self.budget_note, "blocked": self.blocked,
            "pause_detail": self.pause_detail() if self.paused else "",
            "exposure": float(self.exposure()), "day_pnl": float(self.day_pnl()), "risk": self.risk_hold(),
            "limits": {"exposure": float(self.s.max_exposure_usd), "daily_loss": float(self.s.max_daily_loss_usd)},
            "key_exclusive": self.s.key_exclusive,
            "balance_age": round(now - self.balance_at, 1) if self.balance_at else None,
            "cooldown": self.cooldown_reason if now < self.cooldown_until else "",
            "snapshot_age": round(now - self.snapshot_at, 1) if self.snapshot_at else None,
            "queue": self.queue.qsize(), "waiting": len(self.timers), "placing": len(self.placing),
            "id_is_hash": self.id_is_hash, "tier": getattr(self.x, "tier", "") or "", "lead_h": round(self.lead / 3600, 2),
            "upcoming": upcoming, "fills": fills, "pending": self.pending(),
            "unmanaged_rows": [{"id": r.id[:10] + "…", "price": float(r.price) * 100, "size": float(r.size), "matched": float(r.matched)}
                               for r in self.unmanaged[:10]],
            "errors": [{"when": mk.hms(ts), "text": text[:300]} for ts, text in list(self.errors)[-10:]][::-1],
        }

    def status_text(self) -> str:
        c = self.counts()
        t = self.today()
        now = self.now()
        starts = mk.window_starts(now, self.s)
        front = self.frontier(starts)
        listed_until = max((l.start for l in self.listings.values()), default=0)
        lines = [
            f"📌 Polymarket 5 分钟涨跌预挂 · {self.s.mode()}" + (f" · ⏸ 已暂停（{self.pause_text()}）" if self.paused else ""),
            f"币种：{' '.join(mk.name_of(x) for x in self.s.coins) or '无'}",
            f"每边：{self.s.levels_text()}（post-only GTD）；每场占用 ${self.s.window_usd:.2f}（各场分别核对余额，不共用）",
            f"时间：开场前 {self.s.ahead_s // 60} 分钟内的场次；开场前 {self.s.cancel_s} 秒撤单，{self.s.expiry_s} 秒到期，"
            f"{self.s.entry_s} 秒内不新挂" + ("" if self.s.weekend else "；周末不挂"),
            f"挂着：{c['windows']} 场 {c['resting']} 张单，合计 ${self.reserved():.2f}"
            + (f"（上限 ${self.s.max_open_usd}）" if self.s.max_open_usd else "")
            + (f"；余额 ${self.balance:.2f}" if self.balance is not None else "；余额未读到"),
        ]
        cov = self.coverage()
        if cov["total"]:
            lines.append(f"范围内：{cov['text']}")
        if self.unmanaged:
            lines.append(f"账户里另有 {len(self.unmanaged)} 张本程序没记录的挂单（${self.unmanaged_usd():.2f}）")
        worst = self.exposure()
        if worst > 0 or self.s.max_exposure_usd > 0 or self.s.max_daily_loss_usd > 0:
            lines.append(f"风控：未结算持仓最坏亏损 ${worst:.2f}" + (f"（上限 ${self.s.max_exposure_usd}）" if self.s.max_exposure_usd else "")
                         + f"；今天已结算 {self.day_pnl():+.2f}" + (f"（日亏损上限 ${self.s.max_daily_loss_usd}）" if self.s.max_daily_loss_usd else ""))
        if listed_until:
            lines.append(f"已发布到：{mk.hm(listed_until)}；" + self.coming_text(front))
        if self.open_lags:
            lags = sorted(self.open_lags)
            lines.append(f"开放接单→挂上：中位 {lags[len(lags) // 2]:.2f} 秒（最近 {len(lags)} 场，从看到开放算起；"
                         f"开放前每 {ARM_RETRY_S:g} 秒问一次；V2 市场约在创建后 10 秒开放，旧市场 70～90 秒）")
        elif self.place_lags:
            lags = sorted(self.place_lags)
            lines.append(f"创建→挂上：中位 {lags[len(lags) // 2]:.1f} 秒（最近 {len(lags)} 场）")
        if self.cancel_lags:
            lags = sorted(self.cancel_lags)
            lines.append(f"撤单确认：开场前中位 {lags[len(lags) // 2]:.1f} 秒")
        lines.append(f"今天（北京时间）：成交 {t['fills']} 场（双边 {t['both']}），已结算 {t['settled']} 场，盈亏 {t['pnl']:+.2f} 美元")
        if c["closing"] or c["unknown"]:
            lines.append(f"撤单/核对中 {c['closing']} 场" + (f"，状态未知 {c['unknown']} 张" if c["unknown"] else ""))
        if why := self.hold():
            lines.append(f"暂不新挂：{why}")
        elif self.hold_note and self.queue.qsize() + len(self.timers):
            lines.append(f"等候：{len(self.timers)} 场（{self.hold_note}）")
        if self.budget_note:
            lines.append(f"资金：{self.budget_note}")
        if self.errors:
            ts, text = self.errors[-1]
            lines.append(f"最近问题（{mk.hms(ts)}）：{text[:200]}")
        return "\n".join(lines)

    def check_text(self) -> str:
        """/check (🩺 体检): the verdict first (is every window in range quoted, and if not, why), then each part marked
        ✅ fine / ⚠️ look at it / ⛔ stopped, then the last ten minutes' problems."""
        now = self.now()
        c = self.counts()
        cov = self.coverage()
        n = cov["counts"]
        hold = self.hold()
        resting, total = n.get("resting", 0), cov["total"]
        short = n.get("stopped", 0) + n.get("budget", 0)
        pending = n.get("queued", 0) + n.get("waiting", 0)
        if self.blocked:
            verdict = f"⛔ 不工作：{self.blocked}"
        elif self.paused:
            verdict = f"⏸ 已暂停（{self.pause_text()}），不挂单（▶️ 恢复 或 /resume）" + (
                f"\n   暂停时：{self.pause_detail()}" if self.pause_note.get("detail") else "")
        elif hold:
            verdict = f"⛔ 暂不新挂：{hold}"
        elif not total:
            verdict = "ℹ️ 范围内没有要挂的场次" + ("（周末不挂）" if not self.s.weekend else "")
        elif short:
            verdict = f"⚠️ 范围内 {total} 场，有 {short} 场没挂（原因见「挂单」）"
        elif pending:
            verdict = f"⏳ 正在挂：范围内 {total} 场，挂上 {resting} 场，{pending} 场在排队或等开放"
        else:
            verdict = f"✅ 正常：{cov['text']}"
        twins = self.twins()
        lines = [f"🩺 体检 {mk.hms(now)} · {self.s.mode()}", verdict]
        if twins >= TWINS_ALERT:
            lines.append(f"⛔ 账户里有 {twins} 张和本程序一模一样、但不是本程序下的挂单：很可能还有另一个机器人在用这个账户"
                         "（Railway 上另一个服务、电脑上本地跑的）。只留一个")
        lines += ["", "【挂单】",
                 f"{'⚠️' if short else '✅'} 范围内：{cov['text']}"]
        timeouts = int(getattr(self.x, "_timeouts", 0) or 0)
        if now < self.refused_until:
            lines.append(f"⚠️ 交易所拒单（原话）：{self.refused_reason}；连续第 {self.refused_streak} 次，所有新挂暂停，"
                         f"{self.refused_until - now:.0f} 秒后再试，场次不作废")
        elif now < self.cooldown_until:
            lines.append(f"⚠️ 冷却中：{self.cooldown_reason[:120]}（还有 {self.cooldown_until - now:.0f} 秒）")
        if timeouts:
            lines.append(f"⚠️ 下单超时（order timed out）连续 {timeouts} 次")
        v2 = sum(1 for listing in self.listings.values() if listing.version == "v2")
        allowance = getattr(self.x, "v2_allowance", None)
        if self.v2_blocked():
            lines.append(f"⛔ V2 市场：没授权，不挂（已发现 {v2} 场 V2 市场）。{V2_HELP}")
        elif allowance is not None and allowance > 0:
            lines.append(f"✅ V2 市场：已授权（Polymarket Protocol V2）" + (f"，已发现 {v2} 场" if v2 else ""))
        elif v2:
            lines.append(f"ℹ️ V2 市场：已发现 {v2} 场，交易所还没报授权情况；被拒就会只停 V2 市场并提醒你授权")
        if self.accepted_at:
            lines.append(f"✅ 下单：最近一次被交易所接受是 {mk.span(now - self.accepted_at)}前")
        elif self.last_refusal:
            lines.append("⛔ 下单：这次启动后交易所一张都没接受" + (f"（已启动 {mk.span(now - self.started_at)}）" if self.started_at else ""))
        else:
            lines.append("ℹ️ 下单：这次启动后还没有新下的单（挂着的是接管的）")
        if self.last_refusal and now >= self.refused_until and (not self.accepted_at or self.last_refusal[0] > self.accepted_at):
            when, code, words = self.last_refusal
            help_ = REFUSAL_HELP.get(code, "")
            lines.append(f"⚠️ 最近一次被拒（{mk.span(now - when)}前）：{code} {words[:120]}" + (f"。{help_}" if help_ else ""))
        hour = refusals.summary(self.refusal_log, now, 1)
        if hour:
            lines.append(f"⚠️ 最近 1 小时拒单 {sum(g['count'] for g in hour)} 次："
                         + "，".join(f"{g['title']} × {g['count']}" for g in hour[:3]) + "（点 🧾 拒单 看原因和证据）")
        unknown = [now - float(o.get("unknown_at") or now) for q in self.open_quotes() for o in q["orders"].values()
                   if o["state"] == "unknown"]
        lines.append(f"⚠️ 状态不明的单：{len(unknown)} 张，最久 {mk.span(max(unknown))}（按订单哈希核对中，约 2 分钟内有结论）"
                     if unknown else "✅ 状态不明的单：0")
        if c["closing"]:
            lines.append(f"ℹ️ 撤单/核对中：{c['closing']} 场")
        lines.append("【资金】")
        if self.balance is None:
            lines.append("⛔ 余额：还没读到")
        elif now - self.balance_at > BALANCE_STALE_S:
            lines.append(f"⚠️ 余额：${self.balance:.2f}，但 {now - self.balance_at:.0f} 秒没读到新的（读余额一直失败）")
        elif self.balance < self.s.window_usd:
            lines.append(f"⚠️ 余额：${self.balance:.2f}，不够一场（每场 ${self.s.window_usd:.2f}）")
        else:
            lines.append(f"✅ 余额：${self.balance:.2f}（{now - self.balance_at:.0f} 秒前读到），每场 ${self.s.window_usd:.2f}，"
                         f"成交缓冲 ${self.balance - self.s.window_usd:.2f}")
        lines.append(f"ℹ️ 挂单合计：${self.reserved():.2f}" + (f"（上限 ${self.s.max_open_usd}）" if self.s.max_open_usd else "")
                     + (f"；账户里另有 {len(self.unmanaged)} 张别的挂单 ${self.unmanaged_usd():.2f}" if self.unmanaged else ""))
        claims = collections.Counter(str(q.get("redeem") or "") for q in self.ledger.rows.values())
        queued, by_hand = claims["pending"] + claims["retry"], claims["failed"] + claims["manual"]
        if not self.x.live:
            lines.append("ℹ️ 领取：模拟盘不领取")
        elif not self.s.auto_redeem:
            lines.append("ℹ️ 领取：自动领取关着" + (f"；{by_hand} 场要到网页上领" if by_hand else ""))
        elif self.x.redeem_problem():
            older = self.ledger.unclaimed_count()
            lines.append("⚠️ 领取：没配 Relayer Key，赢的要到网页上手动领" + (f"（现在有 {by_hand} 场）" if by_hand else "")
                         + (f"；更早的 {older} 场已归档，配好后重启会自动补领（网页上领过的会跳过）" if older else "")
                         + "。polymarket.com → Settings → API Keys → Relayer API Keys 建一个，填到 Railway 的 "
                         "POLY_RELAYER_API_KEY / POLY_RELAYER_API_KEY_ADDRESS")
        else:
            lines.append(("⚠️" if by_hand else "✅") + " 领取：会自动领取" + (f"；{queued} 场排着" if queued else "")
                         + (f"；{by_hand} 场领取失败，要到网页上领" if by_hand else ""))
        worst, risk = self.exposure(), self.risk_hold()
        lines.append(("⛔" if risk else "✅") + f" 风控：未结算持仓最坏亏损 ${worst:.2f}"
                     + (f"（上限 ${self.s.max_exposure_usd}）" if self.s.max_exposure_usd else "（未设上限）")
                     + f"；今天已结算 {self.day_pnl():+.2f}" + (f"；{risk}" if risk else ""))
        lines.append("【账户与接口】")
        if self.x.live:
            wallet = str(getattr(self.x, "wallet", "") or "")
            signer = str(getattr(getattr(self.x, "client", None), "signer", "") or "")
            lines.append(f"ℹ️ 账户：钱包 {wallet}（{getattr(self.x, 'wallet_type', '') or '?'}），签名地址 {signer}；API Key："
                         + ("Railway 里填的 POLY_API_KEY" if self.s.clob_key else "用私钥生成") + "。钱包要和网页右上角个人资料里的地址一样")
        lines.append(f"⛔ 账本账户：{self.blocked}" if self.blocked else
                     "✅ 账本账户：一致；撤单范围：" + ("整个 API Key" if self.s.key_exclusive else "只撤本程序签的单"))
        if not self.snapshot_at:
            lines.append("⛔ 账户快照：还没读到")
        else:
            age = now - self.snapshot_at
            lines.append(("⚠️" if age > self.stale_after() else "✅") + f" 账户快照：{age:.0f} 秒前（{len(self.snapshot)} 张挂单，"
                         f"读一次 {self.snapshot_took:.1f} 秒）")
        if self.write_error:
            lines.append(f"⛔ 账本：写不进去（{self.write_error}），暂停新挂；{len(self.unsaved)} 条记录等着写")
        size = self.ledger.size()
        if size is not None:  # audit 11-27: the ledger grows a few MB a day and nothing said when the disk ran low
            used, free = size
            low = free < LEDGER_FREE_WARN
            lines.append(("⚠️" if low else "✅") + f" 账本：{used / 2**20:.0f} MB，磁盘还剩 {free / 2**30:.1f} GB"
                         + ("：快满了，满了以后新单记不下来，会暂停新挂；在 Railway 给 /data 扩容" if low else ""))
        lines.append(f"ℹ️ 订单号=哈希：{'是' if self.id_is_hash else '否' if self.id_is_hash is False else '未知'}；"
                     f"限流档位：{getattr(self.x, 'tier', '') or '未知'}"
                     + ("；⚠️ 交易所发过限流警告" if getattr(self.x, "rate_warning", False) else ""))
        backend, ms = getattr(self.x, "signing", None) or ("", 0.0)
        if backend:  # live: which library signs the orders (the fast one is in the image; eth-keys falls back by itself)
            fast = "CoinCurve" in backend
            lines.append(("✅ 签名：coincurve" if fast else "⚠️ 签名：纯 Python（慢：coincurve 没装上或加载不了）")
                         + f"，每张单约 {ms:.1f} 毫秒")
        rs = self.x.rate_status() if hasattr(self.x, "rate_status") else None
        if rs:
            line = (f"{'⚠️' if rs['limited_hour'] else '✅'} 下单速度：现在 {rs['rate']:g} 单/秒（设置 {rs['configured']:g}"
                    + (f"，{rs['tier']} 档上限 {rs['tier_rate']}" if rs["tier_rate"] else "") + "）")
            if rs["configured"] > rs["cap"]:
                line += f"；设置超过档位上限，按 {rs['cap']:g} 跑"
            if rs["limited_hour"]:
                line += f"；最近 1 小时被限流（429）{rs['limited_hour']} 次：" + refusals.rate_note(rs)
            lines.append(line)
        listed_until = max((l.start for l in self.listings.values()), default=0)
        front = self.frontier(mk.window_starts(now, self.s))
        lines.append(("✅" if listed_until else "⚠️") + " 发现市场："
                     + (f"已发布到 {mk.hm(listed_until)}；" if listed_until else "还没找到；")
                     + self.coming_text(front) + f"（发布提前量 {self.lead_text()}）")
        public = getattr(self.pub, "health", None)
        health = public() if callable(public) else ""
        lines.append(("⚠️" if "失败" in health or "限流" in health else "✅") + f" 公共接口：{health or '—'}")
        if self.ghosts:
            lines.append(f"ℹ️ 按哈希待撤（判为没发出的）：{len(self.ghosts)}")
        if self.openings:
            lines.append("【最近开盘】看到开放后几秒发单→结果（挂上的括号里是挂上的秒数）")
            lines += self.openings_text()
        recent = [(ts, text) for ts, text in self.errors if now - ts < 600]
        lines.append("【最近 10 分钟的问题】" + ("" if recent else "无"))
        lines += [f"  {mk.hms(ts)} {text[:160]}" for ts, text in recent[-8:]]
        return "\n".join(lines)
