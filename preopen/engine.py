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
import collections
import contextlib
import logging
import time
from dataclasses import dataclass
from decimal import Decimal as D
from typing import Any, Callable

from . import markets as mk
from .config import GTD_THRESHOLD_S, Settings
from .exchange import WINDOW_CODES, Cancelled, Exchange, OrderSpec, Placed, Row, Throttled, refusal_code, retryable, unrecognised
from .gamma import PublicThrottled
from .ledger import ENDED, MAY_REST, NEVER_PLACED, Ledger, quote_key

LOG = logging.getLogger(__name__)

FULL_SCAN_S = 60          # a full pass over the range's unlisted windows this often (between them: each coin's next one)
SKIP_RETRY_S = 60         # a listed window that is no candidate is read again this often
FAST_BEFORE_S = 15        # poll fast from this long before a listing's expected moment...
FAST_AFTER_S = 180        # ...to this long after (a listing can come minutes late: 11-13 02:52)
HOLD_RETRY_S = 5          # a window held back (pause, cooldown, stale snapshot) is tried again this long on
BUDGET_RETRY_S = 30       # ...held back by the budget
ARM_EARLY_S = 1.0         # a fresh listing the CLOB does not accept yet is asked again this often...
ARM_RETRY_S = 0.2         # ...and this often from ARM_FAST_AFTER_S on: markets turn active ~75-90 s after their
ARM_FAST_AFTER_S = 55     #    creation (11-13: +71 to +92 s), and the crowd posts within a second of it
ARM_FRESH_S = 300         # ...while this young; an older one waits HOLD_RETRY_S
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
SWEEP_EVERY_S = 5         # a hash not answered for (an unknown order, a send judged refused) is cancelled again this often
BALANCE_EVERY_S = 30      # the balance is read this often...
BALANCE_STALE_S = 3 * BALANCE_EVERY_S + 15   # ...and no new order is placed on a balance older than this
SETTLE_AFTER_S = 60       # a window's result is looked for this long after its end
RESULT_RETRY_S = 30
REDEEM_RETRY_S = 600
FILL_LOG_TRIES = 3        # a window's trades read for the fills' exact times this many passes, then settled without them
REDEEM_TRIES = 6
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
EXTERNAL_MASS = 3         # the bot's orders cancelled elsewhere (plain CANCELED: the website's 取消所有, another program
EXTERNAL_WINDOW_S = 60    # with the key) in this many windows within this long: taken as "stop", and the bot pauses


def dnum(value: Any) -> D:
    return D(str(value or 0))


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
        self.lead = float(settings.publish_lead_s)
        self.leads: collections.deque = collections.deque(maxlen=200)
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
        self.budget_note = ""
        self.balance_hold_until = 0.0         # the CLOB said the balance was short: no new order until then
        self.refused_until = 0.0              # the CLOB refused orders for the account's sake, not the window's: no new
        self.refused_reason = ""              # order until then (doubling while it keeps refusing)
        self.refused_code = ""
        self.refused_streak = 0
        self.refused_at: dict[str, float] = {}  # slug -> when its orders were last refused that way
        self.accepted_at = 0.0                # when the CLOB last accepted an order (ends a refusal streak)
        self.last_refusal: tuple[float, str, str] | None = None   # (when, code, words) of the latest order refused
        self.recent_refusals: collections.deque = collections.deque(maxlen=500)  # (when, slug, code) of those refusals
        self.external: collections.deque = collections.deque(maxlen=200)         # (when, slug) of cancels made elsewhere
        self.restart = False                  # set by the console (a LIVE switch): main() starts the process again
        self._archive: tuple[int, list[dict]] = (-1, [])
        self.attempted: dict[str, tuple[str, int]] = {}  # condition id -> (slug, start) of every market an order was sent to
        self.strays: set[str] = set()         # order ids that came back after their record was judged: cancelled
        self.by_hash: dict[str, tuple[str, str]] = {}    # order hash -> (record key, side)
        self.ghosts: dict[str, int] = {}      # hash -> start of an order judged refused before processing: cancelled by
                                              # its hash all the same (a misread refusal never leaves an order unmanaged)
        self.ghost_at = 0.0
        self.trades: dict[str, tuple[float, dict[str, D]]] = {}   # condition id -> (read at, order id -> shares bought)
        self.blocked = ""                     # the ledger belongs to another account: nothing is taken over or placed
        self._risk: tuple[Any, str] = (None, "")
        self.risk_note = ""
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
        self.ledger.save_many(list(quotes))
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
        if self.paused:
            return "已暂停（/resume 恢复）"
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

    # ------------------------------------------------------------------ risk
    def exposure(self) -> D:
        """The worst case of the positions not settled yet: per window (its levels together), what its fills cost less
        the smaller side's shares, since a pair of opposite shares pays $1 whichever side wins. One-sided fills are the
        whole of it; a window filled evenly on both sides adds nothing."""
        windows: dict[str, list[D]] = {}
        for q in self.open_quotes():
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
        day = mk.dt.datetime.fromtimestamp(self.now(), mk.BEIJING).date()
        total = D(0)
        for q in self.ledger.rows.values():
            if q.get("phase") == "settled" and q.get("pnl") is not None and \
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
        """A risk limit reached: the resting quotes with nothing filled are withdrawn (each could only add a one-sided
        position); a window with a fill keeps its other side resting, which can only even it out. Returns how many."""
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
        for q in self.open_quotes():
            if q["phase"] == "active" and not any(dnum(o.get("filled")) > 0 for o in q["orders"].values()):
                self.close(q, "风控：" + why.split("：")[0], free=True)
                n += 1
        return n

    def stale_after(self) -> float:
        return 3 * max(self.s.snapshot_s, self.snapshot_took) + STALE_EXTRA_S

    def hold_balance(self, text: str) -> None:
        """The CLOB refused or cancelled an order for the balance: every placement waits a minute and the balance is
        read again (the local budget does not see orders placed elsewhere, e.g. on the website, nor the allowance)."""
        self.balance_hold_until = self.now() + BALANCE_HOLD_S
        self.balance_at = 0.0
        self.problem(text, key="balance")

    def refuse(self, code: str, message: str) -> None:
        """The CLOB refused orders for a reason that is not the window's own (an exchange rule, the account, words not
        recognised): every new placement waits, 30 s doubling to 10 min while it keeps refusing. No window is struck off
        for it: each is placed again once the refusals stop."""
        self.refused_streak += 1
        wait = min(REFUSE_HOLD_MAX_S, REFUSE_HOLD_S * 2 ** min(self.refused_streak - 1, 6))
        self.refused_until = max(self.refused_until, self.now() + wait)
        self.refused_reason = f"{code}：{message[:120]}" if message else code
        self.refused_code = code
        help_ = REFUSAL_HELP.get(code, "")
        self.problem(f"交易所拒单（{self.refused_reason}）" + (f"。{help_}" if help_ else "")
                     + f"。所有新挂暂停 {wait:.0f} 秒后再试（连续第 {self.refused_streak} 次），场次不作废", key=f"refused-{code}")

    def set_cooldown(self, reason: str, seconds: float) -> None:
        until = self.now() + max(1.0, min(float(seconds), 600.0))
        if until > self.cooldown_until:
            self.cooldown_until, self.cooldown_reason = until, reason
        self.problem(f"接口冷却 {seconds:.0f} 秒：{reason}", key="cooldown")

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

    def poll_wait(self) -> float:
        now = self.now()
        starts = mk.window_starts(now, self.s)
        for t in self.frontier(starts).values():
            expected = t - self.lead
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
            ordered = sorted(self.leads)
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
                self.leads.append(start - listing.created)
                if seen - listing.created < 120:
                    self.listing_lags.append(seen - listing.created)
            if listing.maker_fee:
                self.problem(f"{mk.window_name(coin, start)} 挂单方也收手续费（feeSchedule.takerOnly=false）：不挂", key="maker-fee")
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
                levels = self.fit(wanted, min(self.market_room(listing), cap))
                if not levels:
                    if wanted:
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
        if listing.maker_fee:  # makers would pay a fee on every fill: the pair's 2¢ margin gone; not quoted
            return []
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
            return Hold(why, None if self.paused else HOLD_RETRY_S)
        if not listing.accepting:
            try:
                accepting = await self.pub.accepting(listing.condition_id)
            except PublicThrottled as t:
                return Hold(f"公共接口限流（{t}）", t.seconds)
            except Exception as error:
                LOG.debug("accepting %s: %s", slug, error)
                accepting = None
            if not accepting:
                age = self.now() - (listing.created or listing.seen)
                retry = HOLD_RETRY_S if age >= ARM_FRESH_S else ARM_EARLY_S if age < ARM_FAST_AFTER_S else ARM_RETRY_S
                return Hold("市场还没开放下单", retry)
            listing.accepting = True
            listing.opened = self.now()
            levels = self.wanted(listing)  # judged again after the await
            if not levels:
                return None
            if why := self.hold():
                return Hold(why, None if self.paused else HOLD_RETRY_S)
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
                "listed": listing.created, "seen": listing.seen, "created": now, "phase": "active", "reason": "",
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
        for q in quotes:
            for side in mk.SIDES:
                r = by_ref.get(f"{q['key']}/{side}") or Placed("", "unknown", code="missing", message="没有应答")
                self.apply_placed(q, side, r, done)
                if r.status == "open":
                    self.accepted_at, self.refused_streak = done, 0
                elif r.status in ("rejected", "not_sent") and r.code:
                    self.last_refusal = (done, r.code, r.message)
                if r.wait > 0:
                    self.set_cooldown(f"{r.code}：{r.message[:120]}", r.wait)
                if r.code == "market_not_ready":
                    listing.accepting = False
                    hold = Hold("市场还没开放下单", ARM_RETRY_S)
                if r.code == "not_enough_balance":
                    hold = Hold("余额或授权不足（交易所拒单）", None)
                    self.hold_balance("交易所拒单：余额或授权不足（到 Polymarket 核对 pUSD 余额和交易授权）")
            states = {o["state"] for o in q["orders"].values()}
            codes = [o.get("code", "") for o in q["orders"].values() if o["state"] != "open"]
            account = [c for c in codes if c in ACCOUNT_REFUSALS or unrecognised(c)]
            if states <= {"not_sent", "rejected"} and all(retryable(c) for c in codes):
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
                for o in q["orders"].values():
                    if o["state"] == "not_sent" and o.get("hash"):
                        self.ghosts[o["hash"]] = int(q["start"])
                self.forget(q)
                if account:  # the account's refusal: everything waits (escalating), nothing is struck off
                    self.refused_at[slug] = done
                    self.recent_refusals.append((done, slug, account[0]))
                    self.refuse(account[0], next((o["error"] for o in q["orders"].values() if o.get("code") == account[0]), ""))
                    hold = hold or Hold(f"交易所拒单（{account[0]}）", None)
                hold = hold or Hold(next((o["error"] for o in q["orders"].values() if o["error"]), "没发出"), HOLD_RETRY_S)
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
                self.set_cooldown(t.reason, t.seconds)
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
        of them (cancelled: the refusal was misread and the order rested; not cancelled: it never did)."""
        for h in [h for h, start in self.ghosts.items() if now >= start]:
            del self.ghosts[h]
        if not self.ghosts or now < self.ghost_at:
            return
        self.ghost_at = now + SWEEP_EVERY_S
        ids = list(self.ghosts)
        try:
            r = await self.x.cancel(ids)
        except Throttled as t:
            self.set_cooldown(t.reason, t.seconds)
            return
        except Exception as error:
            LOG.warning("按哈希撤被拒的单失败：%s", error)
            return
        for h in ids:
            if h in r.canceled or h in r.not_canceled or r.throttle is None:
                self.ghosts.pop(h, None)
        if r.canceled:
            self.problem(f"有 {len(r.canceled)} 张判为没发出的单其实挂上了，已按哈希撤掉", key="ghost")

    async def cancel_orders(self, due: list[tuple[dict, str]]) -> None:
        """Cancel by id (an order of unknown state by its hash)."""
        ids = [q["orders"][side]["id"] or q["orders"][side]["hash"] for q, side in due]
        now = self.now()
        try:
            r = await self.x.cancel(ids)
        except Throttled as t:
            self.set_cooldown(t.reason, t.seconds)
            self.retry_later(due, now, t.seconds)
            return
        except Exception as error:
            self.problem(f"撤单失败（{len(ids)} 张）：{error}", key="cancel-error")
            self.retry_later(due, now)
            return
        if r.throttle:
            self.set_cooldown(*r.throttle)
        touched = self.mark_cancelled(r)
        missing = [(q, side) for (q, side), oid in zip(due, ids) if oid not in r.canceled and oid not in r.not_canceled]
        if missing:
            self.retry_later(missing, now, r.throttle[1] if r.throttle else 0.0)
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
            if o["state"] == "open":
                o.update(state="gone", gone=now, read_at=now + (1 if late and oid in r.not_canceled else FINAL_DELAY_S),
                         note=r.not_canceled.get(oid, "已撤"))
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
                             lookup_at=0, cancel_note=r.not_canceled.get(oid, ""))
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

    async def trades_of(self, condition_id: str) -> dict[str, D]:
        """The account's fills on a market (order id -> shares bought), read once for every order that asks soon."""
        at, fills = self.trades.get(condition_id, (0.0, {}))
        if self.now() - at < TRADES_TTL_S:
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
            fills = await self.trades_of(q["condition_id"])
        except Exception as error:
            if now >= float(q["expiration"]) + UNVERIFIED_AFTER_S:
                o.update(state="not_found", unverified=True, note=f"成交记录一直读不到（{error}）：按没有成交记下，请到 Polymarket 核对")
                self.problem(f"{mk.window_name(q['coin'], q['start'])} {mk.SIDES_CN[side]}方的成交读不到，请到 Polymarket 核对",
                             key="unverified")
            else:
                LOG.debug("读成交失败 %s：%s", q["condition_id"], error)
            self.save(q)
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
        except Exception as error:
            o["tries"] = int(o.get("tries") or 0) + 1
            o["retry"] = self.now() + min(RETRY_MAX_S, RETRY_S * 2 ** (o["tries"] - 1))
            self.save(q)
            LOG.debug("读单失败 %s：%s", o["id"], error)
            return
        if row is None:  # the CLOB has no record of it now: what it bought is in the trades
            try:
                fills = await self.trades_of(q["condition_id"])
            except Exception as error:
                o["tries"] = int(o.get("tries") or 0) + 1
                if o["tries"] < 6:
                    o["retry"] = self.now() + min(RETRY_MAX_S, RETRY_S * 2 ** (o["tries"] - 1))
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
        if q["phase"] == "active" and row.matched < row.size and self.now() < q["start"] - self.s.cancel_s:
            state = row.state
            if state == "CANCELED_NOT_ENOUGH_BALANCE":  # fills elsewhere took the balance below this market's orders
                self.close(q, f"{mk.SIDES_CN[side]}方挂单因余额不足被交易所撤掉", free=True, save=False)
                try:  # the balance as it is now: each window then waits for its own market's room, nothing else waits
                    self.balance, self.balance_at = await self.x.balance(), self.now()
                except Exception:
                    self.balance_at = 0.0  # unreadable: nothing is placed until it is read again
                self.problem("余额降到了一场的挂单合计以下，交易所撤掉了这些场次的挂单；余额回来后（结算、领取）自动重挂",
                             key="balance-cancel")
            elif state.startswith("CANCELED_") or state == "INVALID":  # the exchange's own doing (the market resolved or
                # vanished, the order timed out, expired or was invalidated): not the window's failure
                self.close(q, f"{mk.SIDES_CN[side]}方挂单被交易所撤掉（{row.status}）", free=True, save=False)
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
                        self.pause()
                        self.problem("本程序的挂单在别处被成批撤掉（比如网页上的「取消所有」）：已暂停新挂并撤掉其余挂单。"
                                     "要继续请在控制台点「恢复挂单」或发 /resume", key="external-cancel")
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
        if self.now() - self.balance_at >= BALANCE_EVERY_S or self.balance is None:
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
                    results[slug] = mk.winner(m) if m else None
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
            auto = self.s.auto_redeem and self.x.live
            why = self.x.redeem_problem() if auto and filled[win] > 0 else ""
            q.update(phase="settled", result=win, pnl=float(filled[win] - cost), settled_at=now,
                     redeem=("pending" if auto and not why else "manual") if filled[win] > 0 else "none")
            if why:  # nothing tried six times over an hour: said once, claimed after a restart that brings the key
                q["redeem_error"] = why
                self.problem(f"{mk.window_name(q['coin'], q['start'])} 赢了 {mk.fmt(filled[win])} 份，{why}", key="redeem-key")
            self.save(q)
            self.notify(f"结算 {mk.window_name(q['coin'], q['start'])}：结果{mk.SIDES_CN[win]}，"
                        f"成交 涨 {mk.fmt(filled['up'])} / 跌 {mk.fmt(filled['down'])}，盈亏 {q['pnl']:+.2f} 美元")

    def redeem_again(self) -> int:
        """At start: a claim given up on (six failures), or left for the website for want of a Relayer key, is tried
        again when this process can claim (a restart is when a new key comes in)."""
        if not (self.s.auto_redeem and self.x.live) or self.x.redeem_problem():
            return 0
        again = [q for q in self.ledger.rows.values()
                 if q.get("redeem") == "failed" or (q.get("redeem") == "manual" and q.get("redeem_error"))]
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
            if q.get("redeem") not in ("pending", "retry") or now < float(q.get("redeem_retry") or 0):
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
                why = self.matches_settings(q)
                if why:
                    self.close(q, why, free=True, save=False)
                    changed += 1
                else:
                    kept += 1
            self.save(q)
        return kept, changed

    def apply_settings(self, new: Settings) -> int:
        """New settings in force at once (the web console): a resting record that no longer fits them is withdrawn and
        quoted again under the new ones; one that fits keeps its place in the queue. Returns the withdrawn count."""
        self.s = new
        if hasattr(self.x, "settings"):
            self.x.settings = new
        for name, rate, burst in (("orders", new.order_rate, new.order_burst), ("cancels", new.cancel_rate, new.cancel_burst)):
            bucket = getattr(self.x, name, None)
            if bucket is not None and hasattr(bucket, "rate"):
                bucket.rate, bucket.burst = rate, burst
        if not self.x.live and hasattr(self.x, "cash"):
            self.x.cash = new.paper_balance
            self.balance = new.paper_balance
        changed = 0
        for q in self.open_quotes():
            if q["phase"] == "active" and (why := self.matches_settings(q)):
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
        n = self.pause()
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
    def pause(self) -> int:
        self.paused = True
        self.ledger.put("paused", "1")
        return self.close_all("暂停")

    def resume(self) -> None:
        self.paused = False
        self.ledger.put("paused", "0")
        for slug in list(self.timers):
            self.timers.pop(slug).cancel()
            self.enqueue(slug)
        self.sweep()

    async def withdraw_all(self, reason: str, deadline_s: float = 60.0) -> int:
        """Close every active record and keep cancelling until nothing may rest (or the deadline)."""
        if self.blocked:
            return 0
        self.close_all(reason)
        end = time.monotonic() + deadline_s
        while time.monotonic() < end:
            for q in self.open_quotes():
                for o in q["orders"].values():
                    o.pop("retry", None)
            await self.cancel_pass(sweep_all=True)
            left = sum(1 for q in self.open_quotes() for o in q["orders"].values() if o["state"] in ("open",))
            if not left:
                break
            await asyncio.sleep(1)
        return sum(1 for q in self.open_quotes() for o in q["orders"].values() if o["state"] in MAY_REST)

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
        while not self.stop.is_set():
            try:
                await self.settle_pass()
                await self.redeem_pass()
                if self.now() - archive_at > 3600:
                    archive_at = self.now()
                    n = self.ledger.archive_old(self.now())
                    if n:
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
        try:
            server = await self.pub.server_time()
        except Exception:
            server = None
        if server:
            skew = server - self.clock()
            if abs(skew) > 1.0:
                self.skew = skew
                self.problem(f"本机时钟和 Polymarket 差 {skew:+.1f} 秒，按交易所时间算", key="clock")

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
            q["foreign"] = True
        self.save(*old)
        n = len(old)
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
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            for h in self.timers.values():
                h.cancel()
            resting = sum(1 for q in self.open_quotes() for o in q["orders"].values() if o["state"] in MAY_REST)
            if self.s.cancel_on_exit:
                resting = await self.withdraw_all("程序退出", deadline_s=30)
                LOG.info("退出撤单完成，可能还挂着 %d 张", resting)
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

    def counts(self) -> dict[str, Any]:
        """resting: orders the CLOB accepted and holds; sending: signed, the answer not back yet (not resting yet)."""
        active = [q for q in self.open_quotes() if q["phase"] == "active"]
        states = [o["state"] for q in self.open_quotes() for o in q["orders"].values()]
        windows = {q["slug"] for q in active if any(o["state"] == "open" for o in q["orders"].values())}
        return {"active": len(active), "windows": len(windows), "resting": states.count("open"),
                "sending": states.count("placing") + states.count("prepared"), "unknown": states.count("unknown"),
                "closing": sum(1 for q in self.open_quotes() if q["phase"] == "closing")}

    def today(self) -> dict[str, Any]:
        day = mk.dt.datetime.fromtimestamp(self.now(), mk.BEIJING).date()
        fills = pnl = 0
        both = 0
        settled = 0
        for q in self.ledger.rows.values():
            if mk.dt.datetime.fromtimestamp(q["start"], mk.BEIJING).date() != day:
                continue
            f = {s: dnum(o.get("filled")) for s, o in q["orders"].items()}
            if not any(f.values()):
                continue  # withdrawn with nothing filled: not a trade
            fills += 1
            both += 1 if all(f.values()) else 0
            if q.get("phase") == "settled":
                settled += 1
                pnl += float(q.get("pnl") or 0)
        return {"fills": fills, "both": both, "pnl": pnl, "settled": settled}

    def all_records(self) -> list[dict]:
        """The working records and the archived ones (the review reads both; the archive is read again only when it grew)."""
        count = self.ledger.archive_count()
        if self._archive[0] != count:
            self._archive = (count, list(self.ledger.archived()))
        return list(self.ledger.rows.values()) + self._archive[1]

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
        nxt = min(front.values()) if front else 0
        active = sorted((q for q in self.open_quotes() if q["phase"] in ("active", "closing")),
                        key=lambda q: (q["start"], q["coin"], q["level"]))
        upcoming = [{
            "coin": mk.name_of(q["coin"]), "start": q["start"], "when": mk.hm(q["start"]), "in": mk.span(q["start"] - now),
            "level": int(q["level"]), "price": f(q["price"]) * 100, "shares": f(q["shares"]), "phase": q["phase"],
            "up": q["orders"]["up"]["state"], "down": q["orders"]["down"]["state"],
            "up_filled": f(q["orders"]["up"].get("filled")), "down_filled": f(q["orders"]["down"].get("filled")),
        } for q in active[:40]]
        filled = sorted((q for q in self.ledger.rows.values() if any(f(o.get("filled")) > 0 for o in q["orders"].values())),
                        key=lambda q: -q["start"])
        fills = [{
            "coin": mk.name_of(q["coin"]), "start": q["start"], "when": mk.hm(q["start"]), "level": int(q["level"]),
            "price": f(q["price"]) * 100, "up": f(q["orders"]["up"].get("filled")), "down": f(q["orders"]["down"].get("filled")),
            "phase": q["phase"], "result": mk.SIDES_CN.get(str(q.get("result") or ""), ""), "pnl": q.get("pnl"),
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
            "unmanaged": len(self.unmanaged), "unmanaged_usd": float(self.unmanaged_usd()),
            "windows": c["windows"], "resting": c["resting"], "unknown": c["unknown"], "closing": c["closing"],
            "listed_until": mk.hm(listed_until) if listed_until else "",
            "next_listing": {"start": mk.hm(nxt), "expected": mk.hms(nxt - self.lead)} if nxt else None,
            "lags": {"open": median(self.open_lags), "place": median(self.place_lags), "cancel": median(self.cancel_lags),
                     "listing": median(self.listing_lags)},
            "today": t, "hold": self.hold(), "budget_note": self.budget_note, "blocked": self.blocked,
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
            f"📌 Polymarket 5 分钟涨跌预挂 · {self.s.mode()}" + (" · ⏸ 已暂停" if self.paused else ""),
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
            lines.append(f"已发布到：{mk.hm(listed_until)}；" + (
                f"下一场 {mk.hm(min(front.values()))} 预计 {mk.hms(min(front.values()) - self.lead)} 发布" if front else "范围内都已发布"))
        if self.open_lags:
            lags = sorted(self.open_lags)
            lines.append(f"开放接单→挂上：中位 {lags[len(lags) // 2]:.2f} 秒（最近 {len(lags)} 场；市场约在创建后 75～90 秒开放）")
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
            verdict = "⏸ 已暂停，不挂单（▶️ 恢复 或 /resume）"
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
        lines = [f"🩺 体检 {mk.hms(now)} · {self.s.mode()}", verdict, "", "【挂单】",
                 f"{'⚠️' if short else '✅'} 范围内：{cov['text']}"]
        timeouts = int(getattr(self.x, "_timeouts", 0) or 0)
        if now < self.refused_until:
            lines.append(f"⚠️ 交易所拒单（原话）：{self.refused_reason}；连续第 {self.refused_streak} 次，所有新挂暂停，"
                         f"{self.refused_until - now:.0f} 秒后再试，场次不作废")
        elif now < self.cooldown_until:
            lines.append(f"⚠️ 冷却中：{self.cooldown_reason[:120]}（还有 {self.cooldown_until - now:.0f} 秒）")
        if timeouts:
            lines.append(f"⚠️ 下单超时（order timed out）连续 {timeouts} 次")
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
            lines.append("⚠️ 领取：没配 Relayer Key，赢的要到网页上手动领" + (f"（现在有 {by_hand} 场）" if by_hand else "")
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
        lines.append(f"ℹ️ 订单号=哈希：{'是' if self.id_is_hash else '否' if self.id_is_hash is False else '未知'}；"
                     f"限流档位：{getattr(self.x, 'tier', '') or '未知'}"
                     + ("；⚠️ 交易所发过限流警告" if getattr(self.x, "rate_warning", False) else ""))
        listed_until = max((l.start for l in self.listings.values()), default=0)
        front = self.frontier(mk.window_starts(now, self.s))
        lines.append(("✅" if listed_until else "⚠️") + " 发现市场："
                     + (f"已发布到 {mk.hm(listed_until)}；" if listed_until else "还没找到；")
                     + (f"下一场 {mk.hm(min(front.values()))} 预计 {mk.hms(min(front.values()) - self.lead)} 发布" if front else "范围内都已发布")
                     + f"（发布提前量 {self.lead / 3600:.2f} 小时）")
        public = getattr(self.pub, "health", None)
        health = public() if callable(public) else ""
        lines.append(("⚠️" if "失败" in health or "限流" in health else "✅") + f" 公共接口：{health or '—'}")
        if self.ghosts:
            lines.append(f"ℹ️ 按哈希待撤（判为没发出的）：{len(self.ghosts)}")
        recent = [(ts, text) for ts, text in self.errors if now - ts < 600]
        lines.append("【最近 10 分钟的问题】" + ("" if recent else "无"))
        lines += [f"  {mk.hms(ts)} {text[:160]}" for ts, text in recent[-8:]]
        return "\n".join(lines)
