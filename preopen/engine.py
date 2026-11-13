"""The controller: find each window the moment it is listed, quote it post-only on both sides, withdraw before the start.

Loops (all in one asyncio process):
    discovery   every coin's next listing (fast around the expected moment), plus a full pass over the range a minute
    workers     place the queued windows: the freshest listings first, then the soonest starts
    cancel      every second: the windows at their cancel time (and any closing record) withdrawn, sprinting to the expiry
    account     the open orders and the balance: fills, orders gone from the book, the budget
    settle      the fills of a closed window valued by the official result; the winning side redeemed

A window's level is a record (a "quote", ledger.py) with an order per side. The orders are BUY, post-only, GTD:
they expire on their own `expiry_s` before the start (the backstop of the cancel), so a stopped process leaves nothing
on the book past the start; the next process takes the resting orders over.
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
from .exchange import Cancelled, Exchange, OrderSpec, Placed, RETRYABLE_CODES, Row, Throttled
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
GIVE_UP_S = 120           # an unknown placement not found by its hash this long after it was sent never rested
SEEN_MARGIN_S = 3         # an order accepted this long before a snapshot began should be in it
SWEEP_EVERY_S = 5         # a market whose orders are not all known is swept (cancel-market-orders) this often
SETTLE_AFTER_S = 60       # a window's result is looked for this long after its end
RESULT_RETRY_S = 30
REDEEM_RETRY_S = 600
REDEEM_TRIES = 6
STALE_EXTRA_S = 15
ERRORS_KEPT = 30
PERMANENT_CODES = {"invalid", "http_400", "invalid_expiration"}  # refusals that would recur: the level is not quoted again


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
        self.stop = asyncio.Event()
        self.started_at = 0.0
        self.notified: dict[str, float] = {}
        self.redeem_at = 0.0
        self.counters = collections.Counter()
        self.version = 0                      # bumped on every record change: reserved() is cached by it
        self._reserved: tuple[int, D] = (-1, D(0))
        self.budget_note = ""
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
        if self.paused:
            return "已暂停（/resume 恢复）"
        if self.now() < self.cooldown_until:
            return f"接口冷却中（{self.cooldown_reason}，还有 {self.cooldown_until - self.now():.0f} 秒）"
        if not self.snapshot_at:
            return "账户快照还没读到"
        if self.now() - self.snapshot_at > self.stale_after():
            return f"账户快照过期（{self.now() - self.snapshot_at:.0f} 秒前）"
        return ""

    def stale_after(self) -> float:
        return 3 * max(self.s.snapshot_s, self.snapshot_took) + STALE_EXTRA_S

    def set_cooldown(self, reason: str, seconds: float) -> None:
        until = self.now() + max(1.0, min(float(seconds), 600.0))
        if until > self.cooldown_until:
            self.cooldown_until, self.cooldown_reason = until, reason
        self.problem(f"接口冷却 {seconds:.0f} 秒：{reason}", key="cooldown")

    def reserved(self) -> D:
        """The bot's own resting money: every order that may rest, at its price, for what is not filled. Polymarket
        holds an open BUY order's price x size against the pUSD balance, so this is what the balance must cover."""
        if self._reserved[0] == self.version:
            return self._reserved[1]
        total = D(0)
        for q in self.open_quotes():
            for o in q["orders"].values():
                if o["state"] in MAY_REST:
                    total += dnum(q["price"]) * max(D(0), dnum(q["shares"]) - dnum(o.get("filled")))
        self._reserved = (self.version, total)
        return total

    def unmanaged_usd(self) -> D:
        return sum((r.price * max(D(0), r.size - r.matched) for r in self.unmanaged if r.side == "BUY"), D(0))

    def room(self) -> D:
        """What more the bot may reserve now: the cap's room and the balance's, whichever is less."""
        mine = self.reserved()
        room = D("Infinity")
        if self.s.max_open_usd > 0:
            room = self.s.max_open_usd - mine
        if self.balance is None:
            return D(0)
        return min(room, self.balance - mine - self.unmanaged_usd())

    def budget_problem(self, need: D) -> str:
        mine = self.reserved()
        if self.s.max_open_usd > 0 and mine + need > self.s.max_open_usd:
            return f"预留已到上限 PREOPEN_MAX_OPEN_USD={self.s.max_open_usd}（已挂 ${mine:.2f}，这一场要 ${need:.2f}）"
        if self.balance is None:
            return "还没读到余额"
        free = self.balance - mine - self.unmanaged_usd()
        if free < need:
            return f"可用余额不够：余额 ${self.balance:.2f}，已挂 ${mine:.2f}，这一场要 ${need:.2f}"
        return ""

    def need_of(self, levels: list[int]) -> D:
        return sum((2 * self.s.levels[i].usd for i in levels), D(0))

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
        """A full pass over the range's unlisted windows a minute (the batched GET /markets?slug= query, fine for
        windows listed long ago), and every poll each coin's next window by the direct path GET /markets/slug/<slug>,
        which answers within a second of the listing (the batched query lags ~88 s, and the crowd with it)."""
        now = self.now()
        starts = mk.window_starts(now, self.s)
        asked = found = 0
        if now - self.full_scan_at >= FULL_SCAN_S:
            self.full_scan_at = now
            self.prune(starts)
            ask = [mk.slug_of(c, t) for c in self.s.coins for t in starts if self.unlisted(mk.slug_of(c, t), now)]
            if ask:
                got = await self.pub.listings(ask)
                found += self.take(ask, got)
                asked += len(ask)
        front = [mk.slug_of(c, t) for c, t in self.frontier(starts).items()]
        if front:
            got = await self.pub.markets(front)
            found += self.take(front, got)
            asked += len(front)
        if asked:
            self.scan_note = f"{mk.hms(now)} 问了 {asked} 场，新发现 {found} 场"
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
            self.listings[slug] = listing
            if listing.created:
                self.leads.append(start - listing.created)
                if seen - listing.created < 120:
                    self.listing_lags.append(seen - listing.created)
            if listing.maker_fee:
                self.skipped[slug] = (seen, "挂单方也收手续费（feeSchedule.takerOnly=false）")
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
        """Every listed window in range with a level to quote and not queued is queued, the soonest first, as far as
        the budget's room goes (a hold that cleared, a level withdrawn, a deeper level now near, money freed)."""
        now = self.now()
        room = self.room() if not self.hold() else D(0)
        queued_need = sum((self.need_of(self.wanted(self.listings[s])) for s in self.queued | self.placing if s in self.listings), D(0))
        room -= queued_need
        short = 0
        for t in mk.window_starts(now, self.s):
            for coin in self.s.coins:
                slug = mk.slug_of(coin, t)
                if slug not in self.listings or slug in self.queued or slug in self.placing or slug in self.timers:
                    continue
                levels = self.wanted(self.listings[slug])
                if not levels:
                    continue
                need = self.need_of(levels)
                if need > room:
                    short += 1
                    continue
                room -= need
                self.enqueue(slug)
        self.budget_note = f"{short} 场因资金/暂停没挂" if short else ""

    def priority(self, slug: str) -> tuple[int, int]:
        listing = self.listings[slug]
        fresh = listing.created is not None and self.now() - listing.created < ARM_FRESH_S
        return (0 if fresh else 1, listing.start)

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
            _, _, slug = await self.queue.get()
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
        need = self.need_of(levels)
        if why := self.budget_problem(need):
            return Hold(why, None)
        if not listing.accepting:
            try:
                accepting = await self.pub.accepting(listing.condition_id)
            except Exception as error:
                LOG.debug("accepting %s: %s", slug, error)
                accepting = None
            if not accepting:
                age = self.now() - (listing.created or listing.seen)
                retry = HOLD_RETRY_S if age >= ARM_FRESH_S else ARM_EARLY_S if age < ARM_FAST_AFTER_S else ARM_RETRY_S
                return Hold("市场还没开放下单", retry)
            listing.accepting = True
            listing.opened = self.now()
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
        self.save(*quotes)  # durable before anything is sent
        self.x.prime(listing)
        try:
            results = await self.x.place(specs)
        except Exception as error:  # the adapter raised: whether anything was sent is not known
            LOG.exception("下单请求异常 %s", slug)
            results = [Placed(spec.ref, "unknown", code="exception", message=str(error)) for spec in specs]
        by_ref = {r.ref: r for r in results}
        done = self.now()
        hold: Hold | None = None
        for q in quotes:
            for side in mk.SIDES:
                r = by_ref.get(f"{q['key']}/{side}") or Placed("", "unknown", code="missing", message="没有应答")
                self.apply_placed(q, side, r, done)
                if r.wait > 0:
                    self.set_cooldown(f"{r.code}：{r.message[:120]}", r.wait)
                if r.code == "market_not_ready":
                    listing.accepting = False
                    hold = Hold("市场还没开放下单", ARM_RETRY_S)
                if r.code == "not_enough_balance":
                    hold = Hold("余额或授权不足（交易所拒单）", None)
                    self.balance_at = 0.0  # read the balance again before the next placement
                    self.problem("交易所拒单：余额或授权不足（到 Polymarket 核对 pUSD 余额和交易授权）", key="balance")
            states = {o["state"] for o in q["orders"].values()}
            if states <= {"not_sent"}:
                self.forget(q)  # nothing was placed: no record, the window is placed afresh later
                hold = hold or Hold(next((o["error"] for o in q["orders"].values() if o["error"]), "没发出"), HOLD_RETRY_S)
                continue
            if states == {"open"}:
                q["placed"] = done
                if q.get("listed") and done - float(q["listed"]) < ARM_FRESH_S:  # a fresh listing (not a backlog's)
                    self.place_lags.append(done - float(q["listed"]))
                if listing.opened and done - listing.opened < 60:
                    q["opened"] = listing.opened
                    self.open_lags.append(done - listing.opened)
            else:
                codes = [o.get("code", "") for o in q["orders"].values() if o["state"] != "open"]
                self.close(q, "单边没挂上：" + "；".join(f"{mk.SIDES_CN[s]} {o['state']} {o.get('code', '')} {o['error'][:80]}".strip()
                                                    for s, o in q["orders"].items() if o["state"] != "open"),
                           free=all(c in RETRYABLE_CODES for c in codes), save=False)
                if any(c in PERMANENT_CODES for c in codes):
                    q["permanent"] = True
            self.save(q)
            for side, o in q["orders"].items():
                if o["state"] in ("rejected", "unknown") and o.get("code") not in RETRYABLE_CODES:
                    self.problem(f"{mk.window_name(q['coin'], q['start'])} {mk.SIDES_CN[side]} {o['state']}：{o.get('code')} {o['error'][:120]}",
                                 key=f"reject-{o.get('code')}")
        return hold

    def apply_placed(self, q: dict, side: str, r: Placed, at: float) -> None:
        o = q["orders"][side]
        o.update(hash=r.hash, salt=r.salt, code=r.code, error=r.message[:300], sent=at)
        if r.status == "open":
            o.update(state="open", id=r.order_id, filled=str(r.matched))
            self.by_id[r.order_id] = (q["key"], side)
            if r.hash and self.x.live:
                same = r.order_id.lower() == r.hash.lower()
                if self.id_is_hash is None or self.id_is_hash != same:
                    self.id_is_hash = same
                    self.ledger.put("id_is_hash", "1" if same else "0")
                    LOG.info("订单号%s订单哈希", "等于" if same else "不等于")
        elif r.status in ("rejected", "not_sent", "unknown"):
            o["state"] = r.status
        else:
            o["state"] = "unknown"

    # ------------------------------------------------------------------ withdrawal
    def close(self, q: dict, reason: str, free: bool, save: bool = True) -> None:
        if q["phase"] not in ("active",):
            return
        q["phase"] = "closing"
        q["reason"] = reason
        q["closing_at"] = self.now()
        q["counted"] = not free
        if save:
            self.save(q)

    def close_all(self, reason: str) -> int:
        n = 0
        for q in self.open_quotes():
            if q["phase"] == "active":
                self.close(q, reason, free=True)
                n += 1
        return n

    async def cancel_pass(self) -> None:
        now = self.now()
        due: list[tuple[dict, str]] = []
        sweep: dict[str, dict] = {}
        for q in self.open_quotes():
            at_cancel = now >= q["start"] - self.s.cancel_s
            if q["phase"] == "active":
                if at_cancel:
                    self.close(q, "到点撤单", free=True)
                elif self.paused:
                    self.close(q, "暂停", free=True)
                else:
                    continue
            if q["phase"] != "closing":
                continue
            for side, o in q["orders"].items():
                if o["state"] == "open" and now >= float(o.get("retry") or 0):
                    due.append((q, side))
                elif o["state"] in ("unknown", "placing", "prepared") and at_cancel and now < q["start"]:
                    sweep[q["condition_id"]] = q
        if due:
            await self.cancel_orders(due)
        for cid, q in sweep.items():
            if now - self.swept.get(cid, 0.0) >= SWEEP_EVERY_S:
                self.swept[cid] = now
                try:
                    r = await self.x.cancel_market(cid)
                    LOG.info("%s 有状态未知的单：按市场撤 %d 张", q["slug"], len(r.canceled))
                    self.mark_cancelled(r)
                except Throttled as t:
                    self.set_cooldown(t.reason, t.seconds)
                except Exception as error:
                    self.problem(f"按市场撤单失败 {q['slug']}：{error}")

    async def cancel_orders(self, due: list[tuple[dict, str]]) -> None:
        ids = [q["orders"][side]["id"] for q, side in due]
        now = self.now()
        try:
            r = await self.x.cancel(ids)
        except Throttled as t:
            self.set_cooldown(t.reason, t.seconds)
            self.retry_later(due, now)
            return
        except Exception as error:
            self.problem(f"撤单失败（{len(ids)} 张）：{error}", key="cancel-error")
            self.retry_later(due, now)
            return
        touched = self.mark_cancelled(r)
        missing = [(q, side) for q, side in due if q["orders"][side]["id"] not in r.canceled and q["orders"][side]["id"] not in r.not_canceled]
        if missing:
            self.retry_later(missing, now)
        for q, side in due:
            if q["key"] not in touched:
                self.save(q)

    def mark_cancelled(self, r: Cancelled) -> set[str]:
        now = self.now()
        touched: dict[str, dict] = {}
        for oid in list(r.canceled) + list(r.not_canceled):
            ref = self.by_id.get(oid)
            if not ref or ref[0] not in self.ledger.rows:
                continue
            q = self.ledger.rows[ref[0]]
            o = q["orders"][ref[1]]
            if o["state"] in ("open", "unknown"):
                o.update(state="gone", gone=now, note=r.not_canceled.get(oid, "已撤"))
                if q["phase"] == "closing" and q["reason"] == "到点撤单" and oid in r.canceled:
                    self.cancel_lags.append(q["start"] - now)
            touched[q["key"]] = q
        if touched:
            self.save(*touched.values())
        return set(touched)

    def retry_later(self, due: list[tuple[dict, str]], now: float) -> None:
        for q, side in due:
            o = q["orders"][side]
            if now >= q["start"] - self.s.cancel_s:
                o["retry"] = now + SPRINT_S
            else:
                o["tries"] = int(o.get("tries") or 0) + 1
                o["retry"] = now + min(RETRY_MAX_S, RETRY_S * 2 ** (o["tries"] - 1))
        self.save(*{q["key"]: q for q, _ in due}.values())

    # ------------------------------------------------------------------ reconciliation
    async def reconcile_pass(self) -> None:
        """Orders off the book get their final record read; unknown placements are looked for; a record whose orders
        all ended is closed."""
        now = self.now()
        reads: list[tuple[dict, str]] = []
        lookups: list[tuple[dict, str]] = []
        for q in self.open_quotes():
            for side, o in q["orders"].items():
                if o["state"] == "gone" and now >= float(o.get("gone") or 0) + FINAL_DELAY_S and now >= float(o.get("retry") or 0):
                    reads.append((q, side))
                elif o["state"] in ("unknown", "placing", "prepared") and now >= float(o.get("sent") or 0) + SEEN_MARGIN_S:
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
            await asyncio.gather(*(one(q, side) for q, side in reads[:60]), *(look(q, side) for q, side in lookups[:20]))
        now = self.now()
        for q in self.open_quotes():
            if q["phase"] in ("closing", "active") and all(o["state"] in ENDED for o in q["orders"].values()):
                if not q.get("reason"):
                    q["reason"] = "挂单都已结束"
                q["phase"] = "closed"
                q["closed_at"] = now
                self.save(q)

    def claim(self, q: dict, side: str) -> bool:
        """An unknown placement found in the open orders (its token, BUY, its price and size, no record's id)."""
        o = q["orders"][side]
        claimed = self.by_id
        for row in self.snapshot.values():
            if (row.id not in claimed and row.asset == o["asset"] and row.side == "BUY" and row.price == dnum(q["price"])
                    and row.size == dnum(q["shares"])):
                self.adopt(q, side, row, "按挂单列表认领")
                return True
        return False

    def adopt(self, q: dict, side: str, row: Row, note: str) -> None:
        o = q["orders"][side]
        o.update(state="open" if row.resting else "gone", id=row.id, note=note, gone=self.now(), retry=0)
        self.by_id[row.id] = (q["key"], side)
        self.note_fill(q, side, row.matched)
        LOG.info("认领状态未知的单 %s %s -> %s（%s）", q["key"], side, row.id, note)
        self.save(q)

    async def look_up(self, q: dict, side: str) -> None:
        """An unknown placement looked for by its hash (the CLOB's order id is the order's EIP-712 hash), then given up:
        once its signed expiry passed; or once two snapshots taken well after it was sent did not show it."""
        o = q["orders"][side]
        now = self.now()
        o["lookup_at"] = now + 5
        if o.get("hash") and self.id_is_hash is not False:
            try:
                row = await self.x.order(o["hash"])
            except Exception as error:
                LOG.debug("按哈希查单失败 %s：%s", o["hash"], error)
                row = None
            if row is not None:
                self.adopt(q, side, row, "按订单哈希找到")
                return
        sent = float(o.get("sent") or 0)
        expired = now >= q["start"] - self.s.expiry_s + 5
        unseen = now >= sent + GIVE_UP_S and self.snapshot_at - self.snapshot_took > sent + 2 * max(self.s.snapshot_s, 10)
        if expired or unseen:
            o.update(state="not_found", note="签名已到期，视为没挂上" if expired else "按订单哈希和挂单列表都找不到，视为没挂上")
            if q["phase"] == "active":  # the other side rests alone: withdrawn, the level quoted afresh
                self.close(q, f"{mk.SIDES_CN[side]}方状态不明、找不到", free=True, save=False)
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
        if row is None:
            o.update(state="final", final_at=self.now(), note=(o.get("note") or "") + "；最终记录读不到，按已知成交")
            self.save(q)
            return
        self.note_fill(q, side, row.matched)
        if row.resting:  # still on the book: the cancel did not take (or the order never left)
            o.update(state="open", retry=0)
            if q["phase"] == "active" and row.matched > 0:
                pass
            self.save(q)
            return
        o.update(state="final", final_at=self.now(), status=row.status)
        if q["phase"] == "active" and row.matched < row.size and self.now() < q["start"] - self.s.cancel_s:
            self.close(q, f"{mk.SIDES_CN[side]}方挂单被撤或到期（{row.status}）", free=False, save=False)
        self.save(q)

    def note_fill(self, q: dict, side: str, matched: D) -> None:
        o = q["orders"][side]
        before = dnum(o.get("filled"))
        if matched > before:
            o["filled"] = str(matched)
            ahead = q["start"] - self.now()
            self.fills_pending.append(f"{mk.window_name(q['coin'], q['start'])} {mk.SIDES_CN[side]} +{mk.fmt(matched - before)} 份"
                                      f" @{mk.fmt(dnum(q['price']) * 100)}¢（开场前 {mk.span(ahead)}）")
            self.counters["fills"] += 1

    # ------------------------------------------------------------------ account
    async def account_once(self) -> None:
        began = self.now()
        rows = await self.x.open_orders()
        self.snapshot_took = self.now() - began
        self.apply_snapshot(rows, began)
        if self.now() - self.balance_at >= 30 or self.balance is None:
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
        known = set(self.by_id)
        self.unmanaged = [r for r in rows if r.id not in known]
        self.snapshot_at = self.now()
        if changed:
            self.save(*changed.values())

    # ------------------------------------------------------------------ results
    async def settle_pass(self) -> None:
        now = self.now()
        results: dict[str, str | None] = {}
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
            q.update(phase="settled", result=win, pnl=float(filled[win] - cost), settled_at=now,
                     redeem="pending" if filled[win] > 0 else "none")
            self.save(q)
            self.notify(f"结算 {mk.window_name(q['coin'], q['start'])}：结果{mk.SIDES_CN[win]}，"
                        f"成交 涨 {mk.fmt(filled['up'])} / 跌 {mk.fmt(filled['down'])}，盈亏 {q['pnl']:+.2f} 美元")

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
        for q in self.open_quotes():
            for o in q["orders"].values():
                if o["state"] in ("placing", "prepared"):
                    o["state"] = "unknown"  # the previous process stopped mid-send: it may rest, it is looked for
            if q["phase"] == "active":
                why = self.matches_settings(q)
                if why:
                    self.close(q, why, free=True, save=False)
                    changed += 1
                else:
                    kept += 1
            self.save(q)
        return kept, changed

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
        self.close_all(reason)
        end = time.monotonic() + deadline_s
        while time.monotonic() < end:
            for q in self.open_quotes():
                for o in q["orders"].values():
                    o.pop("retry", None)
            await self.cancel_pass()
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
        while not self.stop.is_set():
            try:
                await self.discover()
            except Exception as error:
                self.problem(f"扫描出错：{error}", key="scan-error")
                await self.sleep(5)
            await self.sleep(self.poll_wait())

    async def cancel_loop(self) -> None:
        while not self.stop.is_set():
            try:
                await self.cancel_pass()
                await self.reconcile_pass()
                if self.fills_pending:
                    lines, self.fills_pending = self.fills_pending, []
                    self.notify("成交：\n" + "\n".join(lines[:30]) + (f"\n……共 {len(lines)} 笔" if len(lines) > 30 else ""))
            except Exception as error:
                LOG.exception("撤单循环出错")
                self.problem(f"撤单循环出错：{error}", key="cancel-loop")
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

    async def run(self) -> None:
        await self.x.start()
        self.started_at = self.now()
        await self.sync_clock()
        kept, changed = self.take_over()
        if kept or changed:
            LOG.info("接管 %d 场，%d 场设置不同将撤掉重报", kept, changed)
        try:
            await self.account_once()
        except Exception as error:
            self.problem(f"首次读账户失败：{error}")
        tasks = [asyncio.create_task(self.discovery_loop(), name="discovery"),
                 asyncio.create_task(self.cancel_loop(), name="cancel"),
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
    def counts(self) -> dict[str, Any]:
        active = [q for q in self.open_quotes() if q["phase"] == "active"]
        resting = sum(1 for q in self.open_quotes() for o in q["orders"].values() if o["state"] in MAY_REST)
        unknown = sum(1 for q in self.open_quotes() for o in q["orders"].values() if o["state"] == "unknown")
        windows = {q["slug"] for q in active}
        return {"active": len(active), "windows": len(windows), "resting": resting, "unknown": unknown,
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
            if any(f.values()):
                fills += 1
                both += 1 if all(f.values()) else 0
            if q.get("phase") == "settled":
                settled += 1
                pnl += float(q.get("pnl") or 0)
        return {"fills": fills, "both": both, "pnl": pnl, "settled": settled}

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
            f"每边：{self.s.levels_text()}（post-only GTD）；每场预留 ${self.s.window_usd:.2f}",
            f"时间：开场前 {self.s.ahead_s // 60} 分钟内的场次；开场前 {self.s.cancel_s} 秒撤单，{self.s.expiry_s} 秒到期，"
            f"{self.s.entry_s} 秒内不新挂" + ("" if self.s.weekend else "；周末不挂"),
            f"挂着：{c['windows']} 场 {c['resting']} 张单，预留 ${self.reserved():.2f}"
            + (f"（上限 ${self.s.max_open_usd}）" if self.s.max_open_usd else "")
            + (f"；余额 ${self.balance:.2f}" if self.balance is not None else "；余额未读到"),
        ]
        if self.unmanaged:
            lines.append(f"账户里另有 {len(self.unmanaged)} 张本程序没记录的挂单（${self.unmanaged_usd():.2f}）")
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
        if self.errors:
            ts, text = self.errors[-1]
            lines.append(f"最近问题（{mk.hms(ts)}）：{text[:200]}")
        return "\n".join(lines)

    def check_text(self) -> str:
        now = self.now()
        c = self.counts()
        lines = [f"🩺 体检 {mk.hms(now)}",
                 f"账户快照：{now - self.snapshot_at:.0f} 秒前（{len(self.snapshot)} 张挂单，读一次 {self.snapshot_took:.1f} 秒）" if self.snapshot_at else "账户快照：还没读到",
                 f"冷却：{self.cooldown_reason}（还有 {self.cooldown_until - now:.0f} 秒）" if now < self.cooldown_until else "冷却：无",
                 f"不新挂的原因：{self.hold() or '无'}",
                 f"队列 {self.queue.qsize()}，等候 {len(self.timers)}，正在挂 {len(self.placing)}",
                 f"状态未知的单：{c['unknown']}；撤单/核对中：{c['closing']} 场",
                 f"订单号=哈希：{'是' if self.id_is_hash else '否' if self.id_is_hash is False else '未知'}；限流档位：{getattr(self.x, 'tier', '') or '未知'}",
                 f"发布提前量（学到的）：{self.lead / 3600:.2f} 小时；创建→发现："
                 + (f"中位 {sorted(self.listing_lags)[len(self.listing_lags) // 2]:.1f} 秒" if self.listing_lags else "还没有新发布"),
                 f"扫描：{self.scan_note or '—'}"]
        recent = [(ts, text) for ts, text in self.errors if now - ts < 600]
        if recent:
            lines.append("最近 10 分钟的问题：")
            lines += [f"  {mk.hms(ts)} {text[:160]}" for ts, text in recent[-8:]]
        return "\n".join(lines)
