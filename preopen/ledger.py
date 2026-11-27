"""The quotes' records: one row per window level and attempt, written synchronously before anything is sent.

A record (a "quote") is a dict stored whole as JSON:

    key        <slug>#<level>@<attempt>        e.g. btc-updown-5m-1794624300#0@0
    slug, coin, start, level, attempt, price, shares, expiration, condition_id, up, down
    phase      active -> closing -> closed (every order ended, its fills read) -> settled (result and P&L known)
    orders     {"up": order, "down": order}
    order      {asset, state, id, hash, salt, filled, error, sent, gone, ...}
               state: prepared | placing | open | unknown | gone | final | rejected | not_sent | not_found

Settled records move to the table `archive` two days after their window (one with winnings once they were
redeemed), so the working set the loops walk stays a couple of days long.
"""
from __future__ import annotations

import json
import os
import sqlite3
import threading
from pathlib import Path
from typing import Iterator

MAY_REST = {"prepared", "placing", "open", "unknown"}   # an order that may be resting on the book, for all we know
ENDED = {"final", "rejected", "not_sent", "not_found"}   # nothing left to cancel or read
NEVER_PLACED = {"rejected", "not_sent", "not_found"}
ARCHIVE_AFTER_S = 2 * 86400


def quote_key(slug: str, level: int, attempt: int) -> str:
    return f"{slug}#{level}@{attempt}"


def unclaimed(q: dict) -> bool:
    """Winnings this bot could not claim and means to (six failures; no Relayer key, which a restart may bring)."""
    return not q.get("foreign") and (q.get("redeem") == "failed" or (q.get("redeem") == "manual" and bool(q.get("redeem_error"))))


def archivable(q: dict, before: float) -> bool:
    """Settled two days ago, every order ended, nothing left to redeem (a record with fills goes too once its
    winnings were redeemed, or there were none): the working set stays a couple of days long."""
    return (q.get("phase") == "settled" and int(q.get("start") or 0) + 300 < before
            and q.get("redeem") not in ("pending", "retry")
            and all(o.get("state") in ENDED for o in (q.get("orders") or {}).values()))


class Ledger:
    def __init__(self, path: str):
        if path != ":memory:":
            Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        self.lock = threading.Lock()
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.execute("CREATE TABLE IF NOT EXISTS quotes (key TEXT PRIMARY KEY, slug TEXT, start INTEGER, phase TEXT, data TEXT NOT NULL)")
        self.conn.execute("CREATE TABLE IF NOT EXISTS archive (key TEXT PRIMARY KEY, start INTEGER, data TEXT NOT NULL)")
        self.conn.execute("CREATE TABLE IF NOT EXISTS kv (k TEXT PRIMARY KEY, v TEXT NOT NULL)")
        # archived records whose winnings are still to be claimed by this bot (restored when it can claim them)
        self.conn.execute("CREATE TABLE IF NOT EXISTS unclaimed (key TEXT PRIMARY KEY)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS archive_start ON archive(start, key)")   # the review reads by time
        self.conn.commit()
        self.rows: dict[str, dict] = {}
        for key, data in self.conn.execute("SELECT key, data FROM quotes"):
            self.rows[key] = json.loads(data)

    def save(self, q: dict) -> None:
        self.save_many([q])

    def save_many(self, quotes: list[dict]) -> None:
        if not quotes:
            return
        with self.lock, self.conn:   # one transaction: committed whole, or rolled back (audit 11-27)
            self.conn.executemany(
                "INSERT INTO quotes(key, slug, start, phase, data) VALUES (?, ?, ?, ?, ?) "
                "ON CONFLICT(key) DO UPDATE SET slug=excluded.slug, start=excluded.start, phase=excluded.phase, data=excluded.data",
                [(q["key"], q["slug"], int(q["start"]), q["phase"], json.dumps(q, separators=(",", ":"))) for q in quotes])
        for q in quotes:
            self.rows[q["key"]] = q

    def delete(self, key: str) -> None:
        with self.lock, self.conn:
            self.conn.execute("DELETE FROM quotes WHERE key = ?", (key,))
        self.rows.pop(key, None)

    def archive_old(self, now: float) -> int:
        """Move the finished records to the archive, two days after their window. Winnings this bot still means to
        claim go too (the working set stays a couple of days long) and are listed in `unclaimed`: restored when the bot
        can claim them (restore_unclaimed; audit 11-27: they were archived and forgotten). One transaction: moved whole,
        or not at all (a failure half way left a record in both tables)."""
        old = [q for q in self.rows.values() if archivable(q, now - ARCHIVE_AFTER_S)]
        if not old:
            return 0
        with self.lock, self.conn:
            self.conn.executemany("INSERT OR REPLACE INTO archive(key, start, data) VALUES (?, ?, ?)",
                                  [(q["key"], int(q["start"]), json.dumps(q, separators=(",", ":"))) for q in old])
            self.conn.executemany("INSERT OR IGNORE INTO unclaimed(key) VALUES (?)", [(q["key"],) for q in old if unclaimed(q)])
            self.conn.executemany("DELETE FROM quotes WHERE key = ?", [(q["key"],) for q in old])
        for q in old:
            self.rows.pop(q["key"], None)
        return len(old)

    def archived(self) -> Iterator[dict]:
        with self.lock:
            rows = self.conn.execute("SELECT data FROM archive").fetchall()
        for (data,) in rows:
            yield json.loads(data)

    def archived_between(self, lo: float, hi: float, newest_first: bool = False, batch: int = 2000) -> Iterator[dict]:
        """The archived records of the windows starting from `lo` to `hi`, in start order (or the newest first), read a
        batch at a time: the lock is held for a batch, not for the whole read (the loops keep writing meanwhile), and a
        long archive never sits in memory at once."""
        lo = -1e18 if lo == float("-inf") else lo
        hi = 1e18 if hi == float("inf") else hi
        at, key = (hi + 1, "\uffff") if newest_first else (lo - 1, "")
        sql = ("SELECT key, start, data FROM archive WHERE start >= ? AND start < ? AND (start < ? OR (start = ? AND key < ?)) "
               "ORDER BY start DESC, key DESC LIMIT ?" if newest_first else
               "SELECT key, start, data FROM archive WHERE start >= ? AND start < ? AND (start > ? OR (start = ? AND key > ?)) "
               "ORDER BY start, key LIMIT ?")
        while True:
            with self.lock:
                rows = self.conn.execute(sql, (lo, hi, at, at, key, batch)).fetchall()
            for _, _, data in rows:
                yield json.loads(data)
            if len(rows) < batch:
                return
            key, at = rows[-1][0], rows[-1][1]

    def unclaimed_count(self) -> int:
        with self.lock:
            return int(self.conn.execute("SELECT COUNT(*) FROM unclaimed").fetchone()[0])

    def restore_unclaimed(self) -> list[dict]:
        """The archived records with winnings still to claim, back in the working set (the bot can claim them now)."""
        with self.lock, self.conn:
            rows = self.conn.execute("SELECT a.key, a.data FROM unclaimed u JOIN archive a ON a.key = u.key").fetchall()
            back = [json.loads(data) for _, data in rows]
            self.conn.executemany(
                "INSERT INTO quotes(key, slug, start, phase, data) VALUES (?, ?, ?, ?, ?) "
                "ON CONFLICT(key) DO UPDATE SET slug=excluded.slug, start=excluded.start, phase=excluded.phase, data=excluded.data",
                [(q["key"], q["slug"], int(q["start"]), q["phase"], json.dumps(q, separators=(",", ":"))) for q in back])
            self.conn.executemany("DELETE FROM archive WHERE key = ?", [(q["key"],) for q in back])
            self.conn.execute("DELETE FROM unclaimed")
        for q in back:
            self.rows[q["key"]] = q
        return back

    def size(self) -> tuple[int, int] | None:
        """The ledger's bytes on disk (with its write-ahead log) and the free bytes of its disk; None in memory."""
        if self.path == ":memory:":
            return None
        try:
            used = sum(os.path.getsize(self.path + ext) for ext in ("", "-wal") if os.path.exists(self.path + ext))
            st = os.statvfs(os.path.dirname(os.path.abspath(self.path)))
            return used, st.f_bavail * st.f_frsize
        except OSError:
            return None

    def archive_count(self) -> int:
        with self.lock:
            return int(self.conn.execute("SELECT COUNT(*) FROM archive").fetchone()[0])

    def get(self, k: str, default: str = "") -> str:
        with self.lock:
            row = self.conn.execute("SELECT v FROM kv WHERE k = ?", (k,)).fetchone()
        return row[0] if row else default

    def put(self, k: str, v: str) -> None:
        with self.lock, self.conn:
            self.conn.execute("INSERT INTO kv(k, v) VALUES (?, ?) ON CONFLICT(k) DO UPDATE SET v=excluded.v", (k, v))

    def close(self) -> None:
        with self.lock:
            self.conn.close()
