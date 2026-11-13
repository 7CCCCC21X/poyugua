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
        self.conn.commit()
        self.rows: dict[str, dict] = {}
        for key, data in self.conn.execute("SELECT key, data FROM quotes"):
            self.rows[key] = json.loads(data)

    def save(self, q: dict) -> None:
        self.save_many([q])

    def save_many(self, quotes: list[dict]) -> None:
        if not quotes:
            return
        with self.lock:
            self.conn.executemany(
                "INSERT INTO quotes(key, slug, start, phase, data) VALUES (?, ?, ?, ?, ?) "
                "ON CONFLICT(key) DO UPDATE SET slug=excluded.slug, start=excluded.start, phase=excluded.phase, data=excluded.data",
                [(q["key"], q["slug"], int(q["start"]), q["phase"], json.dumps(q, separators=(",", ":"))) for q in quotes])
            self.conn.commit()
        for q in quotes:
            self.rows[q["key"]] = q

    def delete(self, key: str) -> None:
        with self.lock:
            self.conn.execute("DELETE FROM quotes WHERE key = ?", (key,))
            self.conn.commit()
        self.rows.pop(key, None)

    def archive_old(self, now: float) -> int:
        """Move the finished records that filled nothing to the archive, two days after their window."""
        old = [q for q in self.rows.values() if archivable(q, now - ARCHIVE_AFTER_S)]
        if not old:
            return 0
        with self.lock:
            self.conn.executemany("INSERT OR REPLACE INTO archive(key, start, data) VALUES (?, ?, ?)",
                                  [(q["key"], int(q["start"]), json.dumps(q, separators=(",", ":"))) for q in old])
            self.conn.executemany("DELETE FROM quotes WHERE key = ?", [(q["key"],) for q in old])
            self.conn.commit()
        for q in old:
            self.rows.pop(q["key"], None)
        return len(old)

    def archived(self) -> Iterator[dict]:
        with self.lock:
            rows = self.conn.execute("SELECT data FROM archive").fetchall()
        for (data,) in rows:
            yield json.loads(data)

    def archive_count(self) -> int:
        with self.lock:
            return int(self.conn.execute("SELECT COUNT(*) FROM archive").fetchone()[0])

    def get(self, k: str, default: str = "") -> str:
        with self.lock:
            row = self.conn.execute("SELECT v FROM kv WHERE k = ?", (k,)).fetchone()
        return row[0] if row else default

    def put(self, k: str, v: str) -> None:
        with self.lock:
            self.conn.execute("INSERT INTO kv(k, v) VALUES (?, ?) ON CONFLICT(k) DO UPDATE SET v=excluded.v", (k, v))
            self.conn.commit()

    def close(self) -> None:
        with self.lock:
            self.conn.close()
