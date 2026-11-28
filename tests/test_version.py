"""The review's 上次更新起 starts at the latest version of the trading (config.TRADING_VERSIONS) or settings change. The
list is kept by hand, so a change to the files that decide what the bot does with orders is asked one question here:
did the trading itself change? If it did, the list gets a line saying what (and when it goes out); either way
FINGERPRINT below is set to the new one, so that the answer is a deliberate one and not a forgotten one."""
import datetime as dt
import hashlib
import unittest
from pathlib import Path

from preopen import config

ROOT = Path(__file__).resolve().parent.parent / "preopen"
FILES = ("engine.py", "exchange.py", "markets.py", "config.py", "gamma.py", "ledger.py", "refusals.py")
FINGERPRINT = "e7ae47d4c3971224"   # of FILES as they are with the last line of config.TRADING_VERSIONS


def fingerprint() -> str:
    h = hashlib.sha256()
    for name in FILES:
        h.update(name.encode())
        h.update((ROOT / name).read_bytes())
    return h.hexdigest()[:16]


class TradingVersionTest(unittest.TestCase):
    def test_a_change_to_the_trading_files_is_answered(self):
        self.assertEqual(fingerprint(), FINGERPRINT, (
            "\n\nThe files that trade changed (" + ", ".join(FILES) + ").\n"
            "  - If what the bot does with orders changed: add a line to config.TRADING_VERSIONS (Beijing time it goes out,\n"
            "    what changed, in the operator's words). The review's 上次更新起 will start there.\n"
            "  - If only how things are shown, logged or exported changed: add nothing.\n"
            f"Then set FINGERPRINT in tests/test_version.py to \"{fingerprint()}\"."))

    def test_the_versions_are_dated_and_in_order(self):
        times = [dt.datetime.strptime(when, "%Y-%m-%d %H:%M") for when, _ in config.TRADING_VERSIONS]
        self.assertEqual(times, sorted(set(times)))                     # the latest last, no two at one minute
        self.assertTrue(all(note.strip() for _, note in config.TRADING_VERSIONS))


if __name__ == "__main__":
    unittest.main()
