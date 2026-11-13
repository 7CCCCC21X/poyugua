#!/usr/bin/env python3
"""Run every offline suite (tests/test_*.py). No network, no key: Gamma, the CLOB and the SDK client are fakes.
Exit status is non-zero when anything fails, so `python tests/run_all.py` gates the Docker build."""
import sys
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent))


def main() -> int:
    suite = unittest.defaultTestLoader.discover(str(HERE), pattern="test_*.py", top_level_dir=str(HERE))
    result = unittest.TextTestRunner(verbosity=1).run(suite)
    print(f"\n{result.testsRun - len(result.failures) - len(result.errors)}/{result.testsRun} tests passed")
    return 0 if result.wasSuccessful() and result.testsRun else 1


if __name__ == "__main__":
    sys.exit(main())
