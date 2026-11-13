"""The geoblock verdict: the API-closed countries and regions; the website-only ones pass."""
import unittest

import helpers  # noqa: F401  (no network)

from preopen import region


class RegionTest(unittest.TestCase):
    def test_verdicts(self):
        self.assertFalse(region.verdict({"blocked": True, "ip": "1.2.3.4", "country": "US", "region": "VA"})[0])
        self.assertFalse(region.verdict({"blocked": True, "country": "SG"})[0])
        self.assertFalse(region.verdict({"blocked": True, "country": "CA", "region": "ON"})[0])
        self.assertTrue(region.verdict({"blocked": False, "country": "CA", "region": "NS"})[0])
        ok, line = region.verdict({"blocked": True, "country": "NL", "region": "NH"})  # website-only restriction
        self.assertTrue(ok)
        self.assertIn("API 不受限", line)
        self.assertTrue(region.verdict({"blocked": False, "country": "IE"})[0])


if __name__ == "__main__":
    unittest.main()
