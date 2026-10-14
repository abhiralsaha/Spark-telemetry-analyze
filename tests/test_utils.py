import unittest

from spark_telemetry_analyzer.utils import fmt_bytes, fmt_duration, fmt_pct, iso_utc, median, percentile, safe_div, truncate


class UtilsTest(unittest.TestCase):
    def test_percentile_interpolates(self):
        values = [1, 2, 3, 4, 5]
        self.assertEqual(percentile(values, 0.0), 1)
        self.assertEqual(percentile(values, 0.5), 3)
        self.assertEqual(percentile(values, 1.0), 5)
        self.assertAlmostEqual(percentile(values, 0.25), 2.0)
        self.assertAlmostEqual(percentile([10, 20], 0.5), 15.0)

    def test_percentile_edge_cases(self):
        self.assertEqual(percentile([], 0.5), 0.0)
        self.assertEqual(percentile([7], 0.99), 7.0)
        self.assertEqual(percentile([1, 2], 5), 2.0)  # q is clamped

    def test_median_and_safe_div(self):
        self.assertEqual(median([3, 1, 2]), 2)
        self.assertEqual(safe_div(1, 0), 0.0)
        self.assertEqual(safe_div(1, 0, default=-1), -1)
        self.assertEqual(safe_div(6, 3), 2)

    def test_fmt_bytes(self):
        self.assertEqual(fmt_bytes(0), "0 B")
        self.assertEqual(fmt_bytes(1024), "1.00 KiB")
        self.assertEqual(fmt_bytes(5 * 1024**3), "5.00 GiB")
        self.assertEqual(fmt_bytes(None), "n/a")

    def test_fmt_duration(self):
        self.assertEqual(fmt_duration(250), "250 ms")
        self.assertEqual(fmt_duration(12_300), "12.3 s")
        self.assertEqual(fmt_duration(192_000), "3m 12s")
        self.assertEqual(fmt_duration(3_720_000), "1h 02m")
        self.assertEqual(fmt_duration(None), "n/a")

    def test_misc(self):
        self.assertEqual(fmt_pct(0.1234), "12.3%")
        self.assertEqual(truncate("a" * 10, 5), "aaaa\u2026")
        self.assertEqual(truncate(None), "")
        self.assertIsNone(iso_utc(0))
        self.assertTrue(iso_utc(1_700_000_000_000).startswith("2023-11-14"))


if __name__ == "__main__":
    unittest.main()
