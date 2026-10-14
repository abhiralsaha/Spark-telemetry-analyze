import json
import tempfile
import unittest
from pathlib import Path

from spark_telemetry_analyzer import analyze_source
from spark_telemetry_analyzer.compare import compare_reports, load_report, render_comparison
from spark_telemetry_analyzer.report import render
from spark_telemetry_analyzer.sample import SCENARIOS, generate_event_log, write_event_log

from .helpers import log_bytes


class SampleTest(unittest.TestCase):
    def test_deterministic(self):
        self.assertEqual(list(generate_event_log("skew", seed=1, scale=0.2)), list(generate_event_log("skew", seed=1, scale=0.2)))
        self.assertNotEqual(list(generate_event_log("skew", seed=1, scale=0.2)), list(generate_event_log("skew", seed=2, scale=0.2)))

    def test_every_scenario_is_analyzable(self):
        for s in SCENARIOS:
            self.assertGreater(analyze_source(log_bytes(s, 0.3)).data["summary"]["tasks"], 0, s)

    def test_validation(self):
        with self.assertRaises(ValueError):
            list(generate_event_log("nope"))
        with self.assertRaises(ValueError):
            list(generate_event_log("mixed", executors=1))

    def test_write_gzip(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / "a.json.gz"
            self.assertGreater(write_event_log(str(p), scenario="healthy", scale=0.1), 10)
            self.assertGreater(analyze_source(p).data["summary"]["tasks"], 0)


class CompareTest(unittest.TestCase):
    def test_regression_detected_and_noise_ignored(self):
        healthy = analyze_source(log_bytes("healthy"))
        mixed = analyze_source(log_bytes("mixed"))
        res = compare_reports(healthy, mixed)
        self.assertTrue(res["regressed"])
        self.assertIn("JOB_FAILURE:6", res["new_findings"])
        same = compare_reports(healthy, healthy)
        self.assertFalse(same["regressed"])
        self.assertTrue(all(r["verdict"] == "unchanged" for r in same["metrics"]))
        self.assertIn("no regression", render_comparison(same))

    def test_improvement_and_report_loading(self):
        healthy = analyze_source(log_bytes("healthy"))
        mixed = analyze_source(log_bytes("mixed"))
        res = compare_reports(mixed, healthy)
        self.assertFalse(res["regressed"] and not res["resolved_findings"])
        self.assertIn("improvement", {r["verdict"] for r in res["metrics"]})
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / "r.json"
            p.write_text(render(mixed, "json"))
            self.assertEqual(load_report(p).health_score, mixed.health_score)
            log = Path(tmp) / "log"
            log.write_bytes(log_bytes("skew", 0.3))
            self.assertEqual(load_report(log).data["application"]["id"], "application_1760000000000_0042")


if __name__ == "__main__":
    unittest.main()
