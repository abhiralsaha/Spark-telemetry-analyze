import unittest
from dataclasses import replace

from spark_telemetry_analyzer import Settings, Severity, Thresholds, analyze_source
from spark_telemetry_analyzer.analyzer import health_score
from spark_telemetry_analyzer.models import Finding
from spark_telemetry_analyzer.rules import RULES

from .helpers import LogBuilder, log_bytes

MIB = 1024**2
GIB = 1024**3


def rule_ids(report, severity=None):
    return {f["rule_id"] for f in report.findings if severity is None or f["severity"] == severity}


class ScenarioTest(unittest.TestCase):
    def test_healthy_run_has_no_actionable_findings(self):
        r = analyze_source(log_bytes("healthy"))
        self.assertEqual([f for f in r.findings if f["severity"] != "info"], [])
        self.assertGreaterEqual(r.health_score, 90)

    def test_skew_scenario(self):
        r = analyze_source(log_bytes("skew"))
        self.assertIn("DATA_SKEW", rule_ids(r, "critical"))
        self.assertIn("DISK_SPILL", rule_ids(r))
        skew = next(f for f in r.findings if f["rule_id"] == "DATA_SKEW")
        self.assertEqual(skew["scope_id"], "1.0")
        self.assertGreater(skew["evidence"]["data_ratio"], 20)
        self.assertEqual(len(skew["evidence"]["slowest_tasks"]), 3)

    def test_failures_scenario(self):
        r = analyze_source(log_bytes("failures"))
        self.assertTrue({"JOB_FAILURE", "MEMORY_ERRORS", "TASK_FAILURES", "EXECUTOR_LOSS", "STAGE_RETRY"} <= rule_ids(r))
        self.assertEqual(r.data["summary"]["jobs_failed"], 1)
        self.assertEqual(r.data["summary"]["executors_lost"], 2)

    def test_mixed_scenario_covers_every_major_rule(self):
        r = analyze_source(log_bytes("mixed"))
        expected = {"DATA_SKEW", "STRAGGLER_TASKS", "GC_PRESSURE", "SMALL_TASKS", "SCHEDULING_OVERHEAD", "LOW_PARALLELISM",
                    "LARGE_RESULT", "LOW_UTILIZATION", "SERIALIZATION_OVERHEAD", "CONFIG_ADVISORY", "JOB_FAILURE"}
        self.assertTrue(expected <= rule_ids(r), expected - rule_ids(r))
        self.assertEqual(r.max_severity, Severity.CRITICAL)

    def test_findings_sorted_by_severity(self):
        levels = [Severity.parse(f["severity"]) for f in analyze_source(log_bytes("mixed")).findings]
        self.assertEqual(levels, sorted(levels, reverse=True))

    def test_every_registered_rule_has_a_doc_and_stable_id(self):
        for rule_id, fn in RULES.items():
            self.assertTrue(fn.__doc__, rule_id)
            self.assertEqual(rule_id, rule_id.upper())


class SettingsTest(unittest.TestCase):
    def test_disabled_rules(self):
        s = Settings(disabled_rules=frozenset({"DATA_SKEW", "DISK_SPILL"}))
        ids = rule_ids(analyze_source(log_bytes("skew"), s))
        self.assertNotIn("DATA_SKEW", ids)
        self.assertNotIn("DISK_SPILL", ids)

    def test_thresholds_are_respected(self):
        lax = Settings(thresholds=replace(Thresholds(), skew_data_ratio_warn=1000.0, skew_data_ratio_crit=2000.0))
        self.assertNotIn("DATA_SKEW", rule_ids(analyze_source(log_bytes("skew"), lax)))

    def test_buggy_rule_does_not_break_analysis(self):
        def boom(app, th):
            raise RuntimeError("bug")

        RULES["ZZ_BOOM"] = boom
        try:
            with self.assertLogs("spark_telemetry_analyzer.analyzer", level="ERROR"):
                r = analyze_source(log_bytes("skew"))
            self.assertIn("DATA_SKEW", rule_ids(r))
        finally:
            del RULES["ZZ_BOOM"]


class ScoreTest(unittest.TestCase):
    def test_score_math_and_caps(self):
        f = lambda sev, rid="R": Finding(rid, sev, "t", "m")  # noqa: E731
        self.assertEqual(health_score([]), 100)
        self.assertEqual(health_score([f(Severity.CRITICAL)]), 85)
        self.assertEqual(health_score([f(Severity.WARNING), f(Severity.INFO)]), 94)
        # one noisy rule cannot consume the whole score
        self.assertEqual(health_score([f(Severity.CRITICAL)] * 10), 75)
        self.assertEqual(health_score([f(Severity.CRITICAL, str(i)) for i in range(20)]), 0)


class HandCraftedRulesTest(unittest.TestCase):
    """Isolated, minimal logs: each asserts one rule fires (and, where relevant, doesn't fire)."""

    def base(self, **kw):
        return LogBuilder(**kw)

    def test_executor_imbalance(self):
        b = self.base()
        for e in "12345":
            b.add_executor(e)
        for e in "12345":
            n, dur = (60, 10_000) if e == "1" else (12, 1_000)
            for i in range(n):
                b.task(0, e, 1_000_000_000_000 + 2000 + i, 1_000_000_000_000 + 2000 + i + dur)
        b.stage(0, 1_000_000_000_000 + 2000, 1_000_000_000_000 + 700_000, 108).end(1_000_000_000_000 + 700_000)
        r = analyze_source(b.build())
        f = next(f for f in r.findings if f["rule_id"] == "EXECUTOR_IMBALANCE")
        self.assertEqual(f["scope_id"], "1")

    def test_no_imbalance_when_even(self):
        b = self.base()
        for e in "12345":
            b.add_executor(e)
            for i in range(20):
                b.task(0, e, 1_000_002_000 + i, 1_000_002_000 + i + 1_000)
        b.stage(0, 1_000_002_000, 1_000_700_000, 100).end(1_000_700_000)
        self.assertNotIn("EXECUTOR_IMBALANCE", rule_ids(analyze_source(b.build())))

    def test_large_partitions_and_fetch_wait(self):
        b = self.base().add_executor("1")
        for i in range(4):
            b.task(0, "1", 1_000_002_000, 1_000_062_000, shuffle_read=2 * GIB, fetch_wait=20_000)
        b.stage(0, 1_000_002_000, 1_000_062_000, 4).end(1_000_070_000)
        ids = rule_ids(analyze_source(b.build()))
        self.assertTrue({"LARGE_PARTITIONS", "SHUFFLE_FETCH_WAIT"} <= ids)

    def test_executor_loss_ignores_scale_down(self):
        b = self.base().add_executor("1").add_executor("2")
        b.remove_executor("1", 1_000_100_000, "Executor killed by driver.")
        b.remove_executor("2", 1_000_100_000, "Executor decommission: idle timeout")
        self.assertNotIn("EXECUTOR_LOSS", rule_ids(analyze_source(b.end(1_000_200_000).build())))

    def test_executor_loss_detects_node_failure(self):
        b = self.base().add_executor("1").add_executor("2").add_executor("3").add_executor("4")
        b.remove_executor("1", 1_000_100_000, "Worker lost: node unreachable")
        r = analyze_source(b.end(1_000_200_000).build())
        f = next(f for f in r.findings if f["rule_id"] == "EXECUTOR_LOSS")
        self.assertEqual(f["evidence"]["lost_executors"], 1)
        self.assertEqual(f["severity"], "warning")

    def test_incomplete_log_notice(self):
        r = analyze_source(self.base().add_executor("1").task(0, "1", 1_000_002_000, 1_000_003_000).build())
        self.assertIn("INCOMPLETE_LOG", rule_ids(r, "info"))

    def test_config_advisory_old_spark_and_aqe(self):
        b = self.base(props={"spark.sql.adaptive.enabled": "false"}, version="2.4.8").add_executor("1")
        b.task(0, "1", 1_000_002_000, 1_000_003_000, shuffle_write=1000)
        titles = [f["title"] for f in analyze_source(b.end(1_000_010_000).build()).findings if f["rule_id"] == "CONFIG_ADVISORY"]
        self.assertTrue(any("Adaptive" in t for t in titles))
        self.assertTrue(any("Old Spark" in t for t in titles))

    def test_job_failure_reason_surfaces(self):
        b = self.base().job(0, [0], 1_000_001_000, 1_000_002_000, failure="Job aborted: bad things")
        f = next(f for f in analyze_source(b.end(1_000_003_000).task(0, "1", 1_000_001_100, 1_000_001_200).build()).findings
                 if f["rule_id"] == "JOB_FAILURE")
        self.assertIn("bad things", f["message"])


if __name__ == "__main__":
    unittest.main()
