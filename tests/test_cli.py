import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path

from spark_telemetry_analyzer.cli import main

from .helpers import log_bytes


def run(*argv):
    out, err = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        code = main(list(argv))
    return code, out.getvalue(), err.getvalue()


class CliTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        cls.dir = Path(cls.tmp.name)
        cls.mixed = cls.dir / "mixed.json"
        cls.healthy = cls.dir / "healthy.json"
        cls.mixed.write_bytes(log_bytes("mixed", 0.3))
        cls.healthy.write_bytes(log_bytes("healthy", 0.3))

    @classmethod
    def tearDownClass(cls):
        cls.tmp.cleanup()

    def test_analyze_text_and_json(self):
        code, out, _ = run("analyze", str(self.mixed))
        self.assertEqual(code, 0)
        self.assertIn("Health score", out)
        code, out, _ = run("analyze", str(self.mixed), "-f", "json")
        self.assertEqual(json.loads(out)["schema_version"], 1)

    def test_exit_codes(self):
        self.assertEqual(run("analyze", str(self.mixed), "--fail-on", "critical")[0], 1)
        self.assertEqual(run("analyze", str(self.healthy), "--fail-on", "warning")[0], 0)
        self.assertEqual(run("analyze", str(self.mixed), "--min-health", "50")[0], 1)
        self.assertEqual(run("analyze", str(self.healthy), "--min-health", "50")[0], 0)

    def test_errors_return_2_with_message(self):
        for argv in (("analyze", "/no/such"), ("analyze", str(self.healthy), "--disable-rule", "NOPE"),
                     ("analyze", str(self.healthy), "--top", "0"), ("analyze", str(self.healthy), "-c", "/no/cfg.toml")):
            code, _, err = run(*argv)
            self.assertEqual(code, 2, argv)
            self.assertIn("error:", err)

    def test_directory_with_bad_file_reports_error_but_analyzes_rest(self):
        d = self.dir / "mix"
        d.mkdir()
        (d / "good").write_bytes(log_bytes("healthy", 0.3))
        (d / "bad").write_text("garbage\n")
        code, out, err = run("analyze", str(d), "-f", "json")
        self.assertEqual(code, 2)
        self.assertIn("bad", err)
        self.assertEqual(json.loads(out)["schema_version"], 1)

    def test_output_file_and_dir(self):
        target = self.dir / "o" / "r.md"
        self.assertEqual(run("analyze", str(self.mixed), "-f", "markdown", "-o", str(target))[0], 0)
        self.assertTrue(target.read_text().startswith("# Spark Telemetry Report"))
        out_dir = self.dir / "reports"
        self.assertEqual(run("analyze", str(self.mixed), str(self.healthy), "-f", "html", "--output-dir", str(out_dir))[0], 0)
        self.assertEqual(len(list(out_dir.glob("*.html"))), 1)  # same app id -> same file (documented behaviour)
        code, _, err = run("analyze", str(self.mixed), str(self.healthy), "-f", "html")
        self.assertEqual(code, 2)

    def test_multiple_logs_json_is_a_list_and_prometheus_merges(self):
        code, out, _ = run("analyze", str(self.mixed), str(self.healthy), "-f", "json")
        self.assertEqual(len(json.loads(out)), 2)
        code, out, _ = run("analyze", str(self.mixed), str(self.healthy), "-f", "prometheus")
        self.assertEqual(out.count("# TYPE sta_app_health_score"), 1)

    def test_disable_rule_and_top(self):
        _, out, _ = run("analyze", str(self.mixed), "-f", "json", "--disable-rule", "data_skew")
        self.assertNotIn("DATA_SKEW", {f["rule_id"] for f in json.loads(out)["findings"]})
        _, out, _ = run("analyze", str(self.mixed), "--top", "2")
        self.assertIn("(top 2)", out)

    def test_sample_rules_compare(self):
        target = self.dir / "s.json"
        code, _, err = run("sample", "-s", "skew", "--scale", "0.2", "-o", str(target))
        self.assertEqual(code, 0)
        self.assertIn("Wrote", err)
        self.assertEqual(run("analyze", str(target))[0], 0)
        self.assertEqual(run("sample", "--executors", "1")[0], 2)
        code, out, _ = run("rules")
        self.assertIn("DATA_SKEW", out)
        code, out, _ = run("compare", str(self.healthy), str(self.mixed), "--fail-on-regression")
        self.assertEqual(code, 1)
        self.assertIn("REGRESSION", out)
        code, out, _ = run("compare", str(self.healthy), str(self.healthy), "-f", "json")
        self.assertEqual(code, 0)
        self.assertFalse(json.loads(out)["regressed"])

    def test_version_and_missing_command(self):
        with self.assertRaises(SystemExit) as ctx, contextlib.redirect_stdout(io.StringIO()):
            main(["--version"])
        self.assertEqual(ctx.exception.code, 0)
        with self.assertRaises(SystemExit) as ctx, contextlib.redirect_stderr(io.StringIO()):
            main([])
        self.assertEqual(ctx.exception.code, 2)


if __name__ == "__main__":
    unittest.main()
