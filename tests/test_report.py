import json
import re
import unittest
from html.parser import HTMLParser

from spark_telemetry_analyzer import analyze_source
from spark_telemetry_analyzer.metrics import ServiceMetrics, render_prometheus
from spark_telemetry_analyzer.report import FORMATS, render

from .helpers import LogBuilder, log_bytes


class _Strict(HTMLParser):
    def __init__(self):
        super().__init__()
        self.stack, self.scripts = [], 0

    def handle_starttag(self, tag, attrs):
        if tag == "script":
            self.scripts += 1
        if tag not in ("meta", "br"):
            self.stack.append(tag)

    def handle_endtag(self, tag):
        assert self.stack and self.stack.pop() == tag, f"unbalanced </{tag}>"


class RenderTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.report = analyze_source(log_bytes("mixed", 0.3))

    def test_all_formats_render(self):
        for fmt in FORMATS:
            out = render(self.report, fmt)
            self.assertTrue(out.strip(), fmt)

    def test_json_roundtrip_and_schema(self):
        data = json.loads(render(self.report, "json"))
        self.assertEqual(data["schema_version"], 1)
        for key in ("application", "summary", "findings", "stages", "executors", "health_score", "grade", "parse"):
            self.assertIn(key, data)

    def test_text_contains_findings_and_no_ansi_by_default(self):
        text = render(self.report, "text")
        self.assertIn("Findings", text)
        self.assertNotIn("\x1b[", text)
        self.assertIn("\x1b[", render(self.report, "text", color=True))

    def test_top_n_limits_rows(self):
        text = render(self.report, "text", top_n=1)
        self.assertIn("Longest stages (top 1)", text)

    def test_markdown_tables(self):
        md = render(self.report, "markdown")
        self.assertTrue(md.startswith("# Spark Telemetry Report"))
        self.assertIn("| Metric | Value |", md)

    def test_html_is_well_formed_and_script_free(self):
        parser = _Strict()
        parser.feed(render(self.report, "html"))
        parser.close()
        self.assertEqual(parser.scripts, 0)
        self.assertEqual(parser.stack, ["html"] if parser.stack else [])

    def test_html_escapes_untrusted_values(self):
        evil = '<script>alert(1)</script>'
        b = LogBuilder(name=evil, props={"spark.app.name": evil}).add_executor("1")
        b.task(0, "1", 1_000_002_000, 1_000_003_000).end(1_000_010_000)
        html = render(analyze_source(b.build()), "html")
        self.assertNotIn(evil, html)
        self.assertIn("&lt;script&gt;", html)

    def test_unknown_format(self):
        with self.assertRaises(ValueError):
            render(self.report, "pdf")

    def test_secrets_never_reach_reports(self):
        for fmt in ("json", "text", "markdown", "html"):
            out = render(self.report, fmt)
            self.assertNotIn("not-a-real-secret", out)
            self.assertNotIn("hunter2", out)


class PrometheusTest(unittest.TestCase):
    SAMPLE = re.compile(r'^[a-zA-Z_:][a-zA-Z0-9_:]*(\{[^}]*\})? -?[0-9.e+\-]+(inf)?$')

    def test_exposition_is_syntactically_valid(self):
        text = render_prometheus([analyze_source(log_bytes("mixed", 0.3)), analyze_source(log_bytes("healthy", 0.1))])
        self.assertTrue(text.endswith("\n"))
        for line in text.splitlines():
            if line.startswith("#"):
                self.assertRegex(line, r"^# (HELP|TYPE) \S+ .+")
            else:
                self.assertRegex(line, self.SAMPLE)
        self.assertIn("sta_findings{", text)
        # every metric family is declared exactly once
        types = re.findall(r"^# TYPE (\S+)", text, re.M)
        self.assertEqual(len(types), len(set(types)))

    def test_label_escaping(self):
        b = LogBuilder(name='we"ird\\name').add_executor("1")
        b.task(0, "1", 1_000_002_000, 1_000_003_000).end(1_000_010_000)
        text = render_prometheus([analyze_source(b.build())])
        self.assertIn('app_name="we\\"ird\\\\name"', text)

    def test_service_metrics(self):
        m = ServiceMetrics()
        m.inc("x_total", status="ok")
        m.inc("x_total", 2, status="ok")
        m.set_gauge("g", 3)
        out = m.render()
        self.assertIn('x_total{status="ok"} 3.0', out)
        self.assertIn("g 3", out)


if __name__ == "__main__":
    unittest.main()
