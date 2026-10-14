import json
import tempfile
import time
import unittest
import urllib.error
import urllib.request
from pathlib import Path

from spark_telemetry_analyzer.config import Settings
from spark_telemetry_analyzer.errors import AnalyzerError
from spark_telemetry_analyzer.server import AnalyzerServer, ServerConfig

from .helpers import log_bytes


def call(server, path, method="GET", data=None, token=None, headers=None):
    req = urllib.request.Request(f"http://127.0.0.1:{server.port}{path}", data=data, method=method, headers=headers or {})
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status, resp.read(), dict(resp.headers)
    except urllib.error.HTTPError as exc:
        try:
            return exc.code, exc.read(), dict(exc.headers)
        finally:
            exc.close()


class ServerTest(unittest.TestCase):
    def start(self, **kw):
        server = AnalyzerServer(ServerConfig(port=0, **kw))
        server.start_background()
        self.addCleanup(server.stop)
        return server

    def test_probes_and_metrics(self):
        s = self.start()
        self.assertEqual(call(s, "/healthz")[0], 200)
        self.assertEqual(call(s, "/readyz")[0], 200)
        status, body, headers = call(s, "/metrics")
        self.assertEqual(status, 200)
        self.assertIn("text/plain", headers["Content-Type"])
        self.assertEqual(call(s, "/nope")[0], 404)

    def test_analyze_then_fetch_in_all_formats(self):
        s = self.start()
        status, body, _ = call(s, "/api/v1/analyze", "POST", log_bytes("mixed", 0.3))
        self.assertEqual(status, 200)
        report = json.loads(body)
        app_id = report["application"]["id"]
        listing = json.loads(call(s, "/api/v1/reports")[1])["reports"]
        self.assertEqual([r["app_id"] for r in listing], [app_id])
        for fmt, marker in (("json", b"schema_version"), ("html", b"<!doctype html>"), ("markdown", b"# Spark"), ("text", b"Health score")):
            status, body, headers = call(s, f"/api/v1/reports/{app_id}?format={fmt}")
            self.assertEqual(status, 200, fmt)
            self.assertIn(marker, body)
        html_headers = call(s, f"/api/v1/reports/{app_id}?format=html")[2]
        self.assertIn("default-src 'none'", html_headers["Content-Security-Policy"])
        self.assertEqual(call(s, f"/api/v1/reports/{app_id}?format=pdf")[0], 400)
        self.assertEqual(call(s, "/api/v1/reports/unknown")[0], 404)
        metrics = call(s, "/metrics")[1].decode()
        self.assertIn(f'sta_app_health_score{{app_id="{app_id}"', metrics)
        self.assertIn('sta_analyses_total{status="ok"} 1.0', metrics)

    def test_bad_requests(self):
        s = self.start(max_body_bytes=2048)
        self.assertEqual(call(s, "/api/v1/analyze", "POST", b"")[0], 400)
        self.assertEqual(call(s, "/api/v1/analyze", "POST", b"garbage\n")[0], 422)
        self.assertEqual(call(s, "/api/v1/analyze", "POST", b"x" * 5000)[0], 413)
        self.assertEqual(call(s, "/api/v1/other", "POST", b"x")[0], 404)
        # server still healthy afterwards
        self.assertEqual(call(s, "/healthz")[0], 200)

    def test_bearer_token(self):
        s = self.start(token="s3cret")
        self.assertEqual(call(s, "/healthz")[0], 200)  # probes and metrics stay open
        self.assertEqual(call(s, "/metrics")[0], 200)
        status, _, headers = call(s, "/api/v1/reports")
        self.assertEqual(status, 401)
        self.assertIn("Bearer", headers["WWW-Authenticate"])
        self.assertEqual(call(s, "/api/v1/reports", token="wrong")[0], 401)
        self.assertEqual(call(s, "/api/v1/reports", token="s3cret")[0], 200)
        self.assertEqual(call(s, "/api/v1/analyze", "POST", log_bytes("healthy", 0.1))[0], 401)
        self.assertEqual(call(s, "/api/v1/analyze", "POST", log_bytes("healthy", 0.1), token="s3cret")[0], 200)

    def test_store_is_bounded(self):
        s = self.start(max_reports=2)
        for seed in (1, 2, 3):
            data = log_bytes("healthy", 0.1, seed).replace(b"application_1760000000000_0042", f"app_{seed}".encode())
            self.assertEqual(call(s, "/api/v1/analyze", "POST", data)[0], 200)
        ids = [r["app_id"] for r in json.loads(call(s, "/api/v1/reports")[1])["reports"]]
        self.assertEqual(ids, ["app_2", "app_3"])

    def test_directory_watcher(self):
        with tempfile.TemporaryDirectory() as tmp:
            (Path(tmp) / "good.json").write_bytes(log_bytes("skew", 0.3))
            (Path(tmp) / "bad.json").write_text("not a log\n")
            (Path(tmp) / "running.json.inprogress").write_bytes(log_bytes("skew", 0.3))
            s = self.start(watch_dir=Path(tmp), settle_seconds=0, poll_interval=0.2)
            deadline = time.time() + 10
            while time.time() < deadline and call(s, "/readyz")[0] != 200:
                time.sleep(0.05)
            self.assertEqual(call(s, "/readyz")[0], 200)
            reports = json.loads(call(s, "/api/v1/reports")[1])["reports"]
            self.assertEqual(len(reports), 1)  # good analyzed, bad skipped, in-progress ignored
            metrics = call(s, "/metrics")[1].decode()
            self.assertIn('sta_analyses_total{status="rejected"}', metrics)

    def test_missing_watch_dir(self):
        with self.assertRaises(AnalyzerError):
            AnalyzerServer(ServerConfig(port=0, watch_dir=Path("/no/such/dir")))

    def test_busy_server_sheds_load(self):
        s = self.start(max_concurrent_analyses=1)
        for _ in range(1):
            s.analysis._slots.acquire()  # simulate an analysis in flight
        status, body, headers = call(s, "/api/v1/analyze", "POST", log_bytes("healthy", 0.1))
        self.assertEqual(status, 503)
        self.assertEqual(headers["Retry-After"], "5")


if __name__ == "__main__":
    unittest.main()
