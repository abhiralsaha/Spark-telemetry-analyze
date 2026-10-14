import contextlib
import io
import json
import tempfile
import threading
import unittest
import zipfile
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from spark_telemetry_analyzer.cli import main
from spark_telemetry_analyzer.errors import HistoryServerError
from spark_telemetry_analyzer.history import HistoryServerClient

from .helpers import log_bytes


def _zip(raw: bytes) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("eventLogs_app/events_1_app", raw)
    return buf.getvalue()


class FakeHistory(BaseHTTPRequestHandler):
    flaky = {"count": 0}
    seen_auth: list = []

    def log_message(self, *a):
        pass

    def do_GET(self):  # noqa: N802
        FakeHistory.seen_auth.append(self.headers.get("Authorization"))
        if self.path.startswith("/api/v1/applications?"):
            if FakeHistory.flaky["count"] < 1:
                FakeHistory.flaky["count"] += 1
                self.send_error(503)
                return
            body = json.dumps([{"id": "application_1_0001", "attempts": [{"attemptId": "1"}]}]).encode()
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        elif self.path in ("/api/v1/applications/application_1_0001/1/logs", "/api/v1/applications/application_1_0001/logs"):
            body = _zip(log_bytes("skew", 0.3))
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        else:
            self.send_error(404)


class HistoryTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.httpd = ThreadingHTTPServer(("127.0.0.1", 0), FakeHistory)
        threading.Thread(target=cls.httpd.serve_forever, daemon=True).start()
        cls.url = f"http://127.0.0.1:{cls.httpd.server_address[1]}"

    @classmethod
    def tearDownClass(cls):
        cls.httpd.shutdown()
        cls.httpd.server_close()

    def test_list_retries_transient_errors(self):
        FakeHistory.flaky["count"] = 0
        apps = HistoryServerClient(self.url, backoff=0.01).list_applications(limit=5)
        self.assertEqual(apps[0]["id"], "application_1_0001")

    def test_download_and_analyze(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = HistoryServerClient(self.url).download_event_log("application_1_0001", tmp, "1")
            self.assertTrue(path.exists())
            self.assertFalse(list(Path(tmp).glob("*.part")))
            from spark_telemetry_analyzer import analyze_source
            self.assertGreater(analyze_source(path).data["summary"]["tasks"], 0)

    def test_errors(self):
        client = HistoryServerClient(self.url, retries=1)
        with self.assertRaises(HistoryServerError):
            client.download_event_log("missing_app", tempfile.gettempdir())
        for bad in ("../../etc/passwd", "a/b", "a b", ""):
            with self.assertRaises(HistoryServerError):
                client.download_event_log(bad, tempfile.gettempdir())
        for url in ("ftp://x", "not-a-url", "file:///etc/passwd"):
            with self.assertRaises(HistoryServerError):
                HistoryServerClient(url)
        with self.assertRaises(HistoryServerError):
            HistoryServerClient("http://127.0.0.1:1", retries=1, timeout=0.5).list_applications()

    def test_size_limit_aborts_and_cleans_up(self):
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(HistoryServerError):
                HistoryServerClient(self.url, max_download_bytes=100).download_event_log("application_1_0001", tmp, "1")
            self.assertEqual(list(Path(tmp).iterdir()), [])

    def test_auth_header(self):
        FakeHistory.seen_auth.clear()
        with tempfile.TemporaryDirectory() as tmp:
            HistoryServerClient(self.url, token="tok").download_event_log("application_1_0001", tmp, "1")
        self.assertEqual(FakeHistory.seen_auth[-1], "Bearer tok")

    def test_cli_fetch_with_analyze(self):
        FakeHistory.flaky["count"] = 1  # already "warmed up"
        with tempfile.TemporaryDirectory() as tmp:
            out = io.StringIO()
            with contextlib.redirect_stdout(out), contextlib.redirect_stderr(io.StringIO()):
                code = main(["fetch", "--history-url", self.url, "--output-dir", tmp, "--limit", "1", "--analyze", "-f", "json"])
            self.assertEqual(code, 0)
            self.assertEqual(json.loads(out.getvalue())["schema_version"], 1)
            self.assertEqual(len(list(Path(tmp).glob("*.zip"))), 1)

    def test_cli_fetch_requires_url(self):
        with contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(main(["fetch", "--history-url", "", "--output-dir", "/tmp/x"]), 2)


if __name__ == "__main__":
    unittest.main()
