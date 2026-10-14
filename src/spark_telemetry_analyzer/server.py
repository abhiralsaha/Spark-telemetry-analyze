"""Long-running service mode.

* ``GET  /healthz``                      liveness probe
* ``GET  /readyz``                       readiness probe (503 until the first directory scan finished)
* ``GET  /metrics``                      Prometheus metrics for all stored reports + service counters
* ``GET  /api/v1/reports``               list stored report summaries
* ``GET  /api/v1/reports/<app-id>``      one report (``?format=json|html|markdown|text``)
* ``POST /api/v1/analyze``               body = raw event log (plain/gzip/zip), returns the report

Optionally watches a directory (local disk, NFS, or a mounted bucket) and analyzes new / finished event
logs automatically. ``/api/*`` can be protected with a bearer token. Standard library only.
"""

from __future__ import annotations

import hmac
import json
import logging
import signal
import threading
import time
from collections import OrderedDict
from dataclasses import dataclass, field
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, unquote, urlsplit

from . import __version__
from .analyzer import Report, analyze_source
from .config import Settings
from .errors import AnalyzerError
from .metrics import CONTENT_TYPE as PROM_CONTENT_TYPE
from .metrics import ServiceMetrics, render_prometheus
from .parser import discover_logs
from .report import render

log = logging.getLogger(__name__)

_CONTENT_TYPES = {
    "json": "application/json; charset=utf-8",
    "html": "text/html; charset=utf-8",
    "markdown": "text/markdown; charset=utf-8",
    "text": "text/plain; charset=utf-8",
}


@dataclass
class ServerConfig:
    host: str = "127.0.0.1"
    port: int = 8080
    watch_dir: Path | None = None
    token: str | None = None
    max_body_bytes: int = 256 * 1024 * 1024
    max_reports: int = 200
    max_concurrent_analyses: int = 4
    poll_interval: float = 15.0
    settle_seconds: float = 5.0
    include_inprogress: bool = False
    settings: Settings = field(default_factory=Settings)


class ReportStore:
    """Thread-safe, size-bounded (LRU by insertion) report store."""

    def __init__(self, capacity: int) -> None:
        self._capacity = max(1, capacity)
        self._items: OrderedDict[str, Report] = OrderedDict()
        self._lock = threading.Lock()

    def put(self, report: Report) -> None:
        with self._lock:
            self._items.pop(report.app_id, None)
            self._items[report.app_id] = report
            while len(self._items) > self._capacity:
                self._items.popitem(last=False)

    def get(self, app_id: str) -> Report | None:
        with self._lock:
            return self._items.get(app_id)

    def all(self) -> list[Report]:
        with self._lock:
            return list(self._items.values())

    def __len__(self) -> int:
        with self._lock:
            return len(self._items)


def summarize(report: Report) -> dict[str, Any]:
    d = report.data
    return {
        "app_id": report.app_id,
        "app_name": d["application"]["name"],
        "duration_ms": d["application"]["duration_ms"],
        "health_score": d["health_score"],
        "grade": d["grade"],
        "finding_counts": d["finding_counts"],
        "generated_at": d["generated_at"],
    }


class Analysis:
    """Shared analysis entry point with bounded concurrency, used by HTTP handlers and the watcher."""

    def __init__(self, config: ServerConfig, store: ReportStore, metrics: ServiceMetrics) -> None:
        self.config, self.store, self.metrics = config, store, metrics
        self._slots = threading.BoundedSemaphore(config.max_concurrent_analyses)

    def run(self, source: Any, *, blocking: bool = True, fallback_id: str = "unknown") -> Report | None:
        if not self._slots.acquire(blocking=blocking):
            return None
        started = time.monotonic()
        try:
            report = analyze_source(source, self.config.settings)
            if report.app_id in ("", "unknown"):
                report.data["application"]["id"] = fallback_id
            self.store.put(report)
            self.metrics.inc("sta_analyses_total", status="ok")
            return report
        except AnalyzerError:
            self.metrics.inc("sta_analyses_total", status="rejected")
            raise
        except Exception:
            self.metrics.inc("sta_analyses_total", status="error")
            raise
        finally:
            self.metrics.inc("sta_analysis_seconds_total", time.monotonic() - started)
            self.metrics.set_gauge("sta_reports_stored", float(len(self.store)))
            self._slots.release()


def _signature(path: Path) -> tuple[int, float]:
    files = [p for p in path.iterdir() if p.is_file()] if path.is_dir() else [path]
    size = sum(p.stat().st_size for p in files)
    mtime = max((p.stat().st_mtime for p in files), default=0.0)
    return size, mtime


class DirectoryWatcher(threading.Thread):
    """Polls a directory and analyzes each log once it stops changing."""

    def __init__(self, analysis: Analysis, config: ServerConfig) -> None:
        super().__init__(name="sta-watcher", daemon=True)
        self.analysis, self.config = analysis, config
        self.ready = threading.Event()
        self.stop_event = threading.Event()
        self._seen: dict[Path, tuple[int, float]] = {}

    def scan_once(self) -> int:
        assert self.config.watch_dir is not None
        analyzed = 0
        for unit in discover_logs([self.config.watch_dir], include_inprogress=self.config.include_inprogress):
            if self.stop_event.is_set():
                break
            try:
                sig = _signature(unit)
            except OSError:
                continue
            if self._seen.get(unit) == sig:
                continue
            if time.time() - sig[1] < self.config.settle_seconds:
                continue  # still being written
            try:
                self.analysis.run(unit, fallback_id=unit.name)
                analyzed += 1
            except Exception as exc:  # keep the watcher alive no matter what one log looks like
                log.warning("Could not analyze %s: %s", unit, exc)
            self._seen[unit] = sig  # remember failures too, to avoid retry storms on bad files
        return analyzed

    def run(self) -> None:
        while not self.stop_event.is_set():
            try:
                self.scan_once()
            except Exception:
                log.exception("Directory scan failed")
            self.ready.set()
            self.stop_event.wait(self.config.poll_interval)


def _make_handler(
    config: ServerConfig, analysis: Analysis, store: ReportStore, metrics: ServiceMetrics, ready: threading.Event
) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        server_version = f"spark-telemetry-analyzer/{__version__}"
        protocol_version = "HTTP/1.1"

        # ---- plumbing ------------------------------------------------------------------
        def log_message(self, fmt: str, *args: Any) -> None:
            log.info("%s - %s", self.address_string(), fmt % args)

        def _send(self, status: int, body: bytes, content_type: str, extra: dict[str, str] | None = None) -> None:
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("X-Content-Type-Options", "nosniff")
            self.send_header("Cache-Control", "no-store")
            if self.close_connection:
                self.send_header("Connection", "close")
            if content_type.startswith("text/html"):
                self.send_header("Content-Security-Policy", "default-src 'none'; style-src 'unsafe-inline'")
            for k, v in (extra or {}).items():
                self.send_header(k, v)
            self.end_headers()
            self.wfile.write(body)
            metrics.inc("sta_http_requests_total", method=self.command, status=str(status))

        def _json(self, status: int, payload: Any) -> None:
            self._send(status, json.dumps(payload).encode(), _CONTENT_TYPES["json"])

        def _error(self, status: HTTPStatus, message: str) -> None:
            self._json(status, {"error": message})

        def _authorized(self) -> bool:
            if not config.token:
                return True
            header = self.headers.get("Authorization", "")
            supplied = header[7:] if header.lower().startswith("bearer ") else ""
            if hmac.compare_digest(supplied.encode(), config.token.encode()):
                return True
            self._send(
                HTTPStatus.UNAUTHORIZED,
                json.dumps({"error": "missing or invalid bearer token"}).encode(),
                _CONTENT_TYPES["json"],
                {"WWW-Authenticate": 'Bearer realm="spark-telemetry-analyzer"'},
            )
            return False

        def _send_report(self, report: Report, query: dict[str, list[str]]) -> None:
            fmt = (query.get("format") or ["json"])[0]
            if fmt not in _CONTENT_TYPES:
                self._error(HTTPStatus.BAD_REQUEST, f"unsupported format '{fmt}' (json, html, markdown, text)")
                return
            self._send(HTTPStatus.OK, render(report, fmt).encode(), _CONTENT_TYPES[fmt])

        # ---- routes --------------------------------------------------------------------
        def do_GET(self) -> None:  # noqa: N802
            url = urlsplit(self.path)
            query = parse_qs(url.query)
            path = url.path.rstrip("/") or "/"
            if path == "/healthz":
                return self._json(HTTPStatus.OK, {"status": "ok", "version": __version__})
            if path == "/readyz":
                if ready.is_set():
                    return self._json(HTTPStatus.OK, {"status": "ready", "reports": len(store)})
                return self._json(HTTPStatus.SERVICE_UNAVAILABLE, {"status": "starting"})
            if path == "/metrics":
                body = render_prometheus(store.all()) + metrics.render()
                return self._send(HTTPStatus.OK, body.encode(), PROM_CONTENT_TYPE)
            if path == "/api/v1/reports":
                if not self._authorized():
                    return None
                return self._json(HTTPStatus.OK, {"reports": [summarize(r) for r in store.all()]})
            if path.startswith("/api/v1/reports/"):
                if not self._authorized():
                    return None
                report = store.get(unquote(path[len("/api/v1/reports/") :]))
                if report is None:
                    return self._error(HTTPStatus.NOT_FOUND, "no such report")
                return self._send_report(report, query)
            return self._error(HTTPStatus.NOT_FOUND, "not found")

        def do_POST(self) -> None:  # noqa: N802
            url = urlsplit(self.path)
            # Unread request bodies would corrupt a kept-alive connection, so close unless we consume the body.
            self.close_connection = True
            if (url.path.rstrip("/") or "/") != "/api/v1/analyze":
                return self._error(HTTPStatus.NOT_FOUND, "not found")
            if not self._authorized():
                return None
            length_header = self.headers.get("Content-Length")
            if length_header is None or not length_header.isdigit():
                return self._error(HTTPStatus.LENGTH_REQUIRED, "Content-Length header is required")
            length = int(length_header)
            if length == 0:
                return self._error(HTTPStatus.BAD_REQUEST, "empty body")
            if length > config.max_body_bytes:
                return self._error(HTTPStatus.REQUEST_ENTITY_TOO_LARGE, f"body exceeds {config.max_body_bytes} bytes")
            body = self.rfile.read(length)
            self.close_connection = False
            try:
                report = analysis.run(body, blocking=False)
            except AnalyzerError as exc:
                return self._error(HTTPStatus.UNPROCESSABLE_ENTITY, str(exc))
            except Exception:
                log.exception("Analysis failed")
                return self._error(HTTPStatus.INTERNAL_SERVER_ERROR, "internal error while analyzing the log")
            if report is None:
                return self._send(
                    HTTPStatus.SERVICE_UNAVAILABLE,
                    json.dumps({"error": "too many concurrent analyses, retry shortly"}).encode(),
                    _CONTENT_TYPES["json"],
                    {"Retry-After": "5"},
                )
            return self._send_report(report, parse_qs(url.query))

    return Handler


class AnalyzerServer:
    """Bundles the HTTP server, the optional watcher and the shared state (handy for embedding/tests)."""

    def __init__(self, config: ServerConfig) -> None:
        self.config = config
        self.store = ReportStore(config.max_reports)
        self.metrics = ServiceMetrics()
        self.analysis = Analysis(config, self.store, self.metrics)
        self.watcher: DirectoryWatcher | None = None
        self.ready = threading.Event()
        if config.watch_dir is not None:
            if not config.watch_dir.is_dir():
                raise AnalyzerError(f"Watch directory does not exist: {config.watch_dir}")
            self.watcher = DirectoryWatcher(self.analysis, config)
            self.ready = self.watcher.ready
        else:
            self.ready.set()
        handler = _make_handler(config, self.analysis, self.store, self.metrics, self.ready)
        self.httpd = ThreadingHTTPServer((config.host, config.port), handler)
        self.httpd.daemon_threads = True

    @property
    def port(self) -> int:
        return int(self.httpd.server_address[1])

    def start_background(self) -> None:
        if self.watcher:
            self.watcher.start()
        threading.Thread(target=self.httpd.serve_forever, name="sta-http", daemon=True).start()

    def stop(self) -> None:
        if self.watcher:
            self.watcher.stop_event.set()
        self.httpd.shutdown()
        self.httpd.server_close()

    def serve_forever(self) -> None:
        """Block until SIGINT/SIGTERM, then shut down gracefully."""
        if self.watcher:
            self.watcher.start()

        def _stop(signum: int, _frame: Any) -> None:
            log.info("Received signal %s, shutting down", signal.Signals(signum).name)
            threading.Thread(target=self.httpd.shutdown, daemon=True).start()

        signal.signal(signal.SIGTERM, _stop)
        signal.signal(signal.SIGINT, _stop)
        log.info("Listening on http://%s:%d (watch_dir=%s)", self.config.host, self.port, self.config.watch_dir)
        try:
            self.httpd.serve_forever()
        finally:
            if self.watcher:
                self.watcher.stop_event.set()
            self.httpd.server_close()
