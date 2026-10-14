"""Prometheus text-format exposition (version 0.0.4) for analysis reports and service counters."""

from __future__ import annotations

import threading
from collections import defaultdict
from collections.abc import Iterable

from .analyzer import Report

CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8"

# (metric name, help, summary key, scale)
_APP_GAUGES: tuple[tuple[str, str, str, float], ...] = (
    ("sta_app_jobs", "Number of Spark jobs.", "jobs", 1),
    ("sta_app_jobs_failed", "Number of failed Spark jobs.", "jobs_failed", 1),
    ("sta_app_stages", "Number of distinct Spark stages that ran tasks.", "stages", 1),
    ("sta_app_tasks", "Number of finished task attempts.", "tasks", 1),
    ("sta_app_tasks_failed", "Number of failed task attempts.", "tasks_failed", 1),
    ("sta_app_executors_lost", "Executors lost unexpectedly.", "executors_lost", 1),
    ("sta_app_executor_run_seconds", "Total executor run time.", "executor_run_ms", 0.001),
    ("sta_app_gc_seconds", "Total JVM GC time inside tasks.", "gc_ms", 0.001),
    ("sta_app_gc_ratio", "GC time divided by executor run time.", "gc_ratio", 1),
    ("sta_app_utilization_ratio", "Busy core-time divided by provisioned core-time.", "utilization", 1),
    ("sta_app_input_bytes", "Bytes read from input sources.", "input_bytes", 1),
    ("sta_app_shuffle_read_bytes", "Bytes read in shuffles.", "shuffle_read_bytes", 1),
    ("sta_app_shuffle_write_bytes", "Bytes written in shuffles.", "shuffle_write_bytes", 1),
    ("sta_app_memory_spilled_bytes", "In-memory size of spilled data.", "memory_spilled_bytes", 1),
    ("sta_app_disk_spilled_bytes", "Bytes spilled to disk.", "disk_spilled_bytes", 1),
)


def _esc(value: object) -> str:
    return str(value).replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


def _labels(**labels: object) -> str:
    return "{" + ",".join(f'{k}="{_esc(v)}"' for k, v in labels.items()) + "}"


def _num(value: float) -> str:
    return repr(float(value)) if isinstance(value, float) else str(value)


def render_prometheus(reports: Iterable[Report]) -> str:
    reports = list(reports)
    lines: list[str] = []

    def family(name: str, help_text: str, kind: str = "gauge") -> None:
        lines.append(f"# HELP {name} {help_text}")
        lines.append(f"# TYPE {name} {kind}")

    family("sta_app_health_score", "Health score 0-100 (higher is better).")
    for r in reports:
        a = r.data["application"]
        lines.append(f"sta_app_health_score{_labels(app_id=a['id'], app_name=a['name'])} {r.health_score}")

    family("sta_app_duration_seconds", "Application wall-clock duration.")
    for r in reports:
        a = r.data["application"]
        lines.append(f"sta_app_duration_seconds{_labels(app_id=a['id'], app_name=a['name'])} {a['duration_ms'] / 1000}")

    for name, help_text, key, scale in _APP_GAUGES:
        samples = []
        for r in reports:
            value = r.data["summary"].get(key)
            if value is None:
                continue
            a = r.data["application"]
            samples.append(f"{name}{_labels(app_id=a['id'], app_name=a['name'])} {_num(value * scale if scale != 1 else value)}")
        if samples:
            family(name, help_text)
            lines.extend(samples)

    family("sta_findings", "Number of findings per application, rule and severity.")
    for r in reports:
        a = r.data["application"]
        counts: dict[tuple[str, str], int] = defaultdict(int)
        for f in r.data["findings"]:
            counts[(f["rule_id"], f["severity"])] += 1
        for (rule_id, severity), n in sorted(counts.items()):
            lines.append(f"sta_findings{_labels(app_id=a['id'], rule=rule_id, severity=severity)} {n}")

    return "\n".join(lines) + "\n"


class ServiceMetrics:
    """Thread-safe counters for the long-running service (requests, analyses, errors)."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._counters: dict[tuple[str, tuple[tuple[str, str], ...]], float] = defaultdict(float)
        self._gauges: dict[str, float] = {}

    def inc(self, name: str, amount: float = 1.0, **labels: str) -> None:
        with self._lock:
            self._counters[(name, tuple(sorted(labels.items())))] += amount

    def set_gauge(self, name: str, value: float) -> None:
        with self._lock:
            self._gauges[name] = value

    def render(self) -> str:
        with self._lock:
            counters = dict(self._counters)
            gauges = dict(self._gauges)
        lines: list[str] = []
        seen: set[str] = set()
        for (name, labels), value in sorted(counters.items()):
            if name not in seen:
                lines += [f"# HELP {name} Service counter.", f"# TYPE {name} counter"]
                seen.add(name)
            lines.append(f"{name}{_labels(**dict(labels)) if labels else ''} {_num(value)}")
        for name, value in sorted(gauges.items()):
            lines += [f"# HELP {name} Service gauge.", f"# TYPE {name} gauge", f"{name} {_num(value)}"]
        return "\n".join(lines) + ("\n" if lines else "")
