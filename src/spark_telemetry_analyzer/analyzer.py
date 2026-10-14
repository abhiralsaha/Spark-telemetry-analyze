"""Run the rules over parsed metrics and assemble a JSON-serializable :class:`Report`."""

from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass
from typing import Any

from . import __version__
from .config import Settings
from .models import (
    COL_DURATION,
    COL_INPUT,
    COL_SHUFFLE_READ,
    ApplicationMetrics,
    Finding,
    Severity,
    StageMetrics,
)
from .parser import LogSource, load_application
from .rules import RULES, is_abnormal_removal
from .utils import iso_utc, now_iso, safe_div

log = logging.getLogger(__name__)

SCHEMA_VERSION = 1
_SEVERITY_PENALTY = {Severity.CRITICAL: 15, Severity.WARNING: 5, Severity.INFO: 1}
_PER_RULE_PENALTY_CAP = 25


@dataclass
class Report:
    """A finished analysis. ``data`` is plain JSON-compatible data (stable schema, see ``schema_version``)."""

    data: dict[str, Any]

    @property
    def app_id(self) -> str:
        return str(self.data["application"]["id"])

    @property
    def health_score(self) -> int:
        return int(self.data["health_score"])

    @property
    def findings(self) -> list[dict[str, Any]]:
        return list(self.data["findings"])

    @property
    def max_severity(self) -> Severity | None:
        levels = [Severity.parse(f["severity"]) for f in self.data["findings"]]
        return max(levels) if levels else None

    def count(self, severity: Severity) -> int:
        return sum(1 for f in self.data["findings"] if f["severity"] == severity.label)

    def has_findings_at_least(self, severity: Severity) -> bool:
        top = self.max_severity
        return top is not None and top >= severity


def health_score(findings: list[Finding]) -> int:
    """100 minus capped per-rule penalties; never below 0."""
    per_rule: dict[str, int] = defaultdict(int)
    for f in findings:
        per_rule[f.rule_id] += _SEVERITY_PENALTY[f.severity]
    penalty = sum(min(p, _PER_RULE_PENALTY_CAP) for p in per_rule.values())
    return max(0, 100 - penalty)


def grade(score: int) -> str:
    for threshold, letter in ((90, "A"), (75, "B"), (60, "C"), (40, "D")):
        if score >= threshold:
            return letter
    return "F"


def run_rules(app: ApplicationMetrics, settings: Settings) -> list[Finding]:
    findings: list[Finding] = []
    for rule_id, fn in RULES.items():
        if rule_id in settings.disabled_rules:
            continue
        try:
            findings.extend(fn(app, settings.thresholds))
        except Exception:  # a buggy rule must never take the whole analysis down
            log.exception("Rule %s failed; continuing without it", rule_id)
    findings.sort(key=lambda f: (-int(f.severity), f.rule_id, f.scope_id or ""))
    return findings


# --------------------------------------------------------------------------------------------
# Report assembly
# --------------------------------------------------------------------------------------------
def _stage_dict(st: StageMetrics) -> dict[str, Any]:
    dur = st.distribution(COL_DURATION)
    sr = st.distribution(COL_SHUFFLE_READ)
    inp = st.distribution(COL_INPUT)
    return {
        "key": st.key,
        "stage_id": st.stage_id,
        "attempt_id": st.attempt_id,
        "name": st.name,
        "details": st.details,
        "status": st.status,
        "job_ids": sorted(st.job_ids),
        "submitted_at": iso_utc(st.submission_time),
        "duration_ms": st.duration_ms,
        "failure_reason": st.failure_reason,
        "tasks": {
            "declared": st.num_tasks,
            "succeeded": st.tasks_succeeded,
            "failed": st.tasks_failed,
            "killed": st.tasks_killed,
            "speculative": st.tasks_speculative,
        },
        "task_duration_ms": {k: round(v) for k, v in dur.items()},
        "shuffle_read_per_task_bytes": {k: round(v) for k, v in sr.items()},
        "input_per_task_bytes": {k: round(v) for k, v in inp.items()},
        "totals": {
            "executor_run_ms": st.run_ms,
            "cpu_ms": st.cpu_ns // 1_000_000,
            "gc_ms": st.gc_ms,
            "gc_ratio": round(st.gc_ratio, 4),
            "deserialize_ms": st.deser_ms,
            "input_bytes": st.input_bytes,
            "output_bytes": st.output_bytes,
            "shuffle_read_bytes": st.shuffle_read_bytes,
            "shuffle_write_bytes": st.shuffle_write_bytes,
            "fetch_wait_ms": st.fetch_wait_ms,
            "memory_spilled_bytes": st.mem_spill,
            "disk_spilled_bytes": st.disk_spill,
            "result_bytes": st.result_size,
        },
        "slowest_tasks": st.slowest_tasks(),
        "failure_reasons": [{"reason": r, "count": c} for r, c in st.failure_reasons.most_common(5)],
    }


def _summary(app: ApplicationMetrics) -> dict[str, Any]:
    stages = list(app.stages.values())
    run_ms = sum(s.run_ms for s in stages)
    gc_ms = sum(s.gc_ms for s in stages)
    core_ms = sum(e.cores * app.executor_lifetime_ms(e) for e in app.executors.values() if e.cores > 0)
    real_executors = [e for e in app.executors.values() if e.executor_id != "driver"]
    lost = [e for e in real_executors if e.removed_ts is not None and is_abnormal_removal(e.removed_reason)]
    peak_cores = _peak_cores(app)
    return {
        "jobs": len(app.jobs),
        "jobs_failed": sum(1 for j in app.jobs.values() if j.status == "FAILED"),
        "stages": len({s.stage_id for s in stages if s.tasks_observed}),
        "stage_attempts": sum(1 for s in stages if s.tasks_observed),
        "stages_failed": sum(1 for s in stages if s.status == "FAILED"),
        "tasks": sum(s.tasks_observed for s in stages),
        "tasks_succeeded": sum(s.tasks_succeeded for s in stages),
        "tasks_failed": sum(s.tasks_failed for s in stages),
        "tasks_killed": sum(s.tasks_killed for s in stages),
        "executors": len(real_executors) or len(app.executors),
        "executors_lost": len(lost),
        "peak_cores": peak_cores,
        "executor_run_ms": run_ms,
        "cpu_ms": sum(s.cpu_ns for s in stages) // 1_000_000,
        "gc_ms": gc_ms,
        "gc_ratio": round(safe_div(gc_ms, run_ms), 4),
        "utilization": round(safe_div(run_ms, core_ms), 4) if core_ms else None,
        "input_bytes": sum(s.input_bytes for s in stages),
        "output_bytes": sum(s.output_bytes for s in stages),
        "shuffle_read_bytes": sum(s.shuffle_read_bytes for s in stages),
        "shuffle_write_bytes": sum(s.shuffle_write_bytes for s in stages),
        "memory_spilled_bytes": sum(s.mem_spill for s in stages),
        "disk_spilled_bytes": sum(s.disk_spill for s in stages),
        "result_bytes": sum(s.result_size for s in stages),
    }


def _peak_cores(app: ApplicationMetrics) -> int:
    deltas: list[tuple[int, int]] = []
    for ex in app.executors.values():
        if ex.cores <= 0:
            continue
        start = ex.added_ts if ex.added_ts is not None else (app.start_time or 0)
        end = ex.removed_ts if ex.removed_ts is not None else (app.effective_end or start)
        deltas.append((start, ex.cores))
        deltas.append((end, -ex.cores))
    current = peak = 0
    for _, delta in sorted(deltas, key=lambda d: (d[0], d[1])):  # removals sort before additions at equal time
        current += delta
        peak = max(peak, current)
    return peak


def _executor_dict(app: ApplicationMetrics, e: Any) -> dict[str, Any]:
    return {
        "id": e.executor_id,
        "host": e.host,
        "cores": e.cores,
        "added_at": iso_utc(e.added_ts),
        "removed_at": iso_utc(e.removed_ts),
        "removed_reason": e.removed_reason,
        "lifetime_ms": app.executor_lifetime_ms(e),
        "tasks": {"succeeded": e.tasks_succeeded, "failed": e.tasks_failed, "killed": e.tasks_killed},
        "run_ms": e.run_ms,
        "gc_ms": e.gc_ms,
        "gc_ratio": round(safe_div(e.gc_ms, e.run_ms), 4),
        "input_bytes": e.input_bytes,
        "shuffle_read_bytes": e.shuffle_read_bytes,
        "shuffle_write_bytes": e.shuffle_write_bytes,
        "disk_spilled_bytes": e.disk_spill,
    }


_KEY_PROPERTIES = (
    "spark.master",
    "spark.submit.deployMode",
    "spark.executor.instances",
    "spark.executor.cores",
    "spark.executor.memory",
    "spark.executor.memoryOverhead",
    "spark.driver.memory",
    "spark.dynamicAllocation.enabled",
    "spark.sql.shuffle.partitions",
    "spark.default.parallelism",
    "spark.sql.adaptive.enabled",
    "spark.sql.adaptive.skewJoin.enabled",
    "spark.serializer",
    "spark.speculation",
    "spark.memory.fraction",
    "spark.sql.files.maxPartitionBytes",
)


def build_report(app: ApplicationMetrics, findings: list[Finding], settings: Settings) -> Report:
    stages = sorted(
        (s for s in app.stages.values() if s.tasks_observed > 0),
        key=lambda s: (s.duration_ms, s.run_ms),
        reverse=True,
    )
    jobs = sorted(app.jobs.values(), key=lambda j: j.duration_ms, reverse=True)
    sql = sorted(app.sql.values(), key=lambda q: q.duration_ms, reverse=True)
    executors = sorted(app.executors.values(), key=lambda e: e.run_ms, reverse=True)
    score = health_score(findings)

    data: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "tool": {"name": "spark-telemetry-analyzer", "version": __version__},
        "generated_at": now_iso(),
        "application": {
            "id": app.app_id or "unknown",
            "name": app.app_name,
            "user": app.user,
            "spark_version": app.spark_version,
            "started_at": iso_utc(app.start_time),
            "ended_at": iso_utc(app.end_time),
            "duration_ms": app.duration_ms,
            "completed": app.has_app_end,
        },
        "health_score": score,
        "grade": grade(score),
        "summary": _summary(app),
        "findings": [f.to_dict() for f in findings],
        "finding_counts": {
            sev.label: sum(1 for f in findings if f.severity == sev) for sev in sorted(Severity, reverse=True)
        },
        "jobs": [
            {
                "job_id": j.job_id,
                "status": j.status,
                "duration_ms": j.duration_ms,
                "stage_ids": j.stage_ids,
                "description": j.description,
                "sql_execution_id": j.sql_execution_id,
                "failure_reason": j.failure_reason,
            }
            for j in jobs[: settings.max_listed_stages]
        ],
        "stages": [_stage_dict(s) for s in stages[: settings.max_listed_stages]],
        "stages_truncated": max(0, len(stages) - settings.max_listed_stages),
        "executors": [_executor_dict(app, e) for e in executors[: settings.max_listed_stages]],
        "sql_executions": [
            {
                "execution_id": q.execution_id,
                "description": q.description,
                "duration_ms": q.duration_ms,
                "job_ids": q.job_ids,
            }
            for q in sql[: settings.top_n * 3]
        ],
        "spark_properties": {k: app.spark_properties[k] for k in _KEY_PROPERTIES if k in app.spark_properties},
        "parse": app.parse_stats.to_dict(),
    }
    return Report(data)


def analyze_application(app: ApplicationMetrics, settings: Settings | None = None) -> Report:
    settings = settings or Settings()
    return build_report(app, run_rules(app, settings), settings)


def analyze_source(source: LogSource, settings: Settings | None = None) -> Report:
    """Parse and analyze an event log (path, directory, bytes or binary stream)."""
    settings = settings or Settings()
    app = load_application(source, settings)
    return analyze_application(app, settings)
