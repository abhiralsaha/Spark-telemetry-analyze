"""Compare two analysis reports (e.g. yesterday's run vs today's) and flag regressions."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .analyzer import Report, analyze_source
from .config import Settings
from .errors import ParseError
from .utils import fmt_bytes, fmt_duration, fmt_pct

# key path, label, formatter, higher_is_better
_METRICS: tuple[tuple[tuple[str, str], str, str, bool], ...] = (
    (("application", "duration_ms"), "Duration", "duration", False),
    (("summary", "executor_run_ms"), "Executor run time", "duration", False),
    (("summary", "tasks_failed"), "Failed tasks", "count", False),
    (("summary", "jobs_failed"), "Failed jobs", "count", False),
    (("summary", "executors_lost"), "Lost executors", "count", False),
    (("summary", "gc_ratio"), "GC ratio", "ratio", False),
    (("summary", "utilization"), "Core utilization", "ratio", True),
    (("summary", "shuffle_read_bytes"), "Shuffle read", "bytes", False),
    (("summary", "shuffle_write_bytes"), "Shuffle write", "bytes", False),
    (("summary", "disk_spilled_bytes"), "Disk spill", "bytes", False),
    (("root", "health_score"), "Health score", "count", True),
)

_FORMATTERS = {"duration": fmt_duration, "bytes": fmt_bytes, "ratio": fmt_pct, "count": lambda v: f"{v:,.0f}"}


def load_report(path: str | Path, settings: Settings | None = None) -> Report:
    """Load a saved JSON report, or analyze an event log if the file is not a report."""
    p = Path(path)
    if p.is_file():
        with p.open("rb") as fh:
            head = fh.read(512)
        if b'"schema_version"' in head and head.lstrip().startswith(b"{"):
            try:
                data = json.loads(p.read_text(encoding="utf-8"))
            except ValueError as exc:
                raise ParseError(f"{p} looks like a report but is not valid JSON: {exc}") from exc
            if isinstance(data, dict) and "application" in data and "findings" in data:
                return Report(data)
    return analyze_source(p, settings)


def _get(data: dict[str, Any], path: tuple[str, str]) -> float | None:
    section, key = path
    value = data.get(key) if section == "root" else data.get(section, {}).get(key)
    return None if value is None else float(value)


def compare_reports(base: Report, new: Report, tolerance: float = 0.10) -> dict[str, Any]:
    """Return per-metric deltas, new/resolved findings, and an overall ``regressed`` flag."""
    rows = []
    for path, label, kind, higher_better in _METRICS:
        b, n = _get(base.data, path), _get(new.data, path)
        if b is None or n is None:
            continue
        delta = n - b
        pct = (delta / b) if b else (None if delta == 0 else float("inf"))
        worse = (delta < 0) if higher_better else (delta > 0)
        significant = abs(pct) > tolerance if pct is not None else False
        if kind == "count" and b == 0 and delta != 0:
            significant = True
        verdict = "unchanged"
        if significant:
            verdict = "regression" if worse else "improvement"
        rows.append(
            {
                "metric": label,
                "base": b,
                "new": n,
                "delta": delta,
                "change_pct": None if pct is None or pct == float("inf") else round(pct * 100, 1),
                "kind": kind,
                "verdict": verdict,
            }
        )

    def keys(report: Report) -> set[tuple[str, str]]:
        return {(f["rule_id"], f["scope_id"] or "") for f in report.findings if f["severity"] != "info"}

    new_keys, base_keys = keys(new), keys(base)
    return {
        "base_app": base.data["application"]["id"],
        "new_app": new.data["application"]["id"],
        "tolerance": tolerance,
        "metrics": rows,
        "new_findings": sorted(f"{r}:{s}" if s else r for r, s in new_keys - base_keys),
        "resolved_findings": sorted(f"{r}:{s}" if s else r for r, s in base_keys - new_keys),
        "regressed": any(r["verdict"] == "regression" for r in rows) or bool(new_keys - base_keys),
    }


def render_comparison(cmp: dict[str, Any]) -> str:
    out = [f"Comparison: {cmp['base_app']}  ->  {cmp['new_app']}  (tolerance {cmp['tolerance'] * 100:.0f}%)", ""]
    out.append(f"{'Metric':<20}{'Base':>14}{'New':>14}{'Change':>10}  Verdict")
    out.append("-" * 70)
    for r in cmp["metrics"]:
        fmt = _FORMATTERS[r["kind"]]
        change = "n/a" if r["change_pct"] is None else f"{r['change_pct']:+.1f}%"
        out.append(f"{r['metric']:<20}{fmt(r['base']):>14}{fmt(r['new']):>14}{change:>10}  {r['verdict']}")
    if cmp["new_findings"]:
        out += ["", "New warning/critical findings: " + ", ".join(cmp["new_findings"])]
    if cmp["resolved_findings"]:
        out += ["Resolved findings: " + ", ".join(cmp["resolved_findings"])]
    out += ["", "Result: " + ("REGRESSION detected" if cmp["regressed"] else "no regression")]
    return "\n".join(out) + "\n"
