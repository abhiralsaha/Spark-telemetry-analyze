"""Detection rules.

Each rule is a function ``(app, thresholds) -> Iterable[Finding]`` registered under a stable id.
Rules are pure and independent, so they can be disabled individually via ``disabled_rules``.
"""

from __future__ import annotations

import statistics
from collections import Counter
from collections.abc import Callable, Iterable
from typing import Any

from .config import Thresholds
from .models import (
    COL_DURATION,
    COL_INPUT,
    COL_SHUFFLE_READ,
    ApplicationMetrics,
    Finding,
    Severity,
    StageMetrics,
)
from .utils import fmt_bytes, fmt_duration, fmt_pct, median, safe_div, truncate

RuleFn = Callable[[ApplicationMetrics, Thresholds], Iterable[Finding]]
RULES: dict[str, RuleFn] = {}

MAX_FINDINGS_PER_RULE = 5

_MEMORY_PATTERNS = (
    "outofmemory",
    "out of memory",
    "oom",
    "exceeding memory limits",
    "exceeded memory limit",
    "memoryoverhead",
    "container killed by yarn",
    "killed by kubernetes",
    "exit code 137",
    "java heap space",
    "gc overhead limit",
)
_BENIGN_REMOVAL = (
    "killed by driver",
    "requested to kill",
    "driver requested",
    "decommission",
    "idle",
    "stopped",
    "finished",
    "shut down",
    "shutdown",
    "dynamic allocation",
)


def is_abnormal_removal(reason: str | None) -> bool:
    """True when an executor was removed for a reason other than a normal scale-down/shutdown."""
    if not reason:
        return False
    low = reason.lower()
    return not any(p in low for p in _BENIGN_REMOVAL)


def rule(rule_id: str) -> Callable[[RuleFn], RuleFn]:
    def register(fn: RuleFn) -> RuleFn:
        RULES[rule_id] = fn
        return fn

    return register


# --------------------------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------------------------
def _sid(stage: StageMetrics) -> str:
    return stage.key


def _label(stage: StageMetrics) -> str:
    name = truncate(stage.name, 60) or "unnamed"
    return f"stage {stage.stage_id} (attempt {stage.attempt_id}, '{name}')"


def _active_stages(app: ApplicationMetrics) -> list[StageMetrics]:
    return [s for s in app.stages.values() if s.tasks_observed > 0]


def _long_enough(stage: StageMetrics, th: Thresholds) -> bool:
    return max(stage.duration_ms, stage.max_task_ms) >= th.min_stage_duration_ms


def _sev(value: float, warn: float, crit: float) -> Severity | None:
    if value >= crit:
        return Severity.CRITICAL
    if value >= warn:
        return Severity.WARNING
    return None


def _top(items: list[tuple[float, StageMetrics]]) -> list[tuple[float, StageMetrics]]:
    return sorted(items, key=lambda x: x[0], reverse=True)[:MAX_FINDINGS_PER_RULE]


def _is_memory_error(text: str | None) -> bool:
    low = (text or "").lower()
    return any(p in low for p in _MEMORY_PATTERNS)


# --------------------------------------------------------------------------------------------
# Failures
# --------------------------------------------------------------------------------------------
@rule("JOB_FAILURE")
def job_failure(app: ApplicationMetrics, th: Thresholds) -> Iterable[Finding]:
    """Jobs that ended in a failed state."""
    failed = [j for j in app.jobs.values() if j.status == "FAILED"]
    for job in sorted(failed, key=lambda j: j.job_id)[:MAX_FINDINGS_PER_RULE]:
        yield Finding(
            "JOB_FAILURE",
            Severity.CRITICAL,
            f"Job {job.job_id} failed",
            truncate(job.failure_reason, 300) or "The job ended in a failed state.",
            scope="job",
            scope_id=str(job.job_id),
            evidence={"job_id": job.job_id, "stage_ids": job.stage_ids, "reason": job.failure_reason},
            recommendation="Inspect the failing stage's task errors and executor logs; fix the root cause before tuning.",
        )
    if len(failed) > MAX_FINDINGS_PER_RULE:
        yield Finding(
            "JOB_FAILURE",
            Severity.CRITICAL,
            f"{len(failed) - MAX_FINDINGS_PER_RULE} more failed job(s)",
            f"{len(failed)} jobs failed in total; only the first {MAX_FINDINGS_PER_RULE} are listed.",
            evidence={"failed_jobs": len(failed)},
        )


@rule("STAGE_RETRY")
def stage_retry(app: ApplicationMetrics, th: Thresholds) -> Iterable[Finding]:
    """Stages that had to be re-run (lost executors, FetchFailed)."""
    attempts: dict[int, list[StageMetrics]] = {}
    for st in app.stages.values():
        if st.tasks_observed > 0 or st.submission_time is not None:
            attempts.setdefault(st.stage_id, []).append(st)
    retried = {sid: sts for sid, sts in attempts.items() if len(sts) > 1}
    for sid, sts in sorted(retried.items())[:MAX_FINDINGS_PER_RULE]:
        fetch_failed = sum(s.failure_reasons.get("FetchFailed", 0) for s in sts)
        yield Finding(
            "STAGE_RETRY",
            Severity.WARNING,
            f"Stage {sid} was retried {len(sts) - 1} time(s)",
            "Re-running a stage wastes compute and usually follows lost executors or shuffle fetch failures."
            + (f" {fetch_failed} FetchFailed task(s) were observed." if fetch_failed else ""),
            scope="stage",
            scope_id=str(sid),
            evidence={"attempts": len(sts), "fetch_failed_tasks": fetch_failed},
            recommendation="Check for lost executors / node preemption, enable the external shuffle service or "
            "shuffle tracking, and consider raising spark.executor.memoryOverhead.",
        )


@rule("TASK_FAILURES")
def task_failures(app: ApplicationMetrics, th: Thresholds) -> Iterable[Finding]:
    """Failed task attempts per stage with the top failure reasons."""
    scored: list[tuple[float, StageMetrics]] = []
    for st in _active_stages(app):
        if st.tasks_failed:
            scored.append((st.tasks_failed, st))
    for _, st in _top(scored):
        ratio = safe_div(st.tasks_failed, st.tasks_observed)
        severity = _sev(ratio, th.failed_task_ratio_warn, th.failed_task_ratio_crit) or Severity.INFO
        top = st.failure_reasons.most_common(3)
        yield Finding(
            "TASK_FAILURES",
            severity,
            f"{st.tasks_failed} failed task(s) in stage {st.stage_id}",
            f"{fmt_pct(ratio)} of tasks in {_label(st)} failed. Most common: "
            + "; ".join(f"{r} (x{c})" for r, c in top),
            scope="stage",
            scope_id=_sid(st),
            evidence={
                "failed_tasks": st.tasks_failed,
                "observed_tasks": st.tasks_observed,
                "failure_ratio": round(ratio, 4),
                "top_reasons": [{"reason": r, "count": c} for r, c in top],
            },
            recommendation="Failed attempts re-run work. Review the top failure reasons; if they are transient "
            "(executor loss, fetch failures) address cluster stability, otherwise fix the code/data issue.",
        )


@rule("MEMORY_ERRORS")
def memory_errors(app: ApplicationMetrics, th: Thresholds) -> Iterable[Finding]:
    """OOM errors and container memory-limit kills (YARN/Kubernetes)."""
    hits: Counter[str] = Counter()
    for st in app.stages.values():
        for reason, count in st.failure_reasons.items():
            if _is_memory_error(reason):
                hits[truncate(reason, 140)] += count
        if _is_memory_error(st.failure_reason):
            hits[truncate(st.failure_reason, 140)] += 1
    for ex in app.executors.values():
        if _is_memory_error(ex.removed_reason):
            hits[truncate(ex.removed_reason, 140)] += 1
    for job in app.jobs.values():
        if _is_memory_error(job.failure_reason):
            hits[truncate(job.failure_reason, 140)] += 1
    if not hits:
        return
    yield Finding(
        "MEMORY_ERRORS",
        Severity.CRITICAL,
        "Out-of-memory or container memory-limit kills detected",
        f"{sum(hits.values())} memory-related failure event(s), e.g. " + "; ".join(r for r, _ in hits.most_common(2)),
        evidence={"occurrences": [{"reason": r, "count": c} for r, c in hits.most_common(5)]},
        recommendation="Raise spark.executor.memory and/or spark.executor.memoryOverhead (YARN/K8s kills usually mean "
        "off-heap overhead is too small), reduce partition size (more partitions), avoid collecting large results, "
        "and check for skewed keys.",
    )


# --------------------------------------------------------------------------------------------
# Skew and stragglers
# --------------------------------------------------------------------------------------------
@rule("DATA_SKEW")
def data_skew(app: ApplicationMetrics, th: Thresholds) -> Iterable[Finding]:
    """Skewed data volume across the tasks of a stage (duration-only outliers: STRAGGLER_TASKS)."""
    scored: list[tuple[float, StageMetrics]] = []
    info: dict[str, dict[str, Any]] = {}
    for st in _active_stages(app):
        if st.tasks_succeeded < th.min_tasks_for_skew or not _long_enough(st, th):
            continue
        data_kind, data_ratio, data_max, data_p50 = "", 0.0, 0.0, 0.0
        for kind, col in (("shuffle read", COL_SHUFFLE_READ), ("input", COL_INPUT)):
            d = st.distribution(col)
            ratio = safe_div(d["max"], max(d["p50"], 1.0))
            if d["max"] >= th.skew_min_bytes and ratio > data_ratio:
                data_kind, data_ratio, data_max, data_p50 = kind, ratio, d["max"], d["p50"]
        if data_ratio < th.skew_data_ratio_warn:
            continue
        dur = st.distribution(COL_DURATION)
        dur_ratio = safe_div(dur["max"], max(dur["p50"], 1.0))
        scored.append((max(dur_ratio, data_ratio), st))
        info[st.key] = {
            "dur": dur,
            "dur_ratio": dur_ratio,
            "kind": data_kind,
            "ratio": data_ratio,
            "max": data_max,
            "p50": data_p50,
        }

    for _, st in _top(scored):
        i = info[st.key]
        dur = i["dur"]
        critical = i["ratio"] >= th.skew_data_ratio_crit or (
            dur["max"] >= th.skew_min_max_task_ms and i["dur_ratio"] >= th.skew_duration_ratio_crit
        )
        wasted = max(0.0, dur["max"] - dur["p50"])
        yield Finding(
            "DATA_SKEW",
            Severity.CRITICAL if critical else Severity.WARNING,
            f"Data skew in stage {st.stage_id}",
            f"In {_label(st)} the largest task read {fmt_bytes(i['max'])} of {i['kind']} data vs a median of "
            f"{fmt_bytes(i['p50'])} ({i['ratio']:.1f}x). The slowest task took {fmt_duration(dur['max'])} vs "
            f"{fmt_duration(dur['p50'])} median, so up to {fmt_duration(wasted)} of stage time is spent waiting on it.",
            scope="stage",
            scope_id=_sid(st),
            evidence={
                "tasks": st.tasks_succeeded,
                "data_kind": i["kind"],
                "data_ratio": round(i["ratio"], 2),
                "data_max_bytes": int(i["max"]),
                "data_median_bytes": int(i["p50"]),
                "duration_ms": {k: round(v) for k, v in dur.items()},
                "duration_ratio": round(i["dur_ratio"], 2),
                "slowest_tasks": st.slowest_tasks()[:3],
            },
            recommendation="Enable Adaptive Query Execution skew-join handling (spark.sql.adaptive.enabled and "
            "spark.sql.adaptive.skewJoin.enabled), salt hot keys, pre-aggregate before the shuffle, or broadcast the "
            "smaller side of the join.",
        )


@rule("STRAGGLER_TASKS")
def straggler_tasks(app: ApplicationMetrics, th: Thresholds) -> Iterable[Finding]:
    """A few tasks much slower than the rest *without* matching data skew (bad node, GC, contention)."""
    scored: list[tuple[float, StageMetrics]] = []
    for st in _active_stages(app):
        if st.tasks_succeeded < th.min_tasks_for_skew or not _long_enough(st, th):
            continue
        dur = st.distribution(COL_DURATION)
        ratio = safe_div(dur["max"], max(dur["p50"], 1.0))
        if dur["max"] < th.skew_min_max_task_ms or ratio < th.skew_duration_ratio_warn:
            continue
        sr = st.distribution(COL_SHUFFLE_READ)
        inp = st.distribution(COL_INPUT)
        data_ratio = max(
            safe_div(sr["max"], max(sr["p50"], 1.0)) if sr["max"] >= th.skew_min_bytes else 0.0,
            safe_div(inp["max"], max(inp["p50"], 1.0)) if inp["max"] >= th.skew_min_bytes else 0.0,
        )
        if data_ratio >= th.skew_data_ratio_warn:
            continue  # explained by DATA_SKEW
        scored.append((ratio, st))

    for ratio, st in _top(scored):
        dur = st.distribution(COL_DURATION)
        slow = st.slowest_tasks()
        executors = Counter(t["executor_id"] for t in slow)
        hot_executor, hot_count = executors.most_common(1)[0] if executors else ("", 0)
        node_hint = (
            f" {hot_count} of the {len(slow)} slowest tasks ran on executor {hot_executor}, which points at a "
            "degraded node."
            if hot_count >= 3
            else ""
        )
        yield Finding(
            "STRAGGLER_TASKS",
            Severity.CRITICAL if ratio >= th.skew_duration_ratio_crit else Severity.WARNING,
            f"Straggler tasks in stage {st.stage_id}",
            f"In {_label(st)} the slowest task took {fmt_duration(dur['max'])} vs a median of "
            f"{fmt_duration(dur['p50'])} ({ratio:.1f}x) although input sizes are balanced.{node_hint}",
            scope="stage",
            scope_id=_sid(st),
            evidence={
                "duration_ms": {k: round(v) for k, v in dur.items()},
                "ratio": round(ratio, 2),
                "slowest_tasks": slow[:3],
            },
            recommendation="Enable speculative execution (spark.speculation=true), check the executors/hosts that ran the "
            "slowest tasks for noisy neighbours or bad disks, and look at GC time in those tasks.",
        )


# --------------------------------------------------------------------------------------------
# Memory / GC / serialization
# --------------------------------------------------------------------------------------------
@rule("DISK_SPILL")
def disk_spill(app: ApplicationMetrics, th: Thresholds) -> Iterable[Finding]:
    """Execution memory spilled to disk."""
    scored: list[tuple[float, StageMetrics]] = []
    for st in _active_stages(app):
        if st.disk_spill >= th.spill_disk_warn_bytes or st.mem_spill >= th.spill_memory_warn_bytes:
            scored.append((st.disk_spill + st.mem_spill * 0.1, st))
    for _, st in _top(scored):
        severity = Severity.CRITICAL if st.disk_spill >= th.spill_disk_crit_bytes else Severity.WARNING
        yield Finding(
            "DISK_SPILL",
            severity,
            f"Spill to disk in stage {st.stage_id}",
            f"{_label(st)} spilled {fmt_bytes(st.disk_spill)} to disk ({fmt_bytes(st.mem_spill)} in-memory size). "
            "Spilling means execution memory was too small for the working set and adds heavy disk I/O.",
            scope="stage",
            scope_id=_sid(st),
            evidence={"disk_spilled_bytes": st.disk_spill, "memory_spilled_bytes": st.mem_spill, "tasks": st.tasks_observed},
            recommendation="Increase the number of shuffle partitions (smaller tasks), raise spark.executor.memory or "
            "spark.memory.fraction, reduce executor cores per executor, or filter/aggregate earlier.",
        )


@rule("GC_PRESSURE")
def gc_pressure(app: ApplicationMetrics, th: Thresholds) -> Iterable[Finding]:
    """JVM garbage collection consuming a large share of task time."""
    scored: list[tuple[float, StageMetrics]] = []
    for st in _active_stages(app):
        if st.run_ms >= th.min_run_ms_for_ratios and st.gc_ratio >= th.gc_ratio_warn:
            scored.append((st.gc_ratio, st))
    for ratio, st in _top(scored):
        yield Finding(
            "GC_PRESSURE",
            _sev(ratio, th.gc_ratio_warn, th.gc_ratio_crit) or Severity.WARNING,
            f"High garbage-collection time in stage {st.stage_id}",
            f"JVM GC consumed {fmt_pct(ratio)} of executor run time in {_label(st)} "
            f"({fmt_duration(st.gc_ms)} of {fmt_duration(st.run_ms)}).",
            scope="stage",
            scope_id=_sid(st),
            evidence={"gc_ms": st.gc_ms, "run_ms": st.run_ms, "gc_ratio": round(ratio, 4)},
            recommendation="Give executors more memory or fewer cores, prefer the built-in DataFrame API over RDD/UDF "
            "object-heavy code, use Kryo serialization, and consider G1GC (-XX:+UseG1GC).",
        )


@rule("SERIALIZATION_OVERHEAD")
def serialization_overhead(app: ApplicationMetrics, th: Thresholds) -> Iterable[Finding]:
    """Large share of task time spent deserializing closures."""
    scored: list[tuple[float, StageMetrics]] = []
    for st in _active_stages(app):
        total = st.run_ms + st.deser_ms
        if total < th.min_run_ms_for_ratios:
            continue
        ratio = safe_div(st.deser_ms, total)
        if ratio >= th.deserialize_ratio_warn:
            scored.append((ratio, st))
    for ratio, st in _top(scored):
        yield Finding(
            "SERIALIZATION_OVERHEAD",
            _sev(ratio, th.deserialize_ratio_warn, th.deserialize_ratio_crit) or Severity.WARNING,
            f"Task deserialization overhead in stage {st.stage_id}",
            f"{fmt_pct(ratio)} of task time in {_label(st)} is spent deserializing the task closure "
            f"({fmt_duration(st.deser_ms)}).",
            scope="stage",
            scope_id=_sid(st),
            evidence={"deserialize_ms": st.deser_ms, "run_ms": st.run_ms, "ratio": round(ratio, 4)},
            recommendation="Large closures are shipped with every task: broadcast big lookup objects, avoid capturing "
            "large driver-side collections, and use fewer, larger tasks.",
        )


# --------------------------------------------------------------------------------------------
# Partitioning and parallelism
# --------------------------------------------------------------------------------------------
@rule("SMALL_TASKS")
def small_tasks(app: ApplicationMetrics, th: Thresholds) -> Iterable[Finding]:
    """Thousands of tiny tasks (too many partitions)."""
    scored: list[tuple[float, StageMetrics]] = []
    for st in _active_stages(app):
        if st.tasks_succeeded < th.small_task_min_count:
            continue
        med = median(st.reservoir.column(COL_DURATION))
        if med < th.small_task_median_ms:
            scored.append((float(st.tasks_succeeded), st))
    for _, st in _top(scored):
        med = median(st.reservoir.column(COL_DURATION))
        yield Finding(
            "SMALL_TASKS",
            Severity.WARNING,
            f"Too many tiny tasks in stage {st.stage_id}",
            f"{_label(st)} ran {st.tasks_succeeded:,} tasks with a median duration of {fmt_duration(med)}. Task "
            "scheduling and serialization overhead likely dominates useful work.",
            scope="stage",
            scope_id=_sid(st),
            evidence={"tasks": st.tasks_succeeded, "median_task_ms": round(med)},
            recommendation="Reduce partition counts (coalesce, lower spark.sql.shuffle.partitions), enable AQE partition "
            "coalescing (spark.sql.adaptive.coalescePartitions.enabled), or compact small input files.",
        )


@rule("SCHEDULING_OVERHEAD")
def scheduling_overhead(app: ApplicationMetrics, th: Thresholds) -> Iterable[Finding]:
    """Task wall time not spent doing useful work (launch/scheduler delay)."""
    scored: list[tuple[float, StageMetrics]] = []
    for st in _active_stages(app):
        if st.tasks_succeeded < th.scheduling_overhead_min_tasks or st.wall_ms < th.min_run_ms_for_ratios:
            continue
        useful = st.run_ms + st.deser_ms + st.result_ser_ms
        overhead = max(0, st.wall_ms - useful)
        ratio = safe_div(overhead, st.wall_ms)
        if ratio >= th.scheduling_overhead_ratio_warn:
            scored.append((ratio, st))
    for ratio, st in _top(scored):
        yield Finding(
            "SCHEDULING_OVERHEAD",
            Severity.WARNING,
            f"Scheduler / launch overhead in stage {st.stage_id}",
            f"About {fmt_pct(ratio)} of task wall time in {_label(st)} is not spent running, deserializing or "
            "serializing (scheduler delay, result fetch, launch latency).",
            scope="stage",
            scope_id=_sid(st),
            evidence={"overhead_ratio": round(ratio, 4), "task_wall_ms": st.wall_ms, "run_ms": st.run_ms},
            recommendation="Use fewer, larger tasks, check driver CPU/GC and network latency between driver and "
            "executors, and avoid very large task results.",
        )


@rule("LOW_PARALLELISM")
def low_parallelism(app: ApplicationMetrics, th: Thresholds) -> Iterable[Finding]:
    """Long stages with fewer tasks than available cores."""
    scored: list[tuple[float, StageMetrics]] = []
    cores_by_stage: dict[str, int] = {}
    for st in _active_stages(app):
        if st.duration_ms < th.low_parallelism_min_stage_ms:
            continue
        cores = app.cores_overlapping(st.submission_time, st.completion_time)
        if cores < 2:
            continue
        if st.tasks_observed < cores:
            cores_by_stage[st.key] = cores
            scored.append((float(st.duration_ms), st))
    for _, st in _top(scored):
        cores = cores_by_stage[st.key]
        severity = Severity.CRITICAL if st.tasks_observed * 10 <= cores and st.duration_ms >= 5 * 60_000 else Severity.WARNING
        yield Finding(
            "LOW_PARALLELISM",
            severity,
            f"Stage {st.stage_id} cannot use the whole cluster",
            f"{_label(st)} ran only {st.tasks_observed} task(s) for {fmt_duration(st.duration_ms)} while up to "
            f"{cores} cores were available.",
            scope="stage",
            scope_id=_sid(st),
            evidence={"tasks": st.tasks_observed, "available_cores": cores, "duration_ms": st.duration_ms},
            recommendation="Increase partitions (repartition, spark.sql.shuffle.partitions, spark.default.parallelism), "
            "avoid single-partition operations such as global sorts or coalesce(1), and split large non-splittable files.",
        )


@rule("LARGE_PARTITIONS")
def large_partitions(app: ApplicationMetrics, th: Thresholds) -> Iterable[Finding]:
    """Average partition size large enough to risk spills/OOM."""
    scored: list[tuple[float, StageMetrics]] = []
    for st in _active_stages(app):
        if st.tasks_succeeded < 2:
            continue
        per_task = max(st.shuffle_read_bytes, st.input_bytes) / st.tasks_observed
        if per_task >= th.large_partition_warn_bytes:
            scored.append((per_task, st))
    for per_task, st in _top(scored):
        yield Finding(
            "LARGE_PARTITIONS",
            _sev(per_task, th.large_partition_warn_bytes, th.large_partition_crit_bytes) or Severity.WARNING,
            f"Very large partitions in stage {st.stage_id}",
            f"{_label(st)} processes on average {fmt_bytes(per_task)} per task. Large partitions risk spills, GC "
            "pressure and out-of-memory errors.",
            scope="stage",
            scope_id=_sid(st),
            evidence={"avg_bytes_per_task": int(per_task), "tasks": st.tasks_observed},
            recommendation="Aim for 128-512 MiB per partition: raise spark.sql.shuffle.partitions or call repartition(), "
            "and lower spark.sql.files.maxPartitionBytes for input stages.",
        )


# --------------------------------------------------------------------------------------------
# Shuffle / driver
# --------------------------------------------------------------------------------------------
@rule("SHUFFLE_FETCH_WAIT")
def shuffle_fetch_wait(app: ApplicationMetrics, th: Thresholds) -> Iterable[Finding]:
    """Tasks blocked waiting for remote shuffle blocks."""
    scored: list[tuple[float, StageMetrics]] = []
    for st in _active_stages(app):
        if st.run_ms < th.min_run_ms_for_ratios:
            continue
        ratio = safe_div(st.fetch_wait_ms, st.run_ms)
        if ratio >= th.fetch_wait_ratio_warn:
            scored.append((ratio, st))
    for ratio, st in _top(scored):
        yield Finding(
            "SHUFFLE_FETCH_WAIT",
            Severity.WARNING,
            f"Tasks wait on shuffle fetches in stage {st.stage_id}",
            f"Tasks in {_label(st)} spent {fmt_pct(ratio)} of their run time blocked fetching shuffle blocks "
            f"({fmt_bytes(st.shuffle_read_bytes)} read, {fmt_duration(st.fetch_wait_ms)} waiting).",
            scope="stage",
            scope_id=_sid(st),
            evidence={"fetch_wait_ms": st.fetch_wait_ms, "run_ms": st.run_ms, "ratio": round(ratio, 4)},
            recommendation="Reduce shuffle volume (filter/aggregate earlier, broadcast joins), enable shuffle compression, "
            "use the external shuffle service or faster local disks, and increase spark.reducer.maxSizeInFlight.",
        )


@rule("HEAVY_SHUFFLE")
def heavy_shuffle(app: ApplicationMetrics, th: Thresholds) -> Iterable[Finding]:
    """Very large total shuffle volume."""
    written = sum(s.shuffle_write_bytes for s in app.stages.values())
    if written < th.heavy_shuffle_bytes:
        return
    heaviest = max(app.stages.values(), key=lambda s: s.shuffle_write_bytes)
    yield Finding(
        "HEAVY_SHUFFLE",
        Severity.INFO,
        "Very large shuffle volume",
        f"The application wrote {fmt_bytes(written)} of shuffle data; the heaviest stage was {_label(heaviest)} with "
        f"{fmt_bytes(heaviest.shuffle_write_bytes)}.",
        evidence={"shuffle_write_bytes": written, "heaviest_stage": heaviest.key},
        recommendation="Shuffle is the most expensive Spark operation: prune columns early, push down filters, use "
        "broadcast joins where possible and make sure map-side combine is in effect.",
    )


@rule("LARGE_RESULT")
def large_result(app: ApplicationMetrics, th: Thresholds) -> Iterable[Finding]:
    """Large results collected back to the driver."""
    scored: list[tuple[float, StageMetrics]] = []
    for st in _active_stages(app):
        if st.result_size >= th.large_result_warn_bytes:
            scored.append((float(st.result_size), st))
    for size, st in _top(scored):
        yield Finding(
            "LARGE_RESULT",
            _sev(size, th.large_result_warn_bytes, th.large_result_crit_bytes) or Severity.WARNING,
            f"Large result returned to the driver in stage {st.stage_id}",
            f"{_label(st)} sent {fmt_bytes(size)} of task results to the driver (collect/toPandas/take?).",
            scope="stage",
            scope_id=_sid(st),
            evidence={"result_bytes": st.result_size},
            recommendation="Write results to storage instead of collecting them; if collecting is required, aggregate or "
            "limit first and size spark.driver.memory / spark.driver.maxResultSize accordingly.",
        )


# --------------------------------------------------------------------------------------------
# Executors / cluster
# --------------------------------------------------------------------------------------------
@rule("EXECUTOR_LOSS")
def executor_loss(app: ApplicationMetrics, th: Thresholds) -> Iterable[Finding]:
    """Executors lost for reasons other than scale-down."""
    removed = [e for e in app.executors.values() if e.removed_ts is not None and e.executor_id != "driver"]
    abnormal = [e for e in removed if is_abnormal_removal(e.removed_reason)]
    if not abnormal:
        return
    reasons = Counter(truncate(e.removed_reason, 120) for e in abnormal)
    critical = len(abnormal) >= 3 or (len(abnormal) >= 2 and len(abnormal) >= 0.25 * max(len(app.executors), 1))
    severity = Severity.CRITICAL if critical else Severity.WARNING
    yield Finding(
        "EXECUTOR_LOSS",
        severity,
        f"{len(abnormal)} executor(s) were lost unexpectedly",
        "Executors disappeared for reasons other than a normal scale-down: "
        + "; ".join(f"{r} (x{c})" for r, c in reasons.most_common(3)),
        evidence={
            "lost_executors": len(abnormal),
            "total_executors": len(app.executors),
            "reasons": [{"reason": r, "count": c} for r, c in reasons.most_common(5)],
        },
        recommendation="Look for OOM kills, node preemption/spot reclamation, or failing disks. Cached data and shuffle "
        "output on lost executors must be recomputed.",
    )


@rule("EXECUTOR_IMBALANCE")
def executor_imbalance(app: ApplicationMetrics, th: Thresholds) -> Iterable[Finding]:
    """Work unevenly distributed across long-lived executors."""
    app_ms = app.duration_ms
    if app_ms <= 0:
        return
    steady = [
        e
        for e in app.executors.values()
        if e.executor_id != "driver" and e.tasks_total >= 10 and app.executor_lifetime_ms(e) >= 0.8 * app_ms
    ]
    if len(steady) < th.executor_imbalance_min_executors:
        return
    runs = [e.run_ms for e in steady]
    med = statistics.median(runs)
    top = max(steady, key=lambda e: e.run_ms)
    ratio = safe_div(top.run_ms, med)
    if med <= 0 or ratio < th.executor_imbalance_ratio_warn:
        return
    yield Finding(
        "EXECUTOR_IMBALANCE",
        Severity.WARNING,
        "Work is unevenly spread across executors",
        f"Executor {top.executor_id} ({top.host or 'unknown host'}) ran {fmt_duration(top.run_ms)} of tasks, "
        f"{ratio:.1f}x the median executor ({fmt_duration(med)}).",
        scope="executor",
        scope_id=top.executor_id,
        evidence={"executor_id": top.executor_id, "run_ms": top.run_ms, "median_run_ms": round(med), "ratio": round(ratio, 2)},
        recommendation="Check data locality settings (spark.locality.wait), cached-partition placement and skewed keys; "
        "uneven executors waste the rest of the cluster.",
    )


@rule("LOW_UTILIZATION")
def low_utilization(app: ApplicationMetrics, th: Thresholds) -> Iterable[Finding]:
    """Provisioned core-time mostly idle."""
    if app.duration_ms < th.utilization_min_app_ms:
        return
    core_ms = sum(e.cores * app.executor_lifetime_ms(e) for e in app.executors.values() if e.cores > 0)
    if core_ms <= 0:
        return
    busy_ms = sum(e.run_ms for e in app.executors.values())
    util = busy_ms / core_ms
    if util >= th.utilization_warn:
        return
    severity = Severity.CRITICAL if util < th.utilization_crit else Severity.WARNING
    dyn = app.spark_properties.get("spark.dynamicAllocation.enabled", "false").lower() == "true"
    yield Finding(
        "LOW_UTILIZATION",
        severity,
        "Cluster cores were mostly idle",
        f"Tasks kept only {fmt_pct(util)} of the {fmt_duration(core_ms)} of provisioned core-time busy "
        f"(dynamic allocation {'enabled' if dyn else 'disabled'}).",
        evidence={"utilization": round(util, 4), "core_ms": core_ms, "busy_ms": busy_ms, "dynamic_allocation": dyn},
        recommendation="Right-size the cluster or enable dynamic allocation (spark.dynamicAllocation.enabled=true with "
        "shuffle tracking) so executors are released while the driver or a single-threaded stage is working.",
    )


# --------------------------------------------------------------------------------------------
# Configuration & data quality
# --------------------------------------------------------------------------------------------
@rule("CONFIG_ADVISORY")
def config_advisory(app: ApplicationMetrics, th: Thresholds) -> Iterable[Finding]:
    """Spark configuration that is likely hurting this workload."""
    props = app.spark_properties
    if not props:
        return
    shuffle_write = sum(s.shuffle_write_bytes for s in app.stages.values())
    major = app.spark_version.split(".")[0] if app.spark_version else ""
    if props.get("spark.sql.adaptive.enabled", "").lower() == "false" and (shuffle_write > 0 or app.sql):
        yield Finding(
            "CONFIG_ADVISORY",
            Severity.INFO,
            "Adaptive Query Execution is disabled",
            "spark.sql.adaptive.enabled=false. AQE coalesces shuffle partitions, switches join strategies and "
            "splits skewed partitions at runtime.",
            evidence={"spark.sql.adaptive.enabled": "false"},
            recommendation="Set spark.sql.adaptive.enabled=true (default since Spark 3.2).",
        )
    serializer = props.get("spark.serializer", "")
    if shuffle_write >= (1 << 30) and "Kryo" not in serializer:
        yield Finding(
            "CONFIG_ADVISORY",
            Severity.INFO,
            "Java serialization used with significant shuffle volume",
            f"The application shuffled {fmt_bytes(shuffle_write)} using "
            f"{serializer or 'the default JavaSerializer'}, which is slower and larger than Kryo for RDD workloads.",
            evidence={"spark.serializer": serializer or "default", "shuffle_write_bytes": shuffle_write},
            recommendation="Set spark.serializer=org.apache.spark.serializer.KryoSerializer (DataFrame shuffles already "
            "use Tungsten and are unaffected).",
        )
    if major.isdigit() and int(major) < 3:
        yield Finding(
            "CONFIG_ADVISORY",
            Severity.INFO,
            f"Old Spark version ({app.spark_version})",
            "Spark 3.x brings AQE, dynamic partition pruning and many shuffle/join improvements.",
            evidence={"spark_version": app.spark_version},
            recommendation="Plan an upgrade to a supported Spark 3.x or 4.x release.",
        )


@rule("INCOMPLETE_LOG")
def incomplete_log(app: ApplicationMetrics, th: Thresholds) -> Iterable[Finding]:
    """Data-quality notes: missing app-end event, malformed lines."""
    if not app.has_app_end:
        yield Finding(
            "INCOMPLETE_LOG",
            Severity.INFO,
            "Event log has no application-end event",
            "The application may still be running, crashed, or the log was truncated; metrics reflect what was "
            "recorded so far.",
            recommendation="Re-run the analysis after the application finishes for complete numbers.",
        )
    if app.parse_stats.bad_lines:
        yield Finding(
            "INCOMPLETE_LOG",
            Severity.INFO,
            f"{app.parse_stats.bad_lines} malformed log line(s) skipped",
            "Some lines were not valid JSON (typically a truncated final line of an in-progress log).",
            evidence={"bad_lines": app.parse_stats.bad_lines},
        )
