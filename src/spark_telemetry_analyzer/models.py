"""Domain model: streaming accumulators for stages, executors, jobs and the application.

Event logs can contain millions of tasks, so tasks are *never* stored individually. Each stage keeps
exact sums/maxima plus a fixed-size reservoir sample for percentile estimation and a tiny heap of
its slowest tasks.
"""

from __future__ import annotations

import enum
import heapq
import random
from collections import Counter
from dataclasses import dataclass, field
from typing import Any

from .utils import percentile

SLOWEST_TASKS_KEPT = 5

# Column layout of the per-stage reservoir sample rows.
COL_DURATION, COL_SHUFFLE_READ, COL_INPUT, COL_MEM_SPILL, COL_DISK_SPILL = range(5)


class Severity(enum.IntEnum):
    INFO = 0
    WARNING = 1
    CRITICAL = 2

    @classmethod
    def parse(cls, value: str) -> Severity:
        try:
            return cls[value.strip().upper()]
        except KeyError as exc:
            valid = ", ".join(s.name.lower() for s in cls)
            raise ValueError(f"Unknown severity '{value}' (expected one of: {valid})") from exc

    @property
    def label(self) -> str:
        return self.name.lower()


@dataclass(frozen=True)
class Finding:
    rule_id: str
    severity: Severity
    title: str
    message: str
    scope: str = "application"  # application | job | stage | executor
    scope_id: str | None = None
    evidence: dict[str, Any] = field(default_factory=dict)
    recommendation: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "rule_id": self.rule_id,
            "severity": self.severity.label,
            "title": self.title,
            "message": self.message,
            "scope": self.scope,
            "scope_id": self.scope_id,
            "evidence": self.evidence,
            "recommendation": self.recommendation,
        }


class Reservoir:
    """Fixed-capacity uniform reservoir sample (Algorithm R) of numeric tuples."""

    __slots__ = ("capacity", "rows", "seen", "_rng")

    def __init__(self, capacity: int, seed: int = 0) -> None:
        self.capacity = capacity
        self.rows: list[tuple[float, ...]] = []
        self.seen = 0
        self._rng = random.Random(seed)

    def add(self, row: tuple[float, ...]) -> None:
        self.seen += 1
        if len(self.rows) < self.capacity:
            self.rows.append(row)
            return
        j = self._rng.randrange(self.seen)
        if j < self.capacity:
            self.rows[j] = row

    def column(self, index: int) -> list[float]:
        return [r[index] for r in self.rows]


@dataclass(slots=True)
class TaskSample:
    """One finished task, extracted from a ``SparkListenerTaskEnd`` event."""

    task_id: int = 0
    index: int = 0
    attempt: int = 0
    executor_id: str = ""
    host: str = ""
    speculative: bool = False
    status: str = "success"  # success | failed | killed
    failure_label: str = ""
    wall_ms: int = 0
    run_ms: int = 0
    cpu_ns: int = 0
    deser_ms: int = 0
    gc_ms: int = 0
    result_ser_ms: int = 0
    result_size: int = 0
    mem_spill: int = 0
    disk_spill: int = 0
    shuffle_read_remote: int = 0
    shuffle_read_local: int = 0
    fetch_wait_ms: int = 0
    shuffle_read_records: int = 0
    shuffle_write_bytes: int = 0
    shuffle_write_ms: int = 0  # nanoseconds in Spark; converted by the aggregator
    shuffle_write_records: int = 0
    input_bytes: int = 0
    input_records: int = 0
    output_bytes: int = 0
    output_records: int = 0

    @property
    def shuffle_read_bytes(self) -> int:
        return self.shuffle_read_remote + self.shuffle_read_local


@dataclass
class StageMetrics:
    stage_id: int
    attempt_id: int = 0
    name: str = ""
    details: str = ""
    num_tasks: int = 0
    parent_ids: list[int] = field(default_factory=list)
    submission_time: int | None = None
    completion_time: int | None = None
    failure_reason: str | None = None
    job_ids: set[int] = field(default_factory=set)

    tasks_succeeded: int = 0
    tasks_failed: int = 0
    tasks_killed: int = 0
    tasks_speculative: int = 0

    wall_ms: int = 0  # sum of task wall-clock durations
    run_ms: int = 0
    cpu_ns: int = 0
    deser_ms: int = 0
    gc_ms: int = 0
    result_ser_ms: int = 0
    result_size: int = 0
    mem_spill: int = 0
    disk_spill: int = 0
    shuffle_read_remote: int = 0
    shuffle_read_local: int = 0
    fetch_wait_ms: int = 0
    shuffle_read_records: int = 0
    shuffle_write_bytes: int = 0
    shuffle_write_ms: int = 0
    shuffle_write_records: int = 0
    input_bytes: int = 0
    input_records: int = 0
    output_bytes: int = 0
    output_records: int = 0

    max_task_ms: int = 0
    max_shuffle_read: int = 0
    max_input: int = 0
    max_disk_spill: int = 0

    failure_reasons: Counter[str] = field(default_factory=Counter)
    slowest: list[tuple[Any, ...]] = field(default_factory=list)
    reservoir: Reservoir = field(default_factory=lambda: Reservoir(20_000))

    # ---- accumulation -----------------------------------------------------------------------
    def add_task(self, t: TaskSample) -> None:
        if t.status == "success":
            self.tasks_succeeded += 1
        elif t.status == "killed":
            self.tasks_killed += 1
        else:
            self.tasks_failed += 1
            self.failure_reasons[t.failure_label or "Unknown"] += 1
        if t.speculative:
            self.tasks_speculative += 1

        self.wall_ms += t.wall_ms
        self.run_ms += t.run_ms
        self.cpu_ns += t.cpu_ns
        self.deser_ms += t.deser_ms
        self.gc_ms += t.gc_ms
        self.result_ser_ms += t.result_ser_ms
        self.result_size += t.result_size
        self.mem_spill += t.mem_spill
        self.disk_spill += t.disk_spill
        self.shuffle_read_remote += t.shuffle_read_remote
        self.shuffle_read_local += t.shuffle_read_local
        self.fetch_wait_ms += t.fetch_wait_ms
        self.shuffle_read_records += t.shuffle_read_records
        self.shuffle_write_bytes += t.shuffle_write_bytes
        self.shuffle_write_ms += t.shuffle_write_ms
        self.shuffle_write_records += t.shuffle_write_records
        self.input_bytes += t.input_bytes
        self.input_records += t.input_records
        self.output_bytes += t.output_bytes
        self.output_records += t.output_records

        if t.status != "success":
            return  # distributions describe healthy task attempts only

        sr = t.shuffle_read_bytes
        self.max_task_ms = max(self.max_task_ms, t.wall_ms)
        self.max_shuffle_read = max(self.max_shuffle_read, sr)
        self.max_input = max(self.max_input, t.input_bytes)
        self.max_disk_spill = max(self.max_disk_spill, t.disk_spill)
        self.reservoir.add((t.wall_ms, sr, t.input_bytes, t.mem_spill, t.disk_spill))

        entry = (t.wall_ms, t.task_id, t.index, t.attempt, t.executor_id, t.host, sr, t.input_bytes, t.disk_spill)
        if len(self.slowest) < SLOWEST_TASKS_KEPT:
            heapq.heappush(self.slowest, entry)
        elif entry > self.slowest[0]:
            heapq.heapreplace(self.slowest, entry)

    # ---- derived values ---------------------------------------------------------------------
    @property
    def key(self) -> str:
        return f"{self.stage_id}.{self.attempt_id}"

    @property
    def tasks_observed(self) -> int:
        return self.tasks_succeeded + self.tasks_failed + self.tasks_killed

    @property
    def status(self) -> str:
        if self.failure_reason:
            return "FAILED"
        if self.completion_time is not None:
            return "COMPLETE"
        if self.tasks_observed == 0 and self.submission_time is None:
            return "SKIPPED"
        return "ACTIVE"

    @property
    def duration_ms(self) -> int:
        if self.submission_time is not None and self.completion_time is not None:
            return max(0, self.completion_time - self.submission_time)
        return 0

    @property
    def shuffle_read_bytes(self) -> int:
        return self.shuffle_read_remote + self.shuffle_read_local

    @property
    def gc_ratio(self) -> float:
        return self.gc_ms / self.run_ms if self.run_ms else 0.0

    def distribution(self, column: int) -> dict[str, float]:
        """p50/p75/p95 estimated from the reservoir; ``max`` is exact for the tracked columns."""
        values = sorted(self.reservoir.column(column))
        exact_max = {
            COL_DURATION: self.max_task_ms,
            COL_SHUFFLE_READ: self.max_shuffle_read,
            COL_INPUT: self.max_input,
            COL_DISK_SPILL: self.max_disk_spill,
        }.get(column)
        max_value = float(exact_max) if exact_max is not None else (values[-1] if values else 0.0)
        return {
            "min": values[0] if values else 0.0,
            "p50": percentile(values, 0.50),
            "p75": percentile(values, 0.75),
            "p95": percentile(values, 0.95),
            "max": max(max_value, values[-1] if values else 0.0),
        }

    def slowest_tasks(self) -> list[dict[str, Any]]:
        out = []
        for wall, task_id, index, attempt, executor, host, sr, inp, spill in sorted(self.slowest, reverse=True):
            out.append(
                {
                    "task_id": task_id,
                    "index": index,
                    "attempt": attempt,
                    "executor_id": executor,
                    "host": host,
                    "duration_ms": wall,
                    "shuffle_read_bytes": sr,
                    "input_bytes": inp,
                    "disk_spilled_bytes": spill,
                }
            )
        return out


@dataclass
class ExecutorMetrics:
    executor_id: str
    host: str = ""
    cores: int = 0
    added_ts: int | None = None
    removed_ts: int | None = None
    removed_reason: str | None = None
    tasks_succeeded: int = 0
    tasks_failed: int = 0
    tasks_killed: int = 0
    run_ms: int = 0
    gc_ms: int = 0
    input_bytes: int = 0
    shuffle_read_bytes: int = 0
    shuffle_write_bytes: int = 0
    disk_spill: int = 0

    def add_task(self, t: TaskSample) -> None:
        if t.status == "success":
            self.tasks_succeeded += 1
        elif t.status == "killed":
            self.tasks_killed += 1
        else:
            self.tasks_failed += 1
        self.run_ms += t.run_ms
        self.gc_ms += t.gc_ms
        self.input_bytes += t.input_bytes
        self.shuffle_read_bytes += t.shuffle_read_bytes
        self.shuffle_write_bytes += t.shuffle_write_bytes
        self.disk_spill += t.disk_spill
        if t.host and not self.host:
            self.host = t.host

    @property
    def tasks_total(self) -> int:
        return self.tasks_succeeded + self.tasks_failed + self.tasks_killed


@dataclass
class JobMetrics:
    job_id: int
    description: str = ""
    group_id: str = ""
    submission_time: int | None = None
    completion_time: int | None = None
    status: str = "RUNNING"  # RUNNING | SUCCEEDED | FAILED
    failure_reason: str | None = None
    stage_ids: list[int] = field(default_factory=list)
    sql_execution_id: int | None = None

    @property
    def duration_ms(self) -> int:
        if self.submission_time is not None and self.completion_time is not None:
            return max(0, self.completion_time - self.submission_time)
        return 0


@dataclass
class SqlExecution:
    execution_id: int
    description: str = ""
    start_time: int | None = None
    end_time: int | None = None
    job_ids: list[int] = field(default_factory=list)

    @property
    def duration_ms(self) -> int:
        if self.start_time is not None and self.end_time is not None:
            return max(0, self.end_time - self.start_time)
        return 0


@dataclass
class ParseStats:
    files: int = 0
    lines: int = 0
    events_processed: int = 0
    events_skipped: int = 0
    bad_lines: int = 0

    def to_dict(self) -> dict[str, int]:
        return {
            "files": self.files,
            "lines": self.lines,
            "events_processed": self.events_processed,
            "events_skipped": self.events_skipped,
            "bad_lines": self.bad_lines,
        }


@dataclass
class ApplicationMetrics:
    app_id: str = ""
    app_name: str = ""
    user: str = ""
    spark_version: str = ""
    start_time: int | None = None
    end_time: int | None = None
    last_event_time: int | None = None
    has_app_end: bool = False
    spark_properties: dict[str, str] = field(default_factory=dict)
    jobs: dict[int, JobMetrics] = field(default_factory=dict)
    stages: dict[tuple[int, int], StageMetrics] = field(default_factory=dict)
    executors: dict[str, ExecutorMetrics] = field(default_factory=dict)
    sql: dict[int, SqlExecution] = field(default_factory=dict)
    parse_stats: ParseStats = field(default_factory=ParseStats)

    @property
    def effective_end(self) -> int | None:
        return self.end_time if self.end_time is not None else self.last_event_time

    @property
    def duration_ms(self) -> int:
        end = self.effective_end
        if self.start_time is None or end is None:
            return 0
        return max(0, end - self.start_time)

    def executor_lifetime_ms(self, ex: ExecutorMetrics) -> int:
        start = ex.added_ts if ex.added_ts is not None else self.start_time
        end = ex.removed_ts if ex.removed_ts is not None else self.effective_end
        if start is None or end is None:
            return 0
        return max(0, end - start)

    def cores_overlapping(self, start: int | None, end: int | None) -> int:
        """Total executor cores alive at any point within [start, end]."""
        if start is None or end is None:
            return 0
        total = 0
        for ex in self.executors.values():
            if ex.cores <= 0:
                continue
            ex_start = ex.added_ts if ex.added_ts is not None else (self.start_time or 0)
            ex_end = ex.removed_ts if ex.removed_ts is not None else (self.effective_end or end)
            if ex_start <= end and ex_end >= start:
                total += ex.cores
        return total
