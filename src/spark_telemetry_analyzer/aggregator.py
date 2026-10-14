"""Turn a stream of Spark listener events into :class:`ApplicationMetrics`."""

from __future__ import annotations

import re
from collections.abc import Callable
from typing import Any

from .models import (
    ApplicationMetrics,
    ExecutorMetrics,
    JobMetrics,
    Reservoir,
    SqlExecution,
    StageMetrics,
    TaskSample,
)
from .utils import truncate

#: Short (un-packaged) names of the events this tool consumes. Everything else is skipped cheaply.
WANTED_EVENTS = frozenset(
    {
        "SparkListenerLogStart",
        "SparkListenerApplicationStart",
        "SparkListenerApplicationEnd",
        "SparkListenerEnvironmentUpdate",
        "SparkListenerExecutorAdded",
        "SparkListenerExecutorRemoved",
        "SparkListenerJobStart",
        "SparkListenerJobEnd",
        "SparkListenerStageSubmitted",
        "SparkListenerStageCompleted",
        "SparkListenerTaskEnd",
        "SparkListenerSQLExecutionStart",
        "SparkListenerSQLExecutionEnd",
    }
)

_SECRET_KEY = re.compile(r"(secret|password|passwd|token|credential|access[._-]?key|private[._-]?key|api[._-]?key)", re.I)
_SECRET_VALUE = re.compile(r"((?:password|passwd|secret|token|api[_-]?key|access[_-]?key)[\w.]*\s*[=:]\s*)\S+", re.I)
_LOCAL_MASTER = re.compile(r"^local\[(\d+)(?:,\d+)?\]$")
REDACTED = "*********(redacted)"


def _i(value: Any, default: int = 0) -> int:
    """Lenient int conversion (event logs written by other tools are not always clean)."""
    if value is None:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _failure_label(reason: dict[str, Any]) -> str:
    kind = str(reason.get("Reason") or "Unknown")
    if kind == "ExceptionFailure":
        cls = str(reason.get("Class Name") or "Exception")
        desc = str(reason.get("Description") or reason.get("Message") or "")
        desc = desc.split("\n", 1)[0]
        return f"{cls}: {truncate(desc, 100)}" if desc else cls
    if kind == "ExecutorLostFailure":
        loss = reason.get("Loss Reason")
        return f"ExecutorLostFailure: {truncate(str(loss), 120)}" if loss else kind
    if kind == "TaskKilled":
        why = reason.get("Kill Reason")
        return f"TaskKilled: {truncate(str(why), 80)}" if why else kind
    if kind == "FetchFailed":
        return "FetchFailed"
    return kind


class EventAggregator:
    """Incrementally folds Spark listener events into application metrics."""

    def __init__(self, reservoir_size: int = 20_000, redact: bool = True) -> None:
        self.app = ApplicationMetrics()
        self._reservoir_size = reservoir_size
        self._redact = redact
        self.events_consumed = 0
        self._handlers: dict[str, Callable[[dict[str, Any]], None]] = {
            "SparkListenerLogStart": self._on_log_start,
            "SparkListenerApplicationStart": self._on_app_start,
            "SparkListenerApplicationEnd": self._on_app_end,
            "SparkListenerEnvironmentUpdate": self._on_environment,
            "SparkListenerExecutorAdded": self._on_executor_added,
            "SparkListenerExecutorRemoved": self._on_executor_removed,
            "SparkListenerJobStart": self._on_job_start,
            "SparkListenerJobEnd": self._on_job_end,
            "SparkListenerStageSubmitted": self._on_stage_submitted,
            "SparkListenerStageCompleted": self._on_stage_completed,
            "SparkListenerTaskEnd": self._on_task_end,
            "SparkListenerSQLExecutionStart": self._on_sql_start,
            "SparkListenerSQLExecutionEnd": self._on_sql_end,
        }

    # ---- public API -------------------------------------------------------------------------
    def consume(self, event: dict[str, Any]) -> bool:
        """Process one event; returns ``False`` when the event type is not used."""
        name = str(event.get("Event") or "").rsplit(".", 1)[-1]
        handler = self._handlers.get(name)
        if handler is None:
            return False
        handler(event)
        self.events_consumed += 1
        return True

    def build(self) -> ApplicationMetrics:
        app = self.app
        if not app.app_id:
            app.app_id = app.spark_properties.get("spark.app.id", "")
        if not app.app_name:
            app.app_name = app.spark_properties.get("spark.app.name", "")
        self._infer_local_cores()
        return app

    # ---- helpers ----------------------------------------------------------------------------
    def _touch(self, ts: Any) -> None:
        value = _i(ts, 0)
        if value > 0 and (self.app.last_event_time is None or value > self.app.last_event_time):
            self.app.last_event_time = value

    def _stage(self, stage_id: int, attempt: int) -> StageMetrics:
        key = (stage_id, attempt)
        st = self.app.stages.get(key)
        if st is None:
            st = StageMetrics(
                stage_id=stage_id,
                attempt_id=attempt,
                reservoir=Reservoir(self._reservoir_size, seed=stage_id * 7919 + attempt),
            )
            self.app.stages[key] = st
        return st

    def _executor(self, executor_id: str) -> ExecutorMetrics:
        ex = self.app.executors.get(executor_id)
        if ex is None:
            ex = ExecutorMetrics(executor_id=executor_id)
            self.app.executors[executor_id] = ex
        return ex

    def _apply_stage_info(self, info: dict[str, Any]) -> StageMetrics | None:
        if info.get("Stage ID") is None:
            return None
        st = self._stage(_i(info.get("Stage ID")), _i(info.get("Stage Attempt ID")))
        if info.get("Stage Name"):
            st.name = truncate(str(info["Stage Name"]), 300)
        if info.get("Number of Tasks") is not None:
            st.num_tasks = _i(info["Number of Tasks"])
        if info.get("Details"):
            st.details = truncate(str(info["Details"]).split("\n", 1)[0], 300)
        if info.get("Parent IDs"):
            st.parent_ids = [_i(p) for p in info["Parent IDs"]]
        if info.get("Submission Time") is not None:
            st.submission_time = _i(info["Submission Time"])
            self._touch(st.submission_time)
        if info.get("Completion Time") is not None:
            st.completion_time = _i(info["Completion Time"])
            self._touch(st.completion_time)
        if info.get("Failure Reason"):
            st.failure_reason = truncate(str(info["Failure Reason"]), 500)
        return st

    def _infer_local_cores(self) -> None:
        app = self.app
        if any(ex.cores > 0 for ex in app.executors.values()):
            return
        match = _LOCAL_MASTER.match(app.spark_properties.get("spark.master", ""))
        if match and "driver" in app.executors:
            app.executors["driver"].cores = int(match.group(1))

    # ---- handlers ---------------------------------------------------------------------------
    def _on_log_start(self, ev: dict[str, Any]) -> None:
        self.app.spark_version = str(ev.get("Spark Version") or "")

    def _on_app_start(self, ev: dict[str, Any]) -> None:
        app = self.app
        app.app_id = str(ev.get("App ID") or app.app_id)
        app.app_name = str(ev.get("App Name") or app.app_name)
        app.user = str(ev.get("User") or "")
        if ev.get("Timestamp") is not None:
            app.start_time = _i(ev["Timestamp"])
            self._touch(app.start_time)

    def _on_app_end(self, ev: dict[str, Any]) -> None:
        self.app.has_app_end = True
        if ev.get("Timestamp") is not None:
            self.app.end_time = _i(ev["Timestamp"])
            self._touch(self.app.end_time)

    def _on_environment(self, ev: dict[str, Any]) -> None:
        raw = ev.get("Spark Properties") or {}
        if isinstance(raw, list):  # older logs: list of [key, value] pairs
            raw = {str(p[0]): p[1] for p in raw if isinstance(p, (list, tuple)) and len(p) == 2}
        props: dict[str, str] = {}
        for key, value in raw.items():
            key = str(key)
            if self._redact and _SECRET_KEY.search(key):
                props[key] = REDACTED
            elif self._redact:
                props[key] = _SECRET_VALUE.sub(lambda m: m.group(1) + REDACTED, str(value))
            else:
                props[key] = str(value)
        self.app.spark_properties = props

    def _on_executor_added(self, ev: dict[str, Any]) -> None:
        ex = self._executor(str(ev.get("Executor ID")))
        info = ev.get("Executor Info") or {}
        ex.host = str(info.get("Host") or ex.host)
        ex.cores = _i(info.get("Total Cores"), ex.cores)
        if ev.get("Timestamp") is not None:
            ex.added_ts = _i(ev["Timestamp"])
            self._touch(ex.added_ts)

    def _on_executor_removed(self, ev: dict[str, Any]) -> None:
        ex = self._executor(str(ev.get("Executor ID")))
        if ev.get("Timestamp") is not None:
            ex.removed_ts = _i(ev["Timestamp"])
            self._touch(ex.removed_ts)
        ex.removed_reason = truncate(str(ev.get("Removed Reason") or ""), 300) or None

    def _on_job_start(self, ev: dict[str, Any]) -> None:
        job_id = _i(ev.get("Job ID"))
        job = self.app.jobs.setdefault(job_id, JobMetrics(job_id=job_id))
        if ev.get("Submission Time") is not None:
            job.submission_time = _i(ev["Submission Time"])
            self._touch(job.submission_time)
        job.stage_ids = [_i(s) for s in (ev.get("Stage IDs") or [])]
        for info in ev.get("Stage Infos") or []:
            st = self._apply_stage_info(info)
            if st is not None:
                st.job_ids.add(job_id)
        props = ev.get("Properties") or {}
        job.description = truncate(str(props.get("spark.job.description") or ""), 300)
        job.group_id = str(props.get("spark.jobGroup.id") or "")
        sql_id = props.get("spark.sql.execution.id")
        if sql_id not in (None, ""):
            job.sql_execution_id = _i(sql_id, -1)
            if job.sql_execution_id >= 0:
                execution = self.app.sql.setdefault(job.sql_execution_id, SqlExecution(job.sql_execution_id))
                execution.job_ids.append(job_id)

    def _on_job_end(self, ev: dict[str, Any]) -> None:
        job_id = _i(ev.get("Job ID"))
        job = self.app.jobs.setdefault(job_id, JobMetrics(job_id=job_id))
        if ev.get("Completion Time") is not None:
            job.completion_time = _i(ev["Completion Time"])
            self._touch(job.completion_time)
        result = ev.get("Job Result") or {}
        if result.get("Result") == "JobSucceeded":
            job.status = "SUCCEEDED"
        else:
            job.status = "FAILED"
            exc = result.get("Exception") or {}
            job.failure_reason = truncate(str(exc.get("Message") or result.get("Result") or "Unknown failure"), 500)

    def _on_stage_submitted(self, ev: dict[str, Any]) -> None:
        self._apply_stage_info(ev.get("Stage Info") or {})

    def _on_stage_completed(self, ev: dict[str, Any]) -> None:
        self._apply_stage_info(ev.get("Stage Info") or {})

    def _on_sql_start(self, ev: dict[str, Any]) -> None:
        if ev.get("executionId") is None:
            return
        eid = _i(ev["executionId"])
        execution = self.app.sql.setdefault(eid, SqlExecution(eid))
        execution.description = truncate(str(ev.get("description") or ""), 300)
        if ev.get("time") is not None:
            execution.start_time = _i(ev["time"])
            self._touch(execution.start_time)

    def _on_sql_end(self, ev: dict[str, Any]) -> None:
        if ev.get("executionId") is None:
            return
        eid = _i(ev["executionId"])
        execution = self.app.sql.setdefault(eid, SqlExecution(eid))
        if ev.get("time") is not None:
            execution.end_time = _i(ev["time"])
            self._touch(execution.end_time)

    def _on_task_end(self, ev: dict[str, Any]) -> None:
        if ev.get("Stage ID") is None:
            return
        info = ev.get("Task Info") or {}
        metrics = ev.get("Task Metrics") or {}
        reason = ev.get("Task End Reason") or {}
        shuffle_read = metrics.get("Shuffle Read Metrics") or {}
        shuffle_write = metrics.get("Shuffle Write Metrics") or {}
        inp = metrics.get("Input Metrics") or {}
        out = metrics.get("Output Metrics") or {}

        launch = _i(info.get("Launch Time"))
        finish = _i(info.get("Finish Time"))
        reason_name = str(reason.get("Reason") or "Success")
        if reason_name == "Success":
            status, label = "success", ""
        elif reason_name == "TaskKilled" or info.get("Killed"):
            status, label = "killed", _failure_label(reason)
        else:
            status, label = "failed", _failure_label(reason)

        sample = TaskSample(
            task_id=_i(info.get("Task ID")),
            index=_i(info.get("Index")),
            attempt=_i(info.get("Attempt")),
            executor_id=str(info.get("Executor ID") or ""),
            host=str(info.get("Host") or ""),
            speculative=bool(info.get("Speculative")),
            status=status,
            failure_label=label,
            wall_ms=max(0, finish - launch) if launch and finish else 0,
            run_ms=_i(metrics.get("Executor Run Time")),
            cpu_ns=_i(metrics.get("Executor CPU Time")),
            deser_ms=_i(metrics.get("Executor Deserialize Time")),
            gc_ms=_i(metrics.get("JVM GC Time")),
            result_ser_ms=_i(metrics.get("Result Serialization Time")),
            result_size=_i(metrics.get("Result Size")),
            mem_spill=_i(metrics.get("Memory Bytes Spilled")),
            disk_spill=_i(metrics.get("Disk Bytes Spilled")),
            shuffle_read_remote=_i(shuffle_read.get("Remote Bytes Read")),
            shuffle_read_local=_i(shuffle_read.get("Local Bytes Read")),
            fetch_wait_ms=_i(shuffle_read.get("Fetch Wait Time")),
            shuffle_read_records=_i(shuffle_read.get("Total Records Read")),
            shuffle_write_bytes=_i(shuffle_write.get("Shuffle Bytes Written")),
            shuffle_write_ms=_i(shuffle_write.get("Shuffle Write Time")) // 1_000_000,
            shuffle_write_records=_i(shuffle_write.get("Shuffle Records Written")),
            input_bytes=_i(inp.get("Bytes Read")),
            input_records=_i(inp.get("Records Read")),
            output_bytes=_i(out.get("Bytes Written")),
            output_records=_i(out.get("Records Written")),
        )
        self._touch(finish)
        stage = self._stage(_i(ev.get("Stage ID")), _i(ev.get("Stage Attempt ID")))
        stage.add_task(sample)
        if sample.executor_id:
            self._executor(sample.executor_id).add_task(sample)
