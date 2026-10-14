"""Shared test helpers: cached synthetic logs and a tiny event-log builder."""

from __future__ import annotations

import functools
import json
from typing import Any

from spark_telemetry_analyzer.sample import generate_event_log


@functools.lru_cache(maxsize=None)
def log_bytes(scenario: str = "mixed", scale: float = 1.0, seed: int = 42) -> bytes:
    return ("\n".join(generate_event_log(scenario, seed=seed, scale=scale)) + "\n").encode()


class LogBuilder:
    """Builds small, hand-crafted event logs so each rule can be tested in isolation."""

    def __init__(self, app_id: str = "app-1", name: str = "test-app", start: int = 1_000_000_000_000,
                 props: dict[str, str] | None = None, version: str = "3.5.1") -> None:
        self.events: list[dict[str, Any]] = [
            {"Event": "SparkListenerLogStart", "Spark Version": version},
            {"Event": "SparkListenerApplicationStart", "App Name": name, "App ID": app_id, "Timestamp": start, "User": "tester"},
        ]
        if props is not None:
            self.events.append({"Event": "SparkListenerEnvironmentUpdate", "Spark Properties": props})
        self.start = start
        self._task_id = 0

    def add_executor(self, eid: str, cores: int = 4, at: int | None = None) -> LogBuilder:
        self.events.append({
            "Event": "SparkListenerExecutorAdded", "Timestamp": at if at is not None else self.start + 1000,
            "Executor ID": eid, "Executor Info": {"Host": f"host-{eid}", "Total Cores": cores},
        })
        return self

    def remove_executor(self, eid: str, at: int, reason: str) -> LogBuilder:
        self.events.append({"Event": "SparkListenerExecutorRemoved", "Timestamp": at, "Executor ID": eid, "Removed Reason": reason})
        return self

    def job(self, job_id: int, stage_ids: list[int], at: int, end: int | None = None, failure: str | None = None) -> LogBuilder:
        self.events.append({
            "Event": "SparkListenerJobStart", "Job ID": job_id, "Submission Time": at, "Stage IDs": stage_ids,
            "Stage Infos": [{"Stage ID": s, "Stage Attempt ID": 0, "Stage Name": f"stage-{s}", "Number of Tasks": 0} for s in stage_ids],
            "Properties": {},
        })
        if end is not None:
            result: dict[str, Any] = {"Result": "JobSucceeded"}
            if failure:
                result = {"Result": "JobFailed", "Exception": {"Message": failure}}
            self.events.append({"Event": "SparkListenerJobEnd", "Job ID": job_id, "Completion Time": end, "Job Result": result})
        return self

    def stage(self, sid: int, sub: int, comp: int, ntasks: int, attempt: int = 0, failure: str | None = None) -> LogBuilder:
        info: dict[str, Any] = {"Stage ID": sid, "Stage Attempt ID": attempt, "Stage Name": f"stage-{sid}",
                                "Number of Tasks": ntasks, "Submission Time": sub, "Completion Time": comp}
        if failure:
            info["Failure Reason"] = failure
        self.events.append({"Event": "SparkListenerStageCompleted", "Stage Info": info})
        return self

    def task(self, sid: int, executor: str, launch: int, finish: int, *, attempt: int = 0, run: int | None = None,
             gc: int = 0, deser: int = 0, shuffle_read: int = 0, input_bytes: int = 0, shuffle_write: int = 0,
             disk_spill: int = 0, mem_spill: int = 0, result_size: int = 0, fetch_wait: int = 0,
             reason: dict[str, Any] | None = None, killed: bool = False) -> LogBuilder:
        failed = reason is not None and not killed
        self.events.append({
            "Event": "SparkListenerTaskEnd", "Stage ID": sid, "Stage Attempt ID": attempt,
            "Task End Reason": reason or {"Reason": "Success"},
            "Task Info": {"Task ID": self._task_id, "Index": self._task_id, "Attempt": 0, "Launch Time": launch,
                          "Finish Time": finish, "Executor ID": executor, "Host": f"host-{executor}",
                          "Speculative": False, "Failed": failed, "Killed": killed},
            "Task Metrics": {
                "Executor Run Time": finish - launch if run is None else run, "Executor Deserialize Time": deser,
                "JVM GC Time": gc, "Result Size": result_size, "Memory Bytes Spilled": mem_spill, "Disk Bytes Spilled": disk_spill,
                "Shuffle Read Metrics": {"Remote Bytes Read": shuffle_read, "Local Bytes Read": 0, "Fetch Wait Time": fetch_wait},
                "Shuffle Write Metrics": {"Shuffle Bytes Written": shuffle_write, "Shuffle Write Time": 0},
                "Input Metrics": {"Bytes Read": input_bytes}, "Output Metrics": {"Bytes Written": 0},
            },
        })
        self._task_id += 1
        return self

    def end(self, at: int) -> LogBuilder:
        self.events.append({"Event": "SparkListenerApplicationEnd", "Timestamp": at})
        return self

    def build(self) -> bytes:
        return ("\n".join(json.dumps(e) for e in self.events) + "\n").encode()
