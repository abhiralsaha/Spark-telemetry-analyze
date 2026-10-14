"""Deterministic synthetic Spark event-log generator.

Used for demos, documentation, smoke tests and as a fixture factory. The generated logs follow the real
event-log JSON-lines format and simulate task scheduling on a fixed set of executor slots, so derived
numbers (utilization, stage durations, skew) are internally consistent.
"""

from __future__ import annotations

import heapq
import json
import random
from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Any

from .utils import GIB, MIB

SCENARIOS = ("mixed", "healthy", "skew", "failures")
T0 = 1_760_000_000_000  # fixed epoch-ms origin so output is reproducible


@dataclass
class _Plan:
    run_ms: int
    deser_ms: int = 8
    gc_ms: int = 0
    input_bytes: int = 0
    shuffle_read: int = 0
    shuffle_write: int = 0
    mem_spill: int = 0
    disk_spill: int = 0
    result_size: int = 2_000
    fetch_wait_ms: int = 0
    overhead_ms: int = 10
    status: str = "success"  # success | failed | killed
    reason: dict[str, Any] = field(default_factory=dict)
    force_executor: str | None = None

    @property
    def wall_ms(self) -> int:
        return self.run_ms + self.deser_ms + 1 + self.overhead_ms


def _metrics(p: _Plan) -> dict[str, Any]:
    return {
        "Executor Deserialize Time": p.deser_ms,
        "Executor Deserialize CPU Time": int(p.deser_ms * 800_000),
        "Executor Run Time": p.run_ms,
        "Executor CPU Time": int(p.run_ms * 850_000),
        "Result Size": p.result_size,
        "JVM GC Time": p.gc_ms,
        "Result Serialization Time": 1,
        "Memory Bytes Spilled": p.mem_spill,
        "Disk Bytes Spilled": p.disk_spill,
        "Shuffle Read Metrics": {
            "Remote Blocks Fetched": 10,
            "Local Blocks Fetched": 3,
            "Fetch Wait Time": p.fetch_wait_ms,
            "Remote Bytes Read": int(p.shuffle_read * 0.8),
            "Remote Bytes Read To Disk": 0,
            "Local Bytes Read": int(p.shuffle_read * 0.2),
            "Total Records Read": p.shuffle_read // 100,
        },
        "Shuffle Write Metrics": {
            "Shuffle Bytes Written": p.shuffle_write,
            "Shuffle Write Time": p.shuffle_write // 50,
            "Shuffle Records Written": p.shuffle_write // 100,
        },
        "Input Metrics": {"Bytes Read": p.input_bytes, "Records Read": p.input_bytes // 120},
        "Output Metrics": {"Bytes Written": 0, "Records Written": 0},
    }


class _Simulator:
    def __init__(self, seed: int, executors: int, cores: int) -> None:
        self.rng = random.Random(seed)
        self.cores = cores
        self.lines: list[str] = []
        self.clock = T0
        self.task_id = 0
        self.stage_id = 0
        self.job_id = 0
        self.sql_id = 0
        self.alive: list[str] = []
        self.next_executor = 1
        self.hosts: dict[str, str] = {}
        self.props = {
            "spark.app.name": "nightly-customer-etl",
            "spark.master": "yarn",
            "spark.submit.deployMode": "cluster",
            "spark.executor.instances": str(executors),
            "spark.executor.cores": str(cores),
            "spark.executor.memory": "8g",
            "spark.driver.memory": "4g",
            "spark.dynamicAllocation.enabled": "false",
            "spark.sql.shuffle.partitions": "200",
            "spark.sql.adaptive.enabled": "false",
            "spark.hadoop.fs.s3a.secret.key": "not-a-real-secret",
            "spark.driver.extraJavaOptions": "-Dlog4j.configuration=file:log4j.properties -Ddb.password=hunter2",
        }

    # ---- emit helpers -----------------------------------------------------------------------
    def emit(self, event: dict[str, Any]) -> None:
        self.lines.append(json.dumps(event, separators=(",", ":")))

    def start_application(self, executors: int) -> None:
        self.emit({"Event": "SparkListenerLogStart", "Spark Version": "3.5.1"})
        self.emit(
            {
                "Event": "SparkListenerApplicationStart",
                "App Name": "nightly-customer-etl",
                "App ID": "application_1760000000000_0042",
                "Timestamp": T0,
                "User": "etl",
            }
        )
        self.emit({"Event": "SparkListenerEnvironmentUpdate", "Spark Properties": self.props})
        self.clock = T0 + 4_000
        for _ in range(executors):
            self.add_executor()

    def add_executor(self) -> str:
        eid = str(self.next_executor)
        self.next_executor += 1
        host = f"worker-{int(eid) % 4 + 1}.cluster.internal"
        self.hosts[eid] = host
        self.alive.append(eid)
        self.emit(
            {
                "Event": "SparkListenerExecutorAdded",
                "Timestamp": self.clock,
                "Executor ID": eid,
                "Executor Info": {"Host": host, "Total Cores": self.cores, "Log Urls": {}, "Attributes": {}},
            }
        )
        return eid

    def remove_executor(self, eid: str, reason: str) -> None:
        self.alive.remove(eid)
        self.emit(
            {"Event": "SparkListenerExecutorRemoved", "Timestamp": self.clock, "Executor ID": eid, "Removed Reason": reason}
        )

    def finish_application(self) -> None:
        self.clock += 2_000
        self.emit({"Event": "SparkListenerApplicationEnd", "Timestamp": self.clock})

    # ---- stage / job simulation -------------------------------------------------------------
    def _stage_info(self, sid: int, attempt: int, name: str, n: int, parents: list[int], **extra: Any) -> dict[str, Any]:
        info: dict[str, Any] = {
            "Stage ID": sid,
            "Stage Attempt ID": attempt,
            "Stage Name": name,
            "Number of Tasks": n,
            "RDD Info": [],
            "Parent IDs": parents,
            "Details": "org.apache.spark.sql.Dataset.collect(Dataset.scala:3000)\n...",
            "Accumulables": [],
        }
        info.update(extra)
        return info

    def run_stage(
        self, sid: int, attempt: int, name: str, plans: list[_Plan], parents: list[int], failure: str | None = None
    ) -> int:
        submit = self.clock
        self.emit(
            {
                "Event": "SparkListenerStageSubmitted",
                "Stage Info": self._stage_info(sid, attempt, name, len(plans), parents, **{"Submission Time": submit}),
            }
        )
        heap = [(submit, int(e), s) for e in self.alive for s in range(self.cores)]
        heapq.heapify(heap)
        end = submit
        attempts: dict[int, int] = {}
        for index, plan in enumerate(plans):
            free, eid, slot = heapq.heappop(heap)
            executor = plan.force_executor or str(eid)
            launch = max(free, submit) + self.rng.randint(1, 6)
            finish = launch + plan.wall_ms
            attempt_no = attempts.get(index, 0)
            attempts[index] = attempt_no + 1
            info = {
                "Task ID": self.task_id,
                "Index": index,
                "Attempt": attempt_no,
                "Launch Time": launch,
                "Executor ID": executor,
                "Host": self.hosts.get(executor, "unknown"),
                "Locality": "PROCESS_LOCAL",
                "Speculative": False,
                "Getting Result Time": 0,
                "Finish Time": finish,
                "Failed": plan.status == "failed",
                "Killed": plan.status == "killed",
            }
            self.task_id += 1
            self.emit(
                {
                    "Event": "SparkListenerTaskEnd",
                    "Stage ID": sid,
                    "Stage Attempt ID": attempt,
                    "Task Type": "ResultTask",
                    "Task End Reason": plan.reason if plan.status != "success" else {"Reason": "Success"},
                    "Task Info": info,
                    "Task Metrics": _metrics(plan),
                }
            )
            heapq.heappush(heap, (finish, eid, slot))
            end = max(end, finish)
        completion = end + 3
        extra: dict[str, Any] = {"Submission Time": submit, "Completion Time": completion}
        if failure:
            extra["Failure Reason"] = failure
        self.emit(
            {
                "Event": "SparkListenerStageCompleted",
                "Stage Info": self._stage_info(sid, attempt, name, len(plans), parents, **extra),
            }
        )
        self.clock = completion
        return completion

    def start_job(self, description: str, stages: list[tuple[int, str, int, list[int]]], sql: bool = True) -> int:
        job_id = self.job_id
        self.job_id += 1
        props: dict[str, str] = {"spark.job.description": description}
        if sql:
            eid = self.sql_id
            self.sql_id += 1
            props["spark.sql.execution.id"] = str(eid)
            self.emit(
                {
                    "Event": "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart",
                    "executionId": eid,
                    "description": description,
                    "details": "org.apache.spark.sql.Dataset.collect(Dataset.scala:3000)",
                    "physicalPlanDescription": "== Physical Plan ==\n" + "HashAggregate\n" * 5,
                    "sparkPlanInfo": {"nodeName": "WholeStageCodegen", "simpleString": "", "children": [], "metadata": {}, "metrics": []},
                    "time": self.clock,
                }
            )
        self.emit(
            {
                "Event": "SparkListenerJobStart",
                "Job ID": job_id,
                "Submission Time": self.clock,
                "Stage Infos": [self._stage_info(sid, 0, name, n, parents) for sid, name, n, parents in stages],
                "Stage IDs": [sid for sid, *_ in stages],
                "Properties": props,
            }
        )
        return job_id

    def end_job(self, job_id: int, sql_end: bool = True, failure: str | None = None) -> None:
        self.clock += 5
        result: dict[str, Any] = {"Result": "JobSucceeded"}
        if failure:
            result = {"Result": "JobFailed", "Exception": {"Message": failure, "Stack Trace": []}}
        self.emit({"Event": "SparkListenerJobEnd", "Job ID": job_id, "Completion Time": self.clock, "Job Result": result})
        if sql_end:
            self.emit(
                {
                    "Event": "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd",
                    "executionId": self.sql_id - 1,
                    "time": self.clock,
                }
            )
        self.clock += self.rng.randint(1_500, 4_000)  # driver-side gap between jobs

    def new_stage_id(self) -> int:
        sid = self.stage_id
        self.stage_id += 1
        return sid

    def jitter(self, base: int, spread: float = 0.2) -> int:
        return max(1, int(base * self.rng.uniform(1 - spread, 1 + spread)))


# --------------------------------------------------------------------------------------------
# Scenario building blocks
# --------------------------------------------------------------------------------------------
def _healthy_job(sim: _Simulator, scale: float) -> None:
    n = max(16, int(640 * scale))
    s0, s1 = sim.new_stage_id(), sim.new_stage_id()
    job = sim.start_job("healthy daily aggregation", [(s0, "scan + partial agg", n, []), (s1, "final aggregation", n, [s0])])
    sim.run_stage(
        s0, 0, "scan + partial agg",
        [_Plan(run_ms=sim.jitter(3_000, 0.15), gc_ms=60, input_bytes=sim.jitter(128 * MIB, 0.1), shuffle_write=sim.jitter(24 * MIB, 0.1))
         for _ in range(n)],
        [],
    )
    sim.run_stage(
        s1, 0, "final aggregation",
        [_Plan(run_ms=sim.jitter(2_500, 0.15), gc_ms=50, shuffle_read=sim.jitter(24 * MIB, 0.1), fetch_wait_ms=20) for _ in range(n)],
        [s0],
    )
    sim.end_job(job)


def _skew_job(sim: _Simulator, scale: float) -> None:
    n = max(32, int(200 * scale))
    s0, s1 = sim.new_stage_id(), sim.new_stage_id()
    job = sim.start_job(
        "orders JOIN customers (skewed on customer_id)",
        [(s0, "scan orders", n, []), (s1, "shuffle join", n, [s0])],
    )
    sim.run_stage(
        s0, 0, "scan orders",
        [_Plan(run_ms=sim.jitter(4_000), gc_ms=120, input_bytes=sim.jitter(128 * MIB), shuffle_write=sim.jitter(64 * MIB)) for _ in range(n)],
        [],
    )
    plans = []
    for i in range(n):
        if i < 3:  # three hot keys
            plans.append(
                _Plan(run_ms=sim.jitter(48_000, 0.1), gc_ms=9_000, shuffle_read=sim.jitter(int(2.4 * GIB), 0.05),
                      mem_spill=int(3 * GIB), disk_spill=int(700 * MIB), fetch_wait_ms=400)
            )
        else:
            plans.append(_Plan(run_ms=sim.jitter(3_000), gc_ms=90, shuffle_read=sim.jitter(60 * MIB), fetch_wait_ms=30))
    sim.run_stage(s1, 0, "shuffle join", plans, [s0])
    sim.end_job(job)


def _gc_and_straggler_job(sim: _Simulator, scale: float) -> None:
    n = max(32, int(200 * scale))
    s = sim.new_stage_id()
    job = sim.start_job("sessionization with large object graphs", [(s, "sessionize", n, [])])
    plans = []
    for i in range(n):
        run = sim.jitter(6_000)
        if i in (10, 11):  # stragglers: same data, 12x slower
            run *= 12
        plans.append(
            _Plan(run_ms=run, gc_ms=int(run * 0.25), input_bytes=sim.jitter(96 * MIB, 0.05), deser_ms=1_800)
        )
    sim.run_stage(s, 0, "sessionize", plans, [])
    sim.end_job(job)


def _small_tasks_job(sim: _Simulator, scale: float) -> None:
    n = max(1_200, int(4_000 * scale))
    s = sim.new_stage_id()
    job = sim.start_job("tiny file compaction check", [(s, "scan many small files", n, [])])
    plans = [_Plan(run_ms=sim.jitter(30, 0.3), deser_ms=5, overhead_ms=22, input_bytes=sim.jitter(64 * 1024)) for _ in range(n)]
    sim.run_stage(s, 0, "scan many small files", plans, [])
    sim.end_job(job)


def _single_task_job(sim: _Simulator, scale: float) -> None:
    s = sim.new_stage_id()
    job = sim.start_job("global sort + collect", [(s, "single partition sort", 1, [])], sql=False)
    plans = [_Plan(run_ms=330_000, gc_ms=45_000, input_bytes=int(1.2 * GIB), result_size=int(1.5 * GIB))]
    sim.run_stage(s, 0, "single partition sort", plans, [])
    sim.end_job(job, sql_end=False)


def _failure_jobs(sim: _Simulator, scale: float) -> None:
    n = max(40, int(100 * scale))
    # --- executor loss from container memory-limit kills; failed tasks are retried ----------
    s = sim.new_stage_id()
    job = sim.start_job("feature extraction", [(s, "extract features", n, [])])
    lost = sim.alive[:2]
    oom = {
        "Reason": "ExecutorLostFailure",
        "Executor ID": lost[0],
        "Exit Caused By App": True,
        "Loss Reason": "Container killed by YARN for exceeding memory limits. 8.4 GB of 8 GB physical memory used. "
        "Consider boosting spark.executor.memoryOverhead.",
    }
    plans: list[_Plan] = []
    for i in range(n):
        plans.append(_Plan(run_ms=sim.jitter(5_000), gc_ms=300, input_bytes=sim.jitter(100 * MIB)))
    for i in range(12):
        plans.append(
            _Plan(run_ms=sim.jitter(2_500), status="failed", reason=oom, force_executor=lost[i % 2], input_bytes=sim.jitter(100 * MIB))
        )
        plans.append(_Plan(run_ms=sim.jitter(5_000), gc_ms=300, input_bytes=sim.jitter(100 * MIB)))  # the retry
    sim.run_stage(s, 0, "extract features", plans, [])
    for eid in lost:
        sim.remove_executor(eid, oom["Loss Reason"])
        sim.add_executor()
    sim.end_job(job)

    # --- stage retried after shuffle FetchFailed ---------------------------------------------
    s_map, s_red = sim.new_stage_id(), sim.new_stage_id()
    job = sim.start_job("rollup after executor loss", [(s_map, "map side", 60, []), (s_red, "reduce side", 60, [s_map])])
    sim.run_stage(s_map, 0, "map side", [_Plan(run_ms=sim.jitter(2_000), shuffle_write=sim.jitter(32 * MIB)) for _ in range(60)], [])
    fetch_failed = {"Reason": "FetchFailed", "Shuffle ID": 3, "Map ID": 7, "Reduce ID": 2, "Message": "Failed to connect"}
    red0 = [_Plan(run_ms=sim.jitter(1_800), shuffle_read=sim.jitter(32 * MIB)) for _ in range(50)]
    red0 += [_Plan(run_ms=400, status="failed", reason=fetch_failed) for _ in range(10)]
    sim.run_stage(s_red, 0, "reduce side", red0, [s_map], failure="FetchFailed(BlockManagerId(2, worker-3, 7337), shuffleId=3)")
    sim.run_stage(s_red, 1, "reduce side", [_Plan(run_ms=sim.jitter(1_800), shuffle_read=sim.jitter(32 * MIB)) for _ in range(60)], [s_map])
    sim.end_job(job)

    # --- a job that fails for good -----------------------------------------------------------
    s_bad = sim.new_stage_id()
    job = sim.start_job("data quality check", [(s_bad, "divide by zero", 8, [])], sql=False)
    boom = {
        "Reason": "ExceptionFailure",
        "Class Name": "java.lang.ArithmeticException",
        "Description": "/ by zero",
        "Stack Trace": [],
        "Accumulator Updates": [],
    }
    plans = [_Plan(run_ms=sim.jitter(1_500)) for _ in range(7)]
    plans += [_Plan(run_ms=200, status="failed", reason=boom) for _ in range(4)]
    sim.run_stage(
        s_bad, 0, "divide by zero", plans, [],
        failure="Job aborted due to stage failure: Task 3 in stage failed 4 times: java.lang.ArithmeticException: / by zero",
    )
    sim.end_job(
        job, sql_end=False,
        failure="Job aborted due to stage failure: Task 3 in stage 7.0 failed 4 times, most recent failure: "
        "java.lang.ArithmeticException: / by zero",
    )


def generate_event_log(
    scenario: str = "mixed", *, seed: int = 42, executors: int = 8, cores: int = 4, scale: float = 1.0
) -> Iterator[str]:
    """Yield event-log lines (without trailing newlines) for the given scenario."""
    if scenario not in SCENARIOS:
        raise ValueError(f"Unknown scenario '{scenario}' (choose from: {', '.join(SCENARIOS)})")
    if executors < 4 or cores < 1:
        raise ValueError("executors must be >= 4 and cores >= 1")
    sim = _Simulator(seed, executors, cores)
    if scenario == "healthy":
        sim.props["spark.sql.adaptive.enabled"] = "true"
    sim.start_application(executors)
    if scenario == "healthy":
        _healthy_job(sim, scale)
    elif scenario == "skew":
        _skew_job(sim, scale)
    elif scenario == "failures":
        _failure_jobs(sim, scale)
    else:  # mixed: every pathology once
        _skew_job(sim, scale)
        _gc_and_straggler_job(sim, scale)
        _small_tasks_job(sim, scale)
        _single_task_job(sim, scale)
        _failure_jobs(sim, scale)
    sim.finish_application()
    yield from sim.lines


def write_event_log(path: str, **kwargs: Any) -> int:
    """Write a generated log to ``path`` (gzip if it ends with .gz); returns the number of lines."""
    import gzip

    opener = gzip.open if str(path).endswith(".gz") else open
    count = 0
    with opener(path, "wt", encoding="utf-8") as fh:  # type: ignore[operator]
        for line in generate_event_log(**kwargs):
            fh.write(line + "\n")
            count += 1
    return count
