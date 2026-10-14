import unittest

from spark_telemetry_analyzer.aggregator import REDACTED, EventAggregator
from spark_telemetry_analyzer.models import Reservoir


def task_end(stage=0, attempt=0, task_id=1, executor="1", launch=1000, finish=2000, reason=None, killed=False, **m):
    return {
        "Event": "SparkListenerTaskEnd", "Stage ID": stage, "Stage Attempt ID": attempt,
        "Task End Reason": reason or {"Reason": "Success"},
        "Task Info": {"Task ID": task_id, "Index": task_id, "Attempt": 0, "Launch Time": launch, "Finish Time": finish,
                      "Executor ID": executor, "Host": "h1", "Speculative": False, "Killed": killed,
                      "Failed": reason is not None and not killed},
        "Task Metrics": {"Executor Run Time": m.get("run", 900), "JVM GC Time": m.get("gc", 90),
                         "Disk Bytes Spilled": m.get("spill", 0),
                         "Shuffle Read Metrics": {"Remote Bytes Read": m.get("sr", 0), "Local Bytes Read": m.get("sl", 0)},
                         "Shuffle Write Metrics": {"Shuffle Bytes Written": m.get("sw", 0), "Shuffle Write Time": 5_000_000},
                         "Input Metrics": {"Bytes Read": m.get("inp", 0)}},
    }


class AggregatorTest(unittest.TestCase):
    def setUp(self):
        self.agg = EventAggregator(reservoir_size=100)

    def test_unknown_events_are_ignored(self):
        self.assertFalse(self.agg.consume({"Event": "SparkListenerBlockManagerAdded"}))
        self.assertFalse(self.agg.consume({}))
        self.assertEqual(self.agg.events_consumed, 0)

    def test_namespaced_sql_events_are_recognised(self):
        self.agg.consume({"Event": "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart",
                          "executionId": 3, "description": "select 1", "time": 5000})
        self.agg.consume({"Event": "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd", "executionId": 3, "time": 9000})
        q = self.agg.build().sql[3]
        self.assertEqual((q.description, q.duration_ms), ("select 1", 4000))

    def test_task_sums_and_status(self):
        self.agg.consume(task_end(task_id=1, sr=100, sl=50, sw=7, inp=11))
        self.agg.consume(task_end(task_id=2, reason={"Reason": "ExceptionFailure", "Class Name": "java.io.IOException", "Description": "disk full\nstack"}))
        self.agg.consume(task_end(task_id=3, reason={"Reason": "TaskKilled", "Kill Reason": "another attempt succeeded"}, killed=True))
        st = self.agg.build().stages[(0, 0)]
        self.assertEqual((st.tasks_succeeded, st.tasks_failed, st.tasks_killed), (1, 1, 1))
        self.assertEqual(st.shuffle_read_bytes, 150)
        self.assertEqual(st.shuffle_write_bytes, 7)
        self.assertEqual(st.shuffle_write_ms, 15)  # 3 tasks x 5ms (nanoseconds converted to ms)
        self.assertEqual(st.input_bytes, 11)
        self.assertEqual(list(st.failure_reasons), ["java.io.IOException: disk full"])
        self.assertEqual(st.reservoir.seen, 1)  # distributions only describe successful attempts

    def test_failure_labels(self):
        cases = [
            ({"Reason": "FetchFailed", "Message": "x"}, "FetchFailed"),
            ({"Reason": "ExecutorLostFailure", "Loss Reason": "node lost"}, "ExecutorLostFailure: node lost"),
            ({"Reason": "TaskCommitDenied"}, "TaskCommitDenied"),
        ]
        for i, (reason, label) in enumerate(cases):
            self.agg.consume(task_end(task_id=i, reason=reason))
        labels = set(self.agg.build().stages[(0, 0)].failure_reasons)
        self.assertEqual(labels, {c[1] for c in cases})

    def test_stage_attempts_are_tracked_separately(self):
        self.agg.consume(task_end(stage=4, attempt=0))
        self.agg.consume(task_end(stage=4, attempt=1))
        self.assertEqual({k for k in self.agg.build().stages}, {(4, 0), (4, 1)})

    def test_exact_max_and_slowest_tasks_survive_sampling(self):
        agg = EventAggregator(reservoir_size=100)
        for i in range(1000):
            agg.consume(task_end(task_id=i, launch=0 + 1, finish=1 + 100 + (50_000 if i == 777 else 0)))
        st = agg.build().stages[(0, 0)]
        self.assertLessEqual(len(st.reservoir.rows), 100)
        self.assertEqual(st.reservoir.seen, 1000)
        self.assertEqual(st.max_task_ms, 50_100)
        self.assertEqual(st.slowest_tasks()[0]["task_id"], 777)
        self.assertEqual(len(st.slowest_tasks()), 5)
        self.assertEqual(st.distribution(0)["max"], 50_100)

    def test_secrets_are_redacted(self):
        self.agg.consume({"Event": "SparkListenerEnvironmentUpdate", "Spark Properties": {
            "spark.hadoop.fs.s3a.secret.key": "abc", "spark.ssl.keyPassword": "pw", "spark.executor.memory": "4g",
            "spark.driver.extraJavaOptions": "-Xmx1g -Ddb.password=hunter2 -Dfoo=bar"}})
        props = self.agg.build().spark_properties
        self.assertEqual(props["spark.hadoop.fs.s3a.secret.key"], REDACTED)
        self.assertEqual(props["spark.ssl.keyPassword"], REDACTED)
        self.assertEqual(props["spark.executor.memory"], "4g")
        self.assertNotIn("hunter2", props["spark.driver.extraJavaOptions"])
        self.assertIn("-Dfoo=bar", props["spark.driver.extraJavaOptions"])

    def test_redaction_can_be_disabled(self):
        agg = EventAggregator(redact=False)
        agg.consume({"Event": "SparkListenerEnvironmentUpdate", "Spark Properties": {"my.secret": "v"}})
        self.assertEqual(agg.build().spark_properties["my.secret"], "v")

    def test_legacy_list_style_properties(self):
        self.agg.consume({"Event": "SparkListenerEnvironmentUpdate", "Spark Properties": [["spark.app.name", "legacy"]]})
        app = self.agg.build()
        self.assertEqual(app.app_name, "legacy")

    def test_local_mode_core_inference(self):
        self.agg.consume({"Event": "SparkListenerEnvironmentUpdate", "Spark Properties": {"spark.master": "local[6]"}})
        self.agg.consume(task_end(executor="driver"))
        self.assertEqual(self.agg.build().executors["driver"].cores, 6)

    def test_job_failure_and_sql_mapping(self):
        self.agg.consume({"Event": "SparkListenerJobStart", "Job ID": 1, "Submission Time": 10, "Stage IDs": [0],
                          "Stage Infos": [], "Properties": {"spark.sql.execution.id": "9", "spark.job.description": "d"}})
        self.agg.consume({"Event": "SparkListenerJobEnd", "Job ID": 1, "Completion Time": 20,
                          "Job Result": {"Result": "JobFailed", "Exception": {"Message": "boom"}}})
        app = self.agg.build()
        self.assertEqual((app.jobs[1].status, app.jobs[1].failure_reason, app.jobs[1].duration_ms), ("FAILED", "boom", 10))
        self.assertEqual(app.sql[9].job_ids, [1])

    def test_lenient_with_missing_and_odd_fields(self):
        self.agg.consume({"Event": "SparkListenerTaskEnd", "Stage ID": 0})  # no info/metrics
        self.agg.consume({"Event": "SparkListenerTaskEnd"})  # no stage at all: ignored
        self.agg.consume({"Event": "SparkListenerStageCompleted", "Stage Info": {"Stage ID": "7", "Number of Tasks": "x"}})
        app = self.agg.build()
        self.assertEqual(app.stages[(0, 0)].tasks_succeeded, 1)
        self.assertIn((7, 0), app.stages)

    def test_duration_falls_back_to_last_event(self):
        self.agg.consume({"Event": "SparkListenerApplicationStart", "App ID": "a", "App Name": "n", "Timestamp": 1000})
        self.agg.consume(task_end(launch=1500, finish=4000))
        app = self.agg.build()
        self.assertFalse(app.has_app_end)
        self.assertEqual(app.duration_ms, 3000)


class ReservoirTest(unittest.TestCase):
    def test_capacity_and_determinism(self):
        a, b = Reservoir(10, seed=1), Reservoir(10, seed=1)
        for i in range(500):
            a.add((i,))
            b.add((i,))
        self.assertEqual(len(a.rows), 10)
        self.assertEqual(a.rows, b.rows)
        self.assertEqual(a.seen, 500)


if __name__ == "__main__":
    unittest.main()
