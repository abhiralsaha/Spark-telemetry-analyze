# Spark Telemetry Report: nightly-customer-etl

**Health score:** 0/100 (grade **F**) &middot; **Findings:** 8 critical, 10 warning, 2 info

## Summary

| Metric | Value |
| --- | --- |
| Application | nightly-customer-etl (application_1760000000000_0042) |
| Spark version | 3.5.1 |
| User | etl |
| Duration | 9m 13s |
| Jobs | 7 (1 failed) |
| Stages | 9 (2 failed attempts) |
| Tasks | 4,916 (26 failed, 0 killed) |
| Executors | 10 (2 lost), peak 32 cores |
| Executor run time | 1h 10m |
| GC time | 7m 59s (11.2%) |
| Core utilization | 24.2% |
| Input / output | 57.03 GiB / 0 B |
| Shuffle read / write | 22.45 GiB / 14.45 GiB |
| Spill (memory / disk) | 9.00 GiB / 2.05 GiB |

## Findings

### Critical (8)

- **DATA_SKEW** _(stage 1.0)_ - Data skew in stage 1
  - In stage 1 (attempt 0, 'shuffle join') the largest task read 2.52 GiB of shuffle read data vs a median of 61.10 MiB (42.2x). The slowest task took 52.3 s vs 3.1 s median, so up to 49.3 s of stage time is spent waiting on it.
  - **Fix:** Enable Adaptive Query Execution skew-join handling (spark.sql.adaptive.enabled and spark.sql.adaptive.skewJoin.enabled), salt hot keys, pre-aggregate before the shuffle, or broadcast the smaller side of the join.
- **GC_PRESSURE** _(stage 2.0)_ - High garbage-collection time in stage 2
  - JVM GC consumed 25.0% of executor run time in stage 2 (attempt 0, 'sessionize') (5m 31s of 22m 05s).
  - **Fix:** Give executors more memory or fewer cores, prefer the built-in DataFrame API over RDD/UDF object-heavy code, use Kryo serialization, and consider G1GC (-XX:+UseG1GC).
- **JOB_FAILURE** _(job 6)_ - Job 6 failed
  - Job aborted due to stage failure: Task 3 in stage 7.0 failed 4 times, most recent failure: java.lang.ArithmeticException: / by zero
  - **Fix:** Inspect the failing stage's task errors and executor logs; fix the root cause before tuning.
- **LOW_PARALLELISM** _(stage 4.0)_ - Stage 4 cannot use the whole cluster
  - stage 4 (attempt 0, 'single partition sort') ran only 1 task(s) for 5m 30s while up to 32 cores were available.
  - **Fix:** Increase partitions (repartition, spark.sql.shuffle.partitions, spark.default.parallelism), avoid single-partition operations such as global sorts or coalesce(1), and split large non-splittable files.
- **MEMORY_ERRORS** - Out-of-memory or container memory-limit kills detected
  - 14 memory-related failure event(s), e.g. ExecutorLostFailure: Container killed by YARN for exceeding memory limits. 8.4 GB of 8 GB physical memory used. Consider boosting spark.exe…; Container killed by YARN for exceeding memory limits. 8.4 GB of 8 GB physical memory used. Consider boosting spark.executor.memoryOverhead.
  - **Fix:** Raise spark.executor.memory and/or spark.executor.memoryOverhead (YARN/K8s kills usually mean off-heap overhead is too small), reduce partition size (more partitions), avoid collecting large results, and check for skewed keys.
- **TASK_FAILURES** _(stage 5.0)_ - 12 failed task(s) in stage 5
  - 9.7% of tasks in stage 5 (attempt 0, 'extract features') failed. Most common: ExecutorLostFailure: Container killed by YARN for exceeding memory limits. 8.4 GB of 8 GB physical memory used. Consider boosting spark.exec… (x12)
  - **Fix:** Failed attempts re-run work. Review the top failure reasons; if they are transient (executor loss, fetch failures) address cluster stability, otherwise fix the code/data issue.
- **TASK_FAILURES** _(stage 7.0)_ - 10 failed task(s) in stage 7
  - 16.7% of tasks in stage 7 (attempt 0, 'reduce side') failed. Most common: FetchFailed (x10)
  - **Fix:** Failed attempts re-run work. Review the top failure reasons; if they are transient (executor loss, fetch failures) address cluster stability, otherwise fix the code/data issue.
- **TASK_FAILURES** _(stage 8.0)_ - 4 failed task(s) in stage 8
  - 36.4% of tasks in stage 8 (attempt 0, 'divide by zero') failed. Most common: java.lang.ArithmeticException: / by zero (x4)
  - **Fix:** Failed attempts re-run work. Review the top failure reasons; if they are transient (executor loss, fetch failures) address cluster stability, otherwise fix the code/data issue.

### Warning (10)

- **DISK_SPILL** _(stage 1.0)_ - Spill to disk in stage 1
  - stage 1 (attempt 0, 'shuffle join') spilled 2.05 GiB to disk (9.00 GiB in-memory size). Spilling means execution memory was too small for the working set and adds heavy disk I/O.
  - **Fix:** Increase the number of shuffle partitions (smaller tasks), raise spark.executor.memory or spark.memory.fraction, reduce executor cores per executor, or filter/aggregate earlier.
- **EXECUTOR_LOSS** - 2 executor(s) were lost unexpectedly
  - Executors disappeared for reasons other than a normal scale-down: Container killed by YARN for exceeding memory limits. 8.4 GB of 8 GB physical memory used. Consider boosting spark.exec… (x2)
  - **Fix:** Look for OOM kills, node preemption/spot reclamation, or failing disks. Cached data and shuffle output on lost executors must be recomputed.
- **GC_PRESSURE** _(stage 4.0)_ - High garbage-collection time in stage 4
  - JVM GC consumed 13.6% of executor run time in stage 4 (attempt 0, 'single partition sort') (45.0 s of 5m 30s).
  - **Fix:** Give executors more memory or fewer cores, prefer the built-in DataFrame API over RDD/UDF object-heavy code, use Kryo serialization, and consider G1GC (-XX:+UseG1GC).
- **LARGE_RESULT** _(stage 4.0)_ - Large result returned to the driver in stage 4
  - stage 4 (attempt 0, 'single partition sort') sent 1.50 GiB of task results to the driver (collect/toPandas/take?).
  - **Fix:** Write results to storage instead of collecting them; if collecting is required, aggregate or limit first and size spark.driver.memory / spark.driver.maxResultSize accordingly.
- **LOW_UTILIZATION** - Cluster cores were mostly idle
  - Tasks kept only 24.2% of the 4h 53m of provisioned core-time busy (dynamic allocation disabled).
  - **Fix:** Right-size the cluster or enable dynamic allocation (spark.dynamicAllocation.enabled=true with shuffle tracking) so executors are released while the driver or a single-threaded stage is working.
- **SCHEDULING_OVERHEAD** _(stage 3.0)_ - Scheduler / launch overhead in stage 3
  - About 38.2% of task wall time in stage 3 (attempt 0, 'scan many small files') is not spent running, deserializing or serializing (scheduler delay, result fetch, launch latency).
  - **Fix:** Use fewer, larger tasks, check driver CPU/GC and network latency between driver and executors, and avoid very large task results.
- **SERIALIZATION_OVERHEAD** _(stage 2.0)_ - Task deserialization overhead in stage 2
  - 21.4% of task time in stage 2 (attempt 0, 'sessionize') is spent deserializing the task closure (6m 00s).
  - **Fix:** Large closures are shipped with every task: broadcast big lookup objects, avoid capturing large driver-side collections, and use fewer, larger tasks.
- **SMALL_TASKS** _(stage 3.0)_ - Too many tiny tasks in stage 3
  - stage 3 (attempt 0, 'scan many small files') ran 4,000 tasks with a median duration of 58 ms. Task scheduling and serialization overhead likely dominates useful work.
  - **Fix:** Reduce partition counts (coalesce, lower spark.sql.shuffle.partitions), enable AQE partition coalescing (spark.sql.adaptive.coalescePartitions.enabled), or compact small input files.
- **STAGE_RETRY** _(stage 7)_ - Stage 7 was retried 1 time(s)
  - Re-running a stage wastes compute and usually follows lost executors or shuffle fetch failures. 10 FetchFailed task(s) were observed.
  - **Fix:** Check for lost executors / node preemption, enable the external shuffle service or shuffle tracking, and consider raising spark.executor.memoryOverhead.
- **STRAGGLER_TASKS** _(stage 2.0)_ - Straggler tasks in stage 2
  - In stage 2 (attempt 0, 'sessionize') the slowest task took 1m 14s vs a median of 7.7 s (9.6x) although input sizes are balanced.
  - **Fix:** Enable speculative execution (spark.speculation=true), check the executors/hosts that ran the slowest tasks for noisy neighbours or bad disks, and look at GC time in those tasks.

### Info (2)

- **CONFIG_ADVISORY** - Adaptive Query Execution is disabled
  - spark.sql.adaptive.enabled=false. AQE coalesces shuffle partitions, switches join strategies and splits skewed partitions at runtime.
  - **Fix:** Set spark.sql.adaptive.enabled=true (default since Spark 3.2).
- **CONFIG_ADVISORY** - Java serialization used with significant shuffle volume
  - The application shuffled 14.45 GiB using the default JavaSerializer, which is slower and larger than Kryo for RDD workloads.
  - **Fix:** Set spark.serializer=org.apache.spark.serializer.KryoSerializer (DataFrame shuffles already use Tungsten and are unaffected).

## Longest stages (top 10)

| Stage | Name | Status | Duration | Tasks | Task p50 / max | Shuffle read | Disk spill | GC |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 4.0 | single partition sort | COMPLETE | 5m 30s | 1 | 5m 30s / 5m 30s | 0 B | 0 B | 13.6% |
| 2.0 | sessionize | COMPLETE | 1m 14s | 200 | 7.7 s / 1m 14s | 0 B | 0 B | 25.0% |
| 1.0 | shuffle join | COMPLETE | 52.4 s | 200 | 3.1 s / 52.3 s | 18.98 GiB | 2.05 GiB | 6.0% |
| 0.0 | scan orders | COMPLETE | 27.3 s | 200 | 4.1 s / 4.8 s | 0 B | 0 B | 3.0% |
| 5.0 | extract features | COMPLETE | 21.6 s | 124 | 5.0 s / 6.0 s | 0 B | 0 B | 5.7% |
| 3.0 | scan many small files | COMPLETE | 7.7 s | 4,000 | 58 ms / 66 ms | 0 B | 0 B | 0.0% |
| 6.0 | map side | COMPLETE | 4.6 s | 60 | 2.0 s / 2.4 s | 0 B | 0 B | 0.0% |
| 7.1 | reduce side | COMPLETE | 4.1 s | 60 | 1.9 s / 2.2 s | 1.93 GiB | 0 B | 0.0% |
| 7.0 | reduce side | FAILED | 3.8 s | 60 | 1.7 s / 2.1 s | 1.54 GiB | 0 B | 0.0% |
| 8.0 | divide by zero | FAILED | 1.6 s | 11 | 1.5 s / 1.6 s | 0 B | 0 B | 0.0% |

## Busiest executors (top 10)

| Executor | Host | Cores | Tasks | Run time | GC | Shuffle read | Removed |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | worker-2.cluster.internal | 4 | 582 | 14m 16s | 14.0% | 7.70 GiB | Container killed by YARN for exceeding memory lim… |
| 3 | worker-4.cluster.internal | 4 | 613 | 8m 55s | 12.0% | 2.13 GiB |  |
| 8 | worker-1.cluster.internal | 4 | 620 | 7m 59s | 10.5% | 2.04 GiB |  |
| 4 | worker-1.cluster.internal | 4 | 620 | 7m 49s | 10.2% | 2.09 GiB |  |
| 5 | worker-2.cluster.internal | 4 | 616 | 7m 47s | 10.6% | 2.04 GiB |  |
| 7 | worker-4.cluster.internal | 4 | 611 | 7m 45s | 10.6% | 1.90 GiB |  |
| 6 | worker-3.cluster.internal | 4 | 613 | 7m 38s | 10.4% | 2.10 GiB |  |
| 2 | worker-3.cluster.internal | 4 | 598 | 7m 28s | 11.2% | 1.62 GiB | Container killed by YARN for exceeding memory lim… |
| 9 | worker-2.cluster.internal | 4 | 22 | 40.0 s | 0.0% | 458.36 MiB |  |
| 10 | worker-3.cluster.internal | 4 | 21 | 37.9 s | 0.0% | 391.61 MiB |  |

## Key Spark properties

| Property | Value |
| --- | --- |
| spark.master | yarn |
| spark.submit.deployMode | cluster |
| spark.executor.instances | 8 |
| spark.executor.cores | 4 |
| spark.executor.memory | 8g |
| spark.driver.memory | 4g |
| spark.dynamicAllocation.enabled | false |
| spark.sql.shuffle.partitions | 200 |
| spark.sql.adaptive.enabled | false |

_Generated 2026-10-14T06:49:26+00:00 by spark-telemetry-analyzer 1.0.0_
