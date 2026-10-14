# Detection rules

Severity: **critical** = likely wasting large amounts of time/money or failing; **warning** = worth fixing; **info** = advisory.
All thresholds are in `config/sta.example.toml` and can be overridden. Disable any rule with `--disable-rule ID` or
`[analysis] disabled_rules`.

| Rule | Detects | Default trigger | Typical fix |
| --- | --- | --- | --- |
| `JOB_FAILURE` | Jobs that ended failed | any failed job (critical) | fix root cause in the failing stage |
| `STAGE_RETRY` | Stage re-attempts (often FetchFailed) | stage with > 1 attempt | stabilise executors, shuffle service/tracking |
| `TASK_FAILURES` | Failed task attempts per stage | any failure; warn >= 1%, critical >= 5% | inspect top failure reasons |
| `MEMORY_ERRORS` | OOM / YARN-K8s memory-limit kills | text match in failures, executor removals | raise memory/overhead, more partitions |
| `DATA_SKEW` | Largest task data >> median | max/median shuffle-read or input >= 5x (critical 20x), max >= 64 MiB | AQE skew join, salting, broadcast |
| `STRAGGLER_TASKS` | Slow tasks without data skew | slowest/median >= 4x (critical 10x), slowest >= 10 s | speculation, check bad node/GC |
| `DISK_SPILL` | Execution memory spilled to disk | disk >= 512 MiB (critical 10 GiB) or memory >= 2 GiB | more partitions / memory |
| `GC_PRESSURE` | GC share of task run time | >= 10% (critical 20%) | memory, Kryo, G1GC, fewer cores |
| `SERIALIZATION_OVERHEAD` | Closure deserialization share | >= 20% (critical 40%) | broadcast large objects |
| `SMALL_TASKS` | Thousands of tiny tasks | >= 1000 tasks, median < 100 ms | coalesce / AQE coalescing |
| `SCHEDULING_OVERHEAD` | Task time not spent doing work | >= 30% over >= 100 tasks | fewer, larger tasks |
| `LOW_PARALLELISM` | Long stage with fewer tasks than cores | stage >= 30 s, tasks < available cores | repartition; avoid `coalesce(1)` |
| `LARGE_PARTITIONS` | Huge average partition | >= 1 GiB per task (critical 4 GiB) | more partitions |
| `SHUFFLE_FETCH_WAIT` | Tasks blocked on shuffle fetch | >= 15% of run time | reduce shuffle, faster disks/network |
| `HEAVY_SHUFFLE` | Very large shuffle volume | >= 50 GiB written (info) | prune early, broadcast joins |
| `LARGE_RESULT` | Large results to the driver | >= 1 GiB per stage (critical 4 GiB) | write instead of collect |
| `EXECUTOR_LOSS` | Executors lost abnormally | removal reason is not a normal scale-down | check OOM kills, preemption |
| `EXECUTOR_IMBALANCE` | Uneven work across long-lived executors | busiest >= 3x median (>= 4 executors) | locality/skew/caching |
| `LOW_UTILIZATION` | Idle provisioned cores | utilization < 30% (critical 10%), app >= 5 min | right-size, dynamic allocation |
| `CONFIG_ADVISORY` | AQE off, Java serializer with big shuffle, Spark 2.x | see rule | enable AQE, Kryo, upgrade |
| `INCOMPLETE_LOG` | Missing app-end event, malformed lines | info | re-run after the app ends |

## Health score

`100 - sum(per-rule penalties)` where critical = 15, warning = 5, info = 1 points per finding and each rule is capped at 25 points,
so one noisy rule cannot dominate. Grades: A >= 90, B >= 75, C >= 60, D >= 40, F below.

## Adding a rule

```python
# src/spark_telemetry_analyzer/rules.py
@rule("MY_RULE")
def my_rule(app, th):
    """One-line description shown by `sta rules`."""
    for st in app.stages.values():
        if ...:
            yield Finding("MY_RULE", Severity.WARNING, "Title", "Message", scope="stage", scope_id=st.key,
                          evidence={...}, recommendation="...")
```
Add a threshold to `Thresholds` in `config.py` if needed, a test in `tests/test_rules.py`, and a row here.
