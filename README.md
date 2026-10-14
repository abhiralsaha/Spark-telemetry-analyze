# Spark Telemetry Analyzer

Turn Apache Spark **event logs** into actionable performance findings: data skew, stragglers, disk spill,
GC pressure, failed/retried stages, lost executors, tiny-task overhead, idle clusters and more. Each finding comes
with evidence and a concrete fix, plus an overall health score (0-100).

* **Zero runtime dependencies** (Python >= 3.11 standard library). Optional `zstd` / `lz4` extras for compressed logs.
* **Streaming and memory-bounded**: tasks are never stored; per-stage sums, exact maxima, a fixed-size reservoir sample
  for percentiles and the 5 slowest tasks are kept, so multi-GB logs with millions of tasks are fine.
* **Four ways to use it**: CLI, Python library, HTTP service (REST + Prometheus `/metrics` + directory watcher), and
  Spark History Server fetcher.
* **Outputs**: text, JSON (stable `schema_version`), Markdown, self-contained HTML, Prometheus.
* **CI-friendly**: `--fail-on`, `--min-health` exit codes and a `compare` command for regression detection between runs.
* **Safe by default**: secrets in Spark properties are redacted, HTML output is escaped, the service supports bearer-token auth,
  request size limits, bounded concurrency and runs as non-root in the container.

## Install

```bash
pip install .                    # from this directory
pip install ".[zstd,lz4]"        # also read zstd / lz4 compressed event logs
```

Python 3.11+ is required. For development: `pip install -e ".[dev]"`.

## Quick start

```bash
sta sample -o demo.json                 # generate a synthetic event log with every kind of problem
sta analyze demo.json                   # colored text report
sta analyze demo.json -f html -o report.html
sta analyze demo.json -f json | jq '.findings[] | {rule_id, severity, title}'
```

A pre-generated sample log and its reports live in `examples/` (`examples/reports/sample_report.html` opens in any browser).

Your own logs: enable Spark event logging (`spark.eventLog.enabled=true`, `spark.eventLog.dir=...`) and point the tool at the
files. Accepted inputs: plain JSON-lines, `.gz`, `.zip` (what the History Server `/logs` endpoint returns), `.zstd` / `.lz4`
(with extras), `.inprogress` logs (truncated last line is tolerated), **rolling** event-log directories
(`eventlog_v2_<app-id>/events_N_<app-id>`), whole directories of logs, and `-` for stdin.

```bash
sta analyze /data/spark-events/application_1700000000000_0042
sta analyze /data/spark-events/ --output-dir reports/ -f html     # one report per application
hdfs dfs -cat /spark-logs/app_123 | sta analyze -
```

## CLI reference

| Command | Purpose |
| --- | --- |
| `sta analyze PATH...` | Analyze logs. `-f text\|json\|markdown\|html\|prometheus`, `-o FILE`, `--output-dir DIR`, `-c CONFIG`, `--top N`, `--disable-rule ID`, `--fail-on {info,warning,critical}`, `--min-health N` |
| `sta fetch --history-url URL` | Download event logs from a Spark History Server (`--app-id`, `--limit`, `--analyze`). Auth via `STA_HISTORY_TOKEN` or `STA_HISTORY_USER`/`STA_HISTORY_PASSWORD` |
| `sta compare BASE NEW` | Compare two runs (logs or saved JSON reports); `--fail-on-regression`, `--tolerance 0.1` |
| `sta serve` | Run the HTTP service (see below) |
| `sta sample` | Generate synthetic logs: `-s mixed\|healthy\|skew\|failures` |
| `sta rules` | List detection rules |

Exit codes: `0` ok, `1` findings/health threshold breached (or regression), `2` error (bad input, config, network).
Logs go to stderr (`-v`/`-vv`, `--log-format json`); reports go to stdout, so pipes are safe.

### Use it as a CI / scheduling gate

```bash
sta analyze logs/ --fail-on critical --min-health 70 -f markdown -o report.md
sta compare yesterday.json today_log --fail-on-regression
```

## HTTP service

```bash
export STA_TOKEN=$(openssl rand -hex 24)
sta serve --host 0.0.0.0 --port 8080 --watch-dir /data/spark-events
```

| Endpoint | Description |
| --- | --- |
| `GET /healthz`, `GET /readyz` | liveness / readiness (ready after the first directory scan) |
| `GET /metrics` | Prometheus metrics: per-app health score, GC ratio, utilization, spill, shuffle, finding counts per rule/severity + service counters |
| `GET /api/v1/reports` | stored report summaries |
| `GET /api/v1/reports/{app_id}?format=json\|html\|markdown\|text` | one report |
| `POST /api/v1/analyze` | body = raw event log (plain / gzip / zip) -> report. `curl -H "Authorization: Bearer $STA_TOKEN" --data-binary @log.json.gz localhost:8080/api/v1/analyze` |

* `/api/*` requires `Authorization: Bearer <STA_TOKEN>` when a token is configured; probes and `/metrics` stay open for scrapers.
* The watcher analyzes each log once it has stopped changing (`--settle-seconds`), ignores `.inprogress` files unless
  `--include-inprogress`, and never dies on a bad file.
* Reports are held in memory (bounded by `--max-reports`); Prometheus is the long-term store. Request bodies are limited by
  `--max-body-mb`, concurrent analyses by `--max-concurrent` (extra requests get `503 Retry-After`).
* Binding to a non-loopback address without a token logs a warning. Put TLS in front (ingress / reverse proxy).

### Docker / Kubernetes

```bash
docker build -t spark-telemetry-analyzer:1.0.0 .
docker compose up --build            # analyzer + Prometheus; drop logs into ./eventlogs
```

`deploy/kubernetes.yaml` (hardened Deployment + Service), `deploy/prometheus.yml` and `deploy/alerts.example.yml` are included.
The container runs as UID 10001, read-only root filesystem, no capabilities, with a healthcheck.

## Python API

```python
from spark_telemetry_analyzer import analyze_source, load_settings, Severity

report = analyze_source("/data/spark-events/application_123", load_settings("sta.toml"))
print(report.health_score, report.max_severity)
for f in report.findings:                      # list of plain dicts
    print(f["severity"], f["rule_id"], f["title"])
if report.has_findings_at_least(Severity.CRITICAL):
    ...
data = report.data                              # JSON-serializable dict, schema_version = 1
```

## What it detects

`sta rules` lists every rule; [docs/RULES.md](docs/RULES.md) explains each rule, its evidence and the default thresholds.
Highlights: `DATA_SKEW`, `STRAGGLER_TASKS`, `DISK_SPILL`, `GC_PRESSURE`, `SMALL_TASKS`, `SCHEDULING_OVERHEAD`, `LOW_PARALLELISM`,
`LARGE_PARTITIONS`, `SHUFFLE_FETCH_WAIT`, `TASK_FAILURES`, `STAGE_RETRY`, `JOB_FAILURE`, `MEMORY_ERRORS`, `EXECUTOR_LOSS`,
`EXECUTOR_IMBALANCE`, `LOW_UTILIZATION`, `LARGE_RESULT`, `CONFIG_ADVISORY`.

Tune everything in a TOML file (see `config/sta.example.toml`), pass it with `-c` or `STA_CONFIG`. Unknown keys are rejected so a
typo can't silently disable a check.

## Environment variables

`STA_CONFIG`, `STA_LOG_LEVEL`, `STA_LOG_FORMAT`, `STA_HOST`, `STA_PORT`, `STA_WATCH_DIR`, `STA_TOKEN`, `STA_HISTORY_URL`,
`STA_HISTORY_TOKEN`, `STA_HISTORY_USER`, `STA_HISTORY_PASSWORD`, `NO_COLOR`.

## Development

```bash
make dev && make test          # 90+ tests, standard-library unittest (pytest also works)
make lint typecheck            # ruff + mypy
make demo                      # regenerate the sample HTML report
```

Project layout: see [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## Limitations (read before relying on it)

* Analysis is only as good as the event log: Spark must write task metrics (default). Snappy-compressed logs are not supported
  (use `spark.eventLog.compress=false`, gzip, zstd or lz4).
* Percentiles are estimated from a per-stage reservoir sample (default 20k tasks); maxima, sums and the slowest tasks are exact.
* Thresholds are heuristics tuned for typical batch workloads; adjust them for your jobs. Findings are leads to investigate,
  not proof.
* Utilization assumes one core per task (`spark.task.cpus=1`) and uses executor add/remove events; with Spark local mode it uses
  `local[N]`.
* The HTTP service keeps reports in memory and is single-process; run one replica per watched directory.

## License

MIT
