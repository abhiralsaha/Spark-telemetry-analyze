# Architecture

```
event log (file / dir / zip / gz / zstd / stdin / HTTP body)
        |  parser.py      streaming line reader, codec sniffing, cheap event-name pre-filter
        v
   aggregator.py        folds events into constant-memory accumulators (models.py)
        |  ApplicationMetrics: jobs, stages (per attempt), executors, SQL executions, properties
        v
     rules.py           independent detection rules -> Finding(severity, evidence, recommendation)
        |
        v
    analyzer.py         health score + JSON-compatible Report (schema_version = 1)
        |
        +--> report.py   text / markdown / html     metrics.py   Prometheus
        +--> compare.py  run-to-run regression      server.py    REST + /metrics + directory watcher
        +--> cli.py      sta analyze|fetch|compare|serve|sample|rules      history.py  History Server client
```

Design notes

* **Memory bounded**: `StageMetrics.add_task` keeps sums, exact maxima, a reservoir sample (Algorithm R, deterministic seed per stage)
  and the 5 slowest tasks. Only successful task attempts feed distributions, so retries don't distort skew detection.
* **Fast**: ~10 event types are used; other lines are skipped after a regex on the first 256 bytes, without JSON decoding.
* **Robust**: malformed lines are counted, not fatal; a crashing rule is logged and skipped; the directory watcher never dies on a bad file.
* **Report is data**: every renderer consumes the same plain dict, so the JSON, HTML, Markdown and Prometheus views cannot drift apart.
* **Security**: secret-looking Spark properties are redacted at parse time; HTML escapes all values and ships a restrictive CSP from the
  service; History Server ids are validated before they are placed in a URL; downloads are size-limited and written atomically.
