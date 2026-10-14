"""Command line interface: ``sta analyze | fetch | compare | serve | sample | rules``."""

from __future__ import annotations

import argparse
import dataclasses
import json
import logging
import os
import sys
from collections.abc import Sequence
from pathlib import Path

from . import __version__
from .analyzer import Report, analyze_source
from .compare import compare_reports, load_report, render_comparison
from .config import Settings, load_settings
from .errors import AnalyzerError
from .history import HistoryServerClient
from .logging_utils import setup_logging
from .metrics import render_prometheus
from .models import Severity
from .parser import discover_logs
from .report import FORMATS, render
from .rules import RULES
from .sample import SCENARIOS, generate_event_log

log = logging.getLogger("sta")

EXIT_OK, EXIT_FINDINGS, EXIT_ERROR = 0, 1, 2
_EXT = {"text": "txt", "json": "json", "markdown": "md", "html": "html", "prometheus": "prom"}


def _env(name: str, default: str | None = None) -> str | None:
    value = os.environ.get(name)
    return value if value not in (None, "") else default


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="sta",
        description="Analyze Apache Spark event-log telemetry and surface performance problems.",
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    parser.add_argument("-v", "--verbose", action="count", default=0, help="more logging (-v info, -vv debug)")
    parser.add_argument("--log-format", choices=("text", "json"), default=_env("STA_LOG_FORMAT", "text"))
    sub = parser.add_subparsers(dest="command", required=True, metavar="COMMAND")

    # ---- analyze ---------------------------------------------------------------------------
    p = sub.add_parser("analyze", help="analyze one or more event logs (files, directories, or '-' for stdin)")
    p.add_argument("paths", nargs="+", help="event log file(s), rolling-log directories, or directories of logs")
    p.add_argument("-f", "--format", choices=FORMATS, default="text")
    p.add_argument("-o", "--output", help="write the report to this file instead of stdout")
    p.add_argument("--output-dir", help="write one report per log into this directory")
    p.add_argument("-c", "--config", help="TOML config file (default: $STA_CONFIG)")
    p.add_argument("--top", type=int, help="rows in 'top N' tables (default from config)")
    p.add_argument("--disable-rule", action="append", default=[], metavar="RULE_ID", help="skip a rule (repeatable)")
    p.add_argument("--fail-on", choices=("none", "info", "warning", "critical"), default="none",
                   help="exit with status 1 if any finding is at least this severe")
    p.add_argument("--min-health", type=int, metavar="SCORE", help="exit with status 1 if any health score is below SCORE")
    p.add_argument("--skip-inprogress", action="store_true", help="ignore *.inprogress logs when scanning directories")
    p.add_argument("--no-color", action="store_true", help="disable ANSI colors in text output")
    p.set_defaults(func=cmd_analyze)

    # ---- fetch -----------------------------------------------------------------------------
    p = sub.add_parser("fetch", help="download event logs from a Spark History Server")
    p.add_argument("--history-url", default=_env("STA_HISTORY_URL"), help="e.g. http://history:18080 ($STA_HISTORY_URL)")
    p.add_argument("--app-id", action="append", default=[], help="application id to fetch (repeatable)")
    p.add_argument("--limit", type=int, default=5, help="when no --app-id: fetch the N most recent applications")
    p.add_argument("--status", choices=("completed", "running"), default="completed")
    p.add_argument("--output-dir", required=True)
    p.add_argument("--timeout", type=float, default=60.0)
    p.add_argument("--analyze", action="store_true", help="analyze the downloaded logs and print the reports")
    p.add_argument("-f", "--format", choices=FORMATS, default="text")
    p.add_argument("-c", "--config")
    p.set_defaults(func=cmd_fetch)

    # ---- compare ---------------------------------------------------------------------------
    p = sub.add_parser("compare", help="compare two runs (event logs or saved JSON reports)")
    p.add_argument("base")
    p.add_argument("new")
    p.add_argument("--tolerance", type=float, default=0.10, help="relative change treated as noise (default 0.10)")
    p.add_argument("-f", "--format", choices=("text", "json"), default="text")
    p.add_argument("-c", "--config")
    p.add_argument("--fail-on-regression", action="store_true")
    p.set_defaults(func=cmd_compare)

    # ---- serve -----------------------------------------------------------------------------
    p = sub.add_parser("serve", help="run the HTTP service (REST API, /metrics, optional directory watcher)")
    p.add_argument("--host", default=_env("STA_HOST", "127.0.0.1"))
    p.add_argument("--port", type=int, default=int(_env("STA_PORT", "8080") or 8080))
    p.add_argument("--watch-dir", default=_env("STA_WATCH_DIR"), help="directory of event logs to analyze automatically")
    p.add_argument("--token", default=_env("STA_TOKEN"), help="bearer token protecting /api/* ($STA_TOKEN)")
    p.add_argument("--max-body-mb", type=int, default=256)
    p.add_argument("--max-reports", type=int, default=200)
    p.add_argument("--max-concurrent", type=int, default=4)
    p.add_argument("--poll-interval", type=float, default=15.0)
    p.add_argument("--settle-seconds", type=float, default=5.0, help="wait this long after the last write before analyzing")
    p.add_argument("--include-inprogress", action="store_true")
    p.add_argument("-c", "--config")
    p.set_defaults(func=cmd_serve)

    # ---- sample ----------------------------------------------------------------------------
    p = sub.add_parser("sample", help="generate a synthetic event log for demos and tests")
    p.add_argument("-s", "--scenario", choices=SCENARIOS, default="mixed")
    p.add_argument("-o", "--output", default="-", help="output file ('-' = stdout; *.gz is compressed)")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--executors", type=int, default=8)
    p.add_argument("--cores", type=int, default=4)
    p.add_argument("--scale", type=float, default=1.0, help="multiply task counts (e.g. 0.1 for a tiny log)")
    p.set_defaults(func=cmd_sample)

    # ---- rules -----------------------------------------------------------------------------
    p = sub.add_parser("rules", help="list detection rules")
    p.set_defaults(func=cmd_rules)
    return parser


def _settings(args: argparse.Namespace) -> Settings:
    settings = load_settings(getattr(args, "config", None))
    extra = frozenset(r.strip().upper() for r in getattr(args, "disable_rule", []) or [])
    unknown = extra - set(RULES)
    if unknown:
        raise AnalyzerError(f"Unknown rule id(s): {', '.join(sorted(unknown))}. See `sta rules`.")
    if extra:
        settings = dataclasses.replace(settings, disabled_rules=settings.disabled_rules | extra)
    top = getattr(args, "top", None)
    if top is not None:
        if top < 1:
            raise AnalyzerError("--top must be >= 1")
        settings = dataclasses.replace(settings, top_n=top)
    return settings


def _color_enabled(args: argparse.Namespace) -> bool:
    return not args.no_color and sys.stdout.isatty() and "NO_COLOR" not in os.environ and not args.output


def _write(path: str | Path, text: str) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    Path(path).write_text(text, encoding="utf-8")


def _safe_name(name: str) -> str:
    return "".join(c if c.isalnum() or c in "._-" else "_" for c in name) or "report"


def _emit(reports: list[Report], args: argparse.Namespace, settings: Settings) -> None:
    fmt = args.format
    if args.output_dir:
        out_dir = Path(args.output_dir)
        if fmt == "prometheus":
            _write(out_dir / "metrics.prom", render_prometheus(reports))
        else:
            for r in reports:
                _write(out_dir / f"{_safe_name(r.app_id)}.{_EXT[fmt]}", render(r, fmt, top_n=settings.top_n))
        print(f"Wrote {len(reports)} report(s) to {out_dir}", file=sys.stderr)
        return
    if fmt == "prometheus":
        text = render_prometheus(reports)
    elif fmt == "json":
        text = json.dumps([r.data for r in reports] if len(reports) > 1 else reports[0].data, indent=2, default=str) + "\n"
    elif fmt == "html" and len(reports) > 1:
        raise AnalyzerError("Multiple logs in HTML format need --output-dir")
    else:
        text = "\n".join(render(r, fmt, top_n=settings.top_n, color=_color_enabled(args)) for r in reports)
    if args.output:
        _write(args.output, text)
        print(f"Wrote report to {args.output}", file=sys.stderr)
    else:
        sys.stdout.write(text)


def _analyze_units(paths: Sequence[str | Path], settings: Settings, include_inprogress: bool) -> tuple[list[Report], int]:
    """Analyze every log under ``paths``. Returns (reports, number_of_failures)."""
    if len(paths) == 1 and str(paths[0]) == "-":
        return [analyze_source("-", settings)], 0
    units = discover_logs(list(paths), include_inprogress=include_inprogress)
    if not units:
        raise AnalyzerError("No event logs found in the given path(s)")
    reports: list[Report] = []
    failures = 0
    for unit in units:
        try:
            report = analyze_source(unit, settings)
            if report.app_id in ("", "unknown"):
                report.data["application"]["id"] = unit.name
            reports.append(report)
        except AnalyzerError as exc:
            failures += 1
            print(f"error: {unit}: {exc}", file=sys.stderr)
    return reports, failures


def _exit_code(reports: list[Report], failures: int, fail_on: str, min_health: int | None) -> int:
    if failures and not reports:
        return EXIT_ERROR
    breached = False
    if fail_on != "none":
        level = Severity.parse(fail_on)
        breached = any(r.has_findings_at_least(level) for r in reports)
    if min_health is not None:
        breached = breached or any(r.health_score < min_health for r in reports)
    if failures:
        return EXIT_ERROR
    return EXIT_FINDINGS if breached else EXIT_OK


# --------------------------------------------------------------------------------------------
# Commands
# --------------------------------------------------------------------------------------------
def cmd_analyze(args: argparse.Namespace) -> int:
    settings = _settings(args)
    reports, failures = _analyze_units(args.paths, settings, not args.skip_inprogress)
    if reports:
        _emit(reports, args, settings)
    return _exit_code(reports, failures, args.fail_on, args.min_health)


def cmd_fetch(args: argparse.Namespace) -> int:
    if not args.history_url:
        raise AnalyzerError("--history-url (or $STA_HISTORY_URL) is required")
    token = _env("STA_HISTORY_TOKEN")
    user, password = _env("STA_HISTORY_USER"), _env("STA_HISTORY_PASSWORD")
    client = HistoryServerClient(
        args.history_url, token=token, basic_auth=(user, password) if user and password else None, timeout=args.timeout
    )
    targets: list[tuple[str, str | None]] = [(a, None) for a in args.app_id]
    if not targets:
        for app in client.list_applications(status=args.status, limit=args.limit):
            attempts = app.get("attempts") or [{}]
            targets.append((str(app["id"]), attempts[0].get("attemptId")))
    if not targets:
        print("No applications found on the History Server.", file=sys.stderr)
        return EXIT_OK
    downloaded: list[Path] = []
    for app_id, attempt in targets:
        path = client.download_event_log(app_id, args.output_dir, attempt)
        print(f"Downloaded {app_id} -> {path}", file=sys.stderr)
        downloaded.append(path)
    if not args.analyze:
        return EXIT_OK
    settings = _settings(args)
    reports, failures = _analyze_units(downloaded, settings, True)
    args.output = args.output_dir = None
    args.no_color = True
    if reports:
        _emit(reports, args, settings)
    return EXIT_ERROR if failures else EXIT_OK


def cmd_compare(args: argparse.Namespace) -> int:
    settings = _settings(args)
    base, new = load_report(args.base, settings), load_report(args.new, settings)
    result = compare_reports(base, new, args.tolerance)
    if args.format == "json":
        print(json.dumps(result, indent=2))
    else:
        sys.stdout.write(render_comparison(result))
    return EXIT_FINDINGS if (args.fail_on_regression and result["regressed"]) else EXIT_OK


def cmd_serve(args: argparse.Namespace) -> int:
    from .server import AnalyzerServer, ServerConfig

    config = ServerConfig(
        host=args.host,
        port=args.port,
        watch_dir=Path(args.watch_dir) if args.watch_dir else None,
        token=args.token or None,
        max_body_bytes=args.max_body_mb * 1024 * 1024,
        max_reports=args.max_reports,
        max_concurrent_analyses=args.max_concurrent,
        poll_interval=args.poll_interval,
        settle_seconds=args.settle_seconds,
        include_inprogress=args.include_inprogress,
        settings=_settings(args),
    )
    if not config.token and config.host not in ("127.0.0.1", "localhost", "::1"):
        log.warning("Serving on %s without --token: anyone who can reach the port can use /api/*", config.host)
    AnalyzerServer(config).serve_forever()
    return EXIT_OK


def cmd_sample(args: argparse.Namespace) -> int:
    kwargs = {"scenario": args.scenario, "seed": args.seed, "executors": args.executors, "cores": args.cores, "scale": args.scale}
    try:
        if args.output == "-":
            for line in generate_event_log(**kwargs):
                sys.stdout.write(line + "\n")
        else:
            from .sample import write_event_log

            Path(args.output).parent.mkdir(parents=True, exist_ok=True)
            n = write_event_log(args.output, **kwargs)
            print(f"Wrote {n} events to {args.output}", file=sys.stderr)
    except ValueError as exc:
        raise AnalyzerError(str(exc)) from exc
    return EXIT_OK


def cmd_rules(_args: argparse.Namespace) -> int:
    for rule_id, fn in RULES.items():
        doc = (fn.__doc__ or "").strip().splitlines()[0] if fn.__doc__ else ""
        print(f"{rule_id:<24}{doc}")
    return EXIT_OK


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    level = "DEBUG" if args.verbose >= 2 else "INFO" if args.verbose == 1 else _env("STA_LOG_LEVEL", "WARNING") or "WARNING"
    setup_logging(level, json_format=args.log_format == "json")
    try:
        return int(args.func(args))
    except AnalyzerError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_ERROR
    except BrokenPipeError:  # e.g. `sta analyze ... | head`
        try:
            sys.stdout.close()
        finally:
            return EXIT_OK  # noqa: B012
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
