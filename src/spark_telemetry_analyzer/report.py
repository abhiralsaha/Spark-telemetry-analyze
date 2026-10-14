"""Render a :class:`Report` as JSON, text, Markdown or a self-contained HTML page."""

from __future__ import annotations

import html
import json
from typing import Any

from .analyzer import Report
from .utils import fmt_bytes, fmt_duration, fmt_pct, truncate

FORMATS = ("text", "json", "markdown", "html", "prometheus")
_SEV_ORDER = ("critical", "warning", "info")
_ANSI = {"critical": "\x1b[31;1m", "warning": "\x1b[33;1m", "info": "\x1b[36m", "bold": "\x1b[1m", "reset": "\x1b[0m"}


def render(report: Report, fmt: str, *, top_n: int = 10, color: bool = False) -> str:
    if fmt == "json":
        return render_json(report)
    if fmt == "text":
        return render_text(report, top_n=top_n, color=color)
    if fmt == "markdown":
        return render_markdown(report, top_n=top_n)
    if fmt == "html":
        return render_html(report, top_n=top_n)
    if fmt == "prometheus":
        from .metrics import render_prometheus

        return render_prometheus([report])
    raise ValueError(f"Unknown format '{fmt}' (choose from: {', '.join(FORMATS)})")


def render_json(report: Report, *, indent: int | None = 2) -> str:
    return json.dumps(report.data, indent=indent, sort_keys=False, default=str)


# --------------------------------------------------------------------------------------------
# Shared pieces
# --------------------------------------------------------------------------------------------
def _kv_summary(d: dict[str, Any]) -> list[tuple[str, str]]:
    a, s = d["application"], d["summary"]
    util = s.get("utilization")
    return [
        ("Application", f"{a['name'] or 'n/a'} ({a['id']})"),
        ("Spark version", a["spark_version"] or "n/a"),
        ("User", a["user"] or "n/a"),
        ("Duration", fmt_duration(a["duration_ms"]) + ("" if a["completed"] else " (incomplete log)")),
        ("Jobs", f"{s['jobs']} ({s['jobs_failed']} failed)"),
        ("Stages", f"{s['stages']} ({s['stages_failed']} failed attempts)"),
        ("Tasks", f"{s['tasks']:,} ({s['tasks_failed']:,} failed, {s['tasks_killed']:,} killed)"),
        ("Executors", f"{s['executors']} ({s['executors_lost']} lost), peak {s['peak_cores']} cores"),
        ("Executor run time", fmt_duration(s["executor_run_ms"])),
        ("GC time", f"{fmt_duration(s['gc_ms'])} ({fmt_pct(s['gc_ratio'])})"),
        ("Core utilization", fmt_pct(util) if util is not None else "n/a"),
        ("Input / output", f"{fmt_bytes(s['input_bytes'])} / {fmt_bytes(s['output_bytes'])}"),
        ("Shuffle read / write", f"{fmt_bytes(s['shuffle_read_bytes'])} / {fmt_bytes(s['shuffle_write_bytes'])}"),
        ("Spill (memory / disk)", f"{fmt_bytes(s['memory_spilled_bytes'])} / {fmt_bytes(s['disk_spilled_bytes'])}"),
    ]


def _counts_line(d: dict[str, Any]) -> str:
    c = d["finding_counts"]
    return f"{c.get('critical', 0)} critical, {c.get('warning', 0)} warning, {c.get('info', 0)} info"


def _stage_rows(d: dict[str, Any], top_n: int) -> list[list[str]]:
    rows = []
    for st in d["stages"][:top_n]:
        t, dur, tot = st["tasks"], st["task_duration_ms"], st["totals"]
        rows.append(
            [
                st["key"],
                truncate(st["name"], 38),
                st["status"],
                fmt_duration(st["duration_ms"]),
                f"{t['succeeded'] + t['failed'] + t['killed']:,}",
                f"{fmt_duration(dur['p50'])} / {fmt_duration(dur['max'])}",
                fmt_bytes(tot["shuffle_read_bytes"]),
                fmt_bytes(tot["disk_spilled_bytes"]),
                fmt_pct(tot["gc_ratio"]),
            ]
        )
    return rows


_STAGE_HEADERS = ["Stage", "Name", "Status", "Duration", "Tasks", "Task p50 / max", "Shuffle read", "Disk spill", "GC"]


def _executor_rows(d: dict[str, Any], top_n: int) -> list[list[str]]:
    return [
        [
            e["id"],
            e["host"] or "-",
            str(e["cores"]),
            f"{sum(e['tasks'].values()):,}",
            fmt_duration(e["run_ms"]),
            fmt_pct(e["gc_ratio"]),
            fmt_bytes(e["shuffle_read_bytes"]),
            truncate(e["removed_reason"], 50) if e["removed_reason"] else "",
        ]
        for e in d["executors"][:top_n]
    ]


_EXECUTOR_HEADERS = ["Executor", "Host", "Cores", "Tasks", "Run time", "GC", "Shuffle read", "Removed"]


def _table_text(headers: list[str], rows: list[list[str]]) -> list[str]:
    widths = [max(len(h), *(len(r[i]) for r in rows)) if rows else len(h) for i, h in enumerate(headers)]
    line = "  ".join(h.ljust(w) for h, w in zip(headers, widths, strict=True))
    out = [line.rstrip(), "  ".join("-" * w for w in widths)]
    out += ["  ".join(c.ljust(w) for c, w in zip(r, widths, strict=True)).rstrip() for r in rows]
    return out


# --------------------------------------------------------------------------------------------
# Text
# --------------------------------------------------------------------------------------------
def render_text(report: Report, *, top_n: int = 10, color: bool = False) -> str:
    d = report.data

    def paint(text: str, key: str) -> str:
        return f"{_ANSI[key]}{text}{_ANSI['reset']}" if color else text

    out: list[str] = []
    title = f"Spark Telemetry Report - {d['application']['name'] or d['application']['id']}"
    out += [paint(title, "bold"), "=" * len(title), ""]
    out.append(f"Health score: {d['health_score']}/100 (grade {d['grade']})  |  Findings: {_counts_line(d)}")
    out.append("")
    for key, value in _kv_summary(d):
        out.append(f"  {key + ':':<24}{value}")
    out.append("")

    out.append(paint("Findings", "bold"))
    out.append("--------")
    if not d["findings"]:
        out.append("  No issues detected.")
    for f in d["findings"]:
        tag = paint(f"[{f['severity'].upper()}]", f["severity"])
        where = f" ({f['scope']} {f['scope_id']})" if f["scope_id"] else ""
        out.append(f"  {tag} {f['rule_id']}{where}: {f['title']}")
        out.append(f"      {f['message']}")
        if f["recommendation"]:
            out.append(f"      -> {f['recommendation']}")
    out.append("")

    if d["stages"]:
        heading = f"Longest stages (top {min(top_n, len(d['stages']))})"
        out.append(paint(heading, "bold"))
        out.append("-" * len(heading))
        out += ["  " + line for line in _table_text(_STAGE_HEADERS, _stage_rows(d, top_n))]
        out.append("")
    if d["executors"]:
        heading = f"Busiest executors (top {min(top_n, len(d['executors']))})"
        out.append(paint(heading, "bold"))
        out.append("-" * len(heading))
        out += ["  " + line for line in _table_text(_EXECUTOR_HEADERS, _executor_rows(d, top_n))]
        out.append("")
    if d["sql_executions"]:
        out.append(paint("Slowest SQL executions", "bold"))
        out.append("----------------------")
        for q in d["sql_executions"][: min(top_n, 5)]:
            out.append(f"  #{q['execution_id']:<4} {fmt_duration(q['duration_ms']):>9}  {truncate(q['description'], 90)}")
        out.append("")
    return "\n".join(out).rstrip() + "\n"


# --------------------------------------------------------------------------------------------
# Markdown
# --------------------------------------------------------------------------------------------
def _md_escape(text: str) -> str:
    return text.replace("|", "\\|").replace("\n", " ")


def _table_md(headers: list[str], rows: list[list[str]]) -> list[str]:
    out = ["| " + " | ".join(headers) + " |", "|" + "|".join(" --- " for _ in headers) + "|"]
    out += ["| " + " | ".join(_md_escape(c) for c in r) + " |" for r in rows]
    return out


def render_markdown(report: Report, *, top_n: int = 10) -> str:
    d = report.data
    out = [f"# Spark Telemetry Report: {d['application']['name'] or d['application']['id']}", ""]
    out.append(f"**Health score:** {d['health_score']}/100 (grade **{d['grade']}**) &middot; **Findings:** {_counts_line(d)}")
    out.append("")
    out += ["## Summary", ""] + _table_md(["Metric", "Value"], [[k, v] for k, v in _kv_summary(d)]) + [""]
    out += ["## Findings", ""]
    if not d["findings"]:
        out += ["No issues detected.", ""]
    for sev in _SEV_ORDER:
        group = [f for f in d["findings"] if f["severity"] == sev]
        if not group:
            continue
        out += [f"### {sev.capitalize()} ({len(group)})", ""]
        for f in group:
            where = f" _({f['scope']} {f['scope_id']})_" if f["scope_id"] else ""
            out.append(f"- **{f['rule_id']}**{where} - {f['title']}")
            out.append(f"  - {f['message']}")
            if f["recommendation"]:
                out.append(f"  - **Fix:** {f['recommendation']}")
        out.append("")
    if d["stages"]:
        out += [f"## Longest stages (top {min(top_n, len(d['stages']))})", ""]
        out += _table_md(_STAGE_HEADERS, _stage_rows(d, top_n)) + [""]
    if d["executors"]:
        out += [f"## Busiest executors (top {min(top_n, len(d['executors']))})", ""]
        out += _table_md(_EXECUTOR_HEADERS, _executor_rows(d, top_n)) + [""]
    if d["spark_properties"]:
        out += ["## Key Spark properties", ""]
        out += _table_md(["Property", "Value"], [[k, v] for k, v in d["spark_properties"].items()]) + [""]
    out.append(f"_Generated {d['generated_at']} by {d['tool']['name']} {d['tool']['version']}_")
    return "\n".join(out) + "\n"


# --------------------------------------------------------------------------------------------
# HTML
# --------------------------------------------------------------------------------------------
_CSS = """
:root{--bg:#f7f8fa;--fg:#1c2330;--muted:#5d6878;--card:#fff;--line:#e2e6ec;--crit:#c62828;--warn:#b26a00;--info:#1565c0;--ok:#2e7d32;--bar:#3b82f6}
@media (prefers-color-scheme:dark){:root{--bg:#10141b;--fg:#e6e9ef;--muted:#9aa6b8;--card:#171c26;--line:#2a3140;--crit:#ff6b6b;--warn:#ffb74d;--info:#64b5f6;--ok:#81c784;--bar:#60a5fa}}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--fg);font:14px/1.5 system-ui,-apple-system,Segoe UI,Roboto,sans-serif}
main{max-width:1180px;margin:0 auto;padding:24px 16px 64px}h1{font-size:22px;margin:0 0 4px}h2{font-size:17px;margin:32px 0 10px}
.muted{color:var(--muted)}.card{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:14px 16px}
.hero{display:flex;gap:16px;flex-wrap:wrap;align-items:stretch;margin-top:14px}
.score{min-width:150px;text-align:center}.score .n{font-size:44px;font-weight:700;line-height:1}.score .g{font-size:13px;color:var(--muted)}
.kv{flex:1;min-width:280px;display:grid;grid-template-columns:max-content 1fr;gap:3px 16px}.kv dt{color:var(--muted)}.kv dd{margin:0}
.finding{border-left:4px solid var(--info);margin:10px 0}.finding.critical{border-left-color:var(--crit)}.finding.warning{border-left-color:var(--warn)}
.badge{display:inline-block;font-size:11px;font-weight:700;letter-spacing:.04em;text-transform:uppercase;padding:1px 7px;border-radius:999px;color:#fff;background:var(--info);margin-right:6px}
.badge.critical{background:var(--crit)}.badge.warning{background:var(--warn)}.finding h3{font-size:15px;margin:0 0 4px}.finding p{margin:4px 0}.fix{color:var(--ok)}
.scroll{overflow-x:auto}table{border-collapse:collapse;width:100%;background:var(--card);border:1px solid var(--line);border-radius:10px}
th,td{padding:6px 10px;border-bottom:1px solid var(--line);text-align:left;white-space:nowrap}th{font-size:12px;color:var(--muted);text-transform:uppercase}
.bar{display:inline-block;height:8px;background:var(--bar);border-radius:4px;vertical-align:middle;margin-right:6px;min-width:2px}
code{font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:12px}
"""


def _h(value: Any) -> str:
    return html.escape(str(value), quote=True)


def _table_html(headers: list[str], rows: list[list[str]], bars: list[float] | None = None) -> str:
    head = "".join(f"<th>{_h(h)}</th>" for h in headers)
    body = []
    for i, row in enumerate(rows):
        cells = []
        for j, cell in enumerate(row):
            if bars is not None and j == 3:
                cells.append(f'<td><span class="bar" style="width:{max(2, int(bars[i] * 90))}px"></span>{_h(cell)}</td>')
            else:
                cells.append(f"<td>{_h(cell)}</td>")
        body.append("<tr>" + "".join(cells) + "</tr>")
    return f'<div class="scroll"><table><thead><tr>{head}</tr></thead><tbody>{"".join(body)}</tbody></table></div>'


def render_html(report: Report, *, top_n: int = 10) -> str:
    d = report.data
    title = d["application"]["name"] or d["application"]["id"]
    score = d["health_score"]
    score_color = "var(--ok)" if score >= 75 else "var(--warn)" if score >= 40 else "var(--crit)"

    kv = "".join(f"<dt>{_h(k)}</dt><dd>{_h(v)}</dd>" for k, v in _kv_summary(d))
    findings = []
    for f in d["findings"]:
        where = f" &middot; {_h(f['scope'])} {_h(f['scope_id'])}" if f["scope_id"] else ""
        fix = f'<p class="fix"><strong>Fix:</strong> {_h(f["recommendation"])}</p>' if f["recommendation"] else ""
        findings.append(
            f'<div class="card finding {_h(f["severity"])}"><h3><span class="badge {_h(f["severity"])}">'
            f'{_h(f["severity"])}</span>{_h(f["title"])}</h3>'
            f'<div class="muted"><code>{_h(f["rule_id"])}</code>{where}</div><p>{_h(f["message"])}</p>{fix}</div>'
        )
    if not findings:
        findings.append('<div class="card">No issues detected.</div>')

    stage_rows = _stage_rows(d, top_n)
    max_dur = max((s["duration_ms"] for s in d["stages"][:top_n]), default=0) or 1
    bars = [s["duration_ms"] / max_dur for s in d["stages"][:top_n]]
    props = "".join(f"<dt>{_h(k)}</dt><dd><code>{_h(v)}</code></dd>" for k, v in d["spark_properties"].items())

    parts = [
        "<!doctype html><html lang='en'><head><meta charset='utf-8'>",
        "<meta name='viewport' content='width=device-width,initial-scale=1'>",
        f"<title>Spark Telemetry Report - {_h(title)}</title><style>{_CSS}</style></head><body><main>",
        f"<h1>Spark Telemetry Report</h1><div class='muted'>{_h(title)} &middot; {_h(d['application']['id'])}</div>",
        "<div class='hero'>",
        f"<div class='card score'><div class='n' style='color:{score_color}'>{score}</div>"
        f"<div class='g'>health score / 100<br>grade {_h(d['grade'])}</div><div class='muted' style='margin-top:8px'>{_h(_counts_line(d))}</div></div>",
        f"<div class='card kv'>{kv}</div></div>",
        "<h2>Findings</h2>",
        "".join(findings),
        f"<h2>Longest stages (top {min(top_n, len(d['stages']))})</h2>",
        _table_html(_STAGE_HEADERS, stage_rows, bars) if stage_rows else "<p class='muted'>No stages.</p>",
        f"<h2>Busiest executors (top {min(top_n, len(d['executors']))})</h2>",
        _table_html(_EXECUTOR_HEADERS, _executor_rows(d, top_n)) if d["executors"] else "<p class='muted'>No executors.</p>",
    ]
    if props:
        parts += ["<h2>Key Spark properties</h2>", f"<div class='card kv'>{props}</div>"]
    parts.append(
        f"<p class='muted' style='margin-top:32px'>Generated {_h(d['generated_at'])} by "
        f"{_h(d['tool']['name'])} {_h(d['tool']['version'])}</p></main></body></html>"
    )
    return "".join(parts)
