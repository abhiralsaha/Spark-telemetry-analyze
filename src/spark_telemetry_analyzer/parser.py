"""Streaming Spark event-log reader.

Supports plain JSON-lines logs, gzip, zip, and (when the optional packages are installed) zstd and
lz4; Spark "rolling" event-log directories (``eventlog_v2_<app-id>/events_N_<app-id>``); in-progress
logs with a truncated final line; and ``-`` for stdin.
"""

from __future__ import annotations

import gzip
import io
import json
import logging
import re
import sys
import zipfile
from collections.abc import Iterator
from pathlib import Path
from typing import IO, Any

from .aggregator import WANTED_EVENTS, EventAggregator
from .config import Settings
from .errors import ParseError, UnsupportedFormatError
from .models import ApplicationMetrics, ParseStats

log = logging.getLogger(__name__)

_EVENT_NAME = re.compile(rb'"Event"\s*:\s*"([^"]+)"')
_ROLLING_EVENTS = re.compile(r"^events_(\d+)_")
_SKIP_FILE = re.compile(r"(^\.|^appstatus_|\.crc$|^_SUCCESS$|\.tmp$)", re.I)

_GZIP, _ZIP, _ZSTD, _LZ4, _SNAPPY = b"\x1f\x8b", b"PK\x03\x04", b"\x28\xb5\x2f\xfd", b"\x04\x22\x4d\x18", b"\x82SNAPPY"

LogSource = str | Path | bytes | bytearray | IO[bytes]


# --------------------------------------------------------------------------------------------
# Raw stream handling
# --------------------------------------------------------------------------------------------
def _peek(raw: IO[bytes], n: int = 8) -> bytes:
    if hasattr(raw, "peek"):
        return raw.peek(n)[:n]  # type: ignore[attr-defined]
    if raw.seekable():
        pos = raw.tell()
        head = raw.read(n)
        raw.seek(pos)
        return head
    return b""


def _decompress(raw: IO[bytes]) -> IO[bytes]:
    head = _peek(raw)
    if head.startswith(_GZIP):
        return gzip.GzipFile(fileobj=raw)
    if head.startswith(_ZSTD):
        try:
            import zstandard  # type: ignore[import-not-found]
        except ImportError as exc:
            raise UnsupportedFormatError(
                "This event log is zstd-compressed. Install support with: pip install 'spark-telemetry-analyzer[zstd]'"
            ) from exc
        return zstandard.ZstdDecompressor().stream_reader(raw)
    if head.startswith(_LZ4):
        try:
            import lz4.frame  # type: ignore[import-not-found]
        except ImportError as exc:
            raise UnsupportedFormatError(
                "This event log is lz4-compressed. Install support with: pip install 'spark-telemetry-analyzer[lz4]'"
            ) from exc
        return lz4.frame.LZ4FrameFile(raw, mode="rb")  # type: ignore[no-any-return]
    if head.startswith(_SNAPPY):
        raise UnsupportedFormatError(
            "Snappy-compressed event logs are not supported. Re-run Spark with spark.eventLog.compress=false "
            "or use zstd/lz4/gzip (spark.eventLog.compression.codec)."
        )
    return raw


def _lines_from_stream(raw: IO[bytes], stats: ParseStats) -> Iterator[bytes]:
    head = _peek(raw)
    if head.startswith(_ZIP):
        if not raw.seekable():
            raise ParseError("Zip event logs must be read from a seekable file, not a pipe.")
        with zipfile.ZipFile(raw) as zf:
            names = sorted(
                (n for n in zf.namelist() if not n.endswith("/") and not _SKIP_FILE.search(Path(n).name)),
                key=_natural_key,
            )
            for name in names:
                with zf.open(name) as member:
                    yield from _lines_from_stream(io.BufferedReader(member), stats)  # type: ignore[arg-type]
        return
    stream = _decompress(raw)
    stats.files += 1
    yield from stream


def _natural_key(name: str) -> tuple[int, int, str]:
    base = Path(name).name
    m = _ROLLING_EVENTS.match(base)
    return (0, int(m.group(1)), base) if m else (1, 0, base)


def _lines_from_path(path: Path, stats: ParseStats) -> Iterator[bytes]:
    if path.is_dir():
        files = sorted(
            (p for p in path.iterdir() if p.is_file() and not _SKIP_FILE.search(p.name)),
            key=lambda p: _natural_key(p.name),
        )
        if not files:
            raise ParseError(f"No event log files found in directory: {path}")
        for f in files:
            yield from _lines_from_path(f, stats)
        return
    try:
        fh = path.open("rb")
    except OSError as exc:
        raise ParseError(f"Cannot read {path}: {exc.strerror or exc}") from exc
    with fh:
        yield from _lines_from_stream(fh, stats)


def iter_lines(source: LogSource, stats: ParseStats | None = None) -> Iterator[bytes]:
    stats = stats if stats is not None else ParseStats()
    if isinstance(source, (bytes, bytearray)):
        return _lines_from_stream(io.BytesIO(bytes(source)), stats)
    if isinstance(source, (str, Path)):
        if str(source) == "-":
            return _lines_from_stream(sys.stdin.buffer, stats)
        return _lines_from_path(Path(source), stats)
    return _lines_from_stream(source, stats)


# --------------------------------------------------------------------------------------------
# Event iteration
# --------------------------------------------------------------------------------------------
def iter_events(source: LogSource, stats: ParseStats | None = None) -> Iterator[dict[str, Any]]:
    """Yield decoded events of the types the analyzer cares about; skip everything else cheaply."""
    stats = stats if stats is not None else ParseStats()
    for line in iter_lines(source, stats):
        line = line.strip()
        if not line:
            continue
        stats.lines += 1
        match = _EVENT_NAME.search(line, 0, 256)
        if match is not None:
            short = match.group(1).decode("utf-8", "replace").rsplit(".", 1)[-1]
            if short not in WANTED_EVENTS:
                stats.events_skipped += 1
                continue
        try:
            event = json.loads(line)
        except (ValueError, UnicodeDecodeError):
            stats.bad_lines += 1
            continue
        if not isinstance(event, dict):
            stats.bad_lines += 1
            continue
        yield event


def load_application(source: LogSource, settings: Settings | None = None) -> ApplicationMetrics:
    """Parse a whole event log (file, directory, bytes or stream) into application metrics."""
    settings = settings or Settings()
    stats = ParseStats()
    agg = EventAggregator(reservoir_size=settings.reservoir_size, redact=settings.redact_properties)
    for event in iter_events(source, stats):
        agg.consume(event)

    if stats.lines >= 100 and stats.bad_lines > stats.lines * 0.5:
        raise ParseError("Input does not look like a Spark event log (most lines are not valid JSON).")
    if agg.events_consumed == 0:
        raise ParseError(
            "No Spark listener events found. Is this a Spark event log "
            "(spark.eventLog.enabled=true)? "
            f"Read {stats.lines} line(s), {stats.bad_lines} malformed."
        )
    if stats.bad_lines:
        log.warning("Skipped %d malformed line(s) (truncated or in-progress log?)", stats.bad_lines)

    stats.events_processed = agg.events_consumed
    app = agg.build()
    app.parse_stats = stats
    return app


# --------------------------------------------------------------------------------------------
# Discovery
# --------------------------------------------------------------------------------------------
def _is_rolling_dir(path: Path) -> bool:
    try:
        return any(_ROLLING_EVENTS.match(p.name) for p in path.iterdir() if p.is_file())
    except OSError:
        return False


def discover_logs(paths: list[str | Path], *, include_inprogress: bool = True) -> list[Path]:
    """Expand files/directories into analyzable units (a file, or one rolling event-log directory)."""
    found: list[Path] = []

    def visit(p: Path) -> None:
        if p.is_file():
            if _SKIP_FILE.search(p.name):
                return
            if not include_inprogress and p.name.endswith(".inprogress"):
                return
            found.append(p)
        elif p.is_dir():
            if _is_rolling_dir(p):
                found.append(p)
                return
            for child in sorted(p.iterdir()):
                visit(child)

    for raw in paths:
        p = Path(raw)
        if not p.exists():
            raise ParseError(f"Path does not exist: {p}")
        visit(p)
    return found
