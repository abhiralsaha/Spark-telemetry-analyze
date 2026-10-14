"""Small, dependency-free helpers: statistics and human-friendly formatting."""

from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Sequence

KIB = 1024
MIB = 1024**2
GIB = 1024**3


def percentile(sorted_values: Sequence[float], q: float) -> float:
    """Linear-interpolated percentile (q in [0, 1]) of an already sorted sequence."""
    if not sorted_values:
        return 0.0
    if len(sorted_values) == 1:
        return float(sorted_values[0])
    q = min(max(q, 0.0), 1.0)
    pos = (len(sorted_values) - 1) * q
    lo = math.floor(pos)
    hi = math.ceil(pos)
    if lo == hi:
        return float(sorted_values[lo])
    frac = pos - lo
    return float(sorted_values[lo]) * (1 - frac) + float(sorted_values[hi]) * frac


def median(values: Sequence[float]) -> float:
    return percentile(sorted(values), 0.5)


def safe_div(a: float, b: float, default: float = 0.0) -> float:
    return a / b if b else default


def truncate(text: str | None, limit: int = 200) -> str:
    if not text:
        return ""
    text = " ".join(str(text).split())
    return text if len(text) <= limit else text[: max(limit - 1, 0)] + "\u2026"


def fmt_bytes(value: float | int | None) -> str:
    if value is None:
        return "n/a"
    n = float(value)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if abs(n) < 1024:
            return f"{n:.0f} {unit}" if unit == "B" else f"{n:.2f} {unit}"
        n /= 1024
    return f"{n:.2f} PiB"


def fmt_duration(ms: float | int | None) -> str:
    if ms is None:
        return "n/a"
    ms = float(ms)
    if ms < 1000:
        return f"{ms:.0f} ms"
    seconds = ms / 1000
    if seconds < 60:
        return f"{seconds:.1f} s"
    minutes, sec = divmod(int(round(seconds)), 60)
    if minutes < 60:
        return f"{minutes}m {sec:02d}s"
    hours, minutes = divmod(minutes, 60)
    return f"{hours}h {minutes:02d}m"


def fmt_pct(ratio: float | None) -> str:
    return "n/a" if ratio is None else f"{ratio * 100:.1f}%"


def iso_utc(epoch_ms: int | float | None) -> str | None:
    if epoch_ms is None or epoch_ms <= 0:
        return None
    return datetime.fromtimestamp(epoch_ms / 1000, tz=timezone.utc).isoformat(timespec="milliseconds")


def now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat(timespec="seconds")
