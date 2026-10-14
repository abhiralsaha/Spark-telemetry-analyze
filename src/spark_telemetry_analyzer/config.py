"""Analysis settings and detection thresholds.

Everything has a sensible default; a TOML file (``--config`` or ``$STA_CONFIG``) can override any
value. Unknown keys are rejected so typos never silently disable a check.
"""

from __future__ import annotations

import os
import tomllib
from dataclasses import dataclass, field, fields, replace
from pathlib import Path
from typing import Any

from .errors import ConfigError
from .utils import GIB, MIB


@dataclass(frozen=True)
class Thresholds:
    # --- general -------------------------------------------------------------------------
    min_stage_duration_ms: int = 5_000  # ignore very short stages for most heuristics
    min_run_ms_for_ratios: int = 10_000  # minimum aggregate executor run time for ratio rules

    # --- skew / stragglers -----------------------------------------------------------------
    min_tasks_for_skew: int = 8
    skew_duration_ratio_warn: float = 4.0  # max task duration / median
    skew_duration_ratio_crit: float = 10.0
    skew_min_max_task_ms: int = 10_000  # slowest task must be at least this long
    skew_data_ratio_warn: float = 5.0  # max shuffle-read (or input) / median
    skew_data_ratio_crit: float = 20.0
    skew_min_bytes: int = 64 * MIB

    # --- spill -----------------------------------------------------------------------------
    spill_disk_warn_bytes: int = 512 * MIB
    spill_disk_crit_bytes: int = 10 * GIB
    spill_memory_warn_bytes: int = 2 * GIB

    # --- GC / serialization ----------------------------------------------------------------
    gc_ratio_warn: float = 0.10
    gc_ratio_crit: float = 0.20
    deserialize_ratio_warn: float = 0.20
    deserialize_ratio_crit: float = 0.40

    # --- failures --------------------------------------------------------------------------
    failed_task_ratio_warn: float = 0.01
    failed_task_ratio_crit: float = 0.05

    # --- partitioning / parallelism --------------------------------------------------------
    small_task_median_ms: int = 100
    small_task_min_count: int = 1_000
    scheduling_overhead_ratio_warn: float = 0.30
    scheduling_overhead_min_tasks: int = 100
    low_parallelism_min_stage_ms: int = 30_000
    large_partition_warn_bytes: int = 1 * GIB
    large_partition_crit_bytes: int = 4 * GIB

    # --- shuffle / driver ------------------------------------------------------------------
    fetch_wait_ratio_warn: float = 0.15
    heavy_shuffle_bytes: int = 50 * GIB
    large_result_warn_bytes: int = 1 * GIB
    large_result_crit_bytes: int = 4 * GIB

    # --- executors / cluster ---------------------------------------------------------------
    executor_imbalance_ratio_warn: float = 3.0
    executor_imbalance_min_executors: int = 4
    utilization_warn: float = 0.30
    utilization_crit: float = 0.10
    utilization_min_app_ms: int = 300_000


@dataclass(frozen=True)
class Settings:
    thresholds: Thresholds = field(default_factory=Thresholds)
    disabled_rules: frozenset[str] = frozenset()
    reservoir_size: int = 20_000  # per-stage sample size for percentile estimation
    top_n: int = 10  # number of "top" items in rendered reports
    max_listed_stages: int = 200  # stages embedded in the report body
    redact_properties: bool = True  # redact secrets in Spark properties


_ANALYSIS_KEYS = {"disabled_rules", "reservoir_size", "top_n", "max_listed_stages", "redact_properties"}


def _coerce(name: str, default: Any, value: Any) -> Any:
    if isinstance(default, bool):
        if not isinstance(value, bool):
            raise ConfigError(f"'{name}' must be true/false, got {value!r}")
        return value
    if isinstance(default, int):
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ConfigError(f"'{name}' must be a number, got {value!r}")
        if value < 0:
            raise ConfigError(f"'{name}' must be >= 0, got {value!r}")
        return int(value)
    if isinstance(default, float):
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ConfigError(f"'{name}' must be a number, got {value!r}")
        if value < 0:
            raise ConfigError(f"'{name}' must be >= 0, got {value!r}")
        return float(value)
    return value


def settings_from_mapping(data: dict[str, Any]) -> Settings:
    """Build :class:`Settings` from a parsed config mapping."""
    unknown_sections = set(data) - {"thresholds", "analysis"}
    if unknown_sections:
        raise ConfigError(f"Unknown config section(s): {', '.join(sorted(unknown_sections))}")

    base = Thresholds()
    valid = {f.name for f in fields(Thresholds)}
    overrides: dict[str, Any] = {}
    for key, value in (data.get("thresholds") or {}).items():
        if key not in valid:
            raise ConfigError(f"Unknown threshold '{key}'")
        overrides[key] = _coerce(key, getattr(base, key), value)
    thresholds = replace(base, **overrides)

    analysis = data.get("analysis") or {}
    bad = set(analysis) - _ANALYSIS_KEYS
    if bad:
        raise ConfigError(f"Unknown [analysis] key(s): {', '.join(sorted(bad))}")

    defaults = Settings()
    kwargs: dict[str, Any] = {}
    for key, value in analysis.items():
        if key == "disabled_rules":
            if not isinstance(value, list) or not all(isinstance(v, str) for v in value):
                raise ConfigError("'disabled_rules' must be a list of rule ids")
            kwargs[key] = frozenset(v.strip().upper() for v in value)
        else:
            kwargs[key] = _coerce(key, getattr(defaults, key), value)
    if kwargs.get("reservoir_size", defaults.reservoir_size) < 100:
        raise ConfigError("'reservoir_size' must be at least 100")
    return Settings(thresholds=thresholds, **kwargs)


def load_settings(path: str | os.PathLike[str] | None = None) -> Settings:
    """Load settings from ``path`` (or ``$STA_CONFIG``); defaults when neither is given."""
    path = path or os.environ.get("STA_CONFIG")
    if not path:
        return Settings()
    p = Path(path)
    try:
        with p.open("rb") as fh:
            data = tomllib.load(fh)
    except FileNotFoundError as exc:
        raise ConfigError(f"Config file not found: {p}") from exc
    except tomllib.TOMLDecodeError as exc:
        raise ConfigError(f"Invalid TOML in {p}: {exc}") from exc
    return settings_from_mapping(data)
