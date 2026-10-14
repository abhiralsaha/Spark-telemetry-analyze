"""Spark Telemetry Analyzer: turn Spark event logs into actionable performance findings."""

__version__ = "1.0.0"

from .analyzer import Report, analyze_application, analyze_source  # noqa: E402
from .config import Settings, Thresholds, load_settings  # noqa: E402
from .errors import AnalyzerError, ConfigError, ParseError  # noqa: E402
from .models import Finding, Severity  # noqa: E402
from .parser import load_application  # noqa: E402

__all__ = [
    "AnalyzerError",
    "ConfigError",
    "Finding",
    "ParseError",
    "Report",
    "Settings",
    "Severity",
    "Thresholds",
    "__version__",
    "analyze_application",
    "analyze_source",
    "load_application",
    "load_settings",
]
