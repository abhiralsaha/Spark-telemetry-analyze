"""Exception hierarchy for Spark Telemetry Analyzer."""

from __future__ import annotations


class AnalyzerError(Exception):
    """Base class for all expected, user-facing errors."""


class ParseError(AnalyzerError):
    """The input is not a usable Spark event log."""


class UnsupportedFormatError(ParseError):
    """The input uses a compression codec that is not available."""


class ConfigError(AnalyzerError):
    """Invalid configuration file or value."""


class HistoryServerError(AnalyzerError):
    """Failure talking to the Spark History Server."""
