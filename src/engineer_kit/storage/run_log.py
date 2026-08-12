"""Backend-agnostic contracts for ingestion run audit events."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date, datetime
from typing import Optional


@dataclass(frozen=True)
class RunLogEntry:
    """Auditable event produced at the end of one ingestion attempt.

    The original 0.1 fields remain first for backwards compatibility. New
    execution/window metadata is optional so custom backends can adopt it
    incrementally.
    """

    connector_name: str
    started_at: datetime
    finished_at: datetime
    status: str
    rows_loaded: int
    extra_fields_seen: list[str]
    error_message: Optional[str] = None
    run_id: Optional[str] = None
    destination: Optional[str] = None
    window_start: Optional[date] = None
    window_end: Optional[date] = None
    watermark_before: Optional[str] = None
    watermark_after: Optional[str] = None


class RunLogBackend(ABC):
    """Observability port used by the Pipeline."""

    @abstractmethod
    def record(self, entry: RunLogEntry) -> None:
        """Persist one execution event."""


_DUCKDB_EXPORTS = {"DuckDBRunLogStore", "RunLogStore"}


def __getattr__(name: str):
    """Keep 0.1 DuckDB imports compatible without making DuckDB mandatory."""
    if name not in _DUCKDB_EXPORTS:
        raise AttributeError(name)

    try:
        from engineer_kit.adapters.duckdb.run_log import DuckDBRunLogStore
    except ModuleNotFoundError as exc:
        if exc.name == "duckdb":
            raise ModuleNotFoundError(
                'DuckDB support is optional. Install `pip install "engineer_kit[duckdb]"`.'
            ) from None
        raise

    return DuckDBRunLogStore


__all__ = ["RunLogBackend", "RunLogEntry", "DuckDBRunLogStore", "RunLogStore"]
