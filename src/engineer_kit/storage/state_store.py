"""Backend-agnostic contracts for incremental ingestion state.

The ingestion core depends only on :class:`StateStore`. Concrete storage
backends (DuckDB locally, Delta/Lakehouse, or user-defined implementations)
live outside this module so importing the core does not require DuckDB.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date, datetime
from typing import Optional


@dataclass(frozen=True)
class Watermark:
    """Checkpoint persisted only after a load is confirmed successful."""

    last_run_at: datetime
    last_data_date: Optional[date]
    cursor_value: Optional[str]


class StateStore(ABC):
    """Persistence port used by incremental strategies.

    Implementations only need to read and atomically replace the watermark
    for a connector. The connector therefore never needs to know whether
    state lives in DuckDB, Delta, a SQL table, or another service.
    """

    @abstractmethod
    def get_watermark(self, connector_name: str) -> Watermark | None:
        """Return the last checkpoint or ``None`` for the first run."""

    @abstractmethod
    def set_watermark(self, connector_name: str, watermark: Watermark) -> None:
        """Persist a checkpoint after the destination has confirmed the load."""


_DUCKDB_EXPORTS = {"DuckDBStateStore", "IngestionStateStore"}


def __getattr__(name: str):
    """Keep 0.1 DuckDB imports compatible without making DuckDB a core dependency."""
    if name not in _DUCKDB_EXPORTS:
        raise AttributeError(name)

    try:
        from engineer_kit.adapters.duckdb.state_store import DuckDBStateStore
    except ModuleNotFoundError as exc:
        if exc.name == "duckdb":
            raise ModuleNotFoundError(
                "DuckDB support is optional. Install it with "
                "`pip install \"engineer_kit[duckdb]\"`."
            ) from None
        raise

    return DuckDBStateStore


# ``__getattr__`` preserves explicit legacy imports. Wildcard exports remain
# backend-neutral so static tooling and core-only installations see only ports.
__all__ = ["StateStore", "Watermark"]
