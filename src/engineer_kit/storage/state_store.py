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


class StateConflictError(RuntimeError):
    """Raised when a checkpoint changed after an extraction window was resolved."""


class StateStore(ABC):
    """Persistence port used by incremental strategies.

    Implementations only need to read and atomically replace the watermark
    for a connector. New implementations should override
    :meth:`compare_and_set_watermark` atomically when the backend supports it.
    The compatibility fallback detects conflicts but cannot make the read/write
    pair atomic across processes.
    """

    @abstractmethod
    def get_watermark(self, connector_name: str) -> Watermark | None:
        """Return the last checkpoint or ``None`` for the first run."""

    @abstractmethod
    def set_watermark(self, connector_name: str, watermark: Watermark) -> None:
        """Persist a checkpoint after the destination has confirmed the load."""

    @property
    def supports_atomic_compare_and_set(self) -> bool:
        return False

    def compare_and_set_watermark(
        self,
        connector_name: str,
        expected: Watermark | None,
        watermark: Watermark,
    ) -> None:
        """Commit only when state still matches the window's starting checkpoint.

        This default keeps third-party StateStore implementations compatible.
        Official mutable backends override it with a backend-atomic operation.
        """
        current = self.get_watermark(connector_name)
        if current != expected:
            raise StateConflictError(
                f"Checkpoint de '{connector_name}' mudou durante a execucao; "
                "o novo watermark nao foi confirmado. Reexecute a partir do estado atual."
            )
        self.set_watermark(connector_name, watermark)


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
__all__ = ["StateConflictError", "StateStore", "Watermark"]
