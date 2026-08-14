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

MAX_STATE_KEY_CHARS = 512


def validate_state_key(value: str) -> str:
    """Validate a portable checkpoint namespace stored as data.

    State keys may contain dots, dashes and other normal namespace characters;
    they are not SQL identifiers. Empty, unbounded and control-character values
    are rejected consistently before reaching any official backend.
    """
    key = str(value)
    if not key or len(key) > MAX_STATE_KEY_CHARS:
        raise ValueError(
            f"state_key deve conter entre 1 e {MAX_STATE_KEY_CHARS} caracteres."
        )
    if any(ord(char) < 32 or ord(char) == 127 for char in key):
        raise ValueError("state_key nao pode conter caracteres de controle.")
    return key


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
        key = validate_state_key(connector_name)
        current = self.get_watermark(key)
        if current != expected:
            raise StateConflictError(
                f"Checkpoint de '{key}' mudou durante a execucao; "
                "o novo watermark nao foi confirmado. Reexecute a partir do estado atual."
            )
        self.set_watermark(key, watermark)


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
__all__ = [
    "MAX_STATE_KEY_CHARS",
    "StateConflictError",
    "StateStore",
    "Watermark",
    "validate_state_key",
]
