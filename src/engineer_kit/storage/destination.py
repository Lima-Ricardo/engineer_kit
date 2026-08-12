"""Backend-agnostic contracts for Bronze persistence.

The ingestion Pipeline knows only :class:`Destination`. Concrete adapters may
write to DuckDB, Parquet, Delta/Lakehouse, or user-defined backends without
changing extraction or orchestration code.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Any, Iterable

from engineer_kit.storage.schema import EndpointSchema

if TYPE_CHECKING:
    from engineer_kit.storage.run_log import RunLogBackend


class WriteMode(str, Enum):
    """Portable write semantics understood by destination adapters.

    ``APPEND`` is the natural Bronze mode. ``OVERWRITE`` replaces the target
    atomically when the adapter can provide that guarantee. More advanced
    upsert/merge semantics stay adapter-specific because they require business
    keys and should not be guessed by the ingestion core.
    """

    APPEND = "append"
    OVERWRITE = "overwrite"

    @classmethod
    def parse(cls, value: "WriteMode | str") -> "WriteMode":
        if isinstance(value, cls):
            return value
        try:
            return cls(str(value).lower())
        except ValueError as exc:
            valid = ", ".join(item.value for item in cls)
            raise ValueError(f"write_mode '{value}' invalido. Use: {valid}.") from exc


@dataclass(frozen=True)
class LoadResult:
    """Portable result returned by every destination after a confirmed load."""

    table: str
    rows_loaded: int
    extra_fields_seen: list[str]


class Destination(ABC):
    """Write port used by :class:`engineer_kit.Pipeline`.

    A destination receives the stable Bronze contract and owns the physical
    details of persistence. It must preserve unknown API fields rather than
    silently dropping them.
    """

    @abstractmethod
    def load(
        self,
        connector_name: str,
        endpoint: str,
        schema: EndpointSchema,
        records: Iterable[dict[str, Any]],
    ) -> LoadResult:
        """Persist records following the declared schema and Bronze contract."""

    def default_run_log_backend(self) -> "RunLogBackend | None":
        """Return the natural audit backend for this destination, if any.

        Programmatic users can rely on this convenience. Declarative runtimes
        may choose a separate run-log adapter explicitly.
        """
        return None
