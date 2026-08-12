"""Backend-agnostic contracts for Bronze persistence."""

from __future__ import annotations

import hashlib
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date, datetime, timezone
from enum import Enum
from typing import TYPE_CHECKING, Any, Iterable
from uuid import uuid4

from engineer_kit.storage.schema import EndpointSchema

if TYPE_CHECKING:
    from engineer_kit.storage.run_log import RunLogBackend


class WriteMode(str, Enum):
    """Portable write semantics understood by destination adapters."""

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
class LoadContext:
    """Identity and incremental window for one ingestion attempt.

    ``run_id`` identifies an individual attempt. ``ingestion_key`` is stable
    for the same connector/window, allowing official destinations to replace a
    previously committed retry instead of appending duplicates when a state
    checkpoint fails after the data transaction has already succeeded.
    """

    run_id: str
    ingestion_key: str
    window_start: date | None
    window_end: date | None
    started_at: datetime

    @classmethod
    def for_window(
        cls,
        connector_name: str,
        window_start: date | None,
        window_end: date,
        *,
        started_at: datetime | None = None,
    ) -> "LoadContext":
        started = started_at or datetime.now(timezone.utc)
        start_text = window_start.isoformat() if window_start else ""
        identity = f"{connector_name}\n{start_text}\n{window_end.isoformat()}"
        ingestion_key = hashlib.sha256(identity.encode("utf-8")).hexdigest()[:32]
        return cls(
            run_id=uuid4().hex,
            ingestion_key=ingestion_key,
            window_start=window_start,
            window_end=window_end,
            started_at=started,
        )

    @classmethod
    def adhoc(cls, connector_name: str) -> "LoadContext":
        """Create a unique non-retry context for direct Destination.load calls."""
        now = datetime.now(timezone.utc)
        run_id = uuid4().hex
        identity = f"{connector_name}\nadhoc\n{run_id}"
        return cls(
            run_id=run_id,
            ingestion_key=hashlib.sha256(identity.encode("utf-8")).hexdigest()[:32],
            window_start=None,
            window_end=None,
            started_at=now,
        )


@dataclass(frozen=True)
class LoadResult:
    """Portable result returned by every destination after a confirmed load."""

    table: str
    rows_loaded: int
    extra_fields_seen: list[str]


class Destination(ABC):
    """Write port used by :class:`engineer_kit.Pipeline`."""

    @abstractmethod
    def load(
        self,
        connector_name: str,
        endpoint: str,
        schema: EndpointSchema,
        records: Iterable[dict[str, Any]],
    ) -> LoadResult:
        """Persist records using legacy/context-free semantics.

        This method stays stable for third-party destinations written against
        the initial engineer_kit API.
        """

    def load_with_context(
        self,
        connector_name: str,
        endpoint: str,
        schema: EndpointSchema,
        records: Iterable[dict[str, Any]],
        context: LoadContext,
    ) -> LoadResult:
        """Persist records with retry identity.

        The default implementation preserves compatibility by delegating to
        ``load``. Official adapters override it to provide idempotent retries.
        """
        return self.load(connector_name, endpoint, schema, records)

    def default_run_log_backend(self) -> "RunLogBackend | None":
        return None
