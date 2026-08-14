"""Resolve incremental extraction windows against a backend-agnostic StateStore."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from enum import Enum
from typing import Optional, Union

from engineer_kit.storage.state_store import StateStore, Watermark


class IncrementalMode(str, Enum):
    DATA_DATE = "data_date"
    INGESTION_DATE = "ingestion_date"


@dataclass(frozen=True)
class IncrementalWindow:
    """Resolved extraction interval plus the checkpoint it was derived from.

    ``start`` preserves the checkpoint boundary used for the next request.
    Date-granularity checkpoints are intentionally inclusive: advancing the
    request to the next day can lose records that arrive later on the same
    calendar date after a successful run. Re-reading the boundary is the
    correctness-first choice; destination retry identity and explicit dedup
    policies handle replay where needed.
    """

    start: Optional[date]
    end: date
    watermark_before: Watermark | None = None
    start_inclusive: bool = True

    @property
    def request_start(self) -> date | None:
        return self.start


class IncrementalStrategy:
    """Resolve and commit checkpoints without knowing the physical backend."""

    def __init__(
        self,
        connector_name: str,
        state_store: StateStore,
        mode: IncrementalMode = IncrementalMode.DATA_DATE,
        initial_start: Optional[date] = None,
    ) -> None:
        self._connector_name = connector_name
        self._state_store = state_store
        self._mode = mode
        self._initial_start = initial_start

    @property
    def state_key(self) -> str:
        return self._connector_name

    @property
    def mode(self) -> IncrementalMode:
        return self._mode

    def resolve_window(self, end: Union[date, str] = "today") -> IncrementalWindow:
        resolved_end = date.today() if end == "today" else end
        if not isinstance(resolved_end, date):
            raise TypeError("end deve ser date ou 'today'.")
        watermark = self._state_store.get_watermark(self._connector_name)

        if watermark is None:
            return IncrementalWindow(
                start=self._initial_start,
                end=resolved_end,
                watermark_before=None,
                start_inclusive=True,
            )

        start = (
            watermark.last_data_date
            if self._mode is IncrementalMode.DATA_DATE
            else watermark.last_run_at.date()
        )
        return IncrementalWindow(
            start=start,
            end=resolved_end,
            watermark_before=watermark,
            start_inclusive=True,
        )

    def commit(
        self,
        window: IncrementalWindow,
        max_data_date: Optional[date] = None,
    ) -> Watermark:
        """Persist and return a checkpoint only if the starting state is unchanged."""
        watermark = Watermark(
            last_run_at=datetime.now(timezone.utc),
            last_data_date=max_data_date or window.end,
            cursor_value=None,
        )
        self._state_store.compare_and_set_watermark(
            self._connector_name,
            window.watermark_before,
            watermark,
        )
        return watermark


class NoIncrementalStrategy(IncrementalStrategy):
    """No-op checkpoint strategy used by the zero-configuration happy path."""

    def __init__(self) -> None:
        pass

    @property
    def state_key(self) -> str:
        return "<disabled>"

    @property
    def mode(self) -> IncrementalMode:
        return IncrementalMode.INGESTION_DATE

    def resolve_window(self, end: Union[date, str] = "today") -> IncrementalWindow:
        resolved_end = date.today() if end == "today" else end
        if not isinstance(resolved_end, date):
            raise TypeError("end deve ser date ou 'today'.")
        return IncrementalWindow(
            start=None,
            end=resolved_end,
            watermark_before=None,
            start_inclusive=True,
        )

    def commit(
        self,
        window: IncrementalWindow,
        max_data_date: Optional[date] = None,
    ) -> Watermark:
        return Watermark(
            last_run_at=datetime.now(timezone.utc),
            last_data_date=max_data_date or window.end,
            cursor_value=None,
        )


__all__ = [
    "IncrementalMode",
    "IncrementalWindow",
    "IncrementalStrategy",
    "NoIncrementalStrategy",
]
