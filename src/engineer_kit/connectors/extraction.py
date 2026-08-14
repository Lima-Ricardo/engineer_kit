"""Streaming-first extraction sessions for embedded and managed ingestion.

An :class:`ExtractionSession` represents one resolved incremental window. The
session is intentionally single-pass: iterating it consumes the API stream once,
and the checkpoint can only be committed after the stream is fully consumed.
"""

from __future__ import annotations

from datetime import date
from typing import Callable, Iterator, Optional, Sequence

from engineer_kit.connectors.date_field import DateFieldSpec, extract_date_value
from engineer_kit.connectors.dedup import ExactKeyDeduplicator, resolve_primary_key
from engineer_kit.connectors.incremental import IncrementalStrategy, IncrementalWindow
from engineer_kit.storage.state_store import Watermark

DEFAULT_EXTRACTION_BATCH_SIZE = 25_000


class InvalidExtractionBatchSizeError(ValueError):
    """Raised when an extraction batch size is not a positive integer."""


def validate_extraction_batch_size(value: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise InvalidExtractionBatchSizeError(
            "extraction batch_size deve ser um inteiro maior que zero."
        )
    return value


class ExtractionSession:
    """Single-pass incremental extraction with an explicit checkpoint boundary."""

    def __init__(
        self,
        *,
        window: IncrementalWindow,
        records: Iterator[dict],
        incremental: IncrementalStrategy,
        date_field: Optional[DateFieldSpec] = None,
        batch_size: int = DEFAULT_EXTRACTION_BATCH_SIZE,
        record_transform: Callable[[dict], dict] | None = None,
        dedup: str | Sequence[str] | bool | None = False,
    ) -> None:
        self.window = window
        self.batch_size = validate_extraction_batch_size(batch_size)
        self._records = records
        self._incremental = incremental
        self._date_field = date_field
        self._record_transform = record_transform
        self._dedup_keys = (
            None if dedup is False or dedup is None else resolve_primary_key(dedup)
        )
        self._started = False
        self._exhausted = False
        self._aborted = False
        self._committed = False
        self._watermark_after: Watermark | None = None
        self._max_data_date_seen: date | None = None
        self._invalid_date_records = 0

    @property
    def exhausted(self) -> bool:
        return self._exhausted

    @property
    def committed(self) -> bool:
        return self._committed

    @property
    def aborted(self) -> bool:
        return self._aborted

    @property
    def dedup_enabled(self) -> bool:
        return self._dedup_keys is not None

    @property
    def dedup_keys(self) -> tuple[str, ...] | None:
        return self._dedup_keys

    @property
    def max_data_date_seen(self) -> date | None:
        return self._max_data_date_seen

    @property
    def invalid_date_records(self) -> int:
        """Rows that could not produce the configured DATA_DATE value."""
        return self._invalid_date_records

    @property
    def watermark_after(self) -> Watermark | None:
        return self._watermark_after

    def __enter__(self) -> "ExtractionSession":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if exc_type is not None and not self._committed:
            self.abort()
        elif not self._committed and not self._aborted:
            self.abort()

    def __iter__(self) -> Iterator[list[dict]]:
        return self.iter_batches()

    def iter_records(self) -> Iterator[dict]:
        self._ensure_can_start()
        self._started = True
        deduplicator = (
            ExactKeyDeduplicator(self._dedup_keys)
            if self._dedup_keys is not None
            else None
        )
        try:
            for record in self._records:
                self._track_max_data_date(record)
                output = self._record_transform(record) if self._record_transform else record
                if deduplicator is not None and deduplicator.add(output) is False:
                    continue
                yield output
            self._exhausted = True
        finally:
            if deduplicator is not None:
                deduplicator.close()

    def iter_batches(self, size: int | None = None) -> Iterator[list[dict]]:
        resolved_size = self.batch_size if size is None else validate_extraction_batch_size(size)
        batch: list[dict] = []
        for record in self.iter_records():
            batch.append(record)
            if len(batch) >= resolved_size:
                yield batch
                batch = []
        if batch:
            yield batch

    def collect(self) -> list[dict]:
        return list(self.iter_records())

    def commit(self, max_data_date: date | None = None) -> Watermark:
        if self._aborted:
            raise RuntimeError("ExtractionSession abortada; checkpoint nao pode ser confirmado.")
        if self._committed:
            assert self._watermark_after is not None
            return self._watermark_after
        if not self._exhausted:
            raise RuntimeError(
                "ExtractionSession ainda nao foi consumida por completo; "
                "checkpoint nao pode avancar parcialmente."
            )
        if self._date_field is not None and self._invalid_date_records:
            raise RuntimeError(
                "checkpoint recusado: "
                f"{self._invalid_date_records} registro(s) nao possuem date_field valido. "
                "Corrija a origem/mapeamento antes de avancar o watermark."
            )
        effective_max_date = (
            max_data_date if max_data_date is not None else self._max_data_date_seen
        )
        self._watermark_after = self._incremental.commit(
            self.window,
            max_data_date=effective_max_date,
        )
        self._committed = True
        return self._watermark_after

    def abort(self) -> None:
        if self._committed:
            raise RuntimeError("ExtractionSession ja confirmada; nao pode ser abortada.")
        if self._aborted:
            return
        self._aborted = True
        close = getattr(self._records, "close", None)
        if callable(close):
            close()

    def _ensure_can_start(self) -> None:
        if self._aborted:
            raise RuntimeError("ExtractionSession abortada.")
        if self._committed:
            raise RuntimeError("ExtractionSession ja confirmada e encerrada.")
        if self._started:
            raise RuntimeError(
                "ExtractionSession e single-pass e ja foi consumida. "
                "Crie uma nova sessao para uma nova chamada de API."
            )

    def _track_max_data_date(self, record: dict) -> None:
        if self._date_field is None:
            return
        seen = extract_date_value(record, self._date_field)
        if seen is None:
            self._invalid_date_records += 1
            return
        if self._max_data_date_seen is None or seen > self._max_data_date_seen:
            self._max_data_date_seen = seen


__all__ = [
    "DEFAULT_EXTRACTION_BATCH_SIZE",
    "ExtractionSession",
    "InvalidExtractionBatchSizeError",
    "validate_extraction_batch_size",
]
