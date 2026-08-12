"""Streaming-first extraction sessions for embedded and managed ingestion.

An :class:`ExtractionSession` represents one resolved incremental window. The
session is intentionally single-pass: iterating it consumes the API stream once,
and the checkpoint can only be committed after the stream is fully consumed.

The default public iteration unit is a batch of 25,000 records. This value is
independent from API pagination and from destination write batching.
"""

from __future__ import annotations

from datetime import date
from typing import Iterator, Optional

from engineer_kit.connectors.date_field import DateFieldSpec, extract_date_value
from engineer_kit.connectors.incremental import IncrementalStrategy, IncrementalWindow
from engineer_kit.storage.state_store import Watermark

DEFAULT_EXTRACTION_BATCH_SIZE = 25_000


class InvalidExtractionBatchSizeError(ValueError):
    """Raised when an extraction batch size is not a positive integer."""


def validate_extraction_batch_size(value: int) -> int:
    """Return a validated extraction batch size."""
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise InvalidExtractionBatchSizeError(
            "extraction batch_size deve ser um inteiro maior que zero."
        )
    return value


class ExtractionSession:
    """Single-pass incremental extraction with an explicit checkpoint boundary.

    Iterating the session yields ``list[dict]`` batches. ``iter_records()`` is
    available for consumers such as managed destinations that already stream
    records internally. ``collect()`` is deliberately explicit because it
    materializes the complete extraction in memory.
    """

    def __init__(
        self,
        *,
        window: IncrementalWindow,
        records: Iterator[dict],
        incremental: IncrementalStrategy,
        date_field: Optional[DateFieldSpec] = None,
        batch_size: int = DEFAULT_EXTRACTION_BATCH_SIZE,
    ) -> None:
        self.window = window
        self.batch_size = validate_extraction_batch_size(batch_size)
        self._records = records
        self._incremental = incremental
        self._date_field = date_field
        self._started = False
        self._exhausted = False
        self._aborted = False
        self._committed = False
        self._watermark_after: Watermark | None = None
        self._max_data_date_seen: date | None = None

    @property
    def exhausted(self) -> bool:
        """Whether the underlying API stream was consumed completely."""
        return self._exhausted

    @property
    def committed(self) -> bool:
        """Whether this session has committed its checkpoint."""
        return self._committed

    @property
    def aborted(self) -> bool:
        """Whether this session was explicitly aborted."""
        return self._aborted

    @property
    def max_data_date_seen(self) -> date | None:
        """Largest configured data date observed while consuming the stream."""
        return self._max_data_date_seen

    @property
    def watermark_after(self) -> Watermark | None:
        """Checkpoint written by ``commit()``, if any."""
        return self._watermark_after

    def __iter__(self) -> Iterator[list[dict]]:
        """Yield extraction batches using the configured default batch size."""
        return self.iter_batches()

    def iter_records(self) -> Iterator[dict]:
        """Consume the extraction as a lazy record stream.

        The session is single-pass. Callers that need to reuse data must persist
        or materialize it themselves rather than causing the API to be fetched
        twice implicitly.
        """
        self._ensure_can_start()
        self._started = True
        try:
            for record in self._records:
                self._track_max_data_date(record)
                yield record
            self._exhausted = True
        finally:
            # If a consumer stops early or closes the generator, _exhausted stays
            # false and commit() refuses to advance the checkpoint.
            pass

    def iter_batches(self, size: int | None = None) -> Iterator[list[dict]]:
        """Consume the extraction in bounded in-memory batches.

        ``size`` defaults to 25,000 records. It is intentionally independent
        from pagination page size and destination write batch size.
        """
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
        """Materialize all records in memory.

        Prefer normal session iteration or ``iter_batches`` for medium/large
        extractions. This method exists for small datasets and convenience.
        """
        return list(self.iter_records())

    def commit(self, max_data_date: date | None = None) -> Watermark:
        """Commit the incremental checkpoint after successful downstream work."""
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
        """Close the session without advancing the checkpoint."""
        if self._committed:
            raise RuntimeError("ExtractionSession ja confirmada; nao pode ser abortada.")
        self._aborted = True

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
            return
        if self._max_data_date_seen is None or seen > self._max_data_date_seen:
            self._max_data_date_seen = seen


__all__ = [
    "DEFAULT_EXTRACTION_BATCH_SIZE",
    "ExtractionSession",
    "InvalidExtractionBatchSizeError",
    "validate_extraction_batch_size",
]
