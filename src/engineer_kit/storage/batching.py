"""Shared bounded-batch utilities for streaming destinations."""

from __future__ import annotations

import itertools
from typing import Any, Iterable, Iterator

MIN_BATCH_SIZE = 100
MAX_BATCH_SIZE = 100_000
DEFAULT_BATCH_SIZE = 5_000


class InvalidBatchSizeError(ValueError):
    """Raised when a destination batch size is outside supported bounds."""


def validate_batch_size(batch_size: int) -> int:
    if isinstance(batch_size, bool) or not isinstance(batch_size, int):
        raise InvalidBatchSizeError("batch_size deve ser um inteiro.")
    if not (MIN_BATCH_SIZE <= batch_size <= MAX_BATCH_SIZE):
        raise InvalidBatchSizeError(
            f"batch_size={batch_size} fora do intervalo permitido "
            f"[{MIN_BATCH_SIZE}, {MAX_BATCH_SIZE}]."
        )
    return batch_size


def iter_in_batches(
    records: Iterable[dict[str, Any]], batch_size: int
) -> Iterator[list[dict[str, Any]]]:
    """Consume an iterable in bounded slices without materializing it all."""
    size = validate_batch_size(batch_size)
    iterator = iter(records)
    while True:
        batch = list(itertools.islice(iterator, size))
        if not batch:
            return
        yield batch
