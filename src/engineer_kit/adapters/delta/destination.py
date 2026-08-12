"""Delta Lake Bronze destination for Lakehouse environments."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Iterable, Iterator

import pyarrow as pa
from deltalake import write_deltalake

from engineer_kit.adapters._arrow import bronze_arrow_schema, rows_to_record_batch
from engineer_kit.adapters.delta._paths import join_table_uri
from engineer_kit.adapters.delta.run_log import DeltaRunLogStore
from engineer_kit.storage.batching import DEFAULT_BATCH_SIZE, iter_in_batches, validate_batch_size
from engineer_kit.storage.bronze import build_bronze_rows
from engineer_kit.storage.destination import Destination, LoadResult
from engineer_kit.storage.identifiers import validate_identifier
from engineer_kit.storage.run_log import RunLogBackend
from engineer_kit.storage.schema import EndpointSchema
from engineer_kit.terminal_log import visual_logger

logger = logging.getLogger("engineer_kit.storage.delta")


class DeltaDestination(Destination):
    """Stream one ingestion run into one atomic Delta Lake transaction.

    ``base_uri`` may be a local directory or an object-store URI supported by
    delta-rs. Each endpoint becomes one Delta table below that root. Record
    batches remain bounded in memory, while the Arrow stream is committed by
    Delta as one append transaction only after the full stream succeeds.
    """

    def __init__(
        self,
        base_uri: str | Path,
        batch_size: int = DEFAULT_BATCH_SIZE,
        storage_options: dict[str, str] | None = None,
    ) -> None:
        self._base_uri = str(base_uri)
        self._batch_size = validate_batch_size(batch_size)
        self._storage_options = dict(storage_options or {})

    def default_run_log_backend(self) -> RunLogBackend:
        return DeltaRunLogStore(self._base_uri, storage_options=self._storage_options)

    def load(
        self,
        connector_name: str,
        endpoint: str,
        schema: EndpointSchema,
        records: Iterable[dict[str, Any]],
    ) -> LoadResult:
        endpoint_name = validate_identifier(endpoint, "endpoint")
        table_uri = join_table_uri(self._base_uri, endpoint_name)
        batches = iter(iter_in_batches(records, self._batch_size))
        first_batch = next(batches, None)

        if first_batch is None:
            return LoadResult(table=table_uri, rows_loaded=0, extra_fields_seen=[])

        total_rows = 0
        all_extra_fields: set[str] = set()
        arrow_schema = bronze_arrow_schema(schema)

        def record_batches() -> Iterator[pa.RecordBatch]:
            nonlocal total_rows
            for batch in _with_first(first_batch, batches):
                rows, extra_fields = build_bronze_rows(
                    connector_name, endpoint_name, schema, batch
                )
                all_extra_fields.update(extra_fields)
                total_rows += len(rows)
                yield rows_to_record_batch(rows, schema)

        reader = pa.RecordBatchReader.from_batches(arrow_schema, record_batches())
        try:
            write_deltalake(
                table_uri,
                reader,
                mode="append",
                storage_options=self._storage_options or None,
            )
        finally:
            reader.close()

        if all_extra_fields:
            logger.warning(
                "Endpoint '%s': campos fora do schema preservados em _extra: %s",
                endpoint_name,
                sorted(all_extra_fields),
            )
            visual_logger.warning(
                "'{}': {} coluna(s) nova(s) preservada(s) em _extra: {}",
                connector_name,
                len(all_extra_fields),
                sorted(all_extra_fields),
            )

        visual_logger.success(
            "'{}': {} registros gravados em Delta: {}",
            connector_name,
            total_rows,
            table_uri,
        )
        return LoadResult(
            table=table_uri,
            rows_loaded=total_rows,
            extra_fields_seen=sorted(all_extra_fields),
        )


def _with_first(
    first_batch: list[dict[str, Any]],
    batches: Iterator[list[dict[str, Any]]],
) -> Iterator[list[dict[str, Any]]]:
    yield first_batch
    yield from batches


__all__ = ["DeltaDestination"]
