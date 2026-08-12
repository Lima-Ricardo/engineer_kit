"""Delta Lake Bronze destination for Lakehouse environments."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Iterable, Iterator

import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

from engineer_kit.adapters._arrow import bronze_arrow_schema, rows_to_record_batch
from engineer_kit.adapters.delta._paths import join_table_uri
from engineer_kit.adapters.delta.run_log import DeltaRunLogStore
from engineer_kit.storage.batching import DEFAULT_BATCH_SIZE, iter_in_batches, validate_batch_size
from engineer_kit.storage.bronze import build_bronze_rows
from engineer_kit.storage.destination import (
    Destination,
    LoadContext,
    LoadResult,
    WriteMode,
)
from engineer_kit.storage.identifiers import validate_identifier
from engineer_kit.storage.run_log import RunLogBackend
from engineer_kit.storage.schema import EndpointSchema
from engineer_kit.terminal_log import visual_logger

logger = logging.getLogger("engineer_kit.storage.delta")


class DeltaDestination(Destination):
    """Stream one ingestion run into one atomic Delta Lake transaction.

    In APPEND mode a Pipeline retry uses Delta predicate overwrite on the
    deterministic ``_ingestion_key``. A new key behaves like an append; the
    same key replaces only that previously committed window. This closes the
    gap between a successful data commit and a failed StateStore checkpoint.
    """

    def __init__(
        self,
        base_uri: str | Path,
        batch_size: int = DEFAULT_BATCH_SIZE,
        storage_options: dict[str, str] | None = None,
        partition_by: list[str] | None = None,
        write_mode: WriteMode | str = WriteMode.APPEND,
        target_file_size: int | None = None,
        metadata_base_uri: str | Path | None = None,
    ) -> None:
        self._base_uri = str(base_uri)
        self._metadata_base_uri = str(metadata_base_uri or base_uri)
        self._batch_size = validate_batch_size(batch_size)
        self._storage_options = dict(storage_options or {})
        self._partition_by = list(partition_by or [])
        self._write_mode = WriteMode.parse(write_mode)
        self._target_file_size = target_file_size
        for field_name in self._partition_by:
            validate_identifier(field_name, "partition_by")

    @property
    def write_mode(self) -> WriteMode:
        return self._write_mode

    def default_run_log_backend(self) -> RunLogBackend:
        return DeltaRunLogStore(
            self._metadata_base_uri,
            storage_options=self._storage_options,
        )

    def load(
        self,
        connector_name: str,
        endpoint: str,
        schema: EndpointSchema,
        records: Iterable[dict[str, Any]],
    ) -> LoadResult:
        return self._load(
            connector_name,
            endpoint,
            schema,
            records,
            LoadContext.adhoc(connector_name),
        )

    def load_with_context(
        self,
        connector_name: str,
        endpoint: str,
        schema: EndpointSchema,
        records: Iterable[dict[str, Any]],
        context: LoadContext,
    ) -> LoadResult:
        return self._load(connector_name, endpoint, schema, records, context)

    def _load(
        self,
        connector_name: str,
        endpoint: str,
        schema: EndpointSchema,
        records: Iterable[dict[str, Any]],
        context: LoadContext,
    ) -> LoadResult:
        endpoint_name = validate_identifier(endpoint, "endpoint")
        table_uri = join_table_uri(self._base_uri, endpoint_name)
        batches = iter(iter_in_batches(records, self._batch_size))
        first_batch = next(batches, None)
        arrow_schema = bronze_arrow_schema(schema)

        if first_batch is None:
            self._commit_empty(table_uri, arrow_schema, context)
            return LoadResult(table=table_uri, rows_loaded=0, extra_fields_seen=[])

        total_rows = 0
        all_extra_fields: set[str] = set()

        def record_batches() -> Iterator[pa.RecordBatch]:
            nonlocal total_rows
            for batch in _with_first(first_batch, batches):
                rows, extra_fields = build_bronze_rows(
                    connector_name,
                    endpoint_name,
                    schema,
                    batch,
                    context=context,
                )
                all_extra_fields.update(extra_fields)
                total_rows += len(rows)
                yield rows_to_record_batch(rows, schema)

        reader = pa.RecordBatchReader.from_batches(arrow_schema, record_batches())
        try:
            self._write(table_uri, reader, context)
        finally:
            reader.close()

        self._report_extra_fields(connector_name, endpoint_name, all_extra_fields)
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

    def _write(self, table_uri: str, data: Any, context: LoadContext) -> None:
        options = self._storage_options or None
        exists = DeltaTable.is_deltatable(table_uri, storage_options=options)
        kwargs: dict[str, Any] = {"storage_options": options}

        if self._target_file_size is not None:
            kwargs["target_file_size"] = self._target_file_size

        if self._write_mode is WriteMode.OVERWRITE:
            kwargs["mode"] = "overwrite"
            if self._partition_by:
                kwargs["partition_by"] = self._partition_by
            write_deltalake(table_uri, data, **kwargs)
            return

        if exists:
            escaped_key = context.ingestion_key.replace("'", "''")
            kwargs["mode"] = "overwrite"
            kwargs["predicate"] = f"_ingestion_key = '{escaped_key}'"
        else:
            kwargs["mode"] = "append"
            if self._partition_by:
                kwargs["partition_by"] = self._partition_by
        write_deltalake(table_uri, data, **kwargs)

    def _commit_empty(
        self,
        table_uri: str,
        arrow_schema: pa.Schema,
        context: LoadContext,
    ) -> None:
        options = self._storage_options or None
        exists = DeltaTable.is_deltatable(table_uri, storage_options=options)

        if self._write_mode is WriteMode.OVERWRITE:
            empty = pa.Table.from_batches([], schema=arrow_schema)
            kwargs: dict[str, Any] = {
                "mode": "overwrite",
                "storage_options": options,
            }
            if self._partition_by:
                kwargs["partition_by"] = self._partition_by
            write_deltalake(table_uri, empty, **kwargs)
            return

        if exists:
            escaped_key = context.ingestion_key.replace("'", "''")
            DeltaTable(table_uri, storage_options=options).delete(
                predicate=f"_ingestion_key = '{escaped_key}'"
            )

    @staticmethod
    def _report_extra_fields(
        connector_name: str, endpoint_name: str, extra_fields: set[str]
    ) -> None:
        if not extra_fields:
            return
        logger.warning(
            "Endpoint '%s': campos fora do schema preservados em _extra: %s",
            endpoint_name,
            sorted(extra_fields),
        )
        visual_logger.warning(
            "'{}': {} coluna(s) nova(s) preservada(s) em _extra: {}",
            connector_name,
            len(extra_fields),
            sorted(extra_fields),
        )


def _with_first(
    first_batch: list[dict[str, Any]],
    batches: Iterator[list[dict[str, Any]]],
) -> Iterator[list[dict[str, Any]]]:
    yield first_batch
    yield from batches


__all__ = ["DeltaDestination"]
