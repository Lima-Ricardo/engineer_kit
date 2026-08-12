"""Streaming Parquet destination for the Bronze layer."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Iterable
from uuid import uuid4

import pyarrow as pa
import pyarrow.parquet as pq

from engineer_kit.storage.batching import DEFAULT_BATCH_SIZE, iter_in_batches, validate_batch_size
from engineer_kit.storage.bronze import build_bronze_rows
from engineer_kit.storage.destination import Destination, LoadResult
from engineer_kit.storage.identifiers import validate_identifier
from engineer_kit.storage.schema import EndpointSchema
from engineer_kit.terminal_log import visual_logger

logger = logging.getLogger("engineer_kit.storage.parquet")


def _bronze_arrow_schema(schema: EndpointSchema) -> pa.Schema:
    fields = [pa.field(column.name, pa.string()) for column in schema.columns]
    fields.extend(
        [
            pa.field("_source", pa.string()),
            pa.field("_endpoint", pa.string()),
            pa.field("_ingested_at", pa.timestamp("us", tz="UTC")),
            pa.field("_raw", pa.string()),
            pa.field("_extra", pa.string()),
        ]
    )
    return pa.schema(fields)


class ParquetDestination(Destination):
    """Append API batches as immutable Parquet files under one endpoint directory."""

    def __init__(
        self,
        base_path: str | Path,
        batch_size: int = DEFAULT_BATCH_SIZE,
        compression: str = "snappy",
    ) -> None:
        self._base_path = Path(base_path)
        self._batch_size = validate_batch_size(batch_size)
        self._compression = compression

    def load(
        self,
        connector_name: str,
        endpoint: str,
        schema: EndpointSchema,
        records: Iterable[dict[str, Any]],
    ) -> LoadResult:
        endpoint_name = validate_identifier(endpoint, "endpoint")
        endpoint_dir = self._base_path / endpoint_name
        endpoint_dir.mkdir(parents=True, exist_ok=True)

        run_id = uuid4().hex
        arrow_schema = _bronze_arrow_schema(schema)
        total_rows = 0
        all_extra_fields: set[str] = set()

        for batch_number, batch in enumerate(iter_in_batches(records, self._batch_size)):
            rows, extra_fields = build_bronze_rows(
                connector_name, endpoint_name, schema, batch
            )
            all_extra_fields.update(extra_fields)
            table = pa.Table.from_pylist(rows, schema=arrow_schema)
            file_path = endpoint_dir / f"part-{run_id}-{batch_number:05d}.parquet"
            pq.write_table(table, file_path, compression=self._compression, flavor="spark")
            total_rows += len(rows)

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
            "'{}': {} registros gravados em Parquet: {}",
            connector_name,
            total_rows,
            endpoint_dir,
        )
        return LoadResult(
            table=str(endpoint_dir),
            rows_loaded=total_rows,
            extra_fields_seen=sorted(all_extra_fields),
        )


__all__ = ["ParquetDestination"]
