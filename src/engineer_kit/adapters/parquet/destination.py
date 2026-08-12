"""Streaming Parquet destination for the Bronze layer."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Iterable
from uuid import uuid4

import pyarrow.parquet as pq

from engineer_kit.adapters._arrow import bronze_arrow_schema, rows_to_table
from engineer_kit.storage.batching import DEFAULT_BATCH_SIZE, iter_in_batches, validate_batch_size
from engineer_kit.storage.bronze import build_bronze_rows
from engineer_kit.storage.destination import Destination, LoadResult
from engineer_kit.storage.identifiers import validate_identifier
from engineer_kit.storage.schema import EndpointSchema
from engineer_kit.terminal_log import visual_logger

logger = logging.getLogger("engineer_kit.storage.parquet")


class ParquetDestination(Destination):
    """Append each successful ingestion run as one immutable Parquet file.

    Batches are streamed as row groups into a temporary file. The final file
    becomes visible only after the writer closes successfully and ``replace``
    promotes it into the endpoint directory. This keeps memory bounded and
    prevents a normal Python exception mid-load from exposing a partial run.
    """

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
        staging_dir = self._base_path / ".engineer_kit_staging" / endpoint_name
        endpoint_dir.mkdir(parents=True, exist_ok=True)
        staging_dir.mkdir(parents=True, exist_ok=True)

        run_id = uuid4().hex
        temp_path = staging_dir / f"{run_id}.parquet.tmp"
        final_path = endpoint_dir / f"part-{run_id}.parquet"
        arrow_schema = bronze_arrow_schema(schema)
        total_rows = 0
        all_extra_fields: set[str] = set()
        writer: pq.ParquetWriter | None = None

        try:
            for batch in iter_in_batches(records, self._batch_size):
                rows, extra_fields = build_bronze_rows(
                    connector_name, endpoint_name, schema, batch
                )
                all_extra_fields.update(extra_fields)
                table = rows_to_table(rows, schema)

                if writer is None:
                    writer = pq.ParquetWriter(
                        temp_path,
                        arrow_schema,
                        compression=self._compression,
                        flavor="spark",
                    )
                writer.write_table(table, row_group_size=len(rows))
                total_rows += len(rows)

            if writer is not None:
                writer.close()
                writer = None
                temp_path.replace(final_path)
        except Exception:
            if writer is not None:
                writer.close()
            temp_path.unlink(missing_ok=True)
            raise

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
