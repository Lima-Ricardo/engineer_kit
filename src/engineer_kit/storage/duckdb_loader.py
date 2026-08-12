"""DuckDB Bronze destination.

DuckDB is the local/zero-infrastructure implementation of ``Destination``.
The ingestion core does not depend on it; other adapters reuse the same
Bronze row contract and batching utilities.
"""

from __future__ import annotations

import logging
import time
from typing import Any, Iterable

import duckdb
from tqdm import tqdm

from engineer_kit.adapters.duckdb.run_log import DuckDBRunLogStore
from engineer_kit.storage.batching import (
    DEFAULT_BATCH_SIZE,
    MAX_BATCH_SIZE,
    MIN_BATCH_SIZE,
    InvalidBatchSizeError,
    iter_in_batches,
    validate_batch_size,
)
from engineer_kit.storage.bronze import METADATA_COLUMNS, build_bronze_rows
from engineer_kit.storage.destination import Destination, LoadResult
from engineer_kit.storage.identifiers import validate_identifier
from engineer_kit.storage.run_log import RunLogBackend
from engineer_kit.storage.schema import EndpointSchema
from engineer_kit.terminal_log import visual_logger

logger = logging.getLogger("engineer_kit.storage")


class DuckDBLoader(Destination):
    """Destination that materializes Bronze tables in DuckDB."""

    def __init__(
        self,
        conn: duckdb.DuckDBPyConnection,
        schema: str = "bronze",
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> None:
        self._conn = conn
        self._db_schema = validate_identifier(schema, "schema")
        self._batch_size = validate_batch_size(batch_size)
        self._conn.execute(f"CREATE SCHEMA IF NOT EXISTS {self._db_schema}")
        self._ensured_tables: set[str] = set()

    @property
    def connection(self) -> duckdb.DuckDBPyConnection:
        """Compatibility accessor for callers that already use the connection."""
        return self._conn

    def default_run_log_backend(self) -> RunLogBackend:
        """Persist audit events in the same local DuckDB connection."""
        return DuckDBRunLogStore(self._conn)

    def load(
        self,
        connector_name: str,
        endpoint: str,
        schema: EndpointSchema,
        records: Iterable[dict[str, Any]],
    ) -> LoadResult:
        table_name = validate_identifier(endpoint, "endpoint")
        full_table = f"{self._db_schema}.{table_name}"
        self._ensure_table(full_table, schema)

        total_rows = 0
        all_extra_fields: set[str] = set()
        started_at = time.monotonic()

        bar_format = "tempo {elapsed} | {desc} | {n_fmt} registros gravados ({rate_fmt})"
        with tqdm(desc=full_table, unit=" registros", bar_format=bar_format) as progress:
            for batch in iter_in_batches(records, self._batch_size):
                rows, extra_fields = build_bronze_rows(
                    connector_name, endpoint, schema, batch
                )
                all_extra_fields.update(extra_fields)

                columns = schema.column_names() + METADATA_COLUMNS
                self._insert_rows(full_table, columns, rows)

                total_rows += len(rows)
                progress.update(len(rows))

        elapsed = time.monotonic() - started_at

        if all_extra_fields:
            logger.warning(
                "Endpoint '%s' (conector '%s'): %d campo(s) fora do schema declarado, "
                "capturados em _extra: %s. Atualize o schema quando for tipar corretamente.",
                endpoint,
                connector_name,
                len(all_extra_fields),
                sorted(all_extra_fields),
            )
            visual_logger.warning(
                "'{}': {} coluna(s) nova(s) na API, fora do schema declarado: {}",
                connector_name,
                len(all_extra_fields),
                sorted(all_extra_fields),
            )

        visual_logger.success(
            "'{}': {} registros gravados em {} em {:.1f}s",
            connector_name,
            total_rows,
            full_table,
            elapsed,
        )

        return LoadResult(
            table=full_table,
            rows_loaded=total_rows,
            extra_fields_seen=sorted(all_extra_fields),
        )

    def _ensure_table(self, full_table: str, schema: EndpointSchema) -> None:
        if full_table in self._ensured_tables:
            return
        column_defs = ", ".join(f'"{c.name}" {c.dtype}' for c in schema.columns)
        self._conn.execute(
            f"CREATE TABLE IF NOT EXISTS {full_table} ("
            f"{column_defs}, "
            f'"_source" VARCHAR, "_endpoint" VARCHAR, "_ingested_at" TIMESTAMP, '
            f'"_raw" VARCHAR, "_extra" VARCHAR)'
        )
        self._ensured_tables.add(full_table)

    def _insert_rows(
        self, full_table: str, columns: list[str], rows: list[dict[str, Any]]
    ) -> None:
        column_list = ", ".join(f'"{column}"' for column in columns)
        self._conn.execute(
            f"INSERT INTO {full_table} ({column_list}) "
            f"SELECT unnest(row, recursive := true) "
            f"FROM (SELECT unnest($1) AS row FROM range(1))",
            [rows],
        )


DuckDBDestination = DuckDBLoader

__all__ = [
    "DuckDBLoader",
    "DuckDBDestination",
    "DEFAULT_BATCH_SIZE",
    "MIN_BATCH_SIZE",
    "MAX_BATCH_SIZE",
    "InvalidBatchSizeError",
]
