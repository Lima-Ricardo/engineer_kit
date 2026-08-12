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

logger = logging.getLogger("engineer_kit.storage.duckdb")

_INTERNAL_METADATA_TYPES = {
    "_source": "VARCHAR",
    "_endpoint": "VARCHAR",
    "_ingested_at": "TIMESTAMP",
    "_run_id": "VARCHAR",
    "_ingestion_key": "VARCHAR",
    "_window_start": "DATE",
    "_window_end": "DATE",
    "_raw": "VARCHAR",
    "_extra": "VARCHAR",
}


class DuckDBLoader(Destination):
    """Materialize the portable Bronze contract in DuckDB.

    A complete load is one transaction. In APPEND mode, official Pipeline
    retries first remove rows with the same deterministic ``_ingestion_key``
    inside that transaction, then rewrite the window. This covers the case
    where data committed successfully but the subsequent state checkpoint did
    not, without changing the legacy ``load`` API for direct callers.
    """

    def __init__(
        self,
        conn: duckdb.DuckDBPyConnection,
        schema: str = "bronze",
        batch_size: int = DEFAULT_BATCH_SIZE,
        write_mode: WriteMode | str = WriteMode.APPEND,
    ) -> None:
        self._conn = conn
        self._db_schema = validate_identifier(schema, "schema")
        self._batch_size = validate_batch_size(batch_size)
        self._write_mode = WriteMode.parse(write_mode)
        # DuckDB cannot bind identifiers. _db_schema is validated above against
        # the strict identifier contract before interpolation.
        self._conn.execute(f"CREATE SCHEMA IF NOT EXISTS {self._db_schema}")
        self._ensured_tables: set[str] = set()

    @property
    def connection(self) -> duckdb.DuckDBPyConnection:
        return self._conn

    @property
    def write_mode(self) -> WriteMode:
        return self._write_mode

    def default_run_log_backend(self) -> RunLogBackend:
        return DuckDBRunLogStore(self._conn)

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
        table_name = validate_identifier(endpoint, "endpoint")
        # Both components of full_table have passed validate_identifier().
        full_table = f"{self._db_schema}.{table_name}"
        self._ensure_table(full_table, schema)

        total_rows = 0
        all_extra_fields: set[str] = set()
        started_at = time.monotonic()
        bar_format = "tempo {elapsed} | {desc} | {n_fmt} registros gravados ({rate_fmt})"

        self._conn.execute("BEGIN TRANSACTION")
        try:
            if self._write_mode is WriteMode.OVERWRITE:
                self._conn.execute(f"DELETE FROM {full_table}")  # nosec B608
            else:
                # The table identifier is validated; the ingestion key remains a
                # bound value parameter and is never interpolated into SQL.
                self._conn.execute(
                    f'DELETE FROM {full_table} WHERE "_ingestion_key" = ?',  # nosec B608
                    [context.ingestion_key],
                )

            with tqdm(desc=full_table, unit=" registros", bar_format=bar_format) as progress:
                for batch in iter_in_batches(records, self._batch_size):
                    rows, extra_fields = build_bronze_rows(
                        connector_name,
                        endpoint,
                        schema,
                        batch,
                        context=context,
                    )
                    all_extra_fields.update(extra_fields)
                    columns = schema.column_names() + METADATA_COLUMNS
                    self._insert_rows(full_table, columns, rows)
                    total_rows += len(rows)
                    progress.update(len(rows))
            self._conn.execute("COMMIT")
        except Exception:
            self._conn.execute("ROLLBACK")
            raise

        elapsed = time.monotonic() - started_at
        self._report_extra_fields(connector_name, endpoint, all_extra_fields)
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

    def _report_extra_fields(
        self, connector_name: str, endpoint: str, extra_fields: set[str]
    ) -> None:
        if not extra_fields:
            return
        logger.warning(
            "Endpoint '%s' (conector '%s'): campos fora do schema preservados em _extra: %s",
            endpoint,
            connector_name,
            sorted(extra_fields),
        )
        visual_logger.warning(
            "'{}': {} coluna(s) nova(s) preservada(s) em _extra: {}",
            connector_name,
            len(extra_fields),
            sorted(extra_fields),
        )

    def _ensure_table(self, full_table: str, schema: EndpointSchema) -> None:
        if full_table in self._ensured_tables:
            return

        column_defs = ", ".join(f'"{column.name}" VARCHAR' for column in schema.columns)
        metadata_defs = ", ".join(
            f'"{name}" {dtype}' for name, dtype in _INTERNAL_METADATA_TYPES.items()
        )
        all_defs = ", ".join(part for part in (column_defs, metadata_defs) if part)
        # full_table and declared column identifiers are validated by the public
        # schema/identifier contracts before they reach this adapter.
        self._conn.execute(
            f"CREATE TABLE IF NOT EXISTS {full_table} ({all_defs})"  # nosec B608
        )

        # Internal metadata is library-owned and may evolve safely between
        # engineer_kit versions. Declared API columns are still never ALTERed
        # automatically, preserving the explicit source-schema contract.
        for name, dtype in _INTERNAL_METADATA_TYPES.items():
            self._conn.execute(
                f'ALTER TABLE {full_table} ADD COLUMN IF NOT EXISTS "{name}" {dtype}'  # nosec B608
            )
        self._ensured_tables.add(full_table)

    def _insert_rows(
        self, full_table: str, columns: list[str], rows: list[dict[str, Any]]
    ) -> None:
        column_list = ", ".join(f'"{column}"' for column in columns)
        # Only validated identifiers are interpolated. Row data is bound through
        # $1 and cannot alter the SQL statement.
        self._conn.execute(
            f"INSERT INTO {full_table} ({column_list}) "  # nosec B608
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
