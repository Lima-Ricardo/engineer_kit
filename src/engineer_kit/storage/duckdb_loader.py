"""Grava lotes de registros extraidos por um conector na camada bronze
do DuckDB, seguindo um schema declarado explicitamente (ver schema.py).

Nao ha inferencia dinamica de colunas nem ALTER TABLE automatico: a
tabela e criada uma vez a partir do schema declarado, e so muda quando
o dev muda o schema. Campo que a API manda fora do schema vai para
`_extra` (JSON) com um aviso simples no log — nunca quebra a carga.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Iterable

import duckdb

from engineer_kit.storage.destination import Destination, LoadResult
from engineer_kit.storage.flatten import flatten_record
from engineer_kit.storage.identifiers import validate_identifier
from engineer_kit.storage.schema import EndpointSchema

logger = logging.getLogger("engineer_kit.storage")

_METADATA_COLUMNS = ["_source", "_endpoint", "_ingested_at", "_raw", "_extra"]


class DuckDBLoader(Destination):
    def __init__(self, conn: duckdb.DuckDBPyConnection, schema: str = "bronze") -> None:
        self._conn = conn
        self._db_schema = validate_identifier(schema, "schema")
        self._conn.execute(f"CREATE SCHEMA IF NOT EXISTS {self._db_schema}")
        self._ensured_tables: set[str] = set()

    def load(
        self,
        connector_name: str,
        endpoint: str,
        schema: EndpointSchema,
        records: Iterable[dict[str, Any]],
    ) -> LoadResult:
        table_name = validate_identifier(endpoint, "endpoint")
        full_table = f"{self._db_schema}.{table_name}"
        records = list(records)

        if not records:
            return LoadResult(table=full_table, rows_loaded=0, extra_fields_seen=[])

        self._ensure_table(full_table, schema)

        known_columns = set(schema.column_names())
        now = datetime.now(timezone.utc)
        extra_fields_seen: set[str] = set()
        rows = []

        for original in records:
            flat = flatten_record(original)
            extras = {key: value for key, value in flat.items() if key not in known_columns}
            extra_fields_seen.update(extras.keys())

            row = {col: flat.get(col) for col in schema.column_names()}
            row["_source"] = connector_name
            row["_endpoint"] = endpoint
            row["_ingested_at"] = now
            row["_raw"] = json.dumps(original, ensure_ascii=False, default=str)
            row["_extra"] = json.dumps(extras, ensure_ascii=False) if extras else None
            rows.append(row)

        if extra_fields_seen:
            logger.warning(
                "Endpoint '%s' (conector '%s'): %d campo(s) fora do schema declarado, "
                "capturados em _extra: %s. Atualize o schema quando for tipar corretamente.",
                endpoint,
                connector_name,
                len(extra_fields_seen),
                sorted(extra_fields_seen),
            )

        columns = schema.column_names() + _METADATA_COLUMNS
        self._insert(full_table, columns, rows)

        return LoadResult(
            table=full_table,
            rows_loaded=len(rows),
            extra_fields_seen=sorted(extra_fields_seen),
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

    def _insert(self, full_table: str, columns: list[str], rows: list[dict[str, Any]]) -> None:
        column_list = ", ".join(f'"{c}"' for c in columns)
        self._conn.execute(
            f"INSERT INTO {full_table} ({column_list}) "
            f"SELECT unnest(row, recursive := true) "
            f"FROM (SELECT unnest($1) AS row FROM range(1))",
            [rows],
        )
