"""Grava lotes de registros extraidos por um conector na camada bronze
do DuckDB, seguindo um schema declarado explicitamente (ver schema.py).

Nao ha inferencia dinamica de colunas nem ALTER TABLE automatico: a
tabela e criada uma vez a partir do schema declarado, e so muda quando
o dev muda o schema. Campo que a API manda fora do schema vai para
`_extra` (JSON) com um aviso simples no log — nunca quebra a carga.

Os registros sao consumidos e gravados em blocos (`batch_size`), nao
tudo de uma vez: para uma extracao grande, isso limita quanto fica na
memoria a qualquer momento, em vez de acumular tudo antes de escrever
a primeira linha no DuckDB.
"""

from __future__ import annotations

import itertools
import json
import logging
import time
from datetime import datetime, timezone
from typing import Any, Iterable, Iterator

import duckdb
from tqdm import tqdm

from engineer_kit.storage.destination import Destination, LoadResult
from engineer_kit.storage.flatten import flatten_record
from engineer_kit.storage.identifiers import validate_identifier
from engineer_kit.storage.schema import EndpointSchema
from engineer_kit.terminal_log import visual_logger

logger = logging.getLogger("engineer_kit.storage")

_METADATA_COLUMNS = ["_source", "_endpoint", "_ingested_at", "_raw", "_extra"]

# limites globais para o tamanho do bloco de gravacao -- protege contra
# um valor absurdamente pequeno (grava linha a linha, lento) ou grande
# (materializa demais de uma vez, volta o problema que isso resolve).
MIN_BATCH_SIZE = 100
MAX_BATCH_SIZE = 100_000
DEFAULT_BATCH_SIZE = 5000


class InvalidBatchSizeError(ValueError):
    """Levantado quando batch_size esta fora dos limites globais permitidos."""


def _validate_batch_size(batch_size: int) -> int:
    if not (MIN_BATCH_SIZE <= batch_size <= MAX_BATCH_SIZE):
        raise InvalidBatchSizeError(
            f"batch_size={batch_size} fora do intervalo permitido "
            f"[{MIN_BATCH_SIZE}, {MAX_BATCH_SIZE}]."
        )
    return batch_size


def _iter_in_batches(records: Iterator[dict[str, Any]], batch_size: int) -> Iterator[list[dict[str, Any]]]:
    """Consome o iterator em fatias de `batch_size`, sem materializar o
    restante -- cada fatia e descartada da memoria assim que gravada."""
    while True:
        batch = list(itertools.islice(records, batch_size))
        if not batch:
            return
        yield batch


class DuckDBLoader(Destination):
    def __init__(
        self,
        conn: duckdb.DuckDBPyConnection,
        schema: str = "bronze",
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> None:
        self._conn = conn
        self._db_schema = validate_identifier(schema, "schema")
        self._batch_size = _validate_batch_size(batch_size)
        self._conn.execute(f"CREATE SCHEMA IF NOT EXISTS {self._db_schema}")
        self._ensured_tables: set[str] = set()

    @property
    def connection(self) -> duckdb.DuckDBPyConnection:
        """Expoe a conexao ja recebida no construtor -- quem criou o
        loader ja tinha essa conexao; isso so permite que outras partes
        da lib (ex.: Pipeline montando um RunLogStore) a reaproveitem
        sem precisar guardar uma referencia separada."""
        return self._conn

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
            for batch in _iter_in_batches(iter(records), self._batch_size):
                rows, extra_fields = self._build_rows(connector_name, endpoint, schema, batch)
                all_extra_fields.update(extra_fields)

                columns = schema.column_names() + _METADATA_COLUMNS
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

    def _build_rows(
        self,
        connector_name: str,
        endpoint: str,
        schema: EndpointSchema,
        batch: list[dict[str, Any]],
    ) -> tuple[list[dict[str, Any]], set[str]]:
        known_columns = set(schema.column_names())
        now = datetime.now(timezone.utc)
        extra_fields: set[str] = set()
        rows = []

        for original in batch:
            flat = flatten_record(original)
            extras = {key: value for key, value in flat.items() if key not in known_columns}
            extra_fields.update(extras.keys())

            row = {col: flat.get(col) for col in schema.column_names()}
            row["_source"] = connector_name
            row["_endpoint"] = endpoint
            row["_ingested_at"] = now
            row["_raw"] = json.dumps(original, ensure_ascii=False, default=str)
            row["_extra"] = json.dumps(extras, ensure_ascii=False) if extras else None
            rows.append(row)

        return rows, extra_fields

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

    def _insert_rows(self, full_table: str, columns: list[str], rows: list[dict[str, Any]]) -> None:
        column_list = ", ".join(f'"{c}"' for c in columns)
        self._conn.execute(
            f"INSERT INTO {full_table} ({column_list}) "
            f"SELECT unnest(row, recursive := true) "
            f"FROM (SELECT unnest($1) AS row FROM range(1))",
            [rows],
        )
