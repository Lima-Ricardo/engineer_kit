"""Registra cada execucao de conector: quando comecou, quando terminou,
status (sucesso/erro), quantidade de registros e quais campos novos
apareceram fora do schema declarado.

Fica em `_meta.run_log`, ao lado de `_meta.ingestion_state` -- o dbt
pode ler essa tabela como qualquer outra fonte. Gravar aqui e opcional:
o Pipeline so grava se receber um RunLogStore; quem nao passar nenhum,
simplesmente nao tem essa tabela.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

import duckdb


@dataclass
class RunLogEntry:
    connector_name: str
    started_at: datetime
    finished_at: datetime
    status: str  # "success" ou "error"
    rows_loaded: int
    extra_fields_seen: list[str]
    error_message: Optional[str] = None


class RunLogStore:
    _SCHEMA = "_meta"
    _TABLE = "run_log"

    def __init__(self, conn: duckdb.DuckDBPyConnection) -> None:
        self._conn = conn
        self._ensure_table()

    def _ensure_table(self) -> None:
        self._conn.execute(f"CREATE SCHEMA IF NOT EXISTS {self._SCHEMA}")
        self._conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self._SCHEMA}.{self._TABLE} (
                connector_name VARCHAR,
                started_at TIMESTAMP,
                finished_at TIMESTAMP,
                status VARCHAR,
                rows_loaded BIGINT,
                extra_fields_seen VARCHAR,
                error_message VARCHAR
            )
            """
        )

    def record(self, entry: RunLogEntry) -> None:
        self._conn.execute(
            f"INSERT INTO {self._SCHEMA}.{self._TABLE} VALUES (?, ?, ?, ?, ?, ?, ?)",
            [
                entry.connector_name,
                entry.started_at,
                entry.finished_at,
                entry.status,
                entry.rows_loaded,
                json.dumps(entry.extra_fields_seen, ensure_ascii=False),
                entry.error_message,
            ],
        )
