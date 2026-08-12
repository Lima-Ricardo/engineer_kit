"""Contratos e implementacoes para registrar execucoes de ingestao.

O core do Pipeline depende apenas de :class:`RunLogBackend`. Onde os
logs persistem e uma decisao de infraestrutura: DuckDB no modo local,
Delta/Lakehouse em plataformas de dados, ou uma implementacao customizada.

`RunLogStore` e mantido como alias compativel da implementacao DuckDB
para nao quebrar codigo existente.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
import json
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

import duckdb


@dataclass(frozen=True)
class RunLogEntry:
    """Evento auditavel produzido ao final de uma tentativa de carga."""

    connector_name: str
    started_at: datetime
    finished_at: datetime
    status: str  # "success" ou "error"
    rows_loaded: int
    extra_fields_seen: list[str]
    error_message: Optional[str] = None


class RunLogBackend(ABC):
    """Porta de observabilidade usada pelo Pipeline.

    Uma implementacao precisa apenas persistir um :class:`RunLogEntry`.
    Isso permite trocar DuckDB por Delta, uma tabela SQL, um servico de
    observabilidade ou um backend em memoria sem alterar a orquestracao.
    """

    @abstractmethod
    def record(self, entry: RunLogEntry) -> None:
        """Persiste um evento de execucao."""


class DuckDBRunLogStore(RunLogBackend):
    """RunLogBackend zero-infra persistido em `_meta.run_log` no DuckDB."""

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


# Compatibilidade com a API 0.1: codigo existente que instancia
# RunLogStore(conn) continua usando DuckDB, enquanto codigo novo pode
# explicitar o backend.
RunLogStore = DuckDBRunLogStore
