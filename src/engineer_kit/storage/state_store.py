"""Contratos e implementacoes para persistir o estado incremental.

O core da ingestao depende apenas de :class:`StateStore`. Onde o
watermark vive e uma decisao de infraestrutura: DuckDB no modo local,
Delta/Lakehouse em plataformas de dados, ou uma implementacao customizada.

`IngestionStateStore` e mantido como alias compativel da implementacao
DuckDB para nao quebrar codigo existente.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date, datetime
from typing import Optional

import duckdb


@dataclass(frozen=True)
class Watermark:
    """Checkpoint persistido depois de uma carga concluida com sucesso."""

    last_run_at: datetime
    last_data_date: Optional[date]
    cursor_value: Optional[str]


class StateStore(ABC):
    """Porta de persistencia usada pelo incremental.

    Implementacoes precisam apenas ler e substituir atomicamente o
    watermark de um conector. Isso mantem APIConnector e
    IncrementalStrategy independentes de DuckDB, Delta ou qualquer
    plataforma especifica.
    """

    @abstractmethod
    def get_watermark(self, connector_name: str) -> Watermark | None:
        """Retorna o ultimo checkpoint do conector ou ``None`` no primeiro run."""

    @abstractmethod
    def set_watermark(self, connector_name: str, watermark: Watermark) -> None:
        """Persiste o checkpoint somente depois que a carga foi confirmada."""


class DuckDBStateStore(StateStore):
    """StateStore zero-infra persistido na area `_meta` do DuckDB."""

    _SCHEMA = "_meta"
    _TABLE = "ingestion_state"

    def __init__(self, conn: duckdb.DuckDBPyConnection) -> None:
        self._conn = conn
        self._ensure_table()

    def _ensure_table(self) -> None:
        self._conn.execute(f"CREATE SCHEMA IF NOT EXISTS {self._SCHEMA}")
        self._conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self._SCHEMA}.{self._TABLE} (
                connector_name VARCHAR PRIMARY KEY,
                last_run_at TIMESTAMP,
                last_data_date DATE,
                cursor_value VARCHAR
            )
            """
        )

    def get_watermark(self, connector_name: str) -> Watermark | None:
        row = self._conn.execute(
            f"SELECT last_run_at, last_data_date, cursor_value "
            f"FROM {self._SCHEMA}.{self._TABLE} WHERE connector_name = ?",
            [connector_name],
        ).fetchone()
        if row is None:
            return None
        last_run_at, last_data_date, cursor_value = row
        return Watermark(
            last_run_at=last_run_at,
            last_data_date=last_data_date,
            cursor_value=cursor_value,
        )

    def set_watermark(self, connector_name: str, watermark: Watermark) -> None:
        """Substitui o checkpoint numa unica transacao."""
        self._conn.execute("BEGIN TRANSACTION")
        try:
            self._conn.execute(
                f"DELETE FROM {self._SCHEMA}.{self._TABLE} WHERE connector_name = ?",
                [connector_name],
            )
            self._conn.execute(
                f"INSERT INTO {self._SCHEMA}.{self._TABLE} VALUES (?, ?, ?, ?)",
                [
                    connector_name,
                    watermark.last_run_at,
                    watermark.last_data_date,
                    watermark.cursor_value,
                ],
            )
            self._conn.execute("COMMIT")
        except Exception:
            self._conn.execute("ROLLBACK")
            raise


# Compatibilidade com a API 0.1: codigo existente continua funcionando,
# enquanto codigo novo pode usar o nome que explicita o backend.
IngestionStateStore = DuckDBStateStore
