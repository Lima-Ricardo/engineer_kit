"""DuckDB implementation of the RunLogBackend contract."""

from __future__ import annotations

import json

import duckdb

from engineer_kit.storage.run_log import RunLogBackend, RunLogEntry


class DuckDBRunLogStore(RunLogBackend):
    """Zero-infrastructure run log persisted in DuckDB ``_meta.run_log``."""

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


RunLogStore = DuckDBRunLogStore

__all__ = ["DuckDBRunLogStore", "RunLogStore"]
