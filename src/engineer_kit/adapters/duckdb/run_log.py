"""DuckDB implementation of the RunLogBackend contract."""

from __future__ import annotations

import json

import duckdb

from engineer_kit.storage.run_log import RunLogBackend, RunLogEntry


class DuckDBRunLogStore(RunLogBackend):
    """Zero-infrastructure run log persisted in DuckDB ``_meta.run_log``."""

    _SCHEMA = "_meta"
    _TABLE = "run_log"
    _OPTIONAL_COLUMNS = {
        "run_id": "VARCHAR",
        "destination": "VARCHAR",
        "window_start": "DATE",
        "window_end": "DATE",
        "watermark_before": "VARCHAR",
        "watermark_after": "VARCHAR",
    }

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
                error_message VARCHAR,
                run_id VARCHAR,
                destination VARCHAR,
                window_start DATE,
                window_end DATE,
                watermark_before VARCHAR,
                watermark_after VARCHAR
            )
            """
        )
        existing = {
            row[0]
            for row in self._conn.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = ? AND table_name = ?",
                [self._SCHEMA, self._TABLE],
            ).fetchall()
        }
        for column, dtype in self._OPTIONAL_COLUMNS.items():
            if column not in existing:
                self._conn.execute(
                    f"ALTER TABLE {self._SCHEMA}.{self._TABLE} ADD COLUMN {column} {dtype}"
                )

    def record(self, entry: RunLogEntry) -> None:
        self._conn.execute(
            f"""
            INSERT INTO {self._SCHEMA}.{self._TABLE} (
                connector_name, started_at, finished_at, status, rows_loaded,
                extra_fields_seen, error_message, run_id, destination,
                window_start, window_end, watermark_before, watermark_after
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                entry.connector_name,
                entry.started_at,
                entry.finished_at,
                entry.status,
                entry.rows_loaded,
                json.dumps(entry.extra_fields_seen, ensure_ascii=False),
                entry.error_message,
                entry.run_id,
                entry.destination,
                entry.window_start,
                entry.window_end,
                entry.watermark_before,
                entry.watermark_after,
            ],
        )


RunLogStore = DuckDBRunLogStore

__all__ = ["DuckDBRunLogStore", "RunLogStore"]
