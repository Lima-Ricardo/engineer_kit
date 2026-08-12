"""DuckDB implementation of the incremental StateStore contract."""

from __future__ import annotations

import duckdb

from engineer_kit.storage.state_store import StateStore, Watermark


class DuckDBStateStore(StateStore):
    """Zero-infrastructure StateStore persisted in DuckDB ``_meta``."""

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
        """Replace a checkpoint in one transaction."""
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


IngestionStateStore = DuckDBStateStore

__all__ = ["DuckDBStateStore", "IngestionStateStore"]
