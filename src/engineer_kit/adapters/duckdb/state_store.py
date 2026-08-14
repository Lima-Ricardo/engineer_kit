"""DuckDB implementation of the incremental StateStore contract."""

from __future__ import annotations

import duckdb

from engineer_kit.storage.state_store import (
    StateConflictError,
    StateStore,
    Watermark,
    validate_state_key,
)


class DuckDBStateStore(StateStore):
    """Zero-infrastructure StateStore persisted in DuckDB ``_meta``."""

    _SCHEMA = "_meta"
    _TABLE = "ingestion_state"

    def __init__(self, conn: duckdb.DuckDBPyConnection) -> None:
        self._conn = conn
        self._ensure_table()

    @property
    def supports_atomic_compare_and_set(self) -> bool:
        return True

    def _ensure_table(self) -> None:
        # This table is library-owned; keeping its identifiers static avoids an
        # unnecessary dynamic-SQL surface while values remain parameterized.
        self._conn.execute("CREATE SCHEMA IF NOT EXISTS _meta")
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS _meta.ingestion_state (
                connector_name VARCHAR PRIMARY KEY,
                last_run_at TIMESTAMP,
                last_data_date DATE,
                cursor_value VARCHAR
            )
            """
        )

    def _get_watermark(self, connector_name: str) -> Watermark | None:
        row = self._conn.execute(
            "SELECT last_run_at, last_data_date, cursor_value "
            "FROM _meta.ingestion_state WHERE connector_name = ?",
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

    def get_watermark(self, connector_name: str) -> Watermark | None:
        return self._get_watermark(validate_state_key(connector_name))

    def _replace_watermark(self, connector_name: str, watermark: Watermark) -> None:
        self._conn.execute(
            "DELETE FROM _meta.ingestion_state WHERE connector_name = ?",
            [connector_name],
        )
        self._conn.execute(
            "INSERT INTO _meta.ingestion_state VALUES (?, ?, ?, ?)",
            [
                connector_name,
                watermark.last_run_at,
                watermark.last_data_date,
                watermark.cursor_value,
            ],
        )

    def set_watermark(self, connector_name: str, watermark: Watermark) -> None:
        """Replace a checkpoint in one transaction."""
        key = validate_state_key(connector_name)
        self._conn.execute("BEGIN TRANSACTION")
        try:
            self._replace_watermark(key, watermark)
            self._conn.execute("COMMIT")
        except Exception:
            self._conn.execute("ROLLBACK")
            raise

    def compare_and_set_watermark(
        self,
        connector_name: str,
        expected: Watermark | None,
        watermark: Watermark,
    ) -> None:
        """Atomically reject a stale writer before replacing the checkpoint."""
        key = validate_state_key(connector_name)
        self._conn.execute("BEGIN TRANSACTION")
        try:
            current = self._get_watermark(key)
            if current != expected:
                raise StateConflictError(
                    f"Checkpoint de '{key}' mudou durante a execucao; "
                    "o commit concorrente foi recusado."
                )
            self._replace_watermark(key, watermark)
            self._conn.execute("COMMIT")
        except Exception:
            self._conn.execute("ROLLBACK")
            raise


IngestionStateStore = DuckDBStateStore

__all__ = ["DuckDBStateStore", "IngestionStateStore"]
