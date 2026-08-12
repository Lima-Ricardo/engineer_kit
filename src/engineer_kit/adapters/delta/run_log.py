"""Delta Lake implementation of the run-log backend."""

from __future__ import annotations

import json
from pathlib import Path

import pyarrow as pa
from deltalake import write_deltalake

from engineer_kit.adapters.delta._paths import join_table_uri
from engineer_kit.storage.run_log import RunLogBackend, RunLogEntry


_RUN_LOG_SCHEMA = pa.schema(
    [
        pa.field("connector_name", pa.string()),
        pa.field("started_at", pa.timestamp("us", tz="UTC")),
        pa.field("finished_at", pa.timestamp("us", tz="UTC")),
        pa.field("status", pa.string()),
        pa.field("rows_loaded", pa.int64()),
        pa.field("extra_fields_seen", pa.string()),
        pa.field("error_message", pa.string()),
    ]
)


class DeltaRunLogStore(RunLogBackend):
    """Append audit events to ``_meta/run_log`` as a Delta table."""

    def __init__(
        self,
        base_uri: str | Path,
        storage_options: dict[str, str] | None = None,
    ) -> None:
        self._table_uri = join_table_uri(base_uri, "_meta", "run_log")
        self._storage_options = dict(storage_options or {})

    @property
    def table_uri(self) -> str:
        return self._table_uri

    def record(self, entry: RunLogEntry) -> None:
        table = pa.Table.from_pylist(
            [
                {
                    "connector_name": entry.connector_name,
                    "started_at": entry.started_at,
                    "finished_at": entry.finished_at,
                    "status": entry.status,
                    "rows_loaded": entry.rows_loaded,
                    "extra_fields_seen": json.dumps(
                        entry.extra_fields_seen, ensure_ascii=False
                    ),
                    "error_message": entry.error_message,
                }
            ],
            schema=_RUN_LOG_SCHEMA,
        )
        write_deltalake(
            self._table_uri,
            table,
            mode="append",
            storage_options=self._storage_options or None,
        )


__all__ = ["DeltaRunLogStore"]
