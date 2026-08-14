"""Delta Lake implementation of incremental ingestion state."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

from engineer_kit.adapters.delta._paths import join_table_uri
from engineer_kit.storage.state_store import StateStore, Watermark, validate_state_key

_STATE_SCHEMA = pa.schema(
    [
        pa.field("connector_name", pa.string()),
        pa.field("last_run_at", pa.timestamp("us", tz="UTC")),
        pa.field("last_data_date", pa.date32()),
        pa.field("cursor_value", pa.string()),
    ]
)


def _predicate_literal(value: str) -> str:
    """Escape a validated data value for delta-rs predicate syntax."""
    return value.replace("'", "''")


class DeltaStateStore(StateStore):
    """Persist one transactional watermark row per connector in Delta.

    The state table lives at ``_meta/ingestion_state`` and is partitioned by
    connector name. Updating a connector uses a predicate overwrite, replacing
    only that connector's tiny partition rather than scanning the Bronze data.
    """

    def __init__(
        self,
        base_uri: str | Path,
        storage_options: dict[str, str] | None = None,
    ) -> None:
        self._table_uri = join_table_uri(base_uri, "_meta", "ingestion_state")
        self._storage_options = dict(storage_options or {})

    @property
    def table_uri(self) -> str:
        return self._table_uri

    def get_watermark(self, connector_name: str) -> Watermark | None:
        connector = validate_state_key(connector_name)
        options = self._storage_options or None
        if not DeltaTable.is_deltatable(self._table_uri, storage_options=options):
            return None

        table = DeltaTable(
            self._table_uri,
            storage_options=options,
        ).to_pyarrow_table(
            filters=[("connector_name", "=", connector)],
            columns=["last_run_at", "last_data_date", "cursor_value"],
        )
        if table.num_rows == 0:
            return None

        rows = table.to_pylist()
        latest = max(rows, key=lambda row: row["last_run_at"])
        return Watermark(
            last_run_at=latest["last_run_at"],
            last_data_date=latest["last_data_date"],
            cursor_value=latest["cursor_value"],
        )

    def set_watermark(self, connector_name: str, watermark: Watermark) -> None:
        connector = validate_state_key(connector_name)
        options = self._storage_options or None
        data = pa.Table.from_pylist(
            [
                {
                    "connector_name": connector,
                    "last_run_at": watermark.last_run_at,
                    "last_data_date": watermark.last_data_date,
                    "cursor_value": watermark.cursor_value,
                }
            ],
            schema=_STATE_SCHEMA,
        )

        if DeltaTable.is_deltatable(self._table_uri, storage_options=options):
            write_deltalake(
                self._table_uri,
                data,
                mode="overwrite",
                predicate=f"connector_name = '{_predicate_literal(connector)}'",
                storage_options=options,
            )
            return

        write_deltalake(
            self._table_uri,
            data,
            mode="error",
            partition_by=["connector_name"],
            storage_options=options,
        )


__all__ = ["DeltaStateStore"]
