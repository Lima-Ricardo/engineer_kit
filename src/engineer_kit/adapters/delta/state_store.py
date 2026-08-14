"""Delta Lake implementation of incremental ingestion state."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

from engineer_kit.adapters.delta._paths import join_table_uri
from engineer_kit.storage.state_store import (
    StateConflictError,
    StateStore,
    Watermark,
    validate_state_key,
)

_STATE_SCHEMA = pa.schema(
    [
        pa.field("connector_name", pa.string()),
        pa.field("last_run_at", pa.timestamp("us", tz="UTC")),
        pa.field("last_data_date", pa.date32()),
        pa.field("cursor_value", pa.string()),
    ]
)


def _predicate_literal(value: str) -> str:
    return value.replace("'", "''")


def _watermark_row(connector: str, watermark: Watermark) -> dict[str, object]:
    return {
        "connector_name": connector,
        "last_run_at": watermark.last_run_at,
        "last_data_date": watermark.last_data_date,
        "cursor_value": watermark.cursor_value,
    }


class DeltaStateStore(StateStore):
    """Persist one transactional watermark row per connector in Delta.

    ``compare_and_set_watermark`` uses Delta MERGE as the atomic compare/write
    boundary. The merge predicate matches the connector, while the matched
    update is conditional on the complete expected watermark. If another writer
    advances the row first, the merge updates zero rows and the stale writer is
    rejected with :class:`StateConflictError`.
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

    @property
    def supports_atomic_compare_and_set(self) -> bool:
        return True

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
        if table.num_rows > 1:
            raise StateConflictError(
                f"State Delta de '{connector}' contem {table.num_rows} linhas; "
                "a unicidade do checkpoint foi violada."
            )

        row = table.to_pylist()[0]
        return Watermark(
            last_run_at=row["last_run_at"],
            last_data_date=row["last_data_date"],
            cursor_value=row["cursor_value"],
        )

    def set_watermark(self, connector_name: str, watermark: Watermark) -> None:
        """Compatibility unconditional set implemented as CAS with one refresh retry."""
        connector = validate_state_key(connector_name)
        expected = self.get_watermark(connector)
        try:
            self.compare_and_set_watermark(connector, expected, watermark)
        except StateConflictError:
            # ``set`` historically means replace current state. Refresh once so
            # callers that intentionally use the non-CAS API retain that meaning.
            expected = self.get_watermark(connector)
            self.compare_and_set_watermark(connector, expected, watermark)

    def compare_and_set_watermark(
        self,
        connector_name: str,
        expected: Watermark | None,
        watermark: Watermark,
    ) -> None:
        connector = validate_state_key(connector_name)
        options = self._storage_options or None

        if not DeltaTable.is_deltatable(self._table_uri, storage_options=options):
            if expected is not None:
                raise StateConflictError(
                    f"Checkpoint de '{connector}' desapareceu antes do commit."
                )
            data = pa.Table.from_pylist(
                [_watermark_row(connector, watermark)],
                schema=_STATE_SCHEMA,
            )
            try:
                write_deltalake(
                    self._table_uri,
                    data,
                    mode="error",
                    partition_by=["connector_name"],
                    storage_options=options,
                )
                return
            except Exception as exc:
                current = self.get_watermark(connector)
                if current is not None:
                    raise StateConflictError(
                        f"Checkpoint de '{connector}' foi criado concorrentemente; "
                        "o commit stale foi recusado."
                    ) from exc
                raise

        dt = DeltaTable(self._table_uri, storage_options=options)
        source_schema = pa.schema(
            [
                *_STATE_SCHEMA,
                pa.field("expected_last_run_at", pa.timestamp("us", tz="UTC")),
                pa.field("expected_last_data_date", pa.date32()),
                pa.field("expected_cursor_value", pa.string()),
                pa.field("expect_missing", pa.bool_()),
            ]
        )
        source = pa.Table.from_pylist(
            [
                {
                    **_watermark_row(connector, watermark),
                    "expected_last_run_at": expected.last_run_at if expected else None,
                    "expected_last_data_date": expected.last_data_date if expected else None,
                    "expected_cursor_value": expected.cursor_value if expected else None,
                    "expect_missing": expected is None,
                }
            ],
            schema=source_schema,
        )

        merger = dt.merge(
            source=source,
            predicate="target.connector_name = source.connector_name",
            source_alias="source",
            target_alias="target",
        )
        if expected is None:
            merger = merger.when_not_matched_insert(
                updates={
                    "connector_name": "source.connector_name",
                    "last_run_at": "source.last_run_at",
                    "last_data_date": "source.last_data_date",
                    "cursor_value": "source.cursor_value",
                }
            )
        else:
            expected_predicate = (
                "target.last_run_at = source.expected_last_run_at AND "
                "(target.last_data_date = source.expected_last_data_date OR "
                "(target.last_data_date IS NULL AND source.expected_last_data_date IS NULL)) AND "
                "(target.cursor_value = source.expected_cursor_value OR "
                "(target.cursor_value IS NULL AND source.expected_cursor_value IS NULL))"
            )
            merger = merger.when_matched_update(
                predicate=expected_predicate,
                updates={
                    "last_run_at": "source.last_run_at",
                    "last_data_date": "source.last_data_date",
                    "cursor_value": "source.cursor_value",
                },
            )

        try:
            metrics = merger.execute()
        except Exception as exc:
            current = self.get_watermark(connector)
            if current != expected:
                raise StateConflictError(
                    f"Checkpoint de '{connector}' mudou durante a transacao Delta; "
                    "o commit concorrente foi recusado."
                ) from exc
            raise

        changed = int(metrics.get("num_target_rows_updated", 0)) + int(
            metrics.get("num_target_rows_inserted", 0)
        )
        if changed != 1:
            raise StateConflictError(
                f"Checkpoint de '{connector}' nao correspondia ao estado esperado; "
                "o commit concorrente foi recusado."
            )


__all__ = ["DeltaStateStore"]
