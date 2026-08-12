from datetime import date, datetime, timezone

from deltalake import DeltaTable
import pytest

from engineer_kit.adapters.delta import (
    DeltaDestination,
    DeltaRunLogStore,
    DeltaStateStore,
)
from engineer_kit.storage.batching import MIN_BATCH_SIZE
from engineer_kit.storage.identifiers import InvalidIdentifierError
from engineer_kit.storage.run_log import RunLogEntry
from engineer_kit.storage.schema import ColumnSpec, EndpointSchema
from engineer_kit.storage.state_store import Watermark


def test_delta_destination_writes_and_appends_bronze(tmp_path):
    base = tmp_path / "bronze"
    schema = EndpointSchema(
        columns=[ColumnSpec("id", dtype="BIGINT"), ColumnSpec("amount", dtype="DOUBLE")]
    )
    destination = DeltaDestination(base, batch_size=MIN_BATCH_SIZE)

    first = destination.load(
        "orders_api",
        "orders",
        schema,
        [{"id": "1", "amount": "10.5", "new_field": "kept"}],
    )
    second = destination.load(
        "orders_api",
        "orders",
        schema,
        [{"id": "2", "amount": "11.5"}],
    )

    assert first.extra_fields_seen == ["new_field"]
    assert first.rows_loaded == 1
    assert second.rows_loaded == 1

    table = DeltaTable(str(base / "orders")).to_pyarrow_table()
    assert table.num_rows == 2
    assert table.schema.field("id").type == "string" or str(table.schema.field("id").type) == "string"
    assert sorted(table.column("id").to_pylist()) == ["1", "2"]


def test_delta_destination_keeps_failed_stream_out_of_table(tmp_path):
    base = tmp_path / "bronze"
    schema = EndpointSchema.from_names(["id"])
    destination = DeltaDestination(base, batch_size=MIN_BATCH_SIZE)
    destination.load("events_api", "events", schema, [{"id": "seed"}])

    def broken_records():
        for index in range(MIN_BATCH_SIZE):
            yield {"id": str(index)}
        raise RuntimeError("source failed after first batch")

    with pytest.raises(Exception):
        destination.load("events_api", "events", schema, broken_records())

    table = DeltaTable(str(base / "events")).to_pyarrow_table()
    assert table.num_rows == 1
    assert table.column("id").to_pylist() == ["seed"]


def test_delta_destination_rejects_endpoint_path_traversal(tmp_path):
    destination = DeltaDestination(tmp_path / "bronze")
    with pytest.raises(InvalidIdentifierError):
        destination.load(
            "unsafe",
            "../../outside",
            EndpointSchema.from_names(["id"]),
            [{"id": "1"}],
        )


def test_delta_state_store_round_trip_and_connector_isolation(tmp_path):
    store = DeltaStateStore(tmp_path / "lake")
    first = Watermark(
        last_run_at=datetime(2026, 8, 10, 12, tzinfo=timezone.utc),
        last_data_date=date(2026, 8, 9),
        cursor_value=None,
    )
    second = Watermark(
        last_run_at=datetime(2026, 8, 11, 12, tzinfo=timezone.utc),
        last_data_date=date(2026, 8, 10),
        cursor_value="cursor-b",
    )
    updated = Watermark(
        last_run_at=datetime(2026, 8, 12, 12, tzinfo=timezone.utc),
        last_data_date=date(2026, 8, 11),
        cursor_value="cursor-a2",
    )

    assert store.get_watermark("source_a") is None
    store.set_watermark("source_a", first)
    store.set_watermark("source_b", second)
    store.set_watermark("source_a", updated)

    assert store.get_watermark("source_a") == updated
    assert store.get_watermark("source_b") == second
    assert DeltaTable(store.table_uri).count() == 2


def test_delta_run_log_appends_audit_entries(tmp_path):
    store = DeltaRunLogStore(tmp_path / "lake")
    started = datetime(2026, 8, 12, 12, tzinfo=timezone.utc)
    finished = datetime(2026, 8, 12, 12, 1, tzinfo=timezone.utc)

    store.record(
        RunLogEntry(
            connector_name="orders",
            started_at=started,
            finished_at=finished,
            status="success",
            rows_loaded=42,
            extra_fields_seen=["new_field"],
        )
    )
    store.record(
        RunLogEntry(
            connector_name="orders",
            started_at=finished,
            finished_at=finished,
            status="failed",
            rows_loaded=0,
            extra_fields_seen=[],
            error_message="boom",
        )
    )

    table = DeltaTable(store.table_uri).to_pyarrow_table()
    assert table.num_rows == 2
    assert sorted(table.column("status").to_pylist()) == ["failed", "success"]
