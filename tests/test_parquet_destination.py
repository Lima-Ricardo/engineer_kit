import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from engineer_kit.adapters.parquet import ParquetDestination
from engineer_kit.storage.batching import MIN_BATCH_SIZE
from engineer_kit.storage.destination import LoadContext
from engineer_kit.storage.identifiers import InvalidIdentifierError
from engineer_kit.storage.schema import ColumnSpec, EndpointSchema


def test_parquet_destination_writes_stable_bronze_dataset(tmp_path):
    schema = EndpointSchema(
        columns=[
            ColumnSpec("id", dtype="BIGINT"),
            ColumnSpec("amount", dtype="DECIMAL(18, 2)"),
        ]
    )
    destination = ParquetDestination(tmp_path, batch_size=MIN_BATCH_SIZE)

    result = destination.load(
        "orders_api",
        "orders",
        schema,
        [
            {"id": "1", "amount": "10.50", "new_field": "preserve-me"},
            {"id": "2"},
        ],
    )

    assert result.rows_loaded == 2
    assert result.extra_fields_seen == ["new_field"]

    table = pq.read_table(tmp_path / "orders")
    assert table.schema.field("id").type == pa.string()
    assert table.schema.field("amount").type == pa.string()
    assert table.column("amount").to_pylist() == ["10.50", None]

    extra = table.column("_extra").to_pylist()[0]
    assert json.loads(extra) == {"new_field": "preserve-me"}
    assert table.column("_source").to_pylist() == ["orders_api", "orders_api"]


def test_parquet_destination_streams_batches_as_row_groups(tmp_path):
    schema = EndpointSchema.from_names(["id"])
    destination = ParquetDestination(tmp_path, batch_size=MIN_BATCH_SIZE)
    records = ({"id": str(index)} for index in range(MIN_BATCH_SIZE * 2 + 7))

    result = destination.load("events_api", "events", schema, records)

    files = sorted((tmp_path / "events").glob("*.parquet"))
    assert len(files) == 1
    metadata = pq.read_metadata(files[0])
    assert metadata.num_row_groups == 3
    assert result.rows_loaded == MIN_BATCH_SIZE * 2 + 7
    assert pq.read_table(tmp_path / "events").num_rows == result.rows_loaded


def test_parquet_destination_appends_new_run_instead_of_overwriting(tmp_path):
    schema = EndpointSchema.from_names(["id"])
    destination = ParquetDestination(tmp_path, batch_size=MIN_BATCH_SIZE)

    destination.load("events_api", "events", schema, [{"id": "first"}])
    destination.load("events_api", "events", schema, [{"id": "second"}])

    table = pq.read_table(tmp_path / "events")
    assert sorted(table.column("id").to_pylist()) == ["first", "second"]
    assert len(list((tmp_path / "events").glob("*.parquet"))) == 2


def test_failed_parquet_run_never_becomes_visible(tmp_path):
    schema = EndpointSchema.from_names(["id"])
    destination = ParquetDestination(tmp_path, batch_size=MIN_BATCH_SIZE)

    def broken_records():
        for index in range(MIN_BATCH_SIZE):
            yield {"id": str(index)}
        raise RuntimeError("source failed after first batch")

    with pytest.raises(RuntimeError, match="source failed"):
        destination.load("events_api", "events", schema, broken_records())

    assert list((tmp_path / "events").glob("*.parquet")) == []
    assert list((tmp_path / ".engineer_kit_staging" / "events").glob("*.tmp")) == []


def test_parquet_destination_rejects_endpoint_path_traversal(tmp_path):
    destination = ParquetDestination(tmp_path)
    with pytest.raises(InvalidIdentifierError):
        destination.load(
            "unsafe",
            "../../outside",
            EndpointSchema.from_names(["id"]),
            [{"id": "1"}],
        )


def test_parquet_staging_never_uses_raw_operator_run_id_as_path(tmp_path):
    destination = ParquetDestination(tmp_path)
    context = LoadContext.adhoc("events_api", run_id="../../escape")

    result = destination.load_with_context(
        "events_api",
        "events",
        EndpointSchema.from_names(["id"]),
        [{"id": "1"}],
        context,
    )

    assert result.rows_loaded == 1
    assert len(list((tmp_path / "events").glob("*.parquet"))) == 1
    assert not (tmp_path.parent / "escape.parquet.tmp").exists()
    assert not (tmp_path.parent / "escape").exists()


def test_empty_parquet_load_is_a_noop(tmp_path):
    destination = ParquetDestination(tmp_path)
    result = destination.load("empty_api", "empty", EndpointSchema.from_names(["id"]), [])

    assert result.rows_loaded == 0
    assert list((tmp_path / "empty").glob("*.parquet")) == []
