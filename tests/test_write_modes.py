import duckdb
from deltalake import DeltaTable
import pyarrow.parquet as pq

from engineer_kit.adapters.delta import DeltaDestination
from engineer_kit.adapters.parquet import ParquetDestination
from engineer_kit.storage.batching import MIN_BATCH_SIZE
from engineer_kit.storage.duckdb_loader import DuckDBDestination
from engineer_kit.storage.schema import EndpointSchema


SCHEMA = EndpointSchema.from_names(["id"])


def test_duckdb_overwrite_replaces_previous_bronze_rows():
    conn = duckdb.connect()
    destination = DuckDBDestination(conn, write_mode="overwrite")
    destination.load("events", "events", SCHEMA, [{"id": "first"}])
    destination.load("events", "events", SCHEMA, [{"id": "second"}])

    assert conn.execute("select id from bronze.events").fetchall() == [("second",)]


def test_parquet_overwrite_replaces_previous_dataset(tmp_path):
    destination = ParquetDestination(
        tmp_path / "bronze",
        batch_size=MIN_BATCH_SIZE,
        write_mode="overwrite",
    )
    destination.load("events", "events", SCHEMA, [{"id": "first"}])
    destination.load("events", "events", SCHEMA, [{"id": "second"}])

    table = pq.read_table(tmp_path / "bronze" / "events")
    assert table.column("id").to_pylist() == ["second"]


def test_delta_overwrite_replaces_previous_table(tmp_path):
    destination = DeltaDestination(
        tmp_path / "bronze",
        batch_size=MIN_BATCH_SIZE,
        write_mode="overwrite",
    )
    destination.load("events", "events", SCHEMA, [{"id": "first"}])
    destination.load("events", "events", SCHEMA, [{"id": "second"}])

    table = DeltaTable(str(tmp_path / "bronze" / "events")).to_pyarrow_table()
    assert table.column("id").to_pylist() == ["second"]
