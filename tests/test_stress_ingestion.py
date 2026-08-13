import duckdb
import pytest

from engineer_kit.adapters.delta import DeltaDestination
from engineer_kit.adapters.parquet import ParquetDestination
from engineer_kit.storage.duckdb_loader import DuckDBLoader
from engineer_kit.storage.schema import EndpointSchema


def _records(count: int):
    for index in range(count):
        record = {
            "id": str(index),
            "payload": f"value-{index}",
            "nested": {"sequence": index, "active": index % 2 == 0},
        }
        if index % 10_000 == 0:
            record["schema_drift_probe"] = f"drift-{index}"
        yield record


@pytest.mark.stress
def test_duckdb_streams_250k_records_without_materializing_source():
    count = 250_000
    conn = duckdb.connect()
    destination = DuckDBLoader(conn, batch_size=5_000)
    schema = EndpointSchema.from_names(["id", "payload", "nested_sequence", "nested_active"])

    result = destination.load("stress_api", "stress_api", schema, _records(count))

    assert result.rows_loaded == count
    assert result.extra_fields_seen == ["schema_drift_probe"]
    assert conn.execute("SELECT count(*) FROM bronze.stress_api").fetchone()[0] == count


@pytest.mark.stress
def test_parquet_streams_250k_records_into_bounded_row_groups(tmp_path):
    import pyarrow.parquet as pq

    count = 250_000
    destination = ParquetDestination(tmp_path, batch_size=5_000)
    schema = EndpointSchema.from_names(["id", "payload", "nested_sequence", "nested_active"])

    result = destination.load("stress_api", "stress_api", schema, _records(count))

    files = list((tmp_path / "stress_api").glob("*.parquet"))
    assert result.rows_loaded == count
    assert len(files) == 1
    metadata = pq.read_metadata(files[0])
    assert metadata.num_rows == count
    assert metadata.num_row_groups == count // 5_000


@pytest.mark.stress
def test_delta_streams_100k_records_in_one_transaction(tmp_path):
    from deltalake import DeltaTable

    count = 100_000
    destination = DeltaDestination(tmp_path / "lake", batch_size=5_000)
    schema = EndpointSchema.from_names(["id", "payload", "nested_sequence", "nested_active"])

    result = destination.load("stress_api", "stress_api", schema, _records(count))

    table = DeltaTable(str(tmp_path / "lake" / "stress_api"))
    assert result.rows_loaded == count
    assert table.to_pyarrow_table(columns=["id"]).num_rows == count
