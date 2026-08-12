import duckdb
import pytest

from engineer_kit.storage.duckdb_loader import (
    MAX_BATCH_SIZE,
    MIN_BATCH_SIZE,
    DuckDBLoader,
    InvalidBatchSizeError,
)
from engineer_kit.storage.identifiers import InvalidIdentifierError
from engineer_kit.storage.schema import ColumnSpec, EndpointSchema


@pytest.fixture
def conn():
    return duckdb.connect()


@pytest.fixture
def schema():
    return EndpointSchema.from_names(
        [
            "sha",
            "commit_author_name",
            "commit_author_date",
            "commit_committer_name",
            "commit_committer_date",
            "commit_message",
            "parents",
        ]
    )


def test_load_creates_table_and_returns_row_count(conn, schema):
    loader = DuckDBLoader(conn, schema="bronze")
    records = [
        {
            "sha": "abc123",
            "commit": {
                "author": {"name": "Alice", "date": "2024-01-01T10:00:00Z"},
                "committer": {"name": "Bob", "date": "2024-01-02T10:00:00Z"},
                "message": "first commit",
            },
            "parents": [{"sha": "zzz"}],
        }
    ]

    result = loader.load("github_commits", "commits", schema, records)

    assert result.rows_loaded == 1
    assert result.extra_fields_seen == []
    row = conn.execute(
        "SELECT sha, commit_author_name, commit_author_date, "
        "commit_committer_name, commit_committer_date FROM bronze.commits"
    ).fetchone()
    assert row == ("abc123", "Alice", "2024-01-01T10:00:00Z", "Bob", "2024-01-02T10:00:00Z")


def test_field_outside_schema_is_captured_in_extra_not_dropped(conn, schema):
    loader = DuckDBLoader(conn, schema="bronze")
    records = [{"sha": "abc123", "surprise_field": "nao declarado"}]

    result = loader.load("github_commits", "commits", schema, records)

    assert result.extra_fields_seen == ["surprise_field"]
    extra = conn.execute("SELECT _extra FROM bronze.commits WHERE sha = 'abc123'").fetchone()[0]
    assert "surprise_field" in extra
    assert "nao declarado" in extra


def test_missing_declared_field_becomes_null(conn, schema):
    loader = DuckDBLoader(conn, schema="bronze")
    records = [{"sha": "abc123"}]  # todos os outros campos declarados estao ausentes

    loader.load("github_commits", "commits", schema, records)

    row = conn.execute("SELECT commit_author_name, parents FROM bronze.commits").fetchone()
    assert row == (None, None)


def test_second_load_reuses_table_without_altering_schema(conn, schema):
    loader = DuckDBLoader(conn, schema="bronze")
    loader.load("github_commits", "commits", schema, [{"sha": "first"}])
    loader.load("github_commits", "commits", schema, [{"sha": "second", "new_field": "x"}])

    total = conn.execute("SELECT count(*) FROM bronze.commits").fetchone()[0]
    assert total == 2
    # coluna nova nao foi promovida a coluna real -- so entrou em _extra
    columns = {
        r[0]
        for r in conn.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema='bronze' AND table_name='commits'"
        ).fetchall()
    }
    assert "new_field" not in columns


def test_empty_batch_is_a_noop(conn, schema):
    loader = DuckDBLoader(conn, schema="bronze")
    result = loader.load("github_commits", "commits", schema, [])
    assert result.rows_loaded == 0


def test_invalid_endpoint_name_is_rejected():
    conn_local = duckdb.connect()
    loader = DuckDBLoader(conn_local, schema="bronze")
    with pytest.raises(InvalidIdentifierError):
        loader.load("conn", "commits; DROP TABLE bronze.commits", EndpointSchema.from_names(["sha"]), [{"sha": "x"}])


def test_invalid_schema_name_is_rejected():
    conn_local = duckdb.connect()
    with pytest.raises(InvalidIdentifierError):
        DuckDBLoader(conn_local, schema="bronze; DROP SCHEMA bronze")


def test_invalid_column_dtype_is_rejected():
    with pytest.raises(InvalidIdentifierError):
        ColumnSpec("valor", dtype="VARCHAR); DROP TABLE bronze.commits; --")


def test_batch_size_below_minimum_is_rejected(conn):
    with pytest.raises(InvalidBatchSizeError):
        DuckDBLoader(conn, schema="bronze", batch_size=MIN_BATCH_SIZE - 1)


def test_batch_size_above_maximum_is_rejected(conn):
    with pytest.raises(InvalidBatchSizeError):
        DuckDBLoader(conn, schema="bronze", batch_size=MAX_BATCH_SIZE + 1)


def test_small_batch_size_still_loads_everything_across_multiple_batches(conn, schema):
    loader = DuckDBLoader(conn, schema="bronze", batch_size=MIN_BATCH_SIZE)
    records = [{"sha": f"commit-{i}", "extra_field": f"drift-{i}"} for i in range(MIN_BATCH_SIZE * 3 + 7)]

    result = loader.load("github_commits", "commits", schema, records)

    assert result.rows_loaded == len(records)
    assert result.extra_fields_seen == ["extra_field"]
    total_in_db = conn.execute("SELECT count(*) FROM bronze.commits").fetchone()[0]
    assert total_in_db == len(records)
