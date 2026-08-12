import duckdb
import pytest

from engineer_kit.orchestration.pipeline import Pipeline, PipelineSource
from engineer_kit.storage.duckdb_loader import DuckDBLoader
from engineer_kit.storage.run_log import RunLogStore
from engineer_kit.storage.schema import EndpointSchema


class FakeConnector:
    """Duble minimo: so o que o Pipeline realmente usa de um conector."""

    def __init__(self, name, records, should_fail=False):
        self.name = name
        self._records = records
        self.should_fail = should_fail
        self.watermark_committed = False

    def extract(self, end="today"):
        if self.should_fail:
            raise RuntimeError("falha simulada na extracao")
        yield from self._records

    def commit_watermark(self, max_data_date=None):
        self.watermark_committed = True


@pytest.fixture
def conn():
    return duckdb.connect()


@pytest.fixture
def schema():
    return EndpointSchema.from_names(["id", "name"])


def test_successful_run_commits_watermark_and_records_success(conn, schema):
    connector = FakeConnector("fake_api", [{"id": "1", "name": "a"}])
    run_log = RunLogStore(conn)
    pipeline = Pipeline(
        sources=[PipelineSource(connector=connector, schema=schema)],
        destination=DuckDBLoader(conn, schema="bronze"),
        run_log_store=run_log,
    )

    result = pipeline.run()

    assert result.success
    assert connector.watermark_committed is True
    assert result.steps[0].rows_loaded == 1

    log_row = conn.execute(
        "SELECT connector_name, status, rows_loaded, error_message FROM _meta.run_log"
    ).fetchone()
    assert log_row == ("fake_api", "success", 1, None)


def test_failed_extraction_does_not_commit_watermark_and_records_error(conn, schema):
    connector = FakeConnector("fake_api", [], should_fail=True)
    run_log = RunLogStore(conn)
    pipeline = Pipeline(
        sources=[PipelineSource(connector=connector, schema=schema)],
        destination=DuckDBLoader(conn, schema="bronze"),
        run_log_store=run_log,
    )

    result = pipeline.run()

    assert not result.success
    assert connector.watermark_committed is False
    assert "falha simulada" in result.steps[0].error

    log_row = conn.execute(
        "SELECT connector_name, status, error_message FROM _meta.run_log"
    ).fetchone()
    assert log_row[0] == "fake_api"
    assert log_row[1] == "error"
    assert "falha simulada" in log_row[2]


def test_pipeline_works_without_run_log_store(conn, schema):
    connector = FakeConnector("fake_api", [{"id": "1", "name": "a"}])
    pipeline = Pipeline(
        sources=[PipelineSource(connector=connector, schema=schema)],
        destination=DuckDBLoader(conn, schema="bronze"),
    )

    result = pipeline.run()

    assert result.success
    tables = conn.execute(
        "SELECT count(*) FROM information_schema.tables WHERE table_schema='_meta' AND table_name='run_log'"
    ).fetchone()[0]
    assert tables == 0  # sem RunLogStore, a tabela nem chega a ser criada


def test_one_source_failing_does_not_block_the_others(conn, schema):
    good = FakeConnector("good_api", [{"id": "1", "name": "a"}])
    bad = FakeConnector("bad_api", [], should_fail=True)
    pipeline = Pipeline(
        sources=[
            PipelineSource(connector=bad, schema=schema),
            PipelineSource(connector=good, schema=schema),
        ],
        destination=DuckDBLoader(conn, schema="bronze"),
    )

    result = pipeline.run()

    assert not result.success
    assert result.steps[0].error is not None
    assert result.steps[1].error is None
    assert good.watermark_committed is True
