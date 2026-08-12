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


class FakeDestinationWithoutConnection:
    """Destination minima que nao expoe `.connection` -- simula um
    destino futuro (ex: Redshift) sem suporte a run_log automatico."""

    def load(self, connector_name, endpoint, schema, records):
        rows = list(records)
        return type("R", (), {"rows_loaded": len(rows), "extra_fields_seen": []})()


@pytest.fixture
def conn():
    return duckdb.connect()


@pytest.fixture
def schema():
    return EndpointSchema.from_names(["id", "name"])


def test_simple_constructor_defaults_run_log_to_true(conn, schema):
    """Caso comum: connector+schema direto, sem PipelineSource nem
    RunLogStore manual -- run_log=True e o padrao."""
    connector = FakeConnector("fake_api", [{"id": "1", "name": "a"}])
    pipeline = Pipeline(connector=connector, schema=schema, destination=DuckDBLoader(conn, schema="bronze"))

    result = pipeline.run()

    assert result.success
    assert connector.watermark_committed is True
    log_row = conn.execute(
        "SELECT connector_name, status, rows_loaded, error_message FROM _meta.run_log"
    ).fetchone()
    assert log_row == ("fake_api", "success", 1, None)


def test_failed_extraction_does_not_commit_watermark_and_records_error(conn, schema):
    connector = FakeConnector("fake_api", [], should_fail=True)
    pipeline = Pipeline(connector=connector, schema=schema, destination=DuckDBLoader(conn, schema="bronze"))

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


def test_run_log_false_disables_run_log_table(conn, schema):
    connector = FakeConnector("fake_api", [{"id": "1", "name": "a"}])
    pipeline = Pipeline(
        connector=connector, schema=schema, destination=DuckDBLoader(conn, schema="bronze"), run_log=False
    )

    result = pipeline.run()

    assert result.success
    tables = conn.execute(
        "SELECT count(*) FROM information_schema.tables WHERE table_schema='_meta' AND table_name='run_log'"
    ).fetchone()[0]
    assert tables == 0


def test_explicit_run_log_store_overrides_default(conn, schema):
    connector = FakeConnector("fake_api", [{"id": "1", "name": "a"}])
    other_conn = duckdb.connect()
    custom_store = RunLogStore(other_conn)
    pipeline = Pipeline(
        connector=connector,
        schema=schema,
        destination=DuckDBLoader(conn, schema="bronze"),
        run_log_store=custom_store,
    )

    pipeline.run()

    assert other_conn.execute("SELECT count(*) FROM _meta.run_log").fetchone()[0] == 1
    # a conexao "principal" nao ganhou a tabela -- foi tudo pro store customizado
    default_tables = conn.execute(
        "SELECT count(*) FROM information_schema.tables WHERE table_schema='_meta' AND table_name='run_log'"
    ).fetchone()[0]
    assert default_tables == 0


def test_run_log_true_without_connection_support_raises(schema):
    connector = FakeConnector("fake_api", [{"id": "1", "name": "a"}])
    with pytest.raises(ValueError):
        Pipeline(connector=connector, schema=schema, destination=FakeDestinationWithoutConnection())


def test_run_log_false_works_with_destination_without_connection(schema):
    connector = FakeConnector("fake_api", [{"id": "1", "name": "a"}])
    pipeline = Pipeline(
        connector=connector,
        schema=schema,
        destination=FakeDestinationWithoutConnection(),
        run_log=False,
    )
    result = pipeline.run()
    assert result.success


def test_multi_source_via_sources_list(conn, schema):
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


def test_passing_both_connector_and_sources_is_rejected(conn, schema):
    connector = FakeConnector("fake_api", [{"id": "1", "name": "a"}])
    with pytest.raises(ValueError):
        Pipeline(
            connector=connector,
            schema=schema,
            sources=[PipelineSource(connector=connector, schema=schema)],
            destination=DuckDBLoader(conn, schema="bronze"),
        )


def test_passing_neither_connector_nor_sources_is_rejected(conn):
    with pytest.raises(ValueError):
        Pipeline(destination=DuckDBLoader(conn, schema="bronze"))
