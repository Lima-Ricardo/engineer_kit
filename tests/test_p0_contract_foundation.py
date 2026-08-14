from __future__ import annotations

from datetime import date
from types import SimpleNamespace

import duckdb
import pytest

from engineer_kit import (
    ColumnSpec,
    EndpointSchema,
    FlattenCollisionError,
    Pipeline,
    RestConnector,
    StateConflictError,
    capability_manifest,
)
from engineer_kit.adapters.delta.state_store import DeltaStateStore
from engineer_kit.adapters.duckdb.state_store import DuckDBStateStore
from engineer_kit.adapters.files.state_store import JsonFileStateStore
from engineer_kit.config.pipeline_config import (
    CURRENT_PIPELINE_CONFIG_VERSION,
    PipelineConfigError,
    build_pipeline,
    load_pipeline_config,
    pipeline_config_from_dict,
)
from engineer_kit.connectors.incremental import IncrementalStrategy
from engineer_kit.connectors.intent import read_path
from engineer_kit.orchestration.pipeline import _checkpoint_identity
from engineer_kit.storage.flatten import flatten_record


class _Response:
    headers = {"Content-Type": "application/json"}
    status_code = 200
    content = b'{"data":{"orders":[]}}'

    def json(self):
        return {
            "data": {
                "orders": [
                    {"id": 1, "customer": {"id": 7}, "items": [{"sku": "A"}]},
                    {"id": 2, "customer": {"id": 8}, "items": [{"sku": "B"}]},
                ]
            },
            "meta": {"next_cursor": "abc"},
        }


class _Client:
    def __init__(self):
        self.calls = 0
        self.kwargs = []

    def request(self, method, **kwargs):
        self.calls += 1
        self.kwargs.append((method, kwargs))
        return _Response()


class _RecordingDestination:
    def __init__(self):
        self.contexts = []

    def load_with_context(self, connector_name, endpoint, schema, records, context):
        materialized = list(records)
        self.contexts.append(context)
        return SimpleNamespace(
            table="memory",
            rows_loaded=len(materialized),
            extra_fields_seen=[],
        )


def test_declarative_short_form_matches_python_intent_surface():
    config = pipeline_config_from_dict(
        {
            "version": 1,
            "name": "orders",
            "connector": {
                "base_url": "https://example.test/orders",
                "pagination": "cursor",
                "incremental": False,
                "records": "data.orders",
                "select": {"customer.id": "customer_id", "id": "order_id"},
                "params": {"status": "open"},
            },
        }
    )

    assert config.version == CURRENT_PIPELINE_CONFIG_VERSION
    assert config.connector.pagination.type == "cursor"
    assert config.connector.incremental.enabled is False
    assert config.connector.resolved_records() == "data.orders"
    assert config.connector.resolved_params() == {"status": "open"}

    conn = duckdb.connect()
    pipeline = build_pipeline(config, conn)
    connector = pipeline._sources[0].connector

    assert connector.resolved_records_path == "data.orders"
    assert connector.selected_fields == ("customer_id", "order_id")
    assert connector.explain()["incremental"] == "NoIncrementalStrategy"
    tables = {
        row[0]
        for row in conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = '_meta'"
        ).fetchall()
    }
    assert "ingestion_state" not in tables


def test_legacy_records_path_and_static_params_remain_supported():
    config = pipeline_config_from_dict(
        {
            "name": "legacy",
            "connector": {
                "base_url": "https://example.test/items",
                "records_path": "results",
                "static_params": {"limit": 10},
                "incremental": {"mode": "ingestion_date"},
            },
        }
    )
    assert config.connector.resolved_records() == "results"
    assert config.connector.resolved_params() == {"limit": 10}
    assert config.connector.incremental.enabled is True


def test_unknown_config_key_is_rejected_instead_of_ignored():
    with pytest.raises(PipelineConfigError, match="desconhecido"):
        pipeline_config_from_dict(
            {
                "name": "x",
                "connector": {
                    "base_url": "https://example.test/items",
                    "pagniation": "cursor",
                },
            }
        )


def test_unsupported_config_version_is_rejected():
    with pytest.raises(PipelineConfigError, match="version"):
        pipeline_config_from_dict(
            {
                "version": 999,
                "name": "x",
                "connector": {"base_url": "https://example.test/items"},
            }
        )


def test_duplicate_yaml_keys_are_rejected(tmp_path):
    path = tmp_path / "duplicate.yaml"
    path.write_text(
        "name: first\nname: second\nconnector:\n  base_url: https://example.test/items\n",
        encoding="utf-8",
    )
    with pytest.raises(PipelineConfigError, match="duplicate key"):
        load_pipeline_config(path)


def test_read_path_supports_array_indexes_and_quoted_keys():
    value = {"payload": {"odd.key": {"items": [{"id": 42}]}}}
    assert read_path(value, 'payload["odd.key"].items[0].id') == 42


def test_select_aliases_are_deterministic_and_collisions_fail_fast():
    connector = RestConnector(
        base_url="https://example.test/orders",
        pagination=False,
        select={"customer.id": "customer_id", "id": "order_id"},
    )
    assert connector.selected_fields == ("customer_id", "order_id")
    assert connector.selected_paths == ("customer.id", "id")

    with pytest.raises(ValueError, match="colisao"):
        RestConnector(
            base_url="https://example.test/orders",
            pagination=False,
            select=["a.b", "a_b"],
        )


def test_flatten_collision_is_rejected_instead_of_overwriting_data():
    with pytest.raises(FlattenCollisionError, match="coluna Bronze 'a_b'"):
        flatten_record({"a": {"b": 1}, "a_b": 2})


def test_reserved_bronze_columns_cannot_be_declared_by_source_schema():
    with pytest.raises(ValueError, match="reservado"):
        ColumnSpec("_raw")


def test_probe_fetches_one_page_without_advancing_checkpoint(tmp_path):
    state_path = tmp_path / "state.json"
    state = JsonFileStateStore(state_path)
    client = _Client()
    connector = RestConnector(
        base_url="https://example.test/orders",
        incremental=True,
        state_store=state,
        select={"customer.id": "customer_id", "items[0].sku": "first_sku"},
        http_client=client,
    )

    result = connector.probe(limit=1, end=date(2026, 1, 1))

    assert client.calls == 1
    assert result.records == [{"customer_id": "7", "first_sku": "A"}]
    assert result.records_path == "data.orders"
    assert result.pagination.endswith("cursor")
    assert result.status_code == 200
    assert result.response_bytes == len(_Response.content)
    assert state.get_watermark("orders") is None
    assert not state_path.exists()


def _assert_stale_writer_is_rejected(first_store, second_store, *, state_key="orders"):
    first = IncrementalStrategy(state_key, first_store, initial_start=date(2024, 1, 1))
    second = IncrementalStrategy(state_key, second_store, initial_start=date(2024, 1, 1))

    # Both runs resolve from the same checkpoint before either one commits,
    # modelling the dangerous concurrent-run interleaving deterministically.
    first_window = first.resolve_window(end=date(2024, 2, 1))
    second_window = second.resolve_window(end=date(2024, 2, 1))
    first.commit(first_window, max_data_date=date(2024, 1, 20))

    with pytest.raises(StateConflictError):
        second.commit(second_window, max_data_date=date(2024, 1, 25))

    assert second_store.get_watermark(state_key).last_data_date == date(2024, 1, 20)


def test_duckdb_checkpoint_compare_and_set_rejects_stale_independent_writer(tmp_path):
    database = str(tmp_path / "state.duckdb")
    first_conn = duckdb.connect(database)
    second_conn = duckdb.connect(database)
    try:
        _assert_stale_writer_is_rejected(
            DuckDBStateStore(first_conn),
            DuckDBStateStore(second_conn),
        )
    finally:
        second_conn.close()
        first_conn.close()


def test_json_checkpoint_compare_and_set_rejects_stale_independent_writer(tmp_path):
    path = tmp_path / "state.json"
    first_store = JsonFileStateStore(path)
    second_store = JsonFileStateStore(path)
    _assert_stale_writer_is_rejected(first_store, second_store)
    if first_store.supports_atomic_compare_and_set:
        assert (tmp_path / ".state.json.lock").exists()


def test_delta_state_namespace_accepts_data_keys_and_rejects_stale_writer(tmp_path):
    first_store = DeltaStateStore(tmp_path / "delta-state")
    second_store = DeltaStateStore(tmp_path / "delta-state")
    state_key = "tenant-a.orders"

    _assert_stale_writer_is_rejected(first_store, second_store, state_key=state_key)
    assert first_store.get_watermark(state_key).last_data_date == date(2024, 1, 20)


def test_state_key_can_namespace_same_logical_connector(tmp_path):
    state = JsonFileStateStore(tmp_path / "state.json")
    first = RestConnector(
        name="orders",
        base_url="https://example.test/orders",
        pagination=False,
        incremental=True,
        state_store=state,
        state_key="tenant_a.orders",
    )
    second = RestConnector(
        name="orders",
        base_url="https://example.test/orders",
        pagination=False,
        incremental=True,
        state_store=state,
        state_key="tenant_b.orders",
    )
    assert first.state_key == "tenant_a.orders"
    assert second.state_key == "tenant_b.orders"


def test_declarative_state_key_reaches_runtime_incremental_strategy():
    config = pipeline_config_from_dict(
        {
            "name": "orders",
            "connector": {
                "base_url": "https://example.test/orders",
                "incremental": {"mode": "ingestion_date"},
                "state_key": "tenant-a.orders",
            },
        }
    )
    connector = build_pipeline(config, duckdb.connect())._sources[0].connector

    assert connector.state_key == "tenant-a.orders"
    assert connector._incremental.state_key == "tenant-a.orders"


def test_state_key_rejects_control_characters_before_backend_access():
    with pytest.raises(ValueError, match="controle"):
        RestConnector(
            name="orders",
            base_url="https://example.test/orders",
            pagination=False,
            incremental=False,
            state_key="tenant\norders",
        )


def test_state_key_participates_in_deterministic_ingestion_identity(tmp_path):
    state = JsonFileStateStore(tmp_path / "identity-state.json")
    first = RestConnector(
        name="orders",
        base_url="https://example.test/orders",
        pagination=False,
        incremental=True,
        state_store=state,
        state_key="tenant_a.orders",
    )
    second = RestConnector(
        name="orders",
        base_url="https://example.test/orders",
        pagination=False,
        incremental=True,
        state_store=state,
        state_key="tenant_b.orders",
    )

    first_window = first.extract_incremental(end=date(2026, 1, 1)).window
    second_window = second.extract_incremental(end=date(2026, 1, 1)).window

    assert _checkpoint_identity(first, first_window) != _checkpoint_identity(second, second_window)


def test_non_incremental_managed_runs_get_distinct_run_scoped_ingestion_keys():
    client = _Client()
    connector = RestConnector(
        name="orders",
        base_url="https://example.test/orders",
        pagination=False,
        incremental=False,
        http_client=client,
    )
    destination = _RecordingDestination()
    pipeline = Pipeline(
        connector=connector,
        schema=EndpointSchema.from_names(["id"]),
        destination=destination,
        run_log=False,
    )

    first = pipeline.run(run_id="run-a")
    second = pipeline.run(run_id="run-b")

    assert first.success and second.success
    assert connector.checkpoint_enabled is False
    assert len(destination.contexts) == 2
    assert destination.contexts[0].ingestion_key != destination.contexts[1].ingestion_key


def test_capability_manifest_is_serializable_and_adapter_aware():
    manifest = capability_manifest()
    assert manifest["config_version"] == CURRENT_PIPELINE_CONFIG_VERSION
    assert manifest["connector"]["preview"] is True
    assert {"duckdb", "parquet", "delta"}.issubset(manifest["destinations"])
    assert {"duckdb", "file", "delta"}.issubset(manifest["state_stores"])
