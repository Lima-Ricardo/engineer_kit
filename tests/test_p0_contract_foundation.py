from __future__ import annotations

from datetime import date

import duckdb
import pytest

from engineer_kit import (
    ColumnSpec,
    FlattenCollisionError,
    RestConnector,
    StateConflictError,
    capability_manifest,
)
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


def _assert_stale_writer_is_rejected(store):
    first = IncrementalStrategy("orders", store, initial_start=date(2024, 1, 1))
    second = IncrementalStrategy("orders", store, initial_start=date(2024, 1, 1))

    first_window = first.resolve_window(end=date(2024, 2, 1))
    second_window = second.resolve_window(end=date(2024, 2, 1))
    first.commit(first_window, max_data_date=date(2024, 1, 20))

    with pytest.raises(StateConflictError):
        second.commit(second_window, max_data_date=date(2024, 1, 25))

    assert store.get_watermark("orders").last_data_date == date(2024, 1, 20)


def test_duckdb_checkpoint_compare_and_set_rejects_stale_concurrent_writer():
    _assert_stale_writer_is_rejected(DuckDBStateStore(duckdb.connect()))


def test_json_checkpoint_compare_and_set_rejects_stale_concurrent_writer(tmp_path):
    store = JsonFileStateStore(tmp_path / "state.json")
    _assert_stale_writer_is_rejected(store)
    if store.supports_atomic_compare_and_set:
        assert (tmp_path / ".state.json.lock").exists()


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


def test_capability_manifest_is_serializable_and_adapter_aware():
    manifest = capability_manifest()
    assert manifest["config_version"] == CURRENT_PIPELINE_CONFIG_VERSION
    assert manifest["connector"]["preview"] is True
    assert {"duckdb", "parquet", "delta"}.issubset(manifest["destinations"])
    assert {"duckdb", "file", "delta"}.issubset(manifest["state_stores"])
