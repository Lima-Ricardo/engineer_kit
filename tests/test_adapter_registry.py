from pathlib import Path

import duckdb
import pytest

from engineer_kit.adapters.registry import available_adapters
from engineer_kit.config.pipeline_config import (
    ColumnConfig,
    ConnectorConfig,
    DestinationConfig,
    IncrementalConfig,
    PipelineConfig,
    RunLogConfig,
    StateConfig,
    build_pipeline,
)
from engineer_kit.storage.destination import WriteMode


def _config(destination: DestinationConfig) -> PipelineConfig:
    return PipelineConfig(
        name="events",
        connector=ConnectorConfig(
            base_url="https://example.test/events",
            incremental=IncrementalConfig(mode="ingestion_date"),
        ),
        columns=[ColumnConfig("id")],
        destination=destination,
    )


def test_builtin_registry_lists_portable_adapters():
    adapters = available_adapters()
    assert {"duckdb", "parquet", "delta"}.issubset(adapters["destination"])
    assert {"duckdb", "parquet", "delta", "file"}.issubset(adapters["state_store"])
    assert {"duckdb", "parquet", "delta", "file"}.issubset(adapters["run_log"])


def test_duckdb_builder_uses_runtime_connection_and_auto_metadata():
    conn = duckdb.connect()
    pipeline = build_pipeline(_config(DestinationConfig(type="duckdb")), conn)
    assert pipeline._destination.write_mode is WriteMode.APPEND
    assert pipeline._run_log_store is not None


def test_parquet_builder_needs_no_duckdb_runtime(tmp_path):
    config = _config(
        DestinationConfig(type="parquet", path=str(tmp_path / "lake"), schema="bronze")
    )
    pipeline = build_pipeline(config)

    assert pipeline._destination.write_mode is WriteMode.APPEND
    assert type(pipeline._sources[0].connector._incremental._state_store).__name__ == "JsonFileStateStore"
    assert type(pipeline._run_log_store).__name__ == "JsonLinesRunLogStore"


def test_state_and_audit_can_be_configured_independently(tmp_path):
    config = _config(
        DestinationConfig(type="parquet", path=str(tmp_path / "lake"), schema="bronze")
    )
    config.state = StateConfig(type="file", path=str(tmp_path / "state.json"))
    config.run_log = RunLogConfig(
        enabled=True,
        type="file",
        path=str(tmp_path / "audit.jsonl"),
    )

    pipeline = build_pipeline(config)
    assert Path(pipeline._sources[0].connector._incremental._state_store._path) == tmp_path / "state.json"
    assert Path(pipeline._run_log_store._path) == tmp_path / "audit.jsonl"


def test_unknown_adapter_has_clear_error():
    config = _config(DestinationConfig(type="unknown-backend"))
    with pytest.raises(ValueError, match="nao registrado"):
        build_pipeline(config)
