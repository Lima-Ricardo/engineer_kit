import duckdb
import pytest

from engineer_kit.config.pipeline_config import (
    AuthConfig,
    ColumnConfig,
    ConnectorConfig,
    DestinationConfig,
    IncrementalConfig,
    PaginationConfig,
    PipelineConfig,
    PipelineConfigError,
    RunLogConfig,
    StateConfig,
    build_pipeline,
    list_pipeline_configs,
    load_pipeline_config,
    pipeline_config_from_dict,
    pipeline_config_to_dict,
    save_pipeline_config,
)
from engineer_kit.connectors.rest import RestConnector


def _minimal_config(name="fake_api") -> PipelineConfig:
    return PipelineConfig(
        name=name,
        connector=ConnectorConfig(
            base_url="https://example.test/items",
            method="GET",
            pagination=PaginationConfig(type="page", params={"page_size": 50}),
            incremental=IncrementalConfig(mode="ingestion_date"),
        ),
        columns=[ColumnConfig(name="id"), ColumnConfig(name="value", dtype="integer")],
        destination=DestinationConfig(schema="bronze", batch_size=500),
    )


def test_round_trip_dict_preserves_everything():
    config = _minimal_config()
    restored = pipeline_config_from_dict(pipeline_config_to_dict(config))
    assert restored == config


def test_round_trip_yaml_file(tmp_path):
    config = _minimal_config()
    config.state = StateConfig(type="file", path=str(tmp_path / "state.json"))
    config.run_log = RunLogConfig(
        enabled=True,
        type="file",
        path=str(tmp_path / "audit.jsonl"),
    )
    path = tmp_path / "fake_api.yaml"
    save_pipeline_config(config, path)
    restored = load_pipeline_config(path)
    assert restored == config


def test_old_boolean_run_log_yaml_remains_compatible():
    config = pipeline_config_from_dict(
        {
            "name": "old",
            "connector": {
                "base_url": "https://example.test/items",
                "incremental": {"mode": "ingestion_date"},
            },
            "run_log": False,
        }
    )
    assert config.run_log.enabled is False


def test_build_pipeline_produces_working_restconnector():
    config = _minimal_config()
    conn = duckdb.connect()
    pipeline = build_pipeline(config, conn)
    assert isinstance(pipeline._sources[0].connector, RestConnector)
    assert pipeline._sources[0].connector.name == "fake_api"


def test_parquet_pipeline_builds_without_runtime_connection(tmp_path):
    config = _minimal_config()
    config.destination = DestinationConfig(
        type="parquet",
        path=str(tmp_path / "lake"),
        schema="bronze",
    )
    pipeline = build_pipeline(config)
    assert type(pipeline._destination).__name__ == "ParquetDestination"
    assert type(pipeline._run_log_store).__name__ == "JsonLinesRunLogStore"


def test_delta_pipeline_builds_without_runtime_connection(tmp_path):
    config = _minimal_config()
    config.destination = DestinationConfig(
        type="delta",
        path=str(tmp_path / "lake"),
        schema="bronze",
    )
    pipeline = build_pipeline(config)
    assert type(pipeline._destination).__name__ == "DeltaDestination"
    assert type(pipeline._run_log_store).__name__ == "DeltaRunLogStore"


def test_missing_base_url_raises_clear_error():
    with pytest.raises(PipelineConfigError):
        pipeline_config_from_dict({"name": "x", "connector": {}})


def test_unknown_pagination_type_raises():
    config = _minimal_config()
    config.connector.pagination = PaginationConfig(type="nao_existe")
    with pytest.raises(PipelineConfigError):
        build_pipeline(config, duckdb.connect())


def test_bearer_auth_without_secret_key_raises():
    config = _minimal_config()
    config.connector.auth = AuthConfig(type="bearer")
    with pytest.raises(PipelineConfigError):
        build_pipeline(config, duckdb.connect())


def test_data_date_mode_without_date_field_raises():
    config = _minimal_config()
    config.connector.incremental.mode = "data_date"
    config.connector.incremental.date_field = None
    with pytest.raises(PipelineConfigError):
        build_pipeline(config, duckdb.connect())


def test_unsupported_destination_type_raises():
    config = _minimal_config()
    config.destination.type = "redshift"
    with pytest.raises(PipelineConfigError, match="nao registrado"):
        build_pipeline(config)


def test_list_pipeline_configs_skips_invalid_and_keeps_valid(tmp_path):
    save_pipeline_config(_minimal_config("good_one"), tmp_path / "good_one.yaml")
    (tmp_path / "broken.yaml").write_text("name: sem_connector\n", encoding="utf-8")

    results = list_pipeline_configs(tmp_path)
    assert [config.name for _, config in results] == ["good_one"]


def test_list_pipeline_configs_on_missing_directory_returns_empty(tmp_path):
    assert list_pipeline_configs(tmp_path / "nao_existe") == []
