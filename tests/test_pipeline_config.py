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
            # ingestion_date nao exige date_field -- mantem o fixture minimo
            # de verdade; o teste de DATA_DATE configura o proprio caso.
            incremental=IncrementalConfig(mode="ingestion_date"),
        ),
        columns=[ColumnConfig(name="id"), ColumnConfig(name="value", dtype="INTEGER")],
        destination=DestinationConfig(schema="bronze", batch_size=500),
    )


def test_round_trip_dict_preserves_everything():
    config = _minimal_config()
    restored = pipeline_config_from_dict(pipeline_config_to_dict(config))
    assert restored == config


def test_round_trip_yaml_file(tmp_path):
    config = _minimal_config()
    path = tmp_path / "fake_api.yaml"
    save_pipeline_config(config, path)
    restored = load_pipeline_config(path)
    assert restored == config


def test_build_pipeline_produces_working_restconnector():
    config = _minimal_config()
    conn = duckdb.connect()
    pipeline = build_pipeline(config, conn)
    assert isinstance(pipeline._sources[0].connector, RestConnector)
    assert pipeline._sources[0].connector.name == "fake_api"


def test_missing_base_url_raises_clear_error():
    with pytest.raises(PipelineConfigError):
        pipeline_config_from_dict({"name": "x", "connector": {}})


def test_unknown_pagination_type_raises():
    config = _minimal_config()
    config.connector.pagination = PaginationConfig(type="nao_existe")
    conn = duckdb.connect()
    with pytest.raises(PipelineConfigError):
        build_pipeline(config, conn)


def test_bearer_auth_without_secret_key_raises():
    config = _minimal_config()
    config.connector.auth = AuthConfig(type="bearer")  # sem secret_key
    conn = duckdb.connect()
    with pytest.raises(PipelineConfigError):
        build_pipeline(config, conn)


def test_data_date_mode_without_date_field_raises():
    config = _minimal_config()
    config.connector.incremental.mode = "data_date"
    config.connector.incremental.date_field = None
    conn = duckdb.connect()
    with pytest.raises(PipelineConfigError):  # RestConnector levanta MissingDateFieldError, subclasse de ValueError
        build_pipeline(config, conn)


def test_unsupported_destination_type_raises():
    config = _minimal_config()
    config.destination.type = "redshift"
    conn = duckdb.connect()
    with pytest.raises(PipelineConfigError):
        build_pipeline(config, conn)


def test_list_pipeline_configs_skips_invalid_and_keeps_valid(tmp_path):
    save_pipeline_config(_minimal_config("good_one"), tmp_path / "good_one.yaml")
    (tmp_path / "broken.yaml").write_text("name: sem_connector\n", encoding="utf-8")

    results = list_pipeline_configs(tmp_path)

    names = [config.name for _, config in results]
    assert names == ["good_one"]


def test_list_pipeline_configs_on_missing_directory_returns_empty(tmp_path):
    assert list_pipeline_configs(tmp_path / "nao_existe") == []
