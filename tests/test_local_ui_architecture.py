import base64

import pytest
from fastapi.testclient import TestClient

from engineer_kit.config.pipeline_config import load_pipeline_config
from engineer_kit.ui.app import create_app

AUTH_HEADER = {
    "Authorization": "Basic " + base64.b64encode(b"admin:admin").decode(),
}


@pytest.fixture
def client(tmp_path):
    app = create_app(workspace_dir=str(tmp_path), username="admin", password="admin")
    return TestClient(app)


def test_architecture_page_explains_core_contracts(client):
    response = client.get("/architecture", headers=AUTH_HEADER)
    assert response.status_code == 200
    assert "StateStore" in response.text
    assert "Destination" in response.text
    assert "RunLogBackend" in response.text
    assert "local lab" in response.text


def test_pipeline_form_exposes_optional_dbt_transform(client):
    response = client.get("/pipelines/new", headers=AUTH_HEADER)
    assert response.status_code == 200
    assert 'value="dbt"' in response.text
    assert "DuckDBStateStore" in response.text
    assert "RunLogBackend" in response.text


def test_form_persists_transform_and_run_log(client, tmp_path):
    response = client.post(
        "/pipelines/save",
        data={
            "name": "with_dbt",
            "base_url": "https://example.test/items",
            "method": "GET",
            "auth_type": "none",
            "pagination_type": "none",
            "incremental_mode": "ingestion_date",
            "destination_type": "duckdb",
            "destination_schema": "bronze",
            "batch_size": "1000",
            "transform_type": "dbt",
            "dbt_select": "tag:daily",
            "run_log": "on",
            "column_name": ["id"],
            "column_dtype": ["string"],
        },
        headers=AUTH_HEADER,
        follow_redirects=False,
    )
    assert response.status_code == 303

    config = load_pipeline_config(tmp_path / "pipelines" / "with_dbt.yaml")
    assert config.transform.type == "dbt"
    assert config.transform.select == "tag:daily"
    assert config.run_log.enabled is True


def test_form_can_disable_run_log(client, tmp_path):
    response = client.post(
        "/pipelines/save",
        data={
            "name": "no_log",
            "base_url": "https://example.test/items",
            "method": "GET",
            "pagination_type": "none",
            "incremental_mode": "ingestion_date",
            "run_log": "off",
        },
        headers=AUTH_HEADER,
        follow_redirects=False,
    )
    assert response.status_code == 303
    config = load_pipeline_config(tmp_path / "pipelines" / "no_log.yaml")
    assert config.run_log.enabled is False
