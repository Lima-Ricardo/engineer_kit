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


def test_dashboard_requires_auth(client):
    response = client.get("/")
    assert response.status_code == 401


def test_dashboard_with_auth_shows_empty_state(client):
    response = client.get("/", headers=AUTH_HEADER)
    assert response.status_code == 200
    assert "Nenhum pipeline configurado" in response.text


def test_new_pipeline_form_renders(client):
    response = client.get("/pipelines/new", headers=AUTH_HEADER)
    assert response.status_code == 200
    assert "Novo pipeline" in response.text
    assert "page" in response.text  # opcao de paginacao presente
    assert "ExtractionSession" in response.text
    assert 'name="extraction_batch_size"' in response.text
    assert 'value="25000"' in response.text


def test_create_pipeline_via_form_then_appears_on_dashboard(client, tmp_path):
    form_data = {
        "name": "test_pipeline",
        "base_url": "https://example.test/items",
        "method": "GET",
        "auth_type": "none",
        "auth_param_name": "api_key",
        "auth_location": "query",
        "pagination_type": "page",
        "page_param": "page",
        "page_size_param": "per_page",
        "page_size": "50",
        "extraction_batch_size": "12500",
        "incremental_mode": "ingestion_date",
        "date_param_start": "since",
        "date_param_end": "until",
        "date_param_format": "%Y-%m-%d",
        "column_name": ["id", "name"],
        "column_dtype": ["VARCHAR", "VARCHAR"],
        "destination_schema": "bronze",
        "batch_size": "1000",
    }
    response = client.post(
        "/pipelines/save",
        data=form_data,
        headers=AUTH_HEADER,
        follow_redirects=False,
    )
    assert response.status_code == 303
    assert response.headers["location"] == "/pipelines/test_pipeline"
    config_path = tmp_path / "pipelines" / "test_pipeline.yaml"
    assert config_path.exists()
    assert load_pipeline_config(config_path).connector.extraction_batch_size == 12_500

    dashboard = client.get("/", headers=AUTH_HEADER)
    assert "test_pipeline" in dashboard.text


def test_create_pipeline_missing_base_url_shows_error(client):
    response = client.post(
        "/pipelines/save",
        data={
            "name": "sem_url",
            "method": "GET",
            "pagination_type": "none",
            "incremental_mode": "ingestion_date",
        },
        headers=AUTH_HEADER,
    )
    assert response.status_code == 400
    assert "obrigat" in response.text.lower()


def test_pipeline_detail_shows_saved_config(client, tmp_path):
    client.post(
        "/pipelines/save",
        data={
            "name": "detalhe_teste",
            "base_url": "https://example.test/x",
            "method": "GET",
            "pagination_type": "none",
            "incremental_mode": "ingestion_date",
            "column_name": ["a"],
            "column_dtype": ["VARCHAR"],
        },
        headers=AUTH_HEADER,
    )
    response = client.get("/pipelines/detalhe_teste", headers=AUTH_HEADER)
    assert response.status_code == 200
    assert "https://example.test/x" in response.text
    assert "Ainda n" in response.text  # "Ainda não rodou"


def test_pipeline_detail_404_for_unknown_pipeline(client):
    response = client.get("/pipelines/nao_existe", headers=AUTH_HEADER)
    assert response.status_code == 404


def test_data_browser_empty_when_no_warehouse_yet(client):
    response = client.get("/data", headers=AUTH_HEADER)
    assert response.status_code == 200
    assert "Nenhuma tabela" in response.text


def test_dbt_models_empty_when_no_dbt_project(client):
    response = client.get("/dbt", headers=AUTH_HEADER)
    assert response.status_code == 200
    assert "Nenhum modelo" in response.text
