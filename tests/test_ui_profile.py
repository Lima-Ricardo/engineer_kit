from __future__ import annotations

import base64

from fastapi.testclient import TestClient

from engineer_kit.config.pipeline_config import pipeline_config_from_dict, save_pipeline_config
from engineer_kit.profiling.engine import profile_records
from engineer_kit.ui import app as ui_app

AUTH_HEADER = {
    "Authorization": "Basic " + base64.b64encode(b"admin:admin").decode(),
}


def _saved_config(tmp_path):
    config = pipeline_config_from_dict(
        {
            "version": 1,
            "name": "orders",
            "connector": {
                "base_url": "https://example.test/orders",
                "pagination": False,
                "incremental": False,
                "dedup": ["id"],
            },
            "run_log": False,
        }
    )
    save_pipeline_config(config, tmp_path / "pipelines" / "orders.yaml")
    return config


def test_local_lab_profile_renders_same_profile_report_and_candidate_pk(tmp_path, monkeypatch):
    _saved_config(tmp_path)

    class FakeConnector:
        def profile(self, *metrics, scope="full", limit=None, key=None, **kwargs):
            assert scope == "sample"
            assert limit == 10_000
            assert metrics == ("duplicates", "nulls", "missing")
            assert key == ["id"]
            return profile_records(
                [
                    {"id": 1, "email": None},
                    {"id": 1, "email": "changed"},
                    {"id": 2},
                ],
                *metrics,
                scope=scope,
                limit=limit,
                key=key,
            )

    monkeypatch.setattr(ui_app, "connector_from_config", lambda config: FakeConnector())
    client = TestClient(
        ui_app.create_app(workspace_dir=str(tmp_path), username="admin", password="admin")
    )

    page = client.get("/pipelines/orders/profile", headers=AUTH_HEADER)
    assert page.status_code == 200
    assert "Data Profile" in page.text
    assert 'value="id"' in page.text

    response = client.post(
        "/pipelines/orders/profile",
        data={
            "scope": "sample",
            "limit": "10000",
            "candidate_key": "id",
            "metric": ["duplicates", "nulls", "missing"],
        },
        headers=AUTH_HEADER,
    )
    assert response.status_code == 200
    assert "Profile Report v1" in response.text
    assert "Duplicatas" in response.text
    assert "PK: id" in response.text
    assert "email" in response.text


def test_local_lab_form_persists_dedup_primary_key(tmp_path):
    client = TestClient(
        ui_app.create_app(workspace_dir=str(tmp_path), username="admin", password="admin")
    )
    response = client.post(
        "/pipelines/save",
        data={
            "name": "dedup_ui",
            "base_url": "https://example.test/items",
            "method": "GET",
            "pagination_type": "none",
            "incremental_mode": "ingestion_date",
            "dedup_key": "tenant_id,customer_id",
        },
        headers=AUTH_HEADER,
        follow_redirects=False,
    )
    assert response.status_code == 303

    from engineer_kit.config.pipeline_config import load_pipeline_config

    saved = load_pipeline_config(tmp_path / "pipelines" / "dedup_ui.yaml")
    assert saved.connector.dedup == ["tenant_id", "customer_id"]
