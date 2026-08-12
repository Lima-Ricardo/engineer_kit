import os
from datetime import date

import pytest
import responses
from fastapi.testclient import TestClient

from engineer_kit.config.pipeline_config import (
    MAX_PIPELINE_CONFIG_BYTES,
    PipelineConfigError,
    _resolve_secret_refs,
    load_pipeline_config,
    pipeline_config_from_dict,
)
from engineer_kit.connectors.api_connector import (
    CrossOriginPaginationError,
    PaginationLimitError,
)
from engineer_kit.connectors.incremental import IncrementalMode
from engineer_kit.connectors.pagination import NextUrlPagination, PageNumberPagination
from engineer_kit.connectors.rest import RestConnector
from engineer_kit.http.auth import ApiKeyAuth, BearerAuth, InvalidAuthValueError
from engineer_kit.http.client import (
    HttpClient,
    ResponseTooLargeError,
    UnsafeRedirectError,
    UnsafeUrlError,
)
from engineer_kit.security.redaction import redact_text
from engineer_kit.security.secrets import (
    FileSecretProvider,
    InvalidSecretKeyError,
    StaticSecretProvider,
)
from engineer_kit.storage.state_store import StateStore, Watermark
from engineer_kit.ui.app import create_app


class MemoryStateStore(StateStore):
    def __init__(self):
        self.value = None

    def get_watermark(self, connector_name: str):
        return self.value

    def set_watermark(self, connector_name: str, watermark: Watermark):
        self.value = watermark


@responses.activate
def test_http_rejects_embedded_url_credentials():
    client = HttpClient()
    with pytest.raises(UnsafeUrlError, match="Credenciais embutidas"):
        client.get("https://user:password@example.test/items")
    assert not responses.calls


@responses.activate
def test_http_blocks_link_local_cloud_metadata_target():
    client = HttpClient()
    with pytest.raises(UnsafeUrlError, match="bloqueado"):
        client.get("https://169.254.169.254/latest/meta-data")
    assert not responses.calls


@responses.activate
def test_http_blocks_cross_origin_redirect_before_forwarding_auth():
    secrets = StaticSecretProvider({"TOKEN": "do-not-forward"})
    client = HttpClient(auth=BearerAuth(secrets, "TOKEN"), max_retries=0)
    responses.add(
        responses.GET,
        "https://api.example.test/start",
        status=302,
        headers={"Location": "https://other.example.test/final"},
    )

    with pytest.raises(UnsafeRedirectError, match="outra origem"):
        client.get("https://api.example.test/start")

    assert len(responses.calls) == 1
    assert responses.calls[0].request.headers["Authorization"] == "Bearer do-not-forward"


@responses.activate
def test_http_allows_same_origin_redirect():
    client = HttpClient(max_retries=0)
    responses.add(
        responses.GET,
        "https://api.example.test/start",
        status=302,
        headers={"Location": "/final"},
    )
    responses.add(
        responses.GET,
        "https://api.example.test/final",
        json={"ok": True},
    )

    response = client.get("https://api.example.test/start")
    assert response.json() == {"ok": True}
    assert len(responses.calls) == 2


@responses.activate
def test_http_rejects_oversized_page_from_content_length():
    client = HttpClient(max_response_bytes=16, max_retries=0)
    responses.add(
        responses.GET,
        "https://api.example.test/items",
        body=b"x" * 32,
        headers={"Content-Length": "32"},
    )

    with pytest.raises(ResponseTooLargeError, match="excede o limite"):
        client.get("https://api.example.test/items")


def test_auth_rejects_header_injection_characters():
    with pytest.raises(InvalidAuthValueError):
        BearerAuth(StaticSecretProvider({"TOKEN": "abc\r\nX-Evil: yes"}), "TOKEN").apply({})
    with pytest.raises(InvalidAuthValueError):
        ApiKeyAuth(
            StaticSecretProvider({"TOKEN": "abc"}),
            "TOKEN",
            param_name="X-Key\nInjected",
            location="header",
        )


def test_redaction_covers_common_secret_shapes():
    text = (
        "Authorization: Bearer abcdefghijklmnop "
        "api_key=super-secret password=hunter2 "
        "https://example.test/items?token=hidden"
    )
    result = redact_text(text)
    assert "abcdefghijklmnop" not in result
    assert "super-secret" not in result
    assert "hunter2" not in result
    assert "token=hidden" not in result


def test_declarative_config_refuses_obvious_inline_secrets():
    with pytest.raises(PipelineConfigError, match="sensivel inline"):
        pipeline_config_from_dict(
            {
                "name": "safe",
                "connector": {
                    "base_url": "https://example.test/items",
                    "incremental": {"mode": "ingestion_date"},
                    "static_params": {"api_key": "literal-secret"},
                },
            }
        )


def test_declarative_config_allows_explicit_training_inline_values():
    config = pipeline_config_from_dict(
        {
            "name": "training",
            "secrets": {"allow_inline_values": True},
            "connector": {
                "base_url": "https://example.test/items",
                "incremental": {"mode": "ingestion_date"},
                "static_params": {"api_key": "training-only-value"},
            },
        }
    )
    assert config.connector.static_params["api_key"] == "training-only-value"


def test_static_secret_provider_remains_available_for_training_code():
    provider = StaticSecretProvider({"TOKEN": "hardcoded-training-token"})
    assert provider.get("TOKEN") == "hardcoded-training-token"


def test_declarative_secret_reference_is_resolved_only_in_memory():
    provider = StaticSecretProvider({"API_KEY": "resolved-secret"})
    value = {"api_key": "${SECRET:API_KEY}", "normal": "value"}
    assert _resolve_secret_refs(value, provider) == {
        "api_key": "resolved-secret",
        "normal": "value",
    }
    assert value["api_key"] == "${SECRET:API_KEY}"


def test_pipeline_yaml_has_bounded_size(tmp_path):
    path = tmp_path / "huge.yaml"
    path.write_bytes(b"x" * (MAX_PIPELINE_CONFIG_BYTES + 1))
    with pytest.raises(PipelineConfigError, match="excede o limite"):
        load_pipeline_config(path)


@pytest.mark.skipif(os.name == "nt", reason="symlink behavior/permissions differ on Windows runners")
def test_file_secret_provider_blocks_symlink_escape(tmp_path):
    secret_dir = tmp_path / "secrets"
    secret_dir.mkdir()
    outside = tmp_path / "outside-token"
    outside.write_text("secret", encoding="utf-8")
    (secret_dir / "TOKEN").symlink_to(outside)

    provider = FileSecretProvider(secret_dir)
    with pytest.raises(InvalidSecretKeyError, match="fora do diretorio"):
        provider.get("TOKEN")


@responses.activate
def test_connector_stops_at_max_pages_before_unbounded_requests():
    state = MemoryStateStore()
    connector = RestConnector(
        name="bounded",
        base_url="https://example.test/items",
        pagination=PageNumberPagination(page_size=1),
        method="GET",
        state_store=state,
        incremental_mode=IncrementalMode.INGESTION_DATE,
        max_pages=1,
    )
    responses.add(responses.GET, "https://example.test/items", json=[{"id": 1}])

    with pytest.raises(PaginationLimitError, match="max_pages=1"):
        list(connector.extract(end=date(2026, 8, 12)))
    assert len(responses.calls) == 1


@responses.activate
def test_cross_origin_next_url_is_blocked_before_second_authenticated_request():
    state = MemoryStateStore()
    secret_provider = StaticSecretProvider({"TOKEN": "pagination-secret"})
    connector = RestConnector(
        name="cross-origin",
        base_url="https://api.example.test/items",
        pagination=NextUrlPagination(next_url_field="next"),
        method="GET",
        state_store=state,
        incremental_mode=IncrementalMode.INGESTION_DATE,
        auth=BearerAuth(secret_provider, "TOKEN"),
    )
    responses.add(
        responses.GET,
        "https://api.example.test/items",
        json={
            "results": [{"id": 1}],
            "next": "https://evil.example.test/items?page=2",
        },
    )
    connector._records_path = "results"

    with pytest.raises(CrossOriginPaginationError, match="outra origem"):
        list(connector.extract(end=date(2026, 8, 12)))

    assert len(responses.calls) == 1
    assert responses.calls[0].request.headers["Authorization"] == "Bearer pagination-secret"


def test_local_ui_adds_security_headers_and_blocks_cross_site_post(tmp_path):
    import base64

    auth = {"Authorization": "Basic " + base64.b64encode(b"admin:admin").decode()}
    client = TestClient(
        create_app(workspace_dir=str(tmp_path), username="admin", password="admin")
    )

    response = client.get("/", headers=auth)
    assert response.status_code == 200
    assert response.headers["x-frame-options"] == "DENY"
    assert response.headers["x-content-type-options"] == "nosniff"
    assert "frame-ancestors 'none'" in response.headers["content-security-policy"]

    blocked = client.post(
        "/pipelines/save",
        headers={**auth, "Sec-Fetch-Site": "cross-site"},
        data={"name": "safe", "base_url": "https://example.test"},
    )
    assert blocked.status_code == 403


def test_local_ui_rejects_pipeline_name_path_traversal(tmp_path):
    import base64

    auth = {"Authorization": "Basic " + base64.b64encode(b"admin:admin").decode()}
    client = TestClient(
        create_app(workspace_dir=str(tmp_path), username="admin", password="admin")
    )
    response = client.post(
        "/pipelines/save",
        headers=auth,
        data={
            "name": "../outside",
            "base_url": "https://example.test/items",
            "method": "GET",
            "pagination_type": "none",
            "incremental_mode": "ingestion_date",
        },
    )
    assert response.status_code == 400
    assert not (tmp_path / "outside.yaml").exists()
