import base64
from copy import deepcopy
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from engineer_kit.config.pipeline_config import PipelineConfigError, pipeline_config_from_dict
from engineer_kit.connectors.pagination import (
    PageNumberPagination,
    ParsedPage,
    resolve_pagination,
)
from engineer_kit.connectors.rest import RestConnector
from engineer_kit.http.auth import BearerAuth
from engineer_kit.http.client import HttpClient
from engineer_kit.orchestration.pipeline import _source_config_identity
from engineer_kit.security.secrets import StaticSecretProvider
from engineer_kit.ui.app import create_app
from engineer_kit.ui.run_manager import RunState


BASE_CONFIG = {
    "version": 1,
    "name": "hardening_test",
    "connector": {"base_url": "https://example.test/items"},
}


def _config_with(path: tuple[str, ...], value):
    data = deepcopy(BASE_CONFIG)
    current = data
    for key in path[:-1]:
        current = current.setdefault(key, {})
    current[path[-1]] = value
    return data


def test_version_rejects_boolean_even_though_bool_is_an_int_subclass():
    with pytest.raises(PipelineConfigError, match="version deve ser um inteiro"):
        pipeline_config_from_dict(_config_with(("version",), True))


@pytest.mark.parametrize(
    ("path", "value"),
    [
        (("connector", "auth"), []),
        (("connector", "date_params"), []),
        (("connector", "params"), []),
        (("connector", "static_params"), ""),
        (("destination",), []),
        (("secrets",), []),
        (("state",), []),
        (("run_log",), []),
    ],
)
def test_falsey_wrong_shape_values_are_not_silently_coerced(path, value):
    with pytest.raises(PipelineConfigError):
        pipeline_config_from_dict(_config_with(path, value))


@pytest.mark.parametrize(
    ("path", "value", "message"),
    [
        (("connector", "incremental"), {"enabled": "false"}, "incremental.enabled"),
        (("run_log",), {"enabled": "false"}, "run_log.enabled"),
        (("secrets",), {"allow_inline_values": "false"}, "secrets.allow_inline_values"),
    ],
)
def test_declarative_booleans_reject_strings(path, value, message):
    with pytest.raises(PipelineConfigError, match=message):
        pipeline_config_from_dict(_config_with(path, value))


def test_intent_page_pagination_does_not_truncate_on_short_page():
    strategy = resolve_pagination({"type": "page", "page_size": 10})
    params = strategy.initial_params()
    short_page = ParsedPage(records=[{"id": 1}], raw=None)
    assert strategy.next_params(short_page, params) == {"page": 2, "per_page": 10}


def test_direct_page_strategy_preserves_legacy_short_page_stop():
    strategy = PageNumberPagination(page_size=10)
    params = strategy.initial_params()
    short_page = ParsedPage(records=[{"id": 1}], raw=None)
    assert strategy.next_params(short_page, params) is None


def test_empty_rest_response_is_an_empty_page_without_json_decode():
    connector = RestConnector(
        name="empty",
        base_url="https://example.test/items",
        incremental=False,
    )
    response = SimpleNamespace(
        status_code=204,
        content=b"",
        headers={},
        json=lambda: pytest.fail("json() should not be called for an empty response"),
    )
    page = connector.parse_response(response)
    assert page.records == []
    assert page.raw == []


def test_retry_source_identity_changes_with_source_or_filter_config():
    first = RestConnector(
        name="orders",
        base_url="https://example.test/orders",
        params={"status": "open"},
        incremental=False,
    )
    different_source = RestConnector(
        name="orders",
        base_url="https://other.example.test/orders",
        params={"status": "open"},
        incremental=False,
    )
    different_filter = RestConnector(
        name="orders",
        base_url="https://example.test/orders",
        params={"status": "closed"},
        incremental=False,
    )

    first_identity = _source_config_identity(first)
    assert _source_config_identity(different_source) != first_identity
    assert _source_config_identity(different_filter) != first_identity


def test_retry_source_identity_does_not_depend_on_secret_value():
    first = RestConnector(
        name="orders",
        base_url="https://example.test/orders",
        params={"token": "secret-a", "status": "open"},
        incremental=False,
    )
    rotated = RestConnector(
        name="orders",
        base_url="https://example.test/orders",
        params={"token": "secret-b", "status": "open"},
        incremental=False,
    )
    assert _source_config_identity(first) == _source_config_identity(rotated)


def test_retry_source_identity_tracks_logical_bearer_principal_not_token_value():
    provider_a = StaticSecretProvider({"TENANT_A_TOKEN": "token-v1"})
    provider_a_rotated = StaticSecretProvider({"TENANT_A_TOKEN": "token-v2"})
    provider_b = StaticSecretProvider({"TENANT_B_TOKEN": "token-v1"})

    tenant_a = RestConnector(
        name="orders",
        base_url="https://example.test/orders",
        http_client=HttpClient(auth=BearerAuth(provider_a, "TENANT_A_TOKEN")),
        incremental=False,
    )
    tenant_a_rotated = RestConnector(
        name="orders",
        base_url="https://example.test/orders",
        http_client=HttpClient(auth=BearerAuth(provider_a_rotated, "TENANT_A_TOKEN")),
        incremental=False,
    )
    tenant_b = RestConnector(
        name="orders",
        base_url="https://example.test/orders",
        http_client=HttpClient(auth=BearerAuth(provider_b, "TENANT_B_TOKEN")),
        incremental=False,
    )

    identity_a = _source_config_identity(tenant_a)
    assert _source_config_identity(tenant_a_rotated) == identity_a
    assert _source_config_identity(tenant_b) != identity_a


def test_run_state_bounds_single_log_line_memory():
    state = RunState(
        run_id="run",
        pipeline_name="pipe",
        started_at=datetime.now(timezone.utc),
    )
    state.append_log("x" * 100, max_lines=10, max_chars=16)
    items, _, _ = state.wait_for_logs(0, timeout=0)
    assert len(items) == 1
    assert items[0][1].startswith("x" * 16)
    assert items[0][1].endswith("[truncated]")
    assert len(items[0][1]) < 100


def test_local_ui_emits_hardening_headers_and_blocks_cross_site_post(tmp_path):
    app = create_app(
        workspace_dir=str(tmp_path),
        username="admin",
        password="safe-local-password",
    )
    client = TestClient(app)
    auth = {
        "Authorization": "Basic "
        + base64.b64encode(b"admin:safe-local-password").decode()
    }

    response = client.get("/", headers=auth)
    assert response.status_code == 200
    assert response.headers["x-content-type-options"] == "nosniff"
    assert response.headers["x-frame-options"] == "DENY"
    assert response.headers["cache-control"] == "no-store"
    assert "frame-ancestors 'none'" in response.headers["content-security-policy"]

    blocked = client.post(
        "/pipelines/save",
        data={"name": "x", "base_url": "https://example.test/items"},
        headers={**auth, "Origin": "https://evil.example"},
    )
    assert blocked.status_code == 403
