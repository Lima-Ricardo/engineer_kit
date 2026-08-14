import base64
from copy import deepcopy
from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient

from engineer_kit.config.pipeline_config import PipelineConfigError, pipeline_config_from_dict
from engineer_kit.connectors.pagination import (
    PageNumberPagination,
    ParsedPage,
    resolve_pagination,
)
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
