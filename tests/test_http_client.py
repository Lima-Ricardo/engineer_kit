import logging

import pytest
import responses

from engineer_kit.http.auth import ApiKeyAuth, BearerAuth
from engineer_kit.http.client import HttpClient, HttpRequestError, InsecureUrlError
from engineer_kit.security.secrets import StaticSecretProvider


def test_plain_http_url_is_rejected_by_default():
    client = HttpClient()
    with pytest.raises(InsecureUrlError):
        client.get("http://example.test/data")


def test_insecure_url_error_redacts_embedded_query_secret():
    client = HttpClient()
    with pytest.raises(InsecureUrlError) as exc_info:
        client.get("http://example.test/data?token=do-not-print")

    message = str(exc_info.value)
    assert "do-not-print" not in message
    assert "?<redacted>" in message


def test_invalid_http_client_tuning_is_rejected():
    with pytest.raises(ValueError):
        HttpClient(timeout=0)
    with pytest.raises(ValueError):
        HttpClient(max_retries=-1)
    with pytest.raises(ValueError):
        HttpClient(backoff_factor=-0.1)


def test_allow_http_override_permits_plain_http():
    client = HttpClient(allow_http=True)
    with responses.RequestsMock() as rsps:
        rsps.add(responses.GET, "http://example.test/data", json={"ok": True})
        response = client.get("http://example.test/data")
    assert response.json() == {"ok": True}


@responses.activate
def test_api_key_in_query_params_is_never_logged(caplog):
    secrets = StaticSecretProvider({"API_KEY": "super-secreto-123"})
    client = HttpClient(auth=ApiKeyAuth(secrets, "API_KEY", param_name="key", location="query"))
    responses.add(responses.GET, "https://example.test/data", json={"ok": True})

    with caplog.at_level(logging.INFO, logger="engineer_kit.http"):
        client.get("https://example.test/data", params={"page": 1})

    assert "super-secreto-123" not in caplog.text
    assert responses.calls[0].request.url.endswith("key=super-secreto-123") or (
        "key=super-secreto-123" in responses.calls[0].request.url
    )


@responses.activate
def test_caller_param_values_are_never_logged(caplog):
    client = HttpClient()
    responses.add(responses.GET, "https://example.test/data", json={"ok": True})

    with caplog.at_level(logging.INFO, logger="engineer_kit.http"):
        client.get(
            "https://example.test/data",
            params={"token": "caller-secret", "page": 7},
        )

    assert "caller-secret" not in caplog.text
    assert "param_keys=['page', 'token']" in caplog.text


@responses.activate
def test_query_embedded_in_url_is_redacted_from_logs(caplog):
    client = HttpClient()
    responses.add(
        responses.GET,
        "https://example.test/data?token=embedded-secret",
        json={"ok": True},
    )

    with caplog.at_level(logging.INFO, logger="engineer_kit.http"):
        client.get("https://example.test/data?token=embedded-secret")

    assert "embedded-secret" not in caplog.text
    assert "https://example.test/data?<redacted>" in caplog.text


@responses.activate
def test_bearer_token_is_never_logged(caplog):
    secrets = StaticSecretProvider({"TOKEN": "outro-segredo-456"})
    client = HttpClient(auth=BearerAuth(secrets, "TOKEN"))
    responses.add(responses.GET, "https://example.test/data", json={"ok": True})

    with caplog.at_level(logging.INFO, logger="engineer_kit.http"):
        client.get("https://example.test/data")

    assert "outro-segredo-456" not in caplog.text
    assert responses.calls[0].request.headers["Authorization"] == "Bearer outro-segredo-456"


@responses.activate
def test_failed_request_error_message_never_contains_query_param_secret():
    secrets = StaticSecretProvider({"API_KEY": "super-secreto-789"})
    client = HttpClient(
        auth=ApiKeyAuth(secrets, "API_KEY", param_name="key", location="query"),
        max_retries=0,
    )
    responses.add(responses.GET, "https://example.test/data", status=401)

    with pytest.raises(HttpRequestError) as exc_info:
        client.get("https://example.test/data")

    message = str(exc_info.value)
    assert "super-secreto-789" not in message
    assert "<redacted>" in message
    assert exc_info.value.__cause__ is None
    assert exc_info.value.__suppress_context__ is True


@responses.activate
def test_failed_request_redacts_secret_embedded_in_url():
    client = HttpClient(max_retries=0)
    responses.add(
        responses.GET,
        "https://example.test/data?token=embedded-error-secret",
        status=500,
    )

    with pytest.raises(HttpRequestError) as exc_info:
        client.get("https://example.test/data?token=embedded-error-secret")

    assert "embedded-error-secret" not in str(exc_info.value)
    assert "?<redacted>" in str(exc_info.value)


@responses.activate
def test_retries_on_server_error_then_succeeds():
    client = HttpClient(max_retries=2, backoff_factor=0)
    responses.add(responses.GET, "https://example.test/data", status=503)
    responses.add(responses.GET, "https://example.test/data", json={"ok": True}, status=200)

    response = client.get("https://example.test/data")
    assert response.json() == {"ok": True}
    assert len(responses.calls) == 2
