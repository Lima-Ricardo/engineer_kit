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
    # a chamada real pro servidor precisa ter ido com a key -- so o log que nao deve conter
    assert responses.calls[0].request.url.endswith("key=super-secreto-123") or (
        "key=super-secreto-123" in responses.calls[0].request.url
    )


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
    # a excecao original do requests (com a URL completa) nao deve ficar acessivel
    # via __cause__/__context__, senao logger.exception() ainda a imprimiria
    assert exc_info.value.__cause__ is None
    assert exc_info.value.__suppress_context__ is True


@responses.activate
def test_retries_on_server_error_then_succeeds():
    client = HttpClient(max_retries=2, backoff_factor=0)
    responses.add(responses.GET, "https://example.test/data", status=503)
    responses.add(responses.GET, "https://example.test/data", json={"ok": True}, status=200)

    response = client.get("https://example.test/data")
    assert response.json() == {"ok": True}
    assert len(responses.calls) == 2
