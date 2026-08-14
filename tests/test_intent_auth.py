from engineer_kit.http.auth_intent import resolve_auth


def test_plain_string_resolves_to_bearer_auth():
    auth = resolve_auth("example-value")
    request = auth.apply({"url": "https://example.test"})
    assert request["headers"]["Authorization"] == "Bearer example-value"
