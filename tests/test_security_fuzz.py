from urllib.parse import quote

import pytest
from hypothesis import given, strategies as st

from engineer_kit.http.client import _safe_request_target
from engineer_kit.security.redaction import redact_text
from engineer_kit.security.secrets import FileSecretProvider, InvalidSecretKeyError
from engineer_kit.ui.security import validate_resource_name


_SAFE_SECRET_CHARS = st.text(
    alphabet=st.characters(min_codepoint=48, max_codepoint=122),
    min_size=8,
    max_size=64,
).filter(lambda value: value and value != "<redacted>")


@given(secret=_SAFE_SECRET_CHARS)
def test_redaction_never_returns_known_secret_verbatim(secret):
    result = redact_text(
        f"token={secret} Authorization: Bearer {secret}",
        known_secrets=[secret],
    )
    assert secret not in result


@given(secret=_SAFE_SECRET_CHARS)
def test_safe_request_target_never_exposes_query_values(secret):
    encoded = quote(secret, safe="")
    target = _safe_request_target(f"https://example.test/items?token={encoded}&page=2")
    assert secret not in target
    assert encoded not in target
    assert target == "https://example.test/items?<redacted>"


@given(
    bad_name=st.one_of(
        st.text(min_size=1, max_size=40).map(lambda value: f"../{value}"),
        st.text(min_size=1, max_size=40).map(lambda value: f"{value}/child"),
        st.text(min_size=1, max_size=40).map(lambda value: f"{value}\\child"),
    )
)
def test_resource_names_never_accept_path_components(bad_name):
    with pytest.raises(ValueError):
        validate_resource_name(bad_name)


@given(
    bad_key=st.one_of(
        st.text(min_size=1, max_size=30).map(lambda value: f"../{value}"),
        st.text(min_size=1, max_size=30).map(lambda value: f"{value}/child"),
        st.text(min_size=1, max_size=30).map(lambda value: f"{value}\\child"),
    )
)
def test_file_secret_key_validation_rejects_path_components(bad_key):
    with pytest.raises(InvalidSecretKeyError):
        FileSecretProvider._validate_key(bad_key)
