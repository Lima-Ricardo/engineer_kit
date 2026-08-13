from __future__ import annotations

from engineer_kit.http.auth import AuthStrategy, BearerAuth, NoAuth
from engineer_kit.security.secrets import StaticSecretProvider


def resolve_auth(value):
    if value is None:
        return NoAuth()
    if isinstance(value, AuthStrategy):
        return value
    if isinstance(value, str):
        provider = StaticSecretProvider({"value": value})
        return BearerAuth(provider, "value")
    raise TypeError("auth deve ser string ou AuthStrategy.")
