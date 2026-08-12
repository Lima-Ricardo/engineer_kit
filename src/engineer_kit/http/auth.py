"""Estrategias de autenticacao para conectores REST.

Cada classe recebe um SecretProvider e sabe transformar isso em headers
ou query params de requisicao. O conector nunca le a chave diretamente —
so pede `auth.apply(request_kwargs)`.
"""

from __future__ import annotations

import re
from abc import ABC, abstractmethod
from typing import Any

from engineer_kit.security.secrets import SecretProvider

_HEADER_NAME_RE = re.compile(r"^[!#$%&'*+\-.^_`|~0-9A-Za-z]+$")


class InvalidAuthValueError(ValueError):
    """Raised before malformed auth input reaches the HTTP stack."""


def _validate_header_name(name: str) -> str:
    if not name or _HEADER_NAME_RE.fullmatch(name) is None:
        raise InvalidAuthValueError("Nome de header de autenticacao invalido.")
    return name


def _validate_header_value(value: str) -> str:
    if "\r" in value or "\n" in value or "\x00" in value:
        raise InvalidAuthValueError(
            "Valor de autenticacao contem caracteres de controle nao permitidos."
        )
    return value


class AuthStrategy(ABC):
    """Contrato: recebe os kwargs de requests.request() e devolve a versao autenticada."""

    @abstractmethod
    def apply(self, request_kwargs: dict[str, Any]) -> dict[str, Any]: ...


class NoAuth(AuthStrategy):
    """Para APIs publicas sem autenticacao."""

    def apply(self, request_kwargs: dict[str, Any]) -> dict[str, Any]:
        return request_kwargs


class BearerAuth(AuthStrategy):
    """Authorization: Bearer <token>, token lido de um SecretProvider."""

    def __init__(self, secret_provider: SecretProvider, secret_key: str) -> None:
        self._secret_provider = secret_provider
        self._secret_key = secret_key

    def apply(self, request_kwargs: dict[str, Any]) -> dict[str, Any]:
        token = _validate_header_value(self._secret_provider.get(self._secret_key))
        headers = dict(request_kwargs.get("headers") or {})
        headers["Authorization"] = f"Bearer {token}"
        return {**request_kwargs, "headers": headers}


class ApiKeyAuth(AuthStrategy):
    """Chave de API enviada como header ou query param nomeado."""

    def __init__(
        self,
        secret_provider: SecretProvider,
        secret_key: str,
        param_name: str = "api_key",
        location: str = "query",
    ) -> None:
        if location not in ("query", "header"):
            raise ValueError("location deve ser 'query' ou 'header'")
        if not param_name or "\x00" in param_name or "\r" in param_name or "\n" in param_name:
            raise InvalidAuthValueError("Nome do parametro de API key invalido.")
        if location == "header":
            _validate_header_name(param_name)
        self._secret_provider = secret_provider
        self._secret_key = secret_key
        self._param_name = param_name
        self._location = location

    def apply(self, request_kwargs: dict[str, Any]) -> dict[str, Any]:
        value = self._secret_provider.get(self._secret_key)
        if self._location == "header":
            value = _validate_header_value(value)
            headers = dict(request_kwargs.get("headers") or {})
            headers[self._param_name] = value
            return {**request_kwargs, "headers": headers}
        params = dict(request_kwargs.get("params") or {})
        params[self._param_name] = value
        return {**request_kwargs, "params": params}
