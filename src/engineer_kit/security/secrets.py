"""Camada de segredos.

Nenhum conector deve ler os.environ diretamente: tudo passa por um
SecretProvider, para que trocar a fonte (env vars, Vault, AWS Secrets
Manager) nao exija tocar em codigo de conector.
"""

from __future__ import annotations

import os
from abc import ABC, abstractmethod


class SecretNotFoundError(KeyError):
    """Levantado quando uma chave de segredo nao existe na fonte configurada."""


class SecretProvider(ABC):
    """Contrato para qualquer fonte de segredos."""

    @abstractmethod
    def get(self, key: str) -> str:
        """Retorna o valor do segredo `key` ou levanta SecretNotFoundError."""


class EnvSecretProvider(SecretProvider):
    """Le segredos de variaveis de ambiente (os.environ, ou um .env ja carregado)."""

    def get(self, key: str) -> str:
        value = os.environ.get(key)
        if value is None:
            raise SecretNotFoundError(
                f"Variavel de ambiente '{key}' nao encontrada. "
                "Defina-a no ambiente ou no arquivo .env antes de rodar o pipeline."
            )
        return value


class StaticSecretProvider(SecretProvider):
    """Segredos fixos em memoria. Uso exclusivo em testes — nunca em producao."""

    def __init__(self, values: dict[str, str]) -> None:
        self._values = values

    def get(self, key: str) -> str:
        try:
            return self._values[key]
        except KeyError as exc:
            raise SecretNotFoundError(f"Segredo '{key}' nao encontrado no StaticSecretProvider.") from exc
