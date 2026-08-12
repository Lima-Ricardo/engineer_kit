"""Camada de segredos.

Nenhum conector deve ler os.environ ou um arquivo diretamente: tudo
passa por um SecretProvider, para que trocar a fonte (env vars, arquivo,
Vault, AWS Secrets Manager) nao exija tocar em codigo de conector.
"""

from __future__ import annotations

import os
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Union


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
    """Segredos fixos em memoria, definidos direto no codigo.

    Uso pratico para scripts internos ou ambientes controlados onde
    hardcode e uma escolha deliberada do time — mas nunca comite um
    segredo real num repositorio compartilhado (mesmo privado). Para
    producao ou qualquer repo versionado, prefira EnvSecretProvider ou
    FileSecretProvider.
    """

    def __init__(self, values: dict[str, str]) -> None:
        self._values = values

    def get(self, key: str) -> str:
        try:
            return self._values[key]
        except KeyError as exc:
            raise SecretNotFoundError(f"Segredo '{key}' nao encontrado no StaticSecretProvider.") from exc


class FileSecretProvider(SecretProvider):
    """Le segredos de arquivos no disco. Dois modos, pelo que `path` aponta:

    - Um arquivo unico: toda chamada a `get()` devolve o conteudo desse
      arquivo (sem espaco/quebra de linha nas pontas), ignorando `key`
      -- uso simples de "um token, um arquivo".
    - Um diretorio: `get(key)` le o arquivo `<path>/<key>` -- convencao
      comum de Docker/Kubernetes secrets (`/run/secrets/<nome>`), um
      arquivo por segredo.

    Le o arquivo a cada chamada, sem cache: uma rotacao de token no
    disco vale no proximo uso, sem precisar reiniciar o processo.
    """

    def __init__(self, path: Union[str, Path]) -> None:
        self._path = Path(path)

    def get(self, key: str) -> str:
        target = self._path if self._path.is_file() else self._path / key
        try:
            return target.read_text(encoding="utf-8").strip()
        except FileNotFoundError as exc:
            raise SecretNotFoundError(f"Arquivo de segredo '{target}' nao encontrado.") from exc
