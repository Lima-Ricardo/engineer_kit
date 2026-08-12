"""Secret-provider abstractions used by connectors and configuration."""

from __future__ import annotations

import os
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Union

MAX_SECRET_BYTES = 64 * 1024


class SecretNotFoundError(KeyError):
    """Raised when a requested secret is absent from the configured source."""


class InvalidSecretKeyError(ValueError):
    """Raised when a file-backed secret key attempts path traversal."""


class SecretTooLargeError(ValueError):
    """Raised before an unexpectedly large mounted secret is materialized."""


class SecretProvider(ABC):
    """Contract for any source of authentication secrets."""

    @abstractmethod
    def get(self, key: str) -> str:
        """Return ``key`` or raise ``SecretNotFoundError``."""


class EnvSecretProvider(SecretProvider):
    """Read secrets from process environment variables."""

    def get(self, key: str) -> str:
        value = os.environ.get(key)
        if value is None:
            raise SecretNotFoundError(
                f"Variavel de ambiente '{key}' nao encontrada. "
                "Defina-a no ambiente antes de rodar o pipeline."
            )
        return value


class StaticSecretProvider(SecretProvider):
    """Fixed in-memory secrets for tests or deliberately controlled scripts.

    Do not commit real values to a shared repository. Production/versioned
    deployments should prefer environment, file/mounted-secret or external
    secret-manager adapters.
    """

    def __init__(self, values: dict[str, str]) -> None:
        self._values = dict(values)

    def get(self, key: str) -> str:
        try:
            return self._values[key]
        except KeyError as exc:
            raise SecretNotFoundError(
                f"Segredo '{key}' nao encontrado no StaticSecretProvider."
            ) from exc


class FileSecretProvider(SecretProvider):
    """Read one secret file or ``<directory>/<key>`` mounted-secret files.

    Values are re-read on every call so file rotation takes effect without a
    process restart. Directory mode accepts only a single filename as ``key``;
    path separators and traversal components are rejected before filesystem
    access. Symlinks that resolve outside the configured directory are refused.
    """

    def __init__(self, path: Union[str, Path]) -> None:
        self._path = Path(path)

    @staticmethod
    def _validate_key(key: str) -> str:
        if not key or key in {".", ".."} or "/" in key or "\\" in key or "\x00" in key:
            raise InvalidSecretKeyError(
                "Chave de segredo de FileSecretProvider deve ser apenas um nome de arquivo."
            )
        return key

    @staticmethod
    def _read_bounded(target: Path) -> str:
        try:
            size = target.stat().st_size
        except FileNotFoundError as exc:
            raise SecretNotFoundError(
                f"Arquivo de segredo '{target}' nao encontrado."
            ) from exc
        if size > MAX_SECRET_BYTES:
            raise SecretTooLargeError(
                f"Arquivo de segredo excede o limite de {MAX_SECRET_BYTES} bytes."
            )
        try:
            raw = target.read_bytes()
        except FileNotFoundError as exc:
            raise SecretNotFoundError(
                f"Arquivo de segredo '{target}' nao encontrado."
            ) from exc
        if len(raw) > MAX_SECRET_BYTES:
            raise SecretTooLargeError(
                f"Arquivo de segredo excede o limite de {MAX_SECRET_BYTES} bytes."
            )
        try:
            return raw.decode("utf-8").strip()
        except UnicodeDecodeError as exc:
            raise ValueError("Arquivo de segredo deve estar em UTF-8.") from exc

    def get(self, key: str) -> str:
        if self._path.is_file():
            target = self._path
        else:
            base = self._path.resolve(strict=False)
            target = base / self._validate_key(key)
            resolved = target.resolve(strict=False)
            try:
                resolved.relative_to(base)
            except ValueError as exc:
                raise InvalidSecretKeyError(
                    "Arquivo de segredo resolve fora do diretorio configurado."
                ) from exc
            target = resolved
        return self._read_bounded(target)


__all__ = [
    "SecretProvider",
    "SecretNotFoundError",
    "InvalidSecretKeyError",
    "SecretTooLargeError",
    "EnvSecretProvider",
    "StaticSecretProvider",
    "FileSecretProvider",
]
