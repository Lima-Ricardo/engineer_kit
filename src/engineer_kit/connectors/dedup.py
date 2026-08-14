"""Streaming exact primary-key deduplication with bounded memory and disk."""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import tempfile
from pathlib import Path
from typing import Any, Sequence

from engineer_kit.connectors.intent import parse_path, read_path

_DEDUP_COMMIT_INTERVAL = 10_000
DEFAULT_MAX_DEDUP_UNIQUE_KEYS = 50_000_000
DEFAULT_MAX_DEDUP_TEMP_BYTES = 8 * 1024 * 1024 * 1024


class DeduplicationError(RuntimeError):
    """Raised when the temporary deduplication backend cannot operate safely."""


class InvalidDeduplicationKeyError(DeduplicationError):
    """Raised when a record does not contain a usable declared primary key."""


def resolve_primary_key(value: str | Sequence[str] | None) -> tuple[str, ...] | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise TypeError(
            "primary_key deve ser uma coluna ou lista de colunas, nao booleano."
        )
    if isinstance(value, str):
        raw_values = [value]
    elif isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray)):
        raw_values = list(value)
    else:
        raise TypeError("primary_key deve ser uma coluna ou uma lista de colunas.")

    keys: list[str] = []
    seen: set[str] = set()
    for raw in raw_values:
        if not isinstance(raw, str):
            raise TypeError("cada coluna de primary_key deve ser uma string.")
        key = raw.strip()
        if not key:
            raise ValueError("coluna de primary_key nao pode ser vazia.")
        parse_path(key)
        if key in seen:
            continue
        seen.add(key)
        keys.append(key)
    if not keys:
        raise ValueError("primary_key precisa declarar pelo menos uma coluna.")
    return tuple(keys)


def resolve_dedup_keys(
    value: str | Sequence[str] | bool | None,
) -> tuple[str, ...] | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise TypeError(
            "dedup booleano nao declara identidade. Use primary_key=<...> separadamente."
        )
    return resolve_primary_key(value)


def canonical_record_bytes(record: dict[str, Any]) -> bytes:
    try:
        text = json.dumps(
            record,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        )
    except (TypeError, ValueError) as exc:
        raise DeduplicationError("registro nao pode ser serializado para deduplicacao") from exc
    return text.encode("utf-8")


def record_fingerprint(record: dict[str, Any]) -> bytes:
    return hashlib.sha256(canonical_record_bytes(record)).digest()


def _key_values(record: dict[str, Any], keys: tuple[str, ...]) -> list[Any]:
    values: list[Any] = []
    for key in keys:
        value = read_path(record, key)
        if value is None or (isinstance(value, str) and not value.strip()):
            raise InvalidDeduplicationKeyError(
                f"primary_key invalida: '{key}' esta ausente, null ou blank."
            )
        if isinstance(value, (dict, list)):
            raise InvalidDeduplicationKeyError(
                f"primary_key invalida: '{key}' deve resolver para valor escalar."
            )
        values.append(value)
    return values


def key_fingerprint(record: dict[str, Any], keys: tuple[str, ...]) -> bytes:
    try:
        payload = json.dumps(
            _key_values(record, keys),
            ensure_ascii=False,
            separators=(",", ":"),
            default=str,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise DeduplicationError("PK nao pode ser serializada para deduplicacao") from exc
    return hashlib.sha256(payload).digest()


def _positive_limit(value: int | None, *, name: str) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{name} deve ser inteiro maior que zero ou None.")
    return value


class _ExactFingerprintStore:
    def __init__(
        self,
        *,
        directory: str | os.PathLike[str] | None = None,
        max_unique_keys: int | None = DEFAULT_MAX_DEDUP_UNIQUE_KEYS,
        max_temp_bytes: int | None = DEFAULT_MAX_DEDUP_TEMP_BYTES,
    ) -> None:
        self._max_unique_keys = _positive_limit(
            max_unique_keys, name="max_unique_keys"
        )
        self._max_temp_bytes = _positive_limit(max_temp_bytes, name="max_temp_bytes")
        fd, raw_path = tempfile.mkstemp(
            prefix="engineer_kit_dedup_",
            suffix=".sqlite3",
            dir=directory,
        )
        os.close(fd)
        self._path = Path(raw_path)
        try:
            self._conn = sqlite3.connect(str(self._path))
            self._conn.execute("PRAGMA journal_mode=OFF")
            self._conn.execute("PRAGMA synchronous=OFF")
            self._conn.execute(
                "CREATE TABLE seen (fingerprint BLOB PRIMARY KEY) WITHOUT ROWID"
            )
            self._conn.commit()
        except Exception:
            self._path.unlink(missing_ok=True)
            raise
        self.unique_count = 0
        self.duplicate_count = 0
        self._pending = 0
        self._closed = False

    @property
    def path(self) -> Path:
        return self._path

    def _check_limits(self) -> None:
        if (
            self._max_unique_keys is not None
            and self.unique_count > self._max_unique_keys
        ):
            raise DeduplicationError(
                "deduplicacao excedeu max_unique_keys; aumente o limite explicitamente "
                "ou reduza o escopo da extracao."
            )
        if self._max_temp_bytes is not None:
            try:
                size = self._path.stat().st_size
            except OSError as exc:
                raise DeduplicationError(
                    "nao foi possivel verificar o tamanho do armazenamento temporario"
                ) from exc
            if size > self._max_temp_bytes:
                raise DeduplicationError(
                    "deduplicacao excedeu max_temp_bytes; aumente o limite explicitamente "
                    "ou configure um diretorio temporario com capacidade adequada."
                )

    def add_fingerprint(self, fingerprint: bytes) -> bool:
        if self._closed:
            raise DeduplicationError("deduplicador ja foi encerrado")
        cursor = self._conn.execute(
            "INSERT OR IGNORE INTO seen(fingerprint) VALUES (?)",
            (sqlite3.Binary(fingerprint),),
        )
        is_new = cursor.rowcount == 1
        if is_new:
            self.unique_count += 1
        else:
            self.duplicate_count += 1
        self._pending += 1
        if self._pending >= _DEDUP_COMMIT_INTERVAL:
            self._conn.commit()
            self._pending = 0
            self._check_limits()
        elif is_new and self._max_unique_keys is not None:
            if self.unique_count > self._max_unique_keys:
                self._check_limits()
        return is_new

    def close(self) -> None:
        if self._closed:
            return
        try:
            if self._pending:
                self._conn.commit()
            self._conn.close()
        finally:
            self._path.unlink(missing_ok=True)
            self._closed = True


class ExactRowDeduplicator:
    def __init__(
        self,
        *,
        directory: str | os.PathLike[str] | None = None,
        max_unique_keys: int | None = DEFAULT_MAX_DEDUP_UNIQUE_KEYS,
        max_temp_bytes: int | None = DEFAULT_MAX_DEDUP_TEMP_BYTES,
    ) -> None:
        self._store = _ExactFingerprintStore(
            directory=directory,
            max_unique_keys=max_unique_keys,
            max_temp_bytes=max_temp_bytes,
        )

    @property
    def path(self) -> Path:
        return self._store.path

    @property
    def unique_count(self) -> int:
        return self._store.unique_count

    @property
    def duplicate_count(self) -> int:
        return self._store.duplicate_count

    def add(self, record: dict[str, Any]) -> bool:
        return self._store.add_fingerprint(record_fingerprint(record))

    def close(self) -> None:
        self._store.close()

    def __enter__(self) -> "ExactRowDeduplicator":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()


class ExactKeyDeduplicator:
    def __init__(
        self,
        keys: str | Sequence[str],
        *,
        strict: bool = True,
        directory: str | os.PathLike[str] | None = None,
        max_unique_keys: int | None = DEFAULT_MAX_DEDUP_UNIQUE_KEYS,
        max_temp_bytes: int | None = DEFAULT_MAX_DEDUP_TEMP_BYTES,
    ) -> None:
        resolved = resolve_primary_key(keys)
        assert resolved is not None
        self.keys = resolved
        self.strict = strict
        self.invalid_key_count = 0
        self._store = _ExactFingerprintStore(
            directory=directory,
            max_unique_keys=max_unique_keys,
            max_temp_bytes=max_temp_bytes,
        )

    @property
    def path(self) -> Path:
        return self._store.path

    @property
    def unique_count(self) -> int:
        return self._store.unique_count

    @property
    def duplicate_count(self) -> int:
        return self._store.duplicate_count

    def add(self, record: dict[str, Any]) -> bool | None:
        try:
            fingerprint = key_fingerprint(record, self.keys)
        except InvalidDeduplicationKeyError:
            self.invalid_key_count += 1
            if self.strict:
                raise
            return None
        return self._store.add_fingerprint(fingerprint)

    def close(self) -> None:
        self._store.close()

    def __enter__(self) -> "ExactKeyDeduplicator":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()


__all__ = [
    "DEFAULT_MAX_DEDUP_TEMP_BYTES",
    "DEFAULT_MAX_DEDUP_UNIQUE_KEYS",
    "DeduplicationError",
    "ExactKeyDeduplicator",
    "ExactRowDeduplicator",
    "InvalidDeduplicationKeyError",
    "canonical_record_bytes",
    "key_fingerprint",
    "record_fingerprint",
    "resolve_dedup_keys",
    "resolve_primary_key",
]
