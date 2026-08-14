"""Streaming exact primary-key deduplication with bounded memory.

Deduplication stores only SHA-256 fingerprints in a temporary SQLite database.
It therefore avoids materializing either records or an unbounded in-memory set
while keeping source values out of the temporary store. Disk usage grows with
the number of unique identities; memory does not.
"""

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


class DeduplicationError(RuntimeError):
    """Raised when the temporary deduplication backend cannot operate safely."""


class InvalidDeduplicationKeyError(DeduplicationError):
    """Raised when a record does not contain a usable declared primary key."""


def resolve_primary_key(value: str | Sequence[str] | None) -> tuple[str, ...] | None:
    """Normalize a simple/composite record identity.

    ``None`` means no declared identity. A string denotes one key path and a
    sequence denotes a composite key. This function validates identity only;
    whether deduplication is enabled is a separate policy decision.
    """
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
    """Compatibility alias for the pre-separation key normalizer.

    New code should call :func:`resolve_primary_key`. ``False``/``None`` keep
    the historical disabled meaning; ``True`` remains invalid because it does
    not carry identity.
    """
    if value is None or value is False:
        return None
    if value is True:
        raise TypeError(
            "dedup=True nao declara identidade. Use primary_key=<...> e dedup=True."
        )
    return resolve_primary_key(value)


def canonical_record_bytes(record: dict[str, Any]) -> bytes:
    """Return a stable JSON representation used for complete-row identity."""
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
    """Return a privacy-preserving 256-bit identity for a complete record."""
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
    """Return a stable fingerprint for a declared simple/composite primary key."""
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


class _ExactFingerprintStore:
    def __init__(self, *, directory: str | os.PathLike[str] | None = None) -> None:
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
        """Temporary path, primarily exposed for diagnostics/tests."""
        return self._path

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
    """Disk-backed duplicate detector for complete rows."""

    def __init__(self, *, directory: str | os.PathLike[str] | None = None) -> None:
        self._store = _ExactFingerprintStore(directory=directory)

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
    """Disk-backed detector for a declared simple/composite primary key.

    ``add(record)`` returns ``True`` only for the first occurrence of the PK.
    Missing/null/blank/non-scalar key values fail fast by default because a
    declared primary key is an identity contract, not a best-effort hint.
    Profiling may opt into ``strict=False`` to count invalid-key rows instead of
    stopping the scan.
    """

    def __init__(
        self,
        keys: str | Sequence[str],
        *,
        strict: bool = True,
        directory: str | os.PathLike[str] | None = None,
    ) -> None:
        resolved = resolve_primary_key(keys)
        assert resolved is not None
        self.keys = resolved
        self.strict = strict
        self.invalid_key_count = 0
        self._store = _ExactFingerprintStore(directory=directory)

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