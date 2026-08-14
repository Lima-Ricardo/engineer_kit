"""Streaming exact-row deduplication with bounded memory.

The tracker stores only SHA-256 fingerprints in a temporary SQLite database.
It therefore avoids materializing either records or an unbounded in-memory set
while keeping source values out of the temporary dedup store.
"""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import tempfile
from pathlib import Path
from typing import Any


class DeduplicationError(RuntimeError):
    """Raised when the temporary deduplication backend cannot operate safely."""


def canonical_record_bytes(record: dict[str, Any]) -> bytes:
    """Return a stable JSON representation used for row identity."""
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


class ExactRowDeduplicator:
    """Disk-backed duplicate detector for one extraction/profile pass.

    ``add(record)`` returns ``True`` only for the first occurrence. The SQLite
    file is process-local, contains only fingerprints, and is removed on close.
    """

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
        except Exception:
            self._path.unlink(missing_ok=True)
            raise
        self.unique_count = 0
        self.duplicate_count = 0
        self._closed = False

    @property
    def path(self) -> Path:
        """Temporary path, primarily exposed for diagnostics/tests."""
        return self._path

    def add(self, record: dict[str, Any]) -> bool:
        if self._closed:
            raise DeduplicationError("deduplicador ja foi encerrado")
        fingerprint = record_fingerprint(record)
        cursor = self._conn.execute(
            "INSERT OR IGNORE INTO seen(fingerprint) VALUES (?)",
            (sqlite3.Binary(fingerprint),),
        )
        is_new = cursor.rowcount == 1
        if is_new:
            self.unique_count += 1
        else:
            self.duplicate_count += 1
        return is_new

    def close(self) -> None:
        if self._closed:
            return
        try:
            self._conn.close()
        finally:
            self._path.unlink(missing_ok=True)
            self._closed = True

    def __enter__(self) -> "ExactRowDeduplicator":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()


__all__ = [
    "DeduplicationError",
    "ExactRowDeduplicator",
    "canonical_record_bytes",
    "record_fingerprint",
]
