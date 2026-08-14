"""Atomic JSON-file StateStore for local or mounted filesystems."""

from __future__ import annotations

import json
import os
import tempfile
import threading
from contextlib import contextmanager
from datetime import date, datetime
from pathlib import Path
from typing import Iterator

from engineer_kit.storage.state_store import (
    StateConflictError,
    StateStore,
    Watermark,
    validate_state_key,
)


class JsonFileStateStore(StateStore):
    """Persist all connector checkpoints in one small JSON document.

    Writes use ``os.replace`` so readers never observe a partially-written
    file. On POSIX, a small sidecar advisory lock coordinates independent
    writers/CAS operations. Plain reads need no process lock because promotion
    is atomic, which keeps diagnostics such as ``probe()`` filesystem-read-only
    when no state file exists. Distributed Lakehouse workloads should prefer
    DeltaStateStore rather than mounted-filesystem locking semantics.
    """

    def __init__(self, path: str | Path) -> None:
        self._path = Path(path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._lock_path = self._path.with_name(f".{self._path.name}.lock")
        self._lock = threading.RLock()

    @property
    def supports_atomic_compare_and_set(self) -> bool:
        return os.name != "nt"

    @contextmanager
    def _process_lock(self) -> Iterator[None]:
        """Coordinate local POSIX writers; use the thread lock elsewhere."""
        if os.name == "nt":
            yield
            return

        import fcntl

        with self._lock_path.open("a+b") as handle:
            try:
                self._lock_path.chmod(0o600)
            except OSError:
                pass
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)

    def _read_all_unlocked(self) -> dict[str, dict[str, str | None]]:
        if not self._path.exists():
            return {}
        with self._path.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
        if not isinstance(data, dict):
            raise ValueError(f"State file invalido: {self._path}")
        return data

    @staticmethod
    def _watermark_from_item(item: dict[str, str | None]) -> Watermark:
        return Watermark(
            last_run_at=datetime.fromisoformat(str(item["last_run_at"])),
            last_data_date=(
                date.fromisoformat(str(item["last_data_date"]))
                if item.get("last_data_date")
                else None
            ),
            cursor_value=(
                str(item["cursor_value"])
                if item.get("cursor_value") is not None
                else None
            ),
        )

    @staticmethod
    def _item_from_watermark(watermark: Watermark) -> dict[str, str | None]:
        return {
            "last_run_at": watermark.last_run_at.isoformat(),
            "last_data_date": (
                watermark.last_data_date.isoformat() if watermark.last_data_date else None
            ),
            "cursor_value": watermark.cursor_value,
        }

    def _write_all_unlocked(self, data: dict[str, dict[str, str | None]]) -> None:
        fd, tmp_name = tempfile.mkstemp(
            prefix=f".{self._path.name}.",
            suffix=".tmp",
            dir=str(self._path.parent),
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(data, handle, ensure_ascii=False, indent=2, sort_keys=True)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp_name, self._path)
            if os.name != "nt":
                self._path.chmod(0o600)
        finally:
            if os.path.exists(tmp_name):
                os.unlink(tmp_name)

    def get_watermark(self, connector_name: str) -> Watermark | None:
        key = validate_state_key(connector_name)
        # Atomic os.replace means an unlocked process reader sees either the old
        # complete document or the new complete document, never a partial file.
        # RLock still protects same-instance thread interactions.
        with self._lock:
            item = self._read_all_unlocked().get(key)
        return self._watermark_from_item(item) if item is not None else None

    def set_watermark(self, connector_name: str, watermark: Watermark) -> None:
        key = validate_state_key(connector_name)
        with self._lock, self._process_lock():
            data = self._read_all_unlocked()
            data[key] = self._item_from_watermark(watermark)
            self._write_all_unlocked(data)

    def compare_and_set_watermark(
        self,
        connector_name: str,
        expected: Watermark | None,
        watermark: Watermark,
    ) -> None:
        key = validate_state_key(connector_name)
        with self._lock, self._process_lock():
            data = self._read_all_unlocked()
            item = data.get(key)
            current = self._watermark_from_item(item) if item is not None else None
            if current != expected:
                raise StateConflictError(
                    f"Checkpoint de '{key}' mudou durante a execucao; "
                    "o commit concorrente foi recusado."
                )
            data[key] = self._item_from_watermark(watermark)
            self._write_all_unlocked(data)


__all__ = ["JsonFileStateStore"]
