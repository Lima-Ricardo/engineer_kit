"""Atomic JSON-file StateStore for local or mounted filesystems."""

from __future__ import annotations

import json
import os
import tempfile
import threading
from datetime import date, datetime
from pathlib import Path

from engineer_kit.storage.state_store import StateStore, Watermark


class JsonFileStateStore(StateStore):
    """Persist all connector checkpoints in one small JSON document.

    Writes use ``os.replace`` so readers never observe a partially-written
    file. This adapter is intended for local development or a mounted
    filesystem; distributed Lakehouse workloads should prefer DeltaStateStore.
    """

    def __init__(self, path: str | Path) -> None:
        self._path = Path(path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()

    def _read_all(self) -> dict[str, dict[str, str | None]]:
        if not self._path.exists():
            return {}
        with self._path.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
        if not isinstance(data, dict):
            raise ValueError(f"State file invalido: {self._path}")
        return data

    def get_watermark(self, connector_name: str) -> Watermark | None:
        with self._lock:
            item = self._read_all().get(connector_name)
        if item is None:
            return None
        return Watermark(
            last_run_at=datetime.fromisoformat(str(item["last_run_at"])),
            last_data_date=(
                date.fromisoformat(str(item["last_data_date"]))
                if item.get("last_data_date")
                else None
            ),
            cursor_value=(str(item["cursor_value"]) if item.get("cursor_value") is not None else None),
        )

    def set_watermark(self, connector_name: str, watermark: Watermark) -> None:
        with self._lock:
            data = self._read_all()
            data[connector_name] = {
                "last_run_at": watermark.last_run_at.isoformat(),
                "last_data_date": (
                    watermark.last_data_date.isoformat() if watermark.last_data_date else None
                ),
                "cursor_value": watermark.cursor_value,
            }
            fd, tmp_name = tempfile.mkstemp(
                prefix=f".{self._path.name}.", suffix=".tmp", dir=str(self._path.parent)
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
