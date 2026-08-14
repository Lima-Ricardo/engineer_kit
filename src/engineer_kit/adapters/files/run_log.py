"""JSON Lines implementation of the RunLogBackend contract."""

from __future__ import annotations

import json
import os
import threading
from contextlib import contextmanager
from dataclasses import asdict
from pathlib import Path
from typing import Iterator

from engineer_kit.storage.run_log import RunLogBackend, RunLogEntry


class JsonLinesRunLogStore(RunLogBackend):
    """Append audit events to a human-readable JSONL file safely across processes."""

    def __init__(self, path: str | Path) -> None:
        self._path = Path(path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._lock_path = self._path.with_name(f".{self._path.name}.lock")
        self._lock = threading.RLock()

    @contextmanager
    def _process_lock(self) -> Iterator[None]:
        with self._lock_path.open("a+b") as handle:
            if os.name == "nt":
                import msvcrt

                # msvcrt.locking locks bytes from the current file position.
                # Keep a permanent sentinel byte so every process contends on
                # exactly the same range even when the lock file was just created.
                handle.seek(0, os.SEEK_END)
                if handle.tell() == 0:
                    handle.write(b"\0")
                    handle.flush()
                    os.fsync(handle.fileno())
                handle.seek(0)
                msvcrt.locking(handle.fileno(), msvcrt.LK_LOCK, 1)
                try:
                    yield
                finally:
                    handle.seek(0)
                    msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
                return

            import fcntl

            try:
                self._lock_path.chmod(0o600)
            except OSError:
                pass
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)

    def record(self, entry: RunLogEntry) -> None:
        payload = asdict(entry)
        payload["started_at"] = entry.started_at.isoformat()
        payload["finished_at"] = entry.finished_at.isoformat()
        line = json.dumps(payload, ensure_ascii=False, default=str)
        with self._lock, self._process_lock(), self._path.open("a", encoding="utf-8") as handle:
            handle.write(line + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        if os.name != "nt":
            self._path.chmod(0o600)


__all__ = ["JsonLinesRunLogStore"]
