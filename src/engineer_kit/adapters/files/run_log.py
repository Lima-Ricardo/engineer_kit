"""JSON Lines implementation of the RunLogBackend contract."""

from __future__ import annotations

import json
import threading
from dataclasses import asdict
from pathlib import Path

from engineer_kit.storage.run_log import RunLogBackend, RunLogEntry


class JsonLinesRunLogStore(RunLogBackend):
    """Append audit events to a human-readable JSONL file."""

    def __init__(self, path: str | Path) -> None:
        self._path = Path(path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()

    def record(self, entry: RunLogEntry) -> None:
        payload = asdict(entry)
        payload["started_at"] = entry.started_at.isoformat()
        payload["finished_at"] = entry.finished_at.isoformat()
        line = json.dumps(payload, ensure_ascii=False, default=str)
        with self._lock, self._path.open("a", encoding="utf-8") as handle:
            handle.write(line + "\n")
            handle.flush()
