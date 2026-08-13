"""Registry builders for dependency-free metadata files."""

from __future__ import annotations

from pathlib import Path

from engineer_kit.adapters.files.run_log import JsonLinesRunLogStore
from engineer_kit.adapters.files.state_store import JsonFileStateStore


def _metadata_root(config, context) -> Path:
    explicit = getattr(config, "path", None)
    if explicit:
        path = Path(explicit)
        return path.parent if path.suffix else path

    destination_path = getattr(context.destination_config, "path", None)
    if not destination_path or "://" in str(destination_path):
        raise ValueError(
            "O adapter 'file' precisa de state/run_log.path explicito quando o destino nao usa um path local."
        )
    path = Path(destination_path)
    return path.parent if path.suffix else path


def build_state_store(config, context):
    explicit = getattr(config, "path", None)
    path = Path(explicit) if explicit else _metadata_root(config, context) / "_meta" / "ingestion_state.json"
    return JsonFileStateStore(path)


def build_run_log(config, context):
    explicit = getattr(config, "path", None)
    path = Path(explicit) if explicit else _metadata_root(config, context) / "_meta" / "run_log.jsonl"
    return JsonLinesRunLogStore(path)
