"""Declarative builders for the local/mounted-filesystem Parquet runtime."""

from __future__ import annotations

from pathlib import Path

from engineer_kit.adapters.files.run_log import JsonLinesRunLogStore
from engineer_kit.adapters.files.state_store import JsonFileStateStore
from engineer_kit.adapters.parquet.destination import ParquetDestination
from engineer_kit.storage.destination import WriteMode


def _root(context) -> Path:
    path = getattr(context.destination_config, "path", None)
    if not path:
        raise ValueError("destination.path e obrigatorio para o adapter parquet.")
    if "://" in str(path):
        raise ValueError(
            "ParquetDestination usa filesystem local/montado. Para URIs de Lakehouse use o adapter delta."
        )
    return Path(path)


def build_destination(config, context):
    root = _root(context)
    schema_name = getattr(config, "schema", "bronze")
    options = getattr(config, "options", {}) or {}
    return ParquetDestination(
        root / schema_name,
        batch_size=getattr(config, "batch_size", 5000),
        compression=options.get("compression", "snappy"),
        write_mode=WriteMode.parse(getattr(config, "write_mode", "append")),
    )


def build_state_store(config, context):
    explicit = getattr(config, "path", None)
    path = Path(explicit) if explicit else _root(context) / "_meta" / "ingestion_state.json"
    return JsonFileStateStore(path)


def build_run_log(config, context):
    explicit = getattr(config, "path", None)
    path = Path(explicit) if explicit else _root(context) / "_meta" / "run_log.jsonl"
    return JsonLinesRunLogStore(path)
