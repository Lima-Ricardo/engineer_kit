"""Declarative builders for Delta/Lakehouse deployments."""

from __future__ import annotations

from engineer_kit.adapters.delta._paths import join_table_uri
from engineer_kit.adapters.delta.destination import DeltaDestination
from engineer_kit.adapters.delta.run_log import DeltaRunLogStore
from engineer_kit.adapters.delta.state_store import DeltaStateStore
from engineer_kit.storage.destination import WriteMode


def _root(context) -> str:
    path = getattr(context.destination_config, "path", None)
    if not path:
        raise ValueError("destination.path e obrigatorio para o adapter delta.")
    return str(path)


def _storage_options(config, context) -> dict[str, str]:
    """Merge destination credentials/options with state/audit-specific overrides."""
    destination_options = getattr(context.destination_config, "options", {}) or {}
    specific_options = getattr(config, "options", {}) or {}
    merged = dict(destination_options.get("storage_options", {}) or {})
    merged.update(specific_options.get("storage_options", {}) or {})
    return {str(key): str(value) for key, value in merged.items()}


def build_destination(config, context):
    root = _root(context)
    schema_name = getattr(config, "schema", "bronze")
    options = getattr(config, "options", {}) or {}
    return DeltaDestination(
        join_table_uri(root, schema_name),
        batch_size=getattr(config, "batch_size", 5000),
        storage_options=_storage_options(config, context),
        partition_by=list(getattr(config, "partition_by", []) or []),
        write_mode=WriteMode.parse(getattr(config, "write_mode", "append")),
        target_file_size=options.get("target_file_size"),
        metadata_base_uri=root,
    )


def build_state_store(config, context):
    base_uri = getattr(config, "path", None) or _root(context)
    return DeltaStateStore(
        base_uri,
        storage_options=_storage_options(config, context),
    )


def build_run_log(config, context):
    base_uri = getattr(config, "path", None) or _root(context)
    return DeltaRunLogStore(
        base_uri,
        storage_options=_storage_options(config, context),
    )
