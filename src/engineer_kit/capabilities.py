"""Serializable capability metadata shared by CLI, YAML tooling and Local Lab.

This is deliberately descriptive rather than executable business logic. The
runtime contracts remain the source of behavior; this manifest gives visual and
declarative clients one place to discover supported choices and adapter-specific
configuration fields without duplicating hard-coded lists in templates.
"""

from __future__ import annotations

from copy import deepcopy
from typing import Any

from engineer_kit.adapters.registry import available_adapters
from engineer_kit.config.pipeline_config import CURRENT_PIPELINE_CONFIG_VERSION
from engineer_kit.connectors.api_connector import VALID_HTTP_METHODS
from engineer_kit.connectors.pagination import STANDARD_PAGINATION_TYPES

_BUILTIN_DESTINATIONS: dict[str, dict[str, Any]] = {
    "duckdb": {
        "label": "DuckDB",
        "requires_runtime": True,
        "path_kind": "file",
        "fields": {
            "schema": {"type": "string", "default": "bronze"},
            "batch_size": {"type": "integer", "default": 5000, "minimum": 1},
            "write_mode": {"type": "choice", "choices": ["append", "overwrite"]},
        },
    },
    "parquet": {
        "label": "Parquet",
        "requires_runtime": False,
        "path_kind": "directory",
        "fields": {
            "path": {"type": "path", "required": True},
            "schema": {"type": "string", "default": "bronze"},
            "batch_size": {"type": "integer", "default": 5000, "minimum": 1},
            "write_mode": {"type": "choice", "choices": ["append", "overwrite"]},
            "options.compression": {"type": "string", "default": "snappy"},
        },
    },
    "delta": {
        "label": "Delta Lake",
        "requires_runtime": False,
        "path_kind": "uri_or_path",
        "fields": {
            "path": {"type": "path_or_uri", "required": True},
            "schema": {"type": "string", "default": "bronze"},
            "batch_size": {"type": "integer", "default": 5000, "minimum": 1},
            "write_mode": {"type": "choice", "choices": ["append", "overwrite"]},
            "partition_by": {"type": "string_list", "default": []},
            "options.target_file_size": {"type": "integer", "required": False},
            "options.storage_options": {"type": "secret_mapping", "required": False},
        },
    },
}


def capability_manifest() -> dict[str, Any]:
    """Return a JSON-serializable snapshot of currently registered capabilities."""
    registered = available_adapters()
    destinations: dict[str, Any] = {}
    for name in registered.get("destination", []):
        destinations[name] = deepcopy(
            _BUILTIN_DESTINATIONS.get(
                name,
                {
                    "label": name,
                    "requires_runtime": None,
                    "path_kind": "adapter_defined",
                    "fields": {},
                },
            )
        )

    return {
        "config_version": CURRENT_PIPELINE_CONFIG_VERSION,
        "connector": {
            "type": "rest",
            "methods": list(VALID_HTTP_METHODS),
            "intent_fields": [
                "base_url",
                "auth",
                "records",
                "select",
                "params",
                "pagination",
                "incremental",
            ],
            "auth": ["none", "bearer", "api_key"],
            "pagination": list(STANDARD_PAGINATION_TYPES),
            "incremental": ["none", "ingestion_date", "data_date"],
            "preview": True,
        },
        "destinations": destinations,
        "state_stores": list(registered.get("state_store", [])),
        "run_logs": list(registered.get("run_log", [])),
        "transforms": {
            "none": {},
            "dbt": {
                "commands": ["run", "build", "test", "compile", "seed", "snapshot"],
            },
        },
    }


__all__ = ["capability_manifest"]
