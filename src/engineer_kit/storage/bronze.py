"""Shared Bronze record preparation used by every destination adapter.

This module owns the ingestion contract that must stay identical across
DuckDB, Parquet and Delta: declared columns are stable, missing declared
fields become null, fields outside the declared schema are preserved in
``_extra``, and the original record remains available in ``_raw``.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from engineer_kit.storage.flatten import flatten_record
from engineer_kit.storage.schema import EndpointSchema

METADATA_COLUMNS = ["_source", "_endpoint", "_ingested_at", "_raw", "_extra"]


def build_bronze_rows(
    connector_name: str,
    endpoint: str,
    schema: EndpointSchema,
    batch: list[dict[str, Any]],
    *,
    ingested_at: datetime | None = None,
) -> tuple[list[dict[str, Any]], set[str]]:
    """Normalize one batch into the stable Bronze row contract."""
    known_columns = set(schema.column_names())
    timestamp = ingested_at or datetime.now(timezone.utc)
    extra_fields: set[str] = set()
    rows: list[dict[str, Any]] = []

    for original in batch:
        flat = flatten_record(original)
        extras = {key: value for key, value in flat.items() if key not in known_columns}
        extra_fields.update(extras.keys())

        row = {column: flat.get(column) for column in schema.column_names()}
        row["_source"] = connector_name
        row["_endpoint"] = endpoint
        row["_ingested_at"] = timestamp
        row["_raw"] = json.dumps(original, ensure_ascii=False, default=str)
        row["_extra"] = json.dumps(extras, ensure_ascii=False, default=str) if extras else None
        rows.append(row)

    return rows, extra_fields
