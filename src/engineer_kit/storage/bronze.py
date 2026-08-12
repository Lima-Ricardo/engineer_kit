"""Shared Bronze row preparation used by every destination adapter.

The physical Bronze contract is deliberately simple and stable: all declared
API fields are persisted as strings, missing fields become null, unknown fields
are preserved in ``_extra``, and the original record is retained in ``_raw``.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from engineer_kit.storage.flatten import flatten_record
from engineer_kit.storage.schema import EndpointSchema

METADATA_COLUMNS = ["_source", "_endpoint", "_ingested_at", "_raw", "_extra"]


def _bronze_scalar(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, default=str)
    return str(value)


def build_bronze_rows(
    connector_name: str,
    endpoint: str,
    schema: EndpointSchema,
    batch: list[dict[str, Any]],
    *,
    ingested_at: datetime | None = None,
) -> tuple[list[dict[str, Any]], set[str]]:
    """Normalize one batch into the portable Bronze row contract."""
    column_names = schema.column_names()
    known_columns = set(column_names)
    timestamp = ingested_at or datetime.now(timezone.utc)
    extra_fields: set[str] = set()
    rows: list[dict[str, Any]] = []

    for original in batch:
        flat = flatten_record(original)
        extras = {key: value for key, value in flat.items() if key not in known_columns}
        extra_fields.update(extras.keys())

        row = {column: _bronze_scalar(flat.get(column)) for column in column_names}
        row["_source"] = connector_name
        row["_endpoint"] = endpoint
        row["_ingested_at"] = timestamp
        row["_raw"] = json.dumps(original, ensure_ascii=False, default=str)
        row["_extra"] = json.dumps(extras, ensure_ascii=False, default=str) if extras else None
        rows.append(row)

    return rows, extra_fields
