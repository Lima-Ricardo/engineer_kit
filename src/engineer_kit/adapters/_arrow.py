"""Shared Arrow conversion for Parquet and Delta adapters."""

from __future__ import annotations

from typing import Any

import pyarrow as pa

from engineer_kit.storage.schema import EndpointSchema


def bronze_arrow_schema(schema: EndpointSchema) -> pa.Schema:
    """Physical Bronze schema: API fields are strings until analytical staging."""
    fields = [pa.field(column.name, pa.string()) for column in schema.columns]
    fields.extend(
        [
            pa.field("_source", pa.string()),
            pa.field("_endpoint", pa.string()),
            pa.field("_ingested_at", pa.timestamp("us", tz="UTC")),
            pa.field("_raw", pa.string()),
            pa.field("_extra", pa.string()),
        ]
    )
    return pa.schema(fields)


def rows_to_record_batch(
    rows: list[dict[str, Any]], schema: EndpointSchema
) -> pa.RecordBatch:
    return pa.RecordBatch.from_pylist(rows, schema=bronze_arrow_schema(schema))


def rows_to_table(rows: list[dict[str, Any]], schema: EndpointSchema) -> pa.Table:
    return pa.Table.from_pylist(rows, schema=bronze_arrow_schema(schema))
