"""Shared PyArrow helpers for Parquet and Delta adapters.

This module is outside the core import graph: importing ``engineer_kit`` does
not require PyArrow until an Arrow-based adapter is selected.
"""

from __future__ import annotations

from typing import Any

import pyarrow as pa

from engineer_kit.storage.schema import EndpointSchema


def bronze_arrow_schema(schema: EndpointSchema) -> pa.Schema:
    """Physical Bronze schema shared by Parquet and Delta."""
    fields = [pa.field(column.name, pa.string()) for column in schema.columns]
    fields.extend(
        [
            pa.field("_source", pa.string(), nullable=False),
            pa.field("_endpoint", pa.string(), nullable=False),
            pa.field("_ingested_at", pa.timestamp("us", tz="UTC"), nullable=False),
            pa.field("_run_id", pa.string(), nullable=False),
            pa.field("_ingestion_key", pa.string(), nullable=False),
            pa.field("_window_start", pa.date32()),
            pa.field("_window_end", pa.date32()),
            pa.field("_raw", pa.string(), nullable=False),
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
