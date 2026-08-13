"""Logical analytical types independent from a physical storage engine.

Bronze ingestion deliberately stores API values as strings. ``ColumnSpec.dtype``
therefore describes the type expected by a downstream staging/transform layer,
not the physical Bronze type. Known logical names are rendered per SQL dialect;
legacy SQL type strings remain supported for backwards compatibility.
"""

from __future__ import annotations

from enum import Enum

from engineer_kit.storage.identifiers import validate_type


class LogicalType(str, Enum):
    STRING = "string"
    INTEGER = "integer"
    BIGINT = "bigint"
    FLOAT = "float"
    DECIMAL = "decimal"
    BOOLEAN = "boolean"
    DATE = "date"
    TIMESTAMP = "timestamp"
    JSON = "json"


_ALIASES = {
    "str": LogicalType.STRING,
    "string": LogicalType.STRING,
    "varchar": LogicalType.STRING,
    "text": LogicalType.STRING,
    "int": LogicalType.INTEGER,
    "integer": LogicalType.INTEGER,
    "int32": LogicalType.INTEGER,
    "bigint": LogicalType.BIGINT,
    "int64": LogicalType.BIGINT,
    "float": LogicalType.FLOAT,
    "double": LogicalType.FLOAT,
    "double precision": LogicalType.FLOAT,
    "bool": LogicalType.BOOLEAN,
    "boolean": LogicalType.BOOLEAN,
    "date": LogicalType.DATE,
    "timestamp": LogicalType.TIMESTAMP,
    "datetime": LogicalType.TIMESTAMP,
    "json": LogicalType.JSON,
}

_DIALECTS: dict[str, dict[LogicalType, str]] = {
    "duckdb": {
        LogicalType.STRING: "VARCHAR",
        LogicalType.INTEGER: "INTEGER",
        LogicalType.BIGINT: "BIGINT",
        LogicalType.FLOAT: "DOUBLE",
        LogicalType.DECIMAL: "DECIMAL",
        LogicalType.BOOLEAN: "BOOLEAN",
        LogicalType.DATE: "DATE",
        LogicalType.TIMESTAMP: "TIMESTAMP",
        LogicalType.JSON: "JSON",
    },
    "spark": {
        LogicalType.STRING: "STRING",
        LogicalType.INTEGER: "INT",
        LogicalType.BIGINT: "BIGINT",
        LogicalType.FLOAT: "DOUBLE",
        LogicalType.DECIMAL: "DECIMAL",
        LogicalType.BOOLEAN: "BOOLEAN",
        LogicalType.DATE: "DATE",
        LogicalType.TIMESTAMP: "TIMESTAMP",
        LogicalType.JSON: "STRING",
    },
}


def resolve_logical_type(dtype: str) -> LogicalType | None:
    """Resolve a portable logical type when ``dtype`` is a known alias."""
    return _ALIASES.get(dtype.strip().lower())


def render_sql_type(dtype: str, dialect: str = "duckdb") -> str:
    """Render ``dtype`` for a target SQL dialect.

    Unknown/legacy SQL type expressions are validated and returned unchanged,
    so existing configs such as ``DECIMAL(18, 2)`` keep working.
    """
    logical = resolve_logical_type(dtype)
    if logical is None:
        return validate_type(dtype)
    mapping = _DIALECTS.get(dialect.lower())
    if mapping is None:
        raise ValueError(f"Dialeto SQL '{dialect}' nao suportado.")
    return mapping[logical]
