"""Flatten nested API records into deterministic Bronze column names.

Nested dict keys are joined with ``_``. Lists are preserved as JSON instead of
being exploded implicitly so row cardinality never changes behind the user's
back. Most importantly, two distinct source paths are never allowed to collapse
onto the same normalized SQL identifier: collisions fail fast instead of
silently overwriting data.
"""

from __future__ import annotations

import json
import re
from typing import Any

_UNSAFE_CHARS = re.compile(r"[^A-Za-z0-9_]")


class FlattenCollisionError(ValueError):
    """Raised when two different JSON paths normalize to one Bronze column."""


def _normalize_column_name(name: str) -> str:
    """Return a deterministic SQL-safe identifier for an external JSON path."""
    normalized = _UNSAFE_CHARS.sub("_", name)
    if not normalized or normalized[0].isdigit():
        normalized = f"_{normalized}"
    return normalized


def flatten_record(record: dict[str, Any], sep: str = "_") -> dict[str, Any]:
    flat: dict[str, Any] = {}
    sources: dict[str, str] = {}
    _flatten_into(record, prefix="", source_path="", sep=sep, out=flat, sources=sources)
    return flat


def _flatten_into(
    value: Any,
    prefix: str,
    source_path: str,
    sep: str,
    out: dict[str, Any],
    sources: dict[str, str],
) -> None:
    if isinstance(value, dict):
        for key, child in value.items():
            key_text = str(key)
            child_prefix = f"{prefix}{sep}{key_text}" if prefix else key_text
            child_source = f"{source_path}.{key_text}" if source_path else key_text
            _flatten_into(
                child,
                prefix=child_prefix,
                source_path=child_source,
                sep=sep,
                out=out,
                sources=sources,
            )
        return

    column = _normalize_column_name(prefix)
    previous_source = sources.get(column)
    if previous_source is not None and previous_source != source_path:
        raise FlattenCollisionError(
            f"Paths JSON distintos '{previous_source}' e '{source_path}' geram a mesma "
            f"coluna Bronze '{column}'. Renomeie/selecione os campos explicitamente."
        )
    sources[column] = source_path

    if isinstance(value, list):
        out[column] = json.dumps(value, ensure_ascii=False, default=str)
    else:
        out[column] = value


__all__ = ["FlattenCollisionError", "flatten_record"]
