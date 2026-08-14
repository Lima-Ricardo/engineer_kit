"""Small, pure helpers for the intent-driven public connector API."""

from __future__ import annotations

import re
from datetime import date
from typing import Any
from urllib.parse import urlsplit


def infer_name(base_url: str) -> str:
    parsed = urlsplit(base_url)
    parts = [part for part in parsed.path.split("/") if part]
    value = parts[-1] if parts else (parsed.hostname or "api").split(".")[0]
    value = re.sub(r"[^A-Za-z0-9_]+", "_", value).strip("_") or "api"
    return f"api_{value}" if value[0].isdigit() else value


def as_date(value: date | str | None) -> date | None:
    if value is None or isinstance(value, date):
        return value
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError("initial_start deve ser date ou YYYY-MM-DD.") from exc


def read_path(value: Any, path: str) -> Any:
    current = value
    for key in path.split("."):
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def _list_candidates(value: Any, prefix: str = "", depth: int = 0) -> list[str]:
    if not isinstance(value, dict) or depth > 3:
        return []
    result: list[str] = []
    for key, nested in value.items():
        path = f"{prefix}.{key}" if prefix else str(key)
        if isinstance(nested, list) and (not nested or all(isinstance(item, dict) for item in nested)):
            result.append(path)
        elif isinstance(nested, dict):
            result.extend(_list_candidates(nested, path, depth + 1))
    return result


def pick_records_path(raw: dict[str, Any]) -> str | None:
    candidates = _list_candidates(raw)
    if not candidates:
        return None
    preferred = {"data": 0, "results": 0, "items": 0, "records": 0, "value": 1}
    ranked = sorted(
        candidates,
        key=lambda path: (preferred.get(path.split(".")[-1].lower(), 10), path.count("."), path),
    )
    if len(ranked) == 1:
        return ranked[0]
    first = (preferred.get(ranked[0].split(".")[-1].lower(), 10), ranked[0].count("."))
    second = (preferred.get(ranked[1].split(".")[-1].lower(), 10), ranked[1].count("."))
    if first < second:
        return ranked[0]
    raise ValueError(
        "Lista de registros ambigua. Passe records='caminho.da.lista'. "
        f"Candidatos: {', '.join(ranked[:8])}."
    )


def resolve_select(value: list[str] | tuple[str, ...] | str | None) -> tuple[str, ...] | None:
    if value is None:
        return None
    values = value.split(",") if isinstance(value, str) else value
    fields = [str(item).strip() for item in values if str(item).strip()]
    return tuple(dict.fromkeys(fields)) or None


def project(record: dict[str, Any], fields: tuple[str, ...] | None) -> dict[str, Any]:
    if not fields:
        return record
    result: dict[str, Any] = {}
    for field in fields:
        result[field.replace(".", "_")] = read_path(record, field) if "." in field else record.get(field)
    return result
