"""Small, pure helpers for the intent-driven public connector API."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from typing import Any, Iterable
from urllib.parse import urlsplit

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_UNSAFE_ALIAS_CHARS = re.compile(r"[^A-Za-z0-9_]+")


@dataclass(frozen=True)
class FieldSelection:
    """One source path projected to one deterministic output alias."""

    path: str
    alias: str


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


def parse_path(path: str) -> tuple[str | int, ...]:
    """Parse dotted paths plus bracket keys/indexes without evaluating code.

    Supported examples::

        data.orders
        data.items[0].id
        payload["odd.key"].value
        payload['odd-key']

    Wildcards are intentionally not supported because a field selector must
    resolve to one value per source record. Collections are handled by the
    records selector instead of changing row cardinality implicitly.
    """
    text = str(path).strip()
    if not text:
        raise ValueError("path nao pode ser vazio.")

    tokens: list[str | int] = []
    i = 0
    n = len(text)
    expect_segment = True

    while i < n:
        if text[i] == ".":
            if expect_segment:
                raise ValueError(f"path invalido: {path!r}.")
            expect_segment = True
            i += 1
            continue

        if text[i] == "[":
            end = text.find("]", i + 1)
            if end < 0:
                raise ValueError(f"path invalido, colchete nao fechado: {path!r}.")
            raw = text[i + 1 : end].strip()
            if not raw:
                raise ValueError(f"path invalido, indice vazio: {path!r}.")
            if raw[0:1] in {"'", '"'}:
                quote = raw[0]
                if len(raw) < 2 or raw[-1] != quote:
                    raise ValueError(f"path invalido, chave entre aspas malformada: {path!r}.")
                key = raw[1:-1]
                if not key:
                    raise ValueError(f"path invalido, chave vazia: {path!r}.")
                tokens.append(key)
            else:
                try:
                    index = int(raw)
                except ValueError as exc:
                    raise ValueError(
                        f"path invalido, use indice inteiro ou chave entre aspas: {path!r}."
                    ) from exc
                if index < 0:
                    raise ValueError("indices negativos nao sao suportados em paths declarativos.")
                tokens.append(index)
            i = end + 1
            expect_segment = False
            continue

        start = i
        while i < n and text[i] not in ".[":
            i += 1
        segment = text[start:i].strip()
        if not segment:
            raise ValueError(f"path invalido: {path!r}.")
        tokens.append(segment)
        expect_segment = False

    if expect_segment:
        raise ValueError(f"path invalido: {path!r}.")
    return tuple(tokens)


def read_path(value: Any, path: str) -> Any:
    current = value
    for key in parse_path(path):
        if isinstance(key, int):
            if not isinstance(current, list) or key >= len(current):
                return None
            current = current[key]
            continue
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


def _default_alias(path: str) -> str:
    tokens = parse_path(path)
    raw = "_".join(str(token) for token in tokens)
    alias = _UNSAFE_ALIAS_CHARS.sub("_", raw).strip("_")
    if not alias:
        alias = "field"
    if alias[0].isdigit():
        alias = f"_{alias}"
    return alias


def _validate_alias(alias: str) -> str:
    value = str(alias).strip()
    if _IDENTIFIER_RE.fullmatch(value) is None:
        raise ValueError(
            f"alias de select invalido: {alias!r}. Use letras, numeros e '_' e nao inicie por numero."
        )
    return value


def _iter_select_values(
    value: list[str] | tuple[str, ...] | str | dict[str, str],
) -> Iterable[tuple[str, str | None]]:
    if isinstance(value, dict):
        for path, alias in value.items():
            yield str(path).strip(), str(alias).strip()
        return
    values = value.split(",") if isinstance(value, str) else value
    for item in values:
        path = str(item).strip()
        if path:
            yield path, None


def resolve_select(
    value: list[str] | tuple[str, ...] | str | dict[str, str] | None,
) -> tuple[FieldSelection, ...] | None:
    if value is None:
        return None
    if not isinstance(value, (dict, list, tuple, str)):
        raise TypeError("select deve ser string, lista/tupla ou mapping path->alias.")

    selections: list[FieldSelection] = []
    seen_paths: set[str] = set()
    alias_to_path: dict[str, str] = {}
    for path, explicit_alias in _iter_select_values(value):
        if not path:
            continue
        parse_path(path)  # validate once during setup, never per record
        alias = _validate_alias(explicit_alias) if explicit_alias else _default_alias(path)
        previous = alias_to_path.get(alias)
        if previous is not None and previous != path:
            raise ValueError(
                f"select gera colisao no alias '{alias}' entre '{previous}' e '{path}'. "
                "Use select={path: alias} para desambiguar."
            )
        alias_to_path[alias] = path
        if path in seen_paths:
            continue
        seen_paths.add(path)
        selections.append(FieldSelection(path=path, alias=alias))
    return tuple(selections) or None


def project(record: dict[str, Any], fields: tuple[FieldSelection, ...] | None) -> dict[str, Any]:
    if not fields:
        return record
    return {field.alias: read_path(record, field.path) for field in fields}


__all__ = [
    "FieldSelection",
    "as_date",
    "infer_name",
    "parse_path",
    "pick_records_path",
    "project",
    "read_path",
    "resolve_select",
]
