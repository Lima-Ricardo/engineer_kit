"""Achata um dict aninhado em um unico nivel, unindo as chaves com "_".

Deliberadamente feito em Python puro — nao usa o unnest nativo do
DuckDB. O unnest recursivo do DuckDB resolve colisao de nome de campo
(ex.: commit.author.date vs commit.committer.date) por ordem de
aparicao no dict (name, name_1, name_2...), nao por caminho: e
imprevisivel e quebra silenciosamente se a API reordenar ou adicionar
campos. Aqui o nome final da coluna e sempre deterministico: o caminho
completo unido por "_" (ex.: commit_author_date).

Listas nao sao explodidas em sub-tabelas nesta versao — viram uma
string JSON. Isso evita mudar a cardinalidade de linhas de forma
implicita, mas e uma limitacao conhecida: uma lista de objetos vira
texto opaco no bronze, exigindo um `json_extract` manual no dbt se
precisar dela desaninhada.
"""

from __future__ import annotations

import json
import re
from typing import Any

_UNSAFE_CHARS = re.compile(r"[^A-Za-z0-9_]")


def _normalize_column_name(name: str) -> str:
    """Garante que o nome da coluna e um identificador SQL seguro, mesmo
    quando o nome vem de uma chave JSON controlada pela API externa."""
    normalized = _UNSAFE_CHARS.sub("_", name)
    if not normalized or normalized[0].isdigit():
        normalized = f"_{normalized}"
    return normalized


def flatten_record(record: dict[str, Any], sep: str = "_") -> dict[str, Any]:
    flat: dict[str, Any] = {}
    _flatten_into(record, prefix="", sep=sep, out=flat)
    return flat


def _flatten_into(value: Any, prefix: str, sep: str, out: dict[str, Any]) -> None:
    if isinstance(value, dict):
        for key, child in value.items():
            child_prefix = f"{prefix}{sep}{key}" if prefix else key
            _flatten_into(child, child_prefix, sep, out)
        return
    column = _normalize_column_name(prefix)
    if isinstance(value, list):
        out[column] = json.dumps(value, ensure_ascii=False)
    else:
        out[column] = value
