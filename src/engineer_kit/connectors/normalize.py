"""Normalizacao de valores: todo dado que sai de um conector vira string
(exceto None, que vira NULL de verdade no DuckDB). Isso evita que schema
drift na origem (API muda um campo de int para string, por exemplo)
quebre a ingestao — a tipagem correta acontece depois, no staging do dbt.
"""

from __future__ import annotations

from typing import Any


def stringify(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, dict):
        return {key: stringify(v) for key, v in value.items()}
    if isinstance(value, list):
        return [stringify(v) for v in value]
    if isinstance(value, str):
        return value
    return str(value)
