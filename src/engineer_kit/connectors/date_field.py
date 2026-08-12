"""Extrai e interpreta a data de um registro para o incremental por
DATA_DATE.

Aceita um caminho de chaves separado por ponto (ex.: "commit.author.date")
ou uma funcao para casos fora do padrao. Atencao: o separador aqui e
ponto, diferente do "_" usado nos nomes de coluna achatados pelo
flatten/EndpointSchema — date_field navega o registro *antes* do
flatten (a forma nativa da API), enquanto o schema descreve a forma
*depois* do flatten. Sao dois pontos diferentes do pipeline.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any, Callable, Optional, Union

DateFieldSpec = Union[str, Callable[[dict], Any]]


def extract_date_value(record: dict[str, Any], spec: DateFieldSpec) -> Optional[date]:
    return _parse_date(_get_raw_value(record, spec))


def _get_raw_value(record: dict[str, Any], spec: DateFieldSpec) -> Any:
    if callable(spec):
        return spec(record)
    current: Any = record
    for key in spec.split("."):
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def _parse_date(value: Any) -> Optional[date]:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if not isinstance(value, str) or not value:
        return None
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(text).date()
    except ValueError:
        pass
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None
