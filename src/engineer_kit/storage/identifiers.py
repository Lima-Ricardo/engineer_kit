"""Validacao de identificadores SQL seguros, usada pelo schema declarado
e pelos destinos (DuckDB hoje, outros depois)."""

from __future__ import annotations

import re

_VALID_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_VALID_TYPE = re.compile(r"^[A-Za-z0-9_(), ]+$")


class InvalidIdentifierError(ValueError):
    """Levantado quando um nome de schema/tabela/coluna ou tipo nao e seguro para SQL."""


def validate_identifier(name: str, what: str) -> str:
    if not _VALID_IDENTIFIER.match(name):
        raise InvalidIdentifierError(
            f"{what} '{name}' nao e um identificador SQL seguro. "
            "Use apenas letras, numeros e underscore, comecando com letra ou underscore."
        )
    return name


def validate_type(dtype: str) -> str:
    if not _VALID_TYPE.match(dtype):
        raise InvalidIdentifierError(f"Tipo de coluna '{dtype}' contem caracteres nao permitidos para SQL.")
    return dtype
