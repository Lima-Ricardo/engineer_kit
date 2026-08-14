"""Validation of SQL-safe identifiers and constrained legacy type expressions."""

from __future__ import annotations

import re

_VALID_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
# Legacy/custom types are data-model declarations, not arbitrary SQL. Accept a
# conservative type name (up to four words) plus optional numeric precision /
# scale, e.g. VARCHAR, DOUBLE PRECISION, DECIMAL(18, 2).
_VALID_TYPE = re.compile(
    r"^[A-Za-z][A-Za-z0-9_]*(?:\s+[A-Za-z][A-Za-z0-9_]*){0,3}"
    r"(?:\(\s*\d+\s*(?:,\s*\d+\s*)?\))?$"
)


class InvalidIdentifierError(ValueError):
    """Raised when a schema/table/column name or SQL type is unsafe."""


def validate_identifier(name: str, what: str) -> str:
    if not isinstance(name, str) or not _VALID_IDENTIFIER.fullmatch(name):
        raise InvalidIdentifierError(
            f"{what} '{name}' nao e um identificador SQL seguro. "
            "Use apenas letras, numeros e underscore, comecando com letra ou underscore."
        )
    return name


def validate_type(dtype: str) -> str:
    if not isinstance(dtype, str):
        raise InvalidIdentifierError("Tipo de coluna deve ser uma string.")
    value = dtype.strip()
    if not value or _VALID_TYPE.fullmatch(value) is None:
        raise InvalidIdentifierError(
            f"Tipo de coluna '{dtype}' nao e uma declaracao SQL de tipo suportada. "
            "Use um tipo logico do engineer_kit ou um tipo simples como DECIMAL(18, 2)."
        )
    return value
