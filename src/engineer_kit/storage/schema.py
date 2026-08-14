"""Declared endpoint schema shared by all destination adapters.

Bronze columns are intentionally stable and string-oriented. ``dtype`` is a
logical/analytical target type used by staging transforms; it is not inferred
from each API response. Unknown API fields are preserved in ``_extra`` by the
shared Bronze contract.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from engineer_kit.storage.identifiers import validate_identifier, validate_type
from engineer_kit.storage.types import LogicalType, render_sql_type, resolve_logical_type

# Library-owned Bronze metadata. Source schemas are never allowed to shadow
# these names because doing so would make retry identity/audit columns
# ambiguous or silently overwrite user data.
RESERVED_COLUMN_NAMES = (
    "_source",
    "_endpoint",
    "_ingested_at",
    "_run_id",
    "_ingestion_key",
    "_window_start",
    "_window_end",
    "_raw",
    "_extra",
)
_RESERVED_COLUMN_SET = frozenset(RESERVED_COLUMN_NAMES)


@dataclass(frozen=True)
class ColumnSpec:
    name: str
    dtype: str = "string"

    def __post_init__(self) -> None:
        validate_identifier(self.name, "Nome de coluna")
        if self.name in _RESERVED_COLUMN_SET:
            raise ValueError(
                f"Nome de coluna '{self.name}' e reservado pelo contrato Bronze do engineer_kit."
            )
        if resolve_logical_type(self.dtype) is None:
            validate_type(self.dtype)

    @property
    def logical_type(self) -> LogicalType | None:
        """Portable logical type, or ``None`` for a legacy/custom SQL type."""
        return resolve_logical_type(self.dtype)

    def sql_type(self, dialect: str = "duckdb") -> str:
        """Render the analytical type for the requested SQL dialect."""
        return render_sql_type(self.dtype, dialect=dialect)


@dataclass(frozen=True)
class EndpointSchema:
    columns: list[ColumnSpec] = field(default_factory=list)

    def __post_init__(self) -> None:
        seen: set[str] = set()
        duplicates: set[str] = set()
        for column in self.columns:
            if column.name in seen:
                duplicates.add(column.name)
            seen.add(column.name)
        if duplicates:
            names = ", ".join(sorted(duplicates))
            raise ValueError(f"Schema contem colunas duplicadas: {names}.")

    @classmethod
    def from_names(cls, names: list[str]) -> "EndpointSchema":
        """Declare expected fields using the portable ``string`` type."""
        return cls(columns=[ColumnSpec(name=name) for name in names])

    def column_names(self) -> list[str]:
        return [column.name for column in self.columns]


__all__ = ["ColumnSpec", "EndpointSchema", "RESERVED_COLUMN_NAMES"]
