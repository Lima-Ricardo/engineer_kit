"""Versioned data contracts returned by engineer_kit profiling."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

PROFILE_REPORT_VERSION = "1"


@dataclass(frozen=True)
class CardinalityEstimate:
    count: int
    precision: str = "exact"
    relative_error: float | None = None


@dataclass(frozen=True)
class FieldProfile:
    path: str
    records_present: int
    occurrences: int
    missing: int | None = None
    nulls: int | None = None
    empty_strings: int | None = None
    blank_strings: int | None = None
    empty_arrays: int | None = None
    empty_objects: int | None = None
    types: dict[str, int] | None = None
    cardinality: CardinalityEstimate | None = None

    @property
    def empty(self) -> int | None:
        values = (
            self.empty_strings,
            self.blank_strings,
            self.empty_arrays,
            self.empty_objects,
        )
        if all(value is None for value in values):
            return None
        return sum(value or 0 for value in values)


@dataclass(frozen=True)
class DuplicateProfile:
    duplicate_rows: int
    unique_rows: int


@dataclass(frozen=True)
class DataQualitySummary:
    duplicate_rows: int | None
    fields_with_missing: int
    fields_with_nulls: int
    fields_with_empty: int
    mixed_type_fields: int


@dataclass(frozen=True)
class ProfileReport:
    version: str
    scope: str
    requested_metrics: tuple[str, ...]
    records_analyzed: int
    fields: dict[str, FieldProfile] = field(default_factory=dict)
    duplicates: DuplicateProfile | None = None
    warnings: tuple[str, ...] = ()
    limit: int | None = None

    def has(self, metric: str) -> bool:
        return metric.strip().lower() in self.requested_metrics

    @property
    def quality(self) -> DataQualitySummary:
        mixed = 0
        missing = 0
        nulls = 0
        empty = 0
        for profile in self.fields.values():
            if (profile.missing or 0) > 0:
                missing += 1
            if (profile.nulls or 0) > 0:
                nulls += 1
            if (profile.empty or 0) > 0:
                empty += 1
            non_null_types = {
                name: count
                for name, count in (profile.types or {}).items()
                if name != "null" and count > 0
            }
            if len(non_null_types) > 1:
                mixed += 1
        return DataQualitySummary(
            duplicate_rows=(self.duplicates.duplicate_rows if self.duplicates else None),
            fields_with_missing=missing,
            fields_with_nulls=nulls,
            fields_with_empty=empty,
            mixed_type_fields=mixed,
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def to_text(self) -> str:
        from engineer_kit.profiling.render import render_text

        return render_text(self)

    def to_html(self) -> str:
        from engineer_kit.profiling.render import render_html

        return render_html(self)


__all__ = [
    "PROFILE_REPORT_VERSION",
    "CardinalityEstimate",
    "DataQualitySummary",
    "DuplicateProfile",
    "FieldProfile",
    "ProfileReport",
]
