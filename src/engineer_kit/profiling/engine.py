"""Streaming profile planner and aggregators."""

from __future__ import annotations

import hashlib
import json
import math
from collections import Counter
from dataclasses import dataclass, field
from typing import Any, Iterable, Iterator, Sequence

from engineer_kit.connectors.dedup import (
    ExactKeyDeduplicator,
    ExactRowDeduplicator,
    resolve_dedup_keys,
)
from engineer_kit.profiling.model import (
    PROFILE_REPORT_VERSION,
    CardinalityEstimate,
    DuplicateProfile,
    FieldProfile,
    ProfileReport,
)

PROFILE_METRICS = frozenset(
    {
        "count",
        "duplicates",
        "nulls",
        "missing",
        "empty",
        "types",
        "cardinality",
        "schema",
    }
)
PROFILE_PRESETS = {
    "quality": frozenset({"duplicates", "nulls", "missing", "empty", "types"}),
    "statistics": frozenset({"count", "cardinality"}),
    "schema": frozenset({"schema", "types", "missing"}),
    "all": PROFILE_METRICS,
}
DEFAULT_PROFILE_METRICS = PROFILE_METRICS
_EXACT_CARDINALITY_THRESHOLD = 10_000
_HLL_PRECISION = 12


class UnknownProfileMetricError(ValueError):
    """Raised before extraction when a profile metric/preset is unknown."""


def resolve_profile_metrics(metrics: Sequence[str]) -> tuple[str, ...]:
    """Expand presets and return a stable metric plan.

    No explicit selector means the complete profile (``all``).
    """
    requested = tuple(metrics) if metrics else ("all",)
    resolved: set[str] = set()
    for raw in requested:
        name = str(raw).strip().lower()
        if not name:
            continue
        if name in PROFILE_PRESETS:
            resolved.update(PROFILE_PRESETS[name])
        elif name in PROFILE_METRICS:
            resolved.add(name)
        else:
            options = sorted(PROFILE_METRICS | PROFILE_PRESETS.keys())
            raise UnknownProfileMetricError(
                f"profile metric '{raw}' desconhecida; use: {', '.join(options)}"
            )
    if not resolved:
        resolved.update(DEFAULT_PROFILE_METRICS)
    return tuple(sorted(resolved))


def _json_type(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "number"
    if isinstance(value, str):
        return "string"
    if isinstance(value, list):
        return "array"
    if isinstance(value, dict):
        return "object"
    return type(value).__name__


def _stable_value(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")


class _HyperLogLog:
    def __init__(self, precision: int = _HLL_PRECISION) -> None:
        self.precision = precision
        self.m = 1 << precision
        self.registers = bytearray(self.m)

    def add_hash(self, digest: bytes) -> None:
        value = int.from_bytes(digest[:8], "big", signed=False)
        index = value >> (64 - self.precision)
        remaining_bits = 64 - self.precision
        remainder = value & ((1 << remaining_bits) - 1)
        rank = (
            remaining_bits + 1
            if remainder == 0
            else remaining_bits - remainder.bit_length() + 1
        )
        if rank > self.registers[index]:
            self.registers[index] = rank

    def estimate(self) -> int:
        m = self.m
        alpha = 0.7213 / (1.0 + 1.079 / m)
        denominator = sum(2.0 ** (-register) for register in self.registers)
        estimate = alpha * m * m / denominator
        zeros = self.registers.count(0)
        if estimate <= 2.5 * m and zeros:
            estimate = m * math.log(m / zeros)
        return max(0, int(round(estimate)))

    @property
    def relative_error(self) -> float:
        return 1.04 / math.sqrt(self.m)


class _AdaptiveCardinality:
    def __init__(self, threshold: int = _EXACT_CARDINALITY_THRESHOLD) -> None:
        self._threshold = threshold
        self._exact: set[bytes] | None = set()
        self._hll: _HyperLogLog | None = None

    def add(self, value: Any) -> None:
        digest = hashlib.blake2b(_stable_value(value), digest_size=16).digest()
        if self._exact is not None:
            self._exact.add(digest)
            if len(self._exact) <= self._threshold:
                return
            hll = _HyperLogLog()
            for existing in self._exact:
                hll.add_hash(existing)
            self._hll = hll
            self._exact = None
            return
        assert self._hll is not None
        self._hll.add_hash(digest)

    def result(self) -> CardinalityEstimate:
        if self._exact is not None:
            return CardinalityEstimate(count=len(self._exact), precision="exact")
        assert self._hll is not None
        return CardinalityEstimate(
            count=self._hll.estimate(),
            precision="approximate",
            relative_error=self._hll.relative_error,
        )


@dataclass
class _FieldAccumulator:
    path: str
    records_present: int = 0
    occurrences: int = 0
    nulls: int = 0
    empty_strings: int = 0
    blank_strings: int = 0
    empty_arrays: int = 0
    empty_objects: int = 0
    types: Counter[str] = field(default_factory=Counter)
    cardinality: _AdaptiveCardinality | None = None


def _walk_json(value: Any, prefix: str = "") -> Iterator[tuple[str, Any]]:
    """Yield observed JSON paths while retaining only aggregate state."""
    if prefix:
        yield prefix, value
    if isinstance(value, dict):
        for key, nested in value.items():
            child = f"{prefix}.{key}" if prefix else str(key)
            yield from _walk_json(nested, child)
    elif isinstance(value, list) and prefix:
        for item in value:
            yield from _walk_json(item, f"{prefix}[]")


def profile_records(
    records: Iterable[dict[str, Any]],
    *metrics: str,
    scope: str = "full",
    limit: int | None = None,
    fields: Sequence[str] | None = None,
    key: str | Sequence[str] | None = None,
) -> ProfileReport:
    """Profile a record stream without materializing the source dataset.

    When ``key`` is provided, duplicate analysis is performed against that
    candidate simple/composite primary key. Invalid key rows are counted rather
    than aborting the profile so the report can explain why the key is unsafe.
    Without ``key``, duplicate analysis compares complete rows.
    """
    plan_tuple = resolve_profile_metrics(metrics)
    plan = set(plan_tuple)
    normalized_scope = scope.strip().lower()
    if normalized_scope not in {"full", "sample"}:
        raise ValueError("profile scope deve ser 'full' ou 'sample'.")
    if limit is not None and (
        isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0
    ):
        raise ValueError("profile limit deve ser um inteiro maior que zero.")
    if normalized_scope == "sample" and limit is None:
        limit = 10_000

    field_filter = set(fields or ()) or None
    field_plan = bool(
        plan & {"nulls", "missing", "empty", "types", "cardinality", "schema"}
    )
    accumulators: dict[str, _FieldAccumulator] = {}
    records_analyzed = 0
    duplicate_keys = resolve_dedup_keys(key) if key is not None else None
    duplicate_tracker: ExactKeyDeduplicator | ExactRowDeduplicator | None = None
    if "duplicates" in plan:
        duplicate_tracker = (
            ExactKeyDeduplicator(duplicate_keys, strict=False)
            if duplicate_keys is not None
            else ExactRowDeduplicator()
        )
    duplicate_rows = 0

    try:
        for record in records:
            if limit is not None and records_analyzed >= limit:
                break
            records_analyzed += 1
            if duplicate_tracker is not None:
                seen = duplicate_tracker.add(record)
                if seen is False:
                    duplicate_rows += 1
            if not field_plan:
                continue

            # Bounded by the number of observed paths in one record, not by the
            # number of source records. This keeps missing-data accounting
            # streaming-safe even for hundreds of millions of rows.
            seen_paths_this_record: set[str] = set()
            for path, value in _walk_json(record):
                if field_filter is not None and path not in field_filter:
                    continue
                accumulator = accumulators.get(path)
                if accumulator is None:
                    accumulator = _FieldAccumulator(path=path)
                    accumulators[path] = accumulator
                if path not in seen_paths_this_record:
                    accumulator.records_present += 1
                    seen_paths_this_record.add(path)
                accumulator.occurrences += 1
                value_type = _json_type(value)
                if "types" in plan or "schema" in plan:
                    accumulator.types[value_type] += 1
                if "nulls" in plan and value is None:
                    accumulator.nulls += 1
                if "empty" in plan:
                    if isinstance(value, str):
                        if value == "":
                            accumulator.empty_strings += 1
                        elif value.strip() == "":
                            accumulator.blank_strings += 1
                    elif isinstance(value, list) and not value:
                        accumulator.empty_arrays += 1
                    elif isinstance(value, dict) and not value:
                        accumulator.empty_objects += 1
                if "cardinality" in plan:
                    if accumulator.cardinality is None:
                        accumulator.cardinality = _AdaptiveCardinality()
                    accumulator.cardinality.add(value)
    finally:
        if duplicate_tracker is not None:
            duplicate_tracker.close()

    field_reports: dict[str, FieldProfile] = {}
    warnings: list[str] = []
    for path in sorted(accumulators):
        accumulator = accumulators[path]
        types = (
            dict(sorted(accumulator.types.items()))
            if ("types" in plan or "schema" in plan)
            else None
        )
        missing = (
            records_analyzed - accumulator.records_present if "missing" in plan else None
        )
        cardinality = (
            accumulator.cardinality.result()
            if accumulator.cardinality is not None
            else None
        )
        field_profile = FieldProfile(
            path=path,
            records_present=accumulator.records_present,
            occurrences=accumulator.occurrences,
            missing=missing,
            nulls=accumulator.nulls if "nulls" in plan else None,
            empty_strings=accumulator.empty_strings if "empty" in plan else None,
            blank_strings=accumulator.blank_strings if "empty" in plan else None,
            empty_arrays=accumulator.empty_arrays if "empty" in plan else None,
            empty_objects=accumulator.empty_objects if "empty" in plan else None,
            types=types,
            cardinality=cardinality,
        )
        field_reports[path] = field_profile
        non_null_types = {
            name for name, count in (types or {}).items() if name != "null" and count
        }
        if len(non_null_types) > 1:
            warnings.append(
                f"{path}: tipos inconsistentes observados "
                f"({', '.join(sorted(non_null_types))})"
            )

    duplicates = None
    if "duplicates" in plan:
        invalid_key_rows = (
            duplicate_tracker.invalid_key_count
            if isinstance(duplicate_tracker, ExactKeyDeduplicator)
            else 0
        )
        unique_rows = records_analyzed - duplicate_rows
        if duplicate_keys is not None:
            unique_rows -= invalid_key_rows
        duplicates = DuplicateProfile(
            duplicate_rows=duplicate_rows,
            unique_rows=unique_rows,
            key_fields=duplicate_keys,
            invalid_key_rows=invalid_key_rows,
        )
        if duplicate_rows:
            label = "PK" if duplicate_keys else "registro completo"
            warnings.append(
                f"{duplicate_rows} duplicata(s) por {label} observada(s)"
            )
        if invalid_key_rows:
            warnings.append(
                f"{invalid_key_rows} registro(s) com PK ausente, null, blank ou nao escalar"
            )

    return ProfileReport(
        version=PROFILE_REPORT_VERSION,
        scope=normalized_scope,
        requested_metrics=plan_tuple,
        records_analyzed=records_analyzed,
        fields=field_reports,
        duplicates=duplicates,
        warnings=tuple(warnings),
        limit=limit,
    )


__all__ = [
    "DEFAULT_PROFILE_METRICS",
    "PROFILE_METRICS",
    "PROFILE_PRESETS",
    "UnknownProfileMetricError",
    "profile_records",
    "resolve_profile_metrics",
]
