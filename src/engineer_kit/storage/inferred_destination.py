"""Destination wrappers used by the intent-driven managed flow."""

from __future__ import annotations

from itertools import chain, islice
from typing import Any, Iterable

from engineer_kit.storage.destination import Destination, LoadContext, LoadResult
from engineer_kit.storage.flatten import flatten_record
from engineer_kit.storage.schema import EndpointSchema


class InferredSchemaDestination(Destination):
    """Infer Bronze columns once from a bounded prefix of the stream."""

    def __init__(self, destination: Destination, *, endpoint: str | None = None, sample_size: int = 100) -> None:
        if sample_size <= 0:
            raise ValueError("sample_size deve ser maior que zero.")
        self._destination = destination
        self._endpoint = endpoint
        self._sample_size = sample_size

    def default_run_log_backend(self):
        factory = getattr(self._destination, "default_run_log_backend", None)
        return factory() if callable(factory) else None

    def _prepare(self, endpoint: str, schema: EndpointSchema, records: Iterable[dict[str, Any]]):
        resolved_endpoint = self._endpoint or endpoint
        if schema.columns:
            return resolved_endpoint, schema, records
        iterator = iter(records)
        sample = list(islice(iterator, self._sample_size))
        names: list[str] = []
        seen: set[str] = set()
        for record in sample:
            for name in flatten_record(record):
                if name not in seen:
                    seen.add(name)
                    names.append(name)
        return resolved_endpoint, EndpointSchema.from_names(names), chain(sample, iterator)

    def load(
        self,
        connector_name: str,
        endpoint: str,
        schema: EndpointSchema,
        records: Iterable[dict[str, Any]],
    ) -> LoadResult:
        endpoint, schema, records = self._prepare(endpoint, schema, records)
        return self._destination.load(connector_name, endpoint, schema, records)

    def load_with_context(
        self,
        connector_name: str,
        endpoint: str,
        schema: EndpointSchema,
        records: Iterable[dict[str, Any]],
        context: LoadContext,
    ) -> LoadResult:
        endpoint, schema, records = self._prepare(endpoint, schema, records)
        contextual = getattr(self._destination, "load_with_context", None)
        if callable(contextual):
            return contextual(connector_name, endpoint, schema, records, context)
        return self._destination.load(connector_name, endpoint, schema, records)
