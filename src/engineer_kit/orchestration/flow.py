"""High-level managed flow for intent-driven destinations."""

from __future__ import annotations

from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any

from engineer_kit.adapters.registry import AdapterContext, build_destination
from engineer_kit.orchestration.pipeline import Pipeline, PipelineResult
from engineer_kit.storage.destination import Destination
from engineer_kit.storage.inferred_destination import InferredSchemaDestination
from engineer_kit.storage.schema import EndpointSchema


@dataclass(frozen=True)
class FlowResult:
    pipeline: PipelineResult

    @property
    def success(self) -> bool:
        return self.pipeline.success

    @property
    def rows_loaded(self) -> int:
        return self.pipeline.rows_loaded


class ManagedFlow:
    def __init__(self, connector, destination: Any, *, target: str | None, options: dict[str, Any]) -> None:
        self._connector = connector
        self._destination = destination
        self._target = target
        self._options = dict(options)

    @staticmethod
    def _target_parts(value: str | None, default_table: str, default_schema: str) -> tuple[str, str]:
        if not value:
            return default_schema, default_table
        if "." in value:
            return tuple(value.rsplit(".", 1))
        return default_schema, value

    def _resolve(self):
        options = dict(self._options)
        sample_size = int(options.pop("schema_sample_size", 100))
        if isinstance(self._destination, Destination):
            endpoint = self._target or self._connector.name
            return InferredSchemaDestination(self._destination, endpoint=endpoint, sample_size=sample_size), None

        kind = str(self._destination).strip().lower()
        schema, endpoint = self._target_parts(
            self._target,
            self._connector.name,
            str(options.pop("schema", "bronze")),
        )
        runtime = options.pop("runtime", options.pop("connection", None))
        owned = None
        if kind == "duckdb" and runtime is None:
            import duckdb
            database = options.pop("path", options.pop("database", "engineer_kit.duckdb"))
            runtime = duckdb.connect(str(database))
            owned = runtime

        config = SimpleNamespace(
            path=options.pop("path", None),
            schema=schema,
            batch_size=int(options.pop("batch_size", 5000)),
            write_mode=options.pop("write_mode", "append"),
            partition_by=list(options.pop("partition_by", []) or []),
            options=options,
        )
        context = AdapterContext(
            pipeline_name=self._connector.name,
            runtime=runtime,
            destination_config=config,
        )
        destination = build_destination(kind, config, context)
        return InferredSchemaDestination(destination, endpoint=endpoint, sample_size=sample_size), owned

    def run(self, run_id: str | None = None) -> FlowResult:
        destination, owned = self._resolve()
        try:
            result = Pipeline(
                connector=self._connector,
                schema=EndpointSchema(),
                destination=destination,
                run_log=False,
            ).run(run_id=run_id)
            return FlowResult(result)
        finally:
            if owned is not None:
                owned.close()


__all__ = ["ManagedFlow", "FlowResult"]
