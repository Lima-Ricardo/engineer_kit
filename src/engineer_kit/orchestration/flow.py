"""High-level source -> destination -> optional transform flow."""

from __future__ import annotations

from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any

from engineer_kit.adapters.registry import (
    AdapterContext,
    AdapterNotFoundError,
    build_destination,
    build_run_log,
    build_state_store,
    resolve_auto,
)
from engineer_kit.orchestration.pipeline import Pipeline, PipelineResult
from engineer_kit.storage.destination import Destination
from engineer_kit.storage.inferred_destination import InferredSchemaDestination
from engineer_kit.storage.schema import EndpointSchema
from engineer_kit.transform.dbt_easy import Dbt
from engineer_kit.transform.dbt_runner import DbtResult


@dataclass(frozen=True)
class FlowResult:
    pipeline: PipelineResult
    transform: DbtResult | None = None

    @property
    def success(self) -> bool:
        return self.pipeline.success and (self.transform is None or self.transform.success)

    @property
    def rows_loaded(self) -> int:
        return self.pipeline.rows_loaded


class ManagedFlow:
    """Resolve destination/state/audit adapters once, then run the typed Pipeline."""

    def __init__(self, connector, destination: Any, *, target: str | None, options: dict[str, Any]) -> None:
        self._connector = connector
        self._destination = destination
        self._target = target
        self._options = dict(options)
        self._dbt: dict[str, Any] | None = None

    def dbt(
        self,
        select: str | None = None,
        *,
        project_dir: str | None = None,
        profiles_dir: str | None = None,
        target: str = "dev",
    ) -> "ManagedFlow":
        self._dbt = {
            "select": select,
            "project_dir": project_dir,
            "profiles_dir": profiles_dir,
            "target": target,
        }
        return self

    @staticmethod
    def _target_parts(value: str | None, default_table: str, default_schema: str) -> tuple[str, str]:
        if not value:
            return default_schema, default_table
        if "." in value:
            schema, table = value.rsplit(".", 1)
            return schema, table
        return default_schema, value

    def _resolve_custom(self, sample_size: int):
        endpoint = self._target or self._connector.name
        destination = InferredSchemaDestination(
            self._destination,
            endpoint=endpoint,
            sample_size=sample_size,
        )
        factory = getattr(self._destination, "default_run_log_backend", None)
        run_log = factory() if callable(factory) else None
        return destination, run_log, None

    def _resolve_builtin(self, kind: str, options: dict[str, Any], sample_size: int):
        schema, endpoint = self._target_parts(
            self._target,
            self._connector.name,
            str(options.pop("schema", "bronze")),
        )
        runtime = options.pop("runtime", options.pop("connection", None))
        owned = None
        if kind == "duckdb" and runtime is None:
            try:
                import duckdb
            except ModuleNotFoundError:
                raise ModuleNotFoundError(
                    'DuckDB support is optional. Install `pip install "engineer_kit[duckdb]"`.'
                ) from None
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
        raw_destination = build_destination(kind, config, context)
        destination = InferredSchemaDestination(
            raw_destination,
            endpoint=endpoint,
            sample_size=sample_size,
        )

        state_config = SimpleNamespace(path=None, options={})
        state_type = resolve_auto("auto", destination_type=kind, kind="state")
        state_store = build_state_store(state_type, state_config, context)
        binder = getattr(self._connector, "_bind_auto_state_store", None)
        if callable(binder):
            binder(state_store)

        run_log = None
        try:
            audit_type = resolve_auto("auto", destination_type=kind, kind="run_log")
            run_log = build_run_log(audit_type, SimpleNamespace(path=None, options={}), context)
        except AdapterNotFoundError:
            factory = getattr(raw_destination, "default_run_log_backend", None)
            run_log = factory() if callable(factory) else None

        return destination, run_log, owned

    def _resolve(self):
        options = dict(self._options)
        sample_size = int(options.pop("schema_sample_size", 100))
        if sample_size <= 0:
            raise ValueError("schema_sample_size deve ser maior que zero.")
        if isinstance(self._destination, Destination):
            return self._resolve_custom(sample_size)
        kind = str(self._destination).strip().lower()
        return self._resolve_builtin(kind, options, sample_size)

    def run(self, run_id: str | None = None) -> FlowResult:
        destination, run_log, owned = self._resolve()
        transform = None
        try:
            pipeline_result = Pipeline(
                connector=self._connector,
                schema=EndpointSchema(),
                destination=destination,
                run_log=run_log is not None,
                run_log_store=run_log,
            ).run(run_id=run_id)
            if pipeline_result.success and self._dbt is not None:
                transform = Dbt(
                    project_dir=self._dbt["project_dir"],
                    profiles_dir=self._dbt["profiles_dir"],
                    target=self._dbt["target"],
                ).run(select=self._dbt["select"])
            return FlowResult(pipeline_result, transform)
        finally:
            if owned is not None:
                owned.close()


__all__ = ["ManagedFlow", "FlowResult"]
