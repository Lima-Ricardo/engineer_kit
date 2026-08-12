"""Run configured pipelines without blocking the localhost learning UI.

The UI is a development/training surface, not a production orchestrator. It
uses the same adapter-driven ``build_pipeline`` as code/YAML users. dbt remains
an optional post-ingestion step and runs only after Bronze + checkpoint success.
"""

from __future__ import annotations

import queue
import threading
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import duckdb

from engineer_kit.config.pipeline_config import PipelineConfig, build_pipeline
from engineer_kit.terminal_log import visual_logger
from engineer_kit.transform.dbt_runner import DbtRunner

_STREAM_END = None


@dataclass
class RunState:
    run_id: str
    pipeline_name: str
    started_at: datetime
    status: str = "running"
    error: Optional[str] = None
    rows_loaded: int = 0
    transform_status: str = "not_selected"
    log_queue: "queue.Queue" = field(default_factory=queue.Queue)


class RunManager:
    def __init__(self, warehouse_path: str, dbt_project_dir: Optional[str] = None) -> None:
        self._warehouse_path = warehouse_path
        self._dbt_project_dir = Path(dbt_project_dir).resolve() if dbt_project_dir else None
        self._runs: dict[str, RunState] = {}
        self._lock = threading.Lock()

    def start_run(self, config: PipelineConfig) -> str:
        run_id = uuid.uuid4().hex[:12]
        state = RunState(
            run_id=run_id,
            pipeline_name=config.name,
            started_at=datetime.now(timezone.utc),
        )
        with self._lock:
            self._runs[run_id] = state

        thread = threading.Thread(
            target=self._execute,
            args=(run_id, config),
            daemon=True,
        )
        thread.start()
        return run_id

    def _execute(self, run_id: str, config: PipelineConfig) -> None:
        state = self._runs[run_id]
        sink_id = visual_logger.add(
            lambda message: state.log_queue.put(message),
            filter=lambda record: record["extra"].get("run_id") == run_id,
            format="{time:HH:mm:ss} | {level: <8} | {message}",
        )
        try:
            with visual_logger.contextualize(run_id=run_id):
                result = self._run_ingestion(config, run_id)
                if not result.success:
                    state.status = "error"
                    state.error = "; ".join(step.error for step in result.steps if step.error)
                    return

                state.rows_loaded = result.rows_loaded

                if config.transform.type == "dbt":
                    if config.destination.type != "duckdb":
                        state.transform_status = "error"
                        state.error = (
                            "A integracao dbt do local lab usa dbt-duckdb. Para Parquet/Delta, "
                            "execute a transformacao da plataforma externamente."
                        )
                        visual_logger.error(state.error)
                        state.status = "error"
                        return
                    self._run_dbt(config, state)
                    if state.transform_status == "error":
                        state.status = "error"
                        return

                state.status = "success"
        except Exception as exc:
            state.status = "error"
            state.error = str(exc)
            visual_logger.error("'{}': {}", config.name, exc)
        finally:
            visual_logger.remove(sink_id)
            state.log_queue.put(_STREAM_END)

    def _run_ingestion(self, config: PipelineConfig, run_id: str):
        if config.destination.type == "duckdb":
            conn = duckdb.connect(self._warehouse_path)
            try:
                return build_pipeline(config, conn).run(run_id=run_id)
            finally:
                conn.close()
        return build_pipeline(config).run(run_id=run_id)

    def _run_dbt(self, config: PipelineConfig, state: RunState) -> None:
        if self._dbt_project_dir is None or not self._dbt_project_dir.exists():
            state.transform_status = "error"
            state.error = "dbt foi selecionado, mas o projeto dbt local nao foi encontrado."
            visual_logger.error(state.error)
            return

        state.transform_status = "running"
        visual_logger.info(
            "'{}': Bronze concluida; iniciando transformacao dbt{}.",
            config.name,
            f" (select={config.transform.select})" if config.transform.select else "",
        )
        dbt_result = DbtRunner(
            project_dir=str(self._dbt_project_dir),
            env={"ENGINEER_KIT_WAREHOUSE_PATH": self._warehouse_path},
        ).run(select=config.transform.select)

        for line in dbt_result.output.splitlines():
            if line.strip():
                visual_logger.info("dbt | {}", line)

        if dbt_result.success:
            state.transform_status = "success"
            visual_logger.success("'{}': transformacao dbt concluida.", config.name)
        else:
            state.transform_status = "error"
            state.error = "A ingestao terminou, mas o dbt falhou. Consulte o log da execucao."
            visual_logger.error("'{}': {}", config.name, state.error)

    def get_state(self, run_id: str) -> Optional[RunState]:
        return self._runs.get(run_id)

    def stream_log(self, run_id: str, timeout: float = 60.0):
        state = self._runs.get(run_id)
        if state is None:
            return
        while True:
            try:
                line = state.log_queue.get(timeout=timeout)
            except queue.Empty:
                return
            if line is _STREAM_END:
                return
            yield line
