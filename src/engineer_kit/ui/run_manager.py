"""Run configured pipelines without blocking the localhost learning UI.

The Local Lab is deliberately bounded: completed runs are retained in a small
in-memory history, log buffers are ring buffers, and duplicate/concurrent heavy
runs are constrained so a browser session cannot grow memory without bound.
"""

from __future__ import annotations

import threading
import uuid
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import duckdb

from engineer_kit.config.pipeline_config import PipelineConfig, build_pipeline
from engineer_kit.security.redaction import redact_text
from engineer_kit.terminal_log import visual_logger
from engineer_kit.transform.dbt_runner import DbtRunner

DEFAULT_MAX_RETAINED_RUNS = 100
DEFAULT_MAX_LOG_LINES = 2_000
DEFAULT_MAX_CONCURRENT_RUNS = 4


@dataclass
class RunState:
    run_id: str
    pipeline_name: str
    started_at: datetime
    status: str = "running"
    error: Optional[str] = None
    rows_loaded: int = 0
    transform_status: str = "not_selected"
    finished_at: datetime | None = None
    _logs: deque[tuple[int, str]] = field(default_factory=deque, repr=False)
    _next_log_index: int = 0
    _finished: bool = False
    _condition: threading.Condition = field(
        default_factory=threading.Condition, repr=False
    )

    def append_log(self, line: str, *, max_lines: int) -> None:
        with self._condition:
            self._logs.append((self._next_log_index, line))
            self._next_log_index += 1
            while len(self._logs) > max_lines:
                self._logs.popleft()
            self._condition.notify_all()

    def finish(self) -> None:
        with self._condition:
            self._finished = True
            self.finished_at = datetime.now(timezone.utc)
            self._condition.notify_all()

    def wait_for_logs(
        self,
        cursor: int,
        *,
        timeout: float,
    ) -> tuple[list[tuple[int, str]], bool, int]:
        with self._condition:
            def available() -> bool:
                return self._finished or any(index >= cursor for index, _ in self._logs)

            if not available():
                self._condition.wait_for(available, timeout=timeout)
            if self._logs and cursor < self._logs[0][0]:
                cursor = self._logs[0][0]
            items = [(index, line) for index, line in self._logs if index >= cursor]
            next_cursor = items[-1][0] + 1 if items else cursor
            return items, self._finished, next_cursor


class RunManager:
    def __init__(
        self,
        warehouse_path: str,
        dbt_project_dir: Optional[str] = None,
        *,
        max_retained_runs: int = DEFAULT_MAX_RETAINED_RUNS,
        max_log_lines: int = DEFAULT_MAX_LOG_LINES,
        max_concurrent_runs: int = DEFAULT_MAX_CONCURRENT_RUNS,
    ) -> None:
        for name, value in {
            "max_retained_runs": max_retained_runs,
            "max_log_lines": max_log_lines,
            "max_concurrent_runs": max_concurrent_runs,
        }.items():
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ValueError(f"{name} deve ser inteiro maior que zero.")
        self._warehouse_path = warehouse_path
        self._dbt_project_dir = Path(dbt_project_dir).resolve() if dbt_project_dir else None
        self._max_retained_runs = max_retained_runs
        self._max_log_lines = max_log_lines
        self._runs: dict[str, RunState] = {}
        self._lock = threading.RLock()
        self._slots = threading.BoundedSemaphore(max_concurrent_runs)

    def _prune_locked(self) -> None:
        if len(self._runs) < self._max_retained_runs:
            return
        completed = sorted(
            (state for state in self._runs.values() if state.status != "running"),
            key=lambda state: state.finished_at or state.started_at,
        )
        while len(self._runs) >= self._max_retained_runs and completed:
            stale = completed.pop(0)
            self._runs.pop(stale.run_id, None)

    def start_run(self, config: PipelineConfig) -> str:
        with self._lock:
            if any(
                state.status == "running" and state.pipeline_name == config.name
                for state in self._runs.values()
            ):
                raise RuntimeError(
                    f"pipeline '{config.name}' ja possui uma execucao ativa no Local Lab."
                )
            self._prune_locked()
            if len(self._runs) >= self._max_retained_runs:
                raise RuntimeError(
                    "Local Lab atingiu o limite de execucoes retidas; finalize/recarregue "
                    "antes de iniciar outra execucao."
                )
            run_id = uuid.uuid4().hex[:12]
            state = RunState(
                run_id=run_id,
                pipeline_name=config.name,
                started_at=datetime.now(timezone.utc),
            )
            self._runs[run_id] = state

        thread = threading.Thread(
            target=self._execute,
            args=(run_id, config),
            daemon=True,
            name=f"engineer-kit-{config.name}-{run_id}",
        )
        thread.start()
        return run_id

    def _execute(self, run_id: str, config: PipelineConfig) -> None:
        state = self.get_state(run_id)
        if state is None:
            return
        with self._slots:
            sink_id = visual_logger.add(
                lambda message: state.append_log(
                    redact_text(message), max_lines=self._max_log_lines
                ),
                filter=lambda record: record["extra"].get("run_id") == run_id,
                format="{time:HH:mm:ss} | {level: <8} | {message}",
            )
            try:
                with visual_logger.contextualize(run_id=run_id):
                    result = self._run_ingestion(config, run_id)
                    if not result.success:
                        state.status = "error"
                        state.error = redact_text(
                            "; ".join(step.error for step in result.steps if step.error)
                        )
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
                state.error = redact_text(exc)
                visual_logger.error("'{}': {}", config.name, state.error)
            finally:
                visual_logger.remove(sink_id)
                state.finish()

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
            env={"ENGINEER_KIT_DUCKDB_PATH": self._warehouse_path},
        ).run(select=config.transform.select)

        for line in dbt_result.output.splitlines():
            if line.strip():
                visual_logger.info("dbt | {}", redact_text(line))

        if dbt_result.success:
            state.transform_status = "success"
            visual_logger.success("'{}': transformacao dbt concluida.", config.name)
        else:
            state.transform_status = "error"
            state.error = "A ingestao terminou, mas o dbt falhou. Consulte o log da execucao."
            visual_logger.error("'{}': {}", config.name, state.error)

    def get_state(self, run_id: str) -> Optional[RunState]:
        with self._lock:
            return self._runs.get(run_id)

    def stream_log(
        self,
        run_id: str,
        timeout: float = 60.0,
        *,
        cursor: int = 0,
    ):
        state = self.get_state(run_id)
        if state is None:
            return
        next_cursor = max(0, int(cursor))
        while True:
            items, finished, next_cursor = state.wait_for_logs(
                next_cursor,
                timeout=timeout,
            )
            for _index, line in items:
                yield line
            if finished:
                return
            if not items:
                return


__all__ = [
    "DEFAULT_MAX_CONCURRENT_RUNS",
    "DEFAULT_MAX_LOG_LINES",
    "DEFAULT_MAX_RETAINED_RUNS",
    "RunManager",
    "RunState",
]
