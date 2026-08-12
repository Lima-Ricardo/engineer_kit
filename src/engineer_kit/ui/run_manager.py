"""Executa pipelines em background (thread separada, a UI nao pode
travar esperando uma extracao terminar) e expoe o log visual de cada
execucao como um stream, pra a pagina mostrar em tempo real.

Usa `visual_logger.contextualize(run_id=...)` pra marcar todo log
emitido durante aquela execucao especifica, e um sink temporario com
filtro por esse run_id -- assim duas execucoes ao mesmo tempo (ex: dois
cliques em pipelines diferentes) nao misturam o log uma da outra.
"""

from __future__ import annotations

import queue
import threading
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

import duckdb

from engineer_kit.config.pipeline_config import PipelineConfig, build_pipeline
from engineer_kit.terminal_log import visual_logger

# sentinela que marca o fim do stream de log de uma execucao
_STREAM_END = None


@dataclass
class RunState:
    run_id: str
    pipeline_name: str
    started_at: datetime
    status: str = "running"  # running | success | error
    error: Optional[str] = None
    rows_loaded: int = 0
    log_queue: "queue.Queue" = field(default_factory=queue.Queue)


class RunManager:
    def __init__(self, warehouse_path: str) -> None:
        self._warehouse_path = warehouse_path
        self._runs: dict[str, RunState] = {}
        self._lock = threading.Lock()

    def start_run(self, config: PipelineConfig) -> str:
        run_id = uuid.uuid4().hex[:12]
        state = RunState(run_id=run_id, pipeline_name=config.name, started_at=datetime.now(timezone.utc))
        with self._lock:
            self._runs[run_id] = state

        thread = threading.Thread(target=self._execute, args=(run_id, config), daemon=True)
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
                conn = duckdb.connect(self._warehouse_path)
                try:
                    pipeline = build_pipeline(config, conn)
                    result = pipeline.run()
                finally:
                    conn.close()

            if result.success:
                state.status = "success"
                state.rows_loaded = sum(step.rows_loaded for step in result.steps)
            else:
                state.status = "error"
                state.error = "; ".join(step.error for step in result.steps if step.error)
        except Exception as exc:
            state.status = "error"
            state.error = str(exc)
        finally:
            visual_logger.remove(sink_id)
            state.log_queue.put(_STREAM_END)

    def get_state(self, run_id: str) -> Optional[RunState]:
        return self._runs.get(run_id)

    def stream_log(self, run_id: str, timeout: float = 60.0):
        """Generator sincrono: itera as linhas de log conforme chegam,
        para quando a execucao termina (ou apos `timeout` sem eventos)."""
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
