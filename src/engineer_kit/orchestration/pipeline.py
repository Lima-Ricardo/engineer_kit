"""Une conector(es) -> destino em uma unidade atomica: e essa unidade
que um scheduler (nosso ou externo, tipo Airflow) chama para rodar uma
carga completa.

O watermark de cada conector so e commitado depois que o load no
destino teve sucesso — uma falha no meio do caminho refaz a mesma
janela no proximo run, sem duplicar nem perder dado. Uma fonte falhar
nao impede as outras de rodar: um problema numa API nao deveria travar
o resto do pipeline.

Se um RunLogStore for passado, cada execucao (inicio, fim, status,
quantidade de registros, colunas novas fora do schema) fica registrada
em `_meta.run_log` no DuckDB, alem de aparecer no log visual do
terminal -- as duas saidas vem da mesma informacao.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from engineer_kit.connectors.api_connector import APIConnector
from engineer_kit.storage.destination import Destination
from engineer_kit.storage.run_log import RunLogEntry, RunLogStore
from engineer_kit.storage.schema import EndpointSchema
from engineer_kit.terminal_log import visual_logger

logger = logging.getLogger("engineer_kit.pipeline")


@dataclass
class PipelineSource:
    connector: APIConnector
    schema: EndpointSchema


@dataclass
class StepResult:
    connector_name: str
    rows_loaded: int
    error: Optional[str] = None


@dataclass
class PipelineResult:
    steps: list[StepResult]

    @property
    def success(self) -> bool:
        return all(step.error is None for step in self.steps)


class Pipeline:
    def __init__(
        self,
        sources: list[PipelineSource],
        destination: Destination,
        run_log_store: Optional[RunLogStore] = None,
    ) -> None:
        self._sources = sources
        self._destination = destination
        self._run_log_store = run_log_store

    def run(self) -> PipelineResult:
        return PipelineResult(steps=[self._run_step(source) for source in self._sources])

    def _run_step(self, source: PipelineSource) -> StepResult:
        connector = source.connector
        started_at = datetime.now(timezone.utc)
        visual_logger.info("'{}': iniciando extracao e carga.", connector.name)

        try:
            records = connector.extract()
            result = self._destination.load(
                connector_name=connector.name,
                endpoint=connector.name,
                schema=source.schema,
                records=records,
            )
            connector.commit_watermark()
            finished_at = datetime.now(timezone.utc)

            logger.info("Conector '%s': %d linha(s) carregada(s).", connector.name, result.rows_loaded)
            visual_logger.success(
                "'{}': concluido com sucesso -- {} registro(s), inicio {} fim {}.",
                connector.name,
                result.rows_loaded,
                started_at.isoformat(timespec="seconds"),
                finished_at.isoformat(timespec="seconds"),
            )

            self._record_run(
                connector.name, started_at, finished_at, "success", result.rows_loaded,
                result.extra_fields_seen, error_message=None,
            )
            return StepResult(connector_name=connector.name, rows_loaded=result.rows_loaded)
        except Exception as exc:
            finished_at = datetime.now(timezone.utc)
            logger.exception(
                "Conector '%s' falhou -- watermark NAO avancado, proximo run refaz a mesma janela.",
                connector.name,
            )
            visual_logger.error(
                "'{}': falhou -- inicio {} fim {}. Motivo: {}",
                connector.name,
                started_at.isoformat(timespec="seconds"),
                finished_at.isoformat(timespec="seconds"),
                exc,
            )

            self._record_run(
                connector.name, started_at, finished_at, "error", 0, [], error_message=str(exc),
            )
            return StepResult(connector_name=connector.name, rows_loaded=0, error=str(exc))

    def _record_run(
        self,
        connector_name: str,
        started_at: datetime,
        finished_at: datetime,
        status: str,
        rows_loaded: int,
        extra_fields_seen: list[str],
        error_message: Optional[str],
    ) -> None:
        if self._run_log_store is None:
            return
        self._run_log_store.record(
            RunLogEntry(
                connector_name=connector_name,
                started_at=started_at,
                finished_at=finished_at,
                status=status,
                rows_loaded=rows_loaded,
                extra_fields_seen=extra_fields_seen,
                error_message=error_message,
            )
        )
