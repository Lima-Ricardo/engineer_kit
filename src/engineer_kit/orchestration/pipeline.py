"""Orquestra connector(s) -> destination como uma unidade atomica de ingestao.

O Pipeline coordena o fluxo, mas nao conhece DuckDB, Delta, Parquet ou
qualquer backend concreto. O watermark so e commitado depois que o load
termina com sucesso. Auditoria e opcional e depende do contrato
`RunLogBackend`, podendo ser fornecida pelo destino ou explicitamente.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from engineer_kit.connectors.api_connector import APIConnector
from engineer_kit.storage.destination import Destination
from engineer_kit.storage.run_log import RunLogBackend, RunLogEntry
from engineer_kit.storage.schema import EndpointSchema
from engineer_kit.terminal_log import visual_logger

logger = logging.getLogger("engineer_kit.pipeline")


@dataclass(frozen=True)
class PipelineSource:
    connector: APIConnector
    schema: EndpointSchema


@dataclass(frozen=True)
class StepResult:
    connector_name: str
    rows_loaded: int
    error: Optional[str] = None


@dataclass(frozen=True)
class PipelineResult:
    steps: list[StepResult]

    @property
    def success(self) -> bool:
        return all(step.error is None for step in self.steps)


class Pipeline:
    """Unidade atomica que um scheduler local ou externo pode executar."""

    def __init__(
        self,
        destination: Destination,
        connector: Optional[APIConnector] = None,
        schema: Optional[EndpointSchema] = None,
        sources: Optional[list[PipelineSource]] = None,
        run_log: bool = True,
        run_log_store: Optional[RunLogBackend] = None,
    ) -> None:
        self._sources = self._resolve_sources(connector, schema, sources)
        self._destination = destination
        self._run_log_store = self._resolve_run_log_store(run_log, run_log_store, destination)

    @staticmethod
    def _resolve_sources(
        connector: Optional[APIConnector],
        schema: Optional[EndpointSchema],
        sources: Optional[list[PipelineSource]],
    ) -> list[PipelineSource]:
        if sources is not None:
            if connector is not None or schema is not None:
                raise ValueError("Passe connector+schema (um conector) ou sources=[...] (varios) — nao os dois.")
            return sources
        if connector is None or schema is None:
            raise ValueError(
                "Passe connector=... e schema=... (caso comum, um conector) "
                "ou sources=[PipelineSource(...), ...] (varios conectores)."
            )
        return [PipelineSource(connector=connector, schema=schema)]

    @staticmethod
    def _resolve_run_log_store(
        run_log: bool,
        run_log_store: Optional[RunLogBackend],
        destination: Destination,
    ) -> Optional[RunLogBackend]:
        if run_log_store is not None:
            return run_log_store
        if not run_log:
            return None

        factory = getattr(destination, "default_run_log_backend", None)
        backend = factory() if callable(factory) else None
        if backend is None:
            raise ValueError(
                "run_log=True, mas este destino nao fornece auditoria automaticamente. "
                "Passe run_log_store=RunLogBackend(...) explicitamente ou run_log=False."
            )
        return backend

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
