"""Backend-agnostic orchestration for one complete ingestion attempt.

The Pipeline coordinates extraction, Bronze persistence, checkpoint commit and
optional audit logging. It never imports a concrete storage engine. Because
Destination and StateStore can be different systems, the cross-system delivery
guarantee is intentionally *at least once*: the destination is confirmed before
the watermark advances, so a state-store failure may cause the same window to
be retried rather than silently losing data.
"""

from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from typing import Optional
from uuid import uuid4

from engineer_kit.connectors.api_connector import APIConnector
from engineer_kit.storage.destination import Destination
from engineer_kit.storage.run_log import RunLogBackend, RunLogEntry
from engineer_kit.storage.schema import EndpointSchema
from engineer_kit.storage.state_store import Watermark
from engineer_kit.terminal_log import visual_logger

logger = logging.getLogger("engineer_kit.pipeline")


@dataclass(frozen=True)
class PipelineSource:
    connector: APIConnector
    schema: EndpointSchema


@dataclass(frozen=True)
class StepResult:
    """Result of one source -> destination ingestion step."""

    connector_name: str
    rows_loaded: int
    error: Optional[str] = None
    destination: Optional[str] = None
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    window_start: Optional[date] = None
    window_end: Optional[date] = None
    watermark_before: Optional[Watermark] = None
    watermark_after: Optional[Watermark] = None
    extra_fields_seen: tuple[str, ...] = ()

    @property
    def success(self) -> bool:
        return self.error is None


@dataclass(frozen=True)
class PipelineResult:
    """Portable execution result suitable for CLIs and external orchestrators."""

    steps: list[StepResult]
    run_id: str = ""
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None

    @property
    def success(self) -> bool:
        return all(step.success for step in self.steps)

    @property
    def rows_loaded(self) -> int:
        return sum(step.rows_loaded for step in self.steps)


class Pipeline:
    """Atomic *ingestion unit* callable by local or external orchestrators."""

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
        self._run_log_store = self._resolve_run_log_store(
            run_log, run_log_store, destination
        )

    @staticmethod
    def _resolve_sources(
        connector: Optional[APIConnector],
        schema: Optional[EndpointSchema],
        sources: Optional[list[PipelineSource]],
    ) -> list[PipelineSource]:
        if sources is not None:
            if connector is not None or schema is not None:
                raise ValueError(
                    "Passe connector+schema (um conector) ou sources=[...] (varios), nao os dois."
                )
            return sources
        if connector is None or schema is None:
            raise ValueError(
                "Passe connector=... e schema=... ou sources=[PipelineSource(...), ...]."
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
                "Passe run_log_store=... ou run_log=False."
            )
        return backend

    def run(self, run_id: str | None = None) -> PipelineResult:
        execution_id = run_id or uuid4().hex
        started_at = datetime.now(timezone.utc)
        steps = [self._run_step(source, execution_id) for source in self._sources]
        return PipelineResult(
            steps=steps,
            run_id=execution_id,
            started_at=started_at,
            finished_at=datetime.now(timezone.utc),
        )

    def _run_step(self, source: PipelineSource, run_id: str) -> StepResult:
        connector = source.connector
        started_at = datetime.now(timezone.utc)
        visual_logger.info("'{}': iniciando extracao e carga.", connector.name)
        window = None
        destination_label: str | None = None

        try:
            records = connector.extract()
            window = connector.current_window
            result = self._destination.load(
                connector_name=connector.name,
                endpoint=connector.name,
                schema=source.schema,
                records=records,
            )
            destination_label = result.table
            watermark_after = connector.commit_watermark()
            finished_at = datetime.now(timezone.utc)

            logger.info(
                "Conector '%s': %d linha(s) carregada(s).",
                connector.name,
                result.rows_loaded,
            )
            visual_logger.success(
                "'{}': concluido com sucesso -- {} registro(s), inicio {} fim {}.",
                connector.name,
                result.rows_loaded,
                started_at.isoformat(timespec="seconds"),
                finished_at.isoformat(timespec="seconds"),
            )
            self._record_run_safely(
                RunLogEntry(
                    connector_name=connector.name,
                    started_at=started_at,
                    finished_at=finished_at,
                    status="success",
                    rows_loaded=result.rows_loaded,
                    extra_fields_seen=result.extra_fields_seen,
                    run_id=run_id,
                    destination=result.table,
                    window_start=window.start if window else None,
                    window_end=window.end if window else None,
                    watermark_before=_watermark_json(
                        window.watermark_before if window else None
                    ),
                    watermark_after=_watermark_json(watermark_after),
                )
            )
            return StepResult(
                connector_name=connector.name,
                rows_loaded=result.rows_loaded,
                destination=result.table,
                started_at=started_at,
                finished_at=finished_at,
                window_start=window.start if window else None,
                window_end=window.end if window else None,
                watermark_before=window.watermark_before if window else None,
                watermark_after=watermark_after,
                extra_fields_seen=tuple(result.extra_fields_seen),
            )
        except Exception as exc:
            finished_at = datetime.now(timezone.utc)
            logger.exception(
                "Conector '%s' falhou; watermark nao avancado quando a falha ocorreu antes do commit.",
                connector.name,
            )
            visual_logger.error(
                "'{}': falhou -- inicio {} fim {}. Motivo: {}",
                connector.name,
                started_at.isoformat(timespec="seconds"),
                finished_at.isoformat(timespec="seconds"),
                exc,
            )
            self._record_run_safely(
                RunLogEntry(
                    connector_name=connector.name,
                    started_at=started_at,
                    finished_at=finished_at,
                    status="error",
                    rows_loaded=0,
                    extra_fields_seen=[],
                    error_message=str(exc),
                    run_id=run_id,
                    destination=destination_label,
                    window_start=window.start if window else None,
                    window_end=window.end if window else None,
                    watermark_before=_watermark_json(
                        window.watermark_before if window else None
                    ),
                )
            )
            return StepResult(
                connector_name=connector.name,
                rows_loaded=0,
                error=str(exc),
                destination=destination_label,
                started_at=started_at,
                finished_at=finished_at,
                window_start=window.start if window else None,
                window_end=window.end if window else None,
                watermark_before=window.watermark_before if window else None,
            )

    def _record_run_safely(self, entry: RunLogEntry) -> None:
        if self._run_log_store is None:
            return
        try:
            self._run_log_store.record(entry)
        except Exception as exc:
            # Observability must never turn an already-committed ingestion into
            # a synthetic extraction failure or mask the original source error.
            logger.exception("Falha ao persistir auditoria da execucao %s", entry.run_id)
            visual_logger.warning(
                "Auditoria da execucao '{}' nao foi persistida: {}",
                entry.run_id or "sem-id",
                exc,
            )


def _watermark_json(watermark: Watermark | None) -> str | None:
    if watermark is None:
        return None
    return json.dumps(asdict(watermark), ensure_ascii=False, default=str, sort_keys=True)
