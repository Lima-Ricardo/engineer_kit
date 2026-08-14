"""Backend-agnostic orchestration for one complete ingestion attempt.

The Pipeline coordinates extraction, Bronze persistence, checkpoint commit and
optional audit logging. It never imports a concrete storage engine.

Destination and StateStore may be different systems, so there is no universal
cross-system transaction. Official engineer_kit destinations use a deterministic
``ingestion_key`` per checkpoint transition to make a retry replace the same
Bronze window when state persistence fails after data persistence. Third-party
destinations remain compatible through ``Destination.load`` and provide
at-least-once semantics unless they implement ``load_with_context``.
"""

from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from typing import Any, Optional
from uuid import uuid4

from engineer_kit.connectors.api_connector import APIConnector
from engineer_kit.security.redaction import redact_text
from engineer_kit.storage.destination import Destination, LoadContext
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
    status: str = "success"
    destination: Optional[str] = None
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    window_start: Optional[date] = None
    window_end: Optional[date] = None
    watermark_before: Optional[Watermark] = None
    watermark_after: Optional[Watermark] = None
    extra_fields_seen: tuple[str, ...] = ()
    ingestion_key: Optional[str] = None

    @property
    def success(self) -> bool:
        return self.error is None and self.status == "success"


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
    """Atomic ingestion unit callable by local or external orchestrators."""

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
        extraction_session = None
        context = LoadContext.adhoc(
            connector.name,
            run_id=run_id,
            started_at=started_at,
        )

        try:
            session_factory = getattr(connector, "extract_incremental", None)
            if callable(session_factory):
                extraction_session = session_factory()
                records = extraction_session.iter_records()
                window = extraction_session.window
            else:
                records = connector.extract()
                window = getattr(connector, "current_window", None)

            # Deterministic retry identity is safe only when a persistent
            # checkpoint transition exists. Older third-party/duck-typed
            # connectors predate ``checkpoint_enabled``; preserve their legacy
            # incremental semantics by treating an observed window as stateful.
            checkpoint_enabled = bool(getattr(connector, "checkpoint_enabled", True))
            if window is not None and checkpoint_enabled:
                context = LoadContext.for_window(
                    connector.name,
                    _window_start(window),
                    _window_end(window),
                    checkpoint_identity=_checkpoint_identity(connector, window),
                    started_at=started_at,
                    run_id=run_id,
                )

            result = self._load_destination(
                connector_name=connector.name,
                schema=source.schema,
                records=records,
                context=context,
            )
        except Exception as exc:
            if extraction_session is not None:
                try:
                    extraction_session.abort()
                except Exception:
                    logger.debug("Falha ao abortar ExtractionSession")
            finished_at = datetime.now(timezone.utc)
            safe_error = redact_text(exc)
            logger.error(
                "Conector '%s' falhou antes de confirmar o destino; checkpoint nao avancado. "
                "erro=%s",
                connector.name,
                type(exc).__name__,
            )
            visual_logger.error("'{}': carga falhou. Motivo: {}", connector.name, safe_error)
            self._record_run_safely(
                self._run_log_entry(
                    connector_name=connector.name,
                    context=context,
                    started_at=started_at,
                    finished_at=finished_at,
                    status="error",
                    rows_loaded=0,
                    extra_fields_seen=[],
                    error_message=safe_error,
                    destination=None,
                    window=window,
                    watermark_after=None,
                )
            )
            return StepResult(
                connector_name=connector.name,
                rows_loaded=0,
                error=safe_error,
                status="error",
                started_at=started_at,
                finished_at=finished_at,
                window_start=_window_start(window),
                window_end=_window_end(window),
                watermark_before=_window_watermark_before(window),
                ingestion_key=context.ingestion_key,
            )

        rows_loaded = int(getattr(result, "rows_loaded", 0))
        extra_fields_seen = list(getattr(result, "extra_fields_seen", []) or [])
        destination_label = getattr(result, "table", None)

        try:
            if extraction_session is not None:
                watermark_after = extraction_session.commit()
            else:
                watermark_after = connector.commit_watermark()
        except Exception as exc:
            finished_at = datetime.now(timezone.utc)
            safe_cause = redact_text(exc)
            error_message = f"checkpoint falhou apos a carga: {safe_cause}"
            logger.error(
                "Conector '%s': destino confirmado, mas checkpoint falhou; erro=%s. "
                "A mesma janela sera tentada novamente.",
                connector.name,
                type(exc).__name__,
            )
            visual_logger.error(
                "'{}': dados gravados, mas checkpoint falhou; retry reutilizara a mesma janela. Motivo: {}",
                connector.name,
                safe_cause,
            )
            self._record_run_safely(
                self._run_log_entry(
                    connector_name=connector.name,
                    context=context,
                    started_at=started_at,
                    finished_at=finished_at,
                    status="checkpoint_error",
                    rows_loaded=rows_loaded,
                    extra_fields_seen=extra_fields_seen,
                    error_message=error_message,
                    destination=destination_label,
                    window=window,
                    watermark_after=None,
                )
            )
            return StepResult(
                connector_name=connector.name,
                rows_loaded=rows_loaded,
                error=error_message,
                status="checkpoint_error",
                destination=destination_label,
                started_at=started_at,
                finished_at=finished_at,
                window_start=_window_start(window),
                window_end=_window_end(window),
                watermark_before=_window_watermark_before(window),
                watermark_after=None,
                extra_fields_seen=tuple(extra_fields_seen),
                ingestion_key=context.ingestion_key,
            )

        finished_at = datetime.now(timezone.utc)
        logger.info("Conector '%s': %d linha(s) carregada(s).", connector.name, rows_loaded)
        visual_logger.success(
            "'{}': concluido com sucesso -- {} registro(s), inicio {} fim {}.",
            connector.name,
            rows_loaded,
            started_at.isoformat(timespec="seconds"),
            finished_at.isoformat(timespec="seconds"),
        )
        self._record_run_safely(
            self._run_log_entry(
                connector_name=connector.name,
                context=context,
                started_at=started_at,
                finished_at=finished_at,
                status="success",
                rows_loaded=rows_loaded,
                extra_fields_seen=extra_fields_seen,
                error_message=None,
                destination=destination_label,
                window=window,
                watermark_after=watermark_after,
            )
        )
        return StepResult(
            connector_name=connector.name,
            rows_loaded=rows_loaded,
            status="success",
            destination=destination_label,
            started_at=started_at,
            finished_at=finished_at,
            window_start=_window_start(window),
            window_end=_window_end(window),
            watermark_before=_window_watermark_before(window),
            watermark_after=watermark_after,
            extra_fields_seen=tuple(extra_fields_seen),
            ingestion_key=context.ingestion_key,
        )

    def _load_destination(
        self,
        *,
        connector_name: str,
        schema: EndpointSchema,
        records,
        context: LoadContext,
    ) -> Any:
        contextual_load = getattr(self._destination, "load_with_context", None)
        if callable(contextual_load):
            return contextual_load(
                connector_name=connector_name,
                endpoint=connector_name,
                schema=schema,
                records=records,
                context=context,
            )
        return self._destination.load(
            connector_name=connector_name,
            endpoint=connector_name,
            schema=schema,
            records=records,
        )

    @staticmethod
    def _run_log_entry(
        *,
        connector_name: str,
        context: LoadContext,
        started_at: datetime,
        finished_at: datetime,
        status: str,
        rows_loaded: int,
        extra_fields_seen: list[str],
        error_message: str | None,
        destination: str | None,
        window,
        watermark_after: Watermark | None,
    ) -> RunLogEntry:
        return RunLogEntry(
            connector_name=connector_name,
            started_at=started_at,
            finished_at=finished_at,
            status=status,
            rows_loaded=rows_loaded,
            extra_fields_seen=extra_fields_seen,
            error_message=redact_text(error_message) if error_message else None,
            run_id=context.run_id,
            ingestion_key=context.ingestion_key,
            destination=destination,
            window_start=_window_start(window),
            window_end=_window_end(window),
            watermark_before=_watermark_json(_window_watermark_before(window)),
            watermark_after=_watermark_json(watermark_after),
        )

    def _record_run_safely(self, entry: RunLogEntry) -> None:
        if self._run_log_store is None:
            return
        try:
            self._run_log_store.record(entry)
        except Exception as exc:
            logger.error(
                "Falha ao persistir auditoria da execucao %s; erro=%s",
                entry.run_id,
                type(exc).__name__,
            )
            visual_logger.warning(
                "Auditoria da execucao '{}' nao foi persistida: {}",
                entry.run_id or "sem-id",
                redact_text(exc),
            )


def _window_start(window) -> date | None:
    return getattr(window, "start", None) if window is not None else None


def _window_end(window) -> date | None:
    return getattr(window, "end", None) if window is not None else None


def _window_watermark_before(window) -> Watermark | None:
    return getattr(window, "watermark_before", None) if window is not None else None


def _watermark_json(watermark: Watermark | None) -> str | None:
    if watermark is None:
        return None
    return json.dumps(asdict(watermark), ensure_ascii=False, default=str, sort_keys=True)


def _checkpoint_identity(connector: APIConnector, window) -> str:
    """Bind retry identity to both state namespace and checkpoint-before.

    ``state_key`` is optional for third-party connectors. Falling back to
    ``connector.name`` preserves the pre-namespace deterministic identity.
    """
    return json.dumps(
        {
            "state_key": str(getattr(connector, "state_key", connector.name)),
            "watermark": _watermark_json(_window_watermark_before(window)),
        },
        ensure_ascii=False,
        sort_keys=True,
    )
