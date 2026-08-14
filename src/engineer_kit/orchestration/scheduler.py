"""Encapsula APScheduler: agenda Pipelines para rodar conforme um
Trigger, sem o usuario da lib precisar importar apscheduler."""

from __future__ import annotations

import logging

from apscheduler.schedulers.blocking import BlockingScheduler

from engineer_kit.orchestration.pipeline import Pipeline, PipelineResult
from engineer_kit.orchestration.trigger import Trigger

logger = logging.getLogger("engineer_kit.scheduler")


class ScheduledPipelineError(RuntimeError):
    """Raised so APScheduler records a failed engineer_kit run as a failed job."""


def _run_scheduled_pipeline(pipeline: Pipeline) -> PipelineResult:
    result = pipeline.run()
    if not result.success:
        failed = [step for step in result.steps if not step.success]
        summary = "; ".join(
            f"{step.connector_name}: {step.status}" for step in failed
        ) or "pipeline returned success=False"
        raise ScheduledPipelineError(f"Pipeline agendada falhou: {summary}")
    return result


class Scheduler:
    def __init__(self) -> None:
        self._backend = BlockingScheduler()

    def schedule(self, pipeline: Pipeline, trigger: Trigger, job_id: str) -> None:
        self._backend.add_job(
            _run_scheduled_pipeline,
            args=[pipeline],
            trigger=trigger.to_apscheduler_trigger(),
            id=job_id,
            replace_existing=True,
        )

    def start(self) -> None:
        logger.info("Scheduler iniciado. Ctrl+C para parar.")
        self._backend.start()


__all__ = ["ScheduledPipelineError", "Scheduler"]
