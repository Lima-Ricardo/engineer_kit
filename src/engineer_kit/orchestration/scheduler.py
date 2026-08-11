"""Encapsula APScheduler: agenda Pipelines para rodar conforme um
Trigger, sem o usuario da lib precisar importar apscheduler."""

from __future__ import annotations

import logging

from apscheduler.schedulers.blocking import BlockingScheduler

from engineer_kit.orchestration.pipeline import Pipeline
from engineer_kit.orchestration.trigger import Trigger

logger = logging.getLogger("engineer_kit.scheduler")


class Scheduler:
    def __init__(self) -> None:
        self._backend = BlockingScheduler()

    def schedule(self, pipeline: Pipeline, trigger: Trigger, job_id: str) -> None:
        self._backend.add_job(
            pipeline.run,
            trigger=trigger.to_apscheduler_trigger(),
            id=job_id,
            replace_existing=True,
        )

    def start(self) -> None:
        logger.info("Scheduler iniciado. Ctrl+C para parar.")
        self._backend.start()
