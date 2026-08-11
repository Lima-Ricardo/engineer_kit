"""Disparadores de execucao de uma Pipeline.

Encapsulam APScheduler por baixo — quem usa a lib nunca importa
apscheduler diretamente, entao o motor pode ser trocado depois sem
quebrar codigo de usuario.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from apscheduler.triggers.cron import CronTrigger as _APCronTrigger
from apscheduler.triggers.interval import IntervalTrigger as _APIntervalTrigger


class Trigger(ABC):
    @abstractmethod
    def to_apscheduler_trigger(self) -> Any: ...


class CronTrigger(Trigger):
    """Ex.: CronTrigger("0 3 * * *") -- todo dia as 3h."""

    def __init__(self, cron_expression: str) -> None:
        self._cron_expression = cron_expression

    def to_apscheduler_trigger(self) -> Any:
        return _APCronTrigger.from_crontab(self._cron_expression)


class IntervalTrigger(Trigger):
    def __init__(self, seconds: int = 0, minutes: int = 0, hours: int = 0) -> None:
        self._seconds = seconds
        self._minutes = minutes
        self._hours = hours

    def to_apscheduler_trigger(self) -> Any:
        return _APIntervalTrigger(seconds=self._seconds, minutes=self._minutes, hours=self._hours)
