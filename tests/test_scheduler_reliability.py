from datetime import datetime, timezone

import pytest

from engineer_kit.orchestration.pipeline import PipelineResult, StepResult
from engineer_kit.orchestration.scheduler import ScheduledPipelineError, _run_scheduled_pipeline


class _FakePipeline:
    def __init__(self, result: PipelineResult) -> None:
        self._result = result

    def run(self) -> PipelineResult:
        return self._result


def _result(step: StepResult) -> PipelineResult:
    now = datetime.now(timezone.utc)
    return PipelineResult(steps=[step], run_id="scheduled", started_at=now, finished_at=now)


def test_scheduler_wrapper_returns_successful_pipeline_result():
    expected = _result(StepResult(connector_name="orders", rows_loaded=3))
    assert _run_scheduled_pipeline(_FakePipeline(expected)) is expected


def test_scheduler_wrapper_raises_when_pipeline_returns_failed_step():
    failed = _result(
        StepResult(
            connector_name="orders",
            rows_loaded=0,
            status="error",
            error="source unavailable",
        )
    )
    with pytest.raises(ScheduledPipelineError, match="orders: error"):
        _run_scheduled_pipeline(_FakePipeline(failed))
