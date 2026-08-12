from datetime import date, datetime, timezone

from deltalake import DeltaTable
import pyarrow.parquet as pq
import pytest

from engineer_kit.adapters.delta import DeltaDestination
from engineer_kit.adapters.parquet import ParquetDestination
from engineer_kit.connectors.incremental import IncrementalWindow
from engineer_kit.orchestration.pipeline import Pipeline
from engineer_kit.storage.batching import MIN_BATCH_SIZE
from engineer_kit.storage.schema import EndpointSchema
from engineer_kit.storage.state_store import Watermark


class RetryConnector:
    def __init__(self):
        self.name = "events"
        self.current_window = None
        self.commit_calls = 0

    def extract(self, end="today"):
        self.current_window = IncrementalWindow(
            start=date(2026, 8, 1),
            end=date(2026, 8, 12),
            watermark_before=None,
        )
        return iter([{"id": "1"}])

    def commit_watermark(self, max_data_date=None):
        self.commit_calls += 1
        if self.commit_calls == 1:
            raise RuntimeError("state unavailable")
        return Watermark(
            last_run_at=datetime(2026, 8, 12, 12, tzinfo=timezone.utc),
            last_data_date=date(2026, 8, 12),
            cursor_value=None,
        )


@pytest.mark.parametrize("backend", ["parquet", "delta"])
def test_official_platform_destinations_replace_same_window_after_checkpoint_failure(
    tmp_path, backend
):
    connector = RetryConnector()
    if backend == "parquet":
        destination = ParquetDestination(
            tmp_path / "bronze",
            batch_size=MIN_BATCH_SIZE,
        )
    else:
        destination = DeltaDestination(
            tmp_path / "bronze",
            batch_size=MIN_BATCH_SIZE,
        )

    pipeline = Pipeline(
        connector=connector,
        schema=EndpointSchema.from_names(["id"]),
        destination=destination,
        run_log=False,
    )

    first = pipeline.run()
    second = pipeline.run()

    assert first.steps[0].status == "checkpoint_error"
    assert second.success
    assert first.steps[0].ingestion_key == second.steps[0].ingestion_key

    if backend == "parquet":
        table = pq.read_table(tmp_path / "bronze" / "events")
        assert table.num_rows == 1
        assert len(list((tmp_path / "bronze" / "events").glob("*.parquet"))) == 1
    else:
        table = DeltaTable(str(tmp_path / "bronze" / "events")).to_pyarrow_table()
        assert table.num_rows == 1
