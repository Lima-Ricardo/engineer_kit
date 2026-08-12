from datetime import date, datetime, timedelta, timezone

import duckdb

from engineer_kit.connectors.incremental import IncrementalWindow
from engineer_kit.orchestration.pipeline import Pipeline
from engineer_kit.storage.duckdb_loader import DuckDBLoader
from engineer_kit.storage.run_log import RunLogBackend, RunLogEntry
from engineer_kit.storage.schema import EndpointSchema
from engineer_kit.storage.state_store import Watermark


class WindowConnector:
    def __init__(self, records, *, checkpoint_failures=0):
        self.name = "events"
        self.records = records
        self.current_window = None
        self.checkpoint_failures = checkpoint_failures
        self.commit_calls = 0

    def extract(self, end="today"):
        self.current_window = IncrementalWindow(
            start=date(2026, 8, 1),
            end=date(2026, 8, 12),
            watermark_before=None,
        )
        return iter(self.records)

    def commit_watermark(self, max_data_date=None):
        self.commit_calls += 1
        if self.commit_calls <= self.checkpoint_failures:
            raise RuntimeError("state backend unavailable")
        return Watermark(
            last_run_at=datetime(2026, 8, 12, 12, tzinfo=timezone.utc),
            last_data_date=date(2026, 8, 12),
            cursor_value=None,
        )


class AdvancingWindowConnector:
    """Same calendar window, but checkpoint identity advances after each success."""

    def __init__(self):
        self.name = "events"
        self.current_window = None
        self.watermark = None
        self.run_number = 0

    def extract(self, end="today"):
        self.run_number += 1
        self.current_window = IncrementalWindow(
            start=date(2026, 8, 12),
            end=date(2026, 8, 12),
            watermark_before=self.watermark,
        )
        return iter([{"id": str(self.run_number)}])

    def commit_watermark(self, max_data_date=None):
        self.watermark = Watermark(
            last_run_at=datetime(2026, 8, 12, 12, tzinfo=timezone.utc)
            + timedelta(seconds=self.run_number),
            last_data_date=date(2026, 8, 12),
            cursor_value=None,
        )
        return self.watermark


class FailingRunLog(RunLogBackend):
    def __init__(self):
        self.calls = 0

    def record(self, entry: RunLogEntry) -> None:
        self.calls += 1
        raise RuntimeError("audit backend unavailable")


def test_checkpoint_failure_reports_committed_rows_and_retry_does_not_duplicate():
    conn = duckdb.connect()
    connector = WindowConnector([{"id": "1"}], checkpoint_failures=1)
    pipeline = Pipeline(
        connector=connector,
        schema=EndpointSchema.from_names(["id"]),
        destination=DuckDBLoader(conn),
        run_log=False,
    )

    first = pipeline.run()
    assert not first.success
    assert first.steps[0].status == "checkpoint_error"
    assert first.steps[0].rows_loaded == 1
    assert conn.execute("SELECT count(*) FROM bronze.events").fetchone()[0] == 1

    second = pipeline.run()
    assert second.success
    assert conn.execute("SELECT count(*) FROM bronze.events").fetchone()[0] == 1
    assert first.steps[0].ingestion_key == second.steps[0].ingestion_key


def test_successful_same_day_runs_get_different_ingestion_keys_after_checkpoint_advances():
    conn = duckdb.connect()
    connector = AdvancingWindowConnector()
    pipeline = Pipeline(
        connector=connector,
        schema=EndpointSchema.from_names(["id"]),
        destination=DuckDBLoader(conn),
        run_log=False,
    )

    first = pipeline.run()
    second = pipeline.run()

    assert first.success and second.success
    assert first.steps[0].window_start == second.steps[0].window_start
    assert first.steps[0].window_end == second.steps[0].window_end
    assert first.steps[0].ingestion_key != second.steps[0].ingestion_key
    assert conn.execute("SELECT id FROM bronze.events ORDER BY id").fetchall() == [("1",), ("2",)]


def test_audit_failure_is_non_fatal_after_data_and_checkpoint_commit():
    conn = duckdb.connect()
    connector = WindowConnector([{"id": "1"}])
    audit = FailingRunLog()
    pipeline = Pipeline(
        connector=connector,
        schema=EndpointSchema.from_names(["id"]),
        destination=DuckDBLoader(conn),
        run_log_store=audit,
    )

    result = pipeline.run()

    assert result.success
    assert connector.commit_calls == 1
    assert audit.calls == 1
    assert conn.execute("SELECT count(*) FROM bronze.events").fetchone()[0] == 1


def test_bronze_exposes_run_and_window_metadata():
    conn = duckdb.connect()
    connector = WindowConnector([{"id": "1"}])
    result = Pipeline(
        connector=connector,
        schema=EndpointSchema.from_names(["id"]),
        destination=DuckDBLoader(conn),
        run_log=False,
    ).run(run_id="known-run")

    row = conn.execute(
        "SELECT _run_id, _ingestion_key, _window_start, _window_end FROM bronze.events"
    ).fetchone()

    assert row[0] == "known-run"
    assert row[1] == result.steps[0].ingestion_key
    assert row[2] == date(2026, 8, 1)
    assert row[3] == date(2026, 8, 12)
