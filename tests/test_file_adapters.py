import json
import os
from datetime import date, datetime, timezone

import pytest

from engineer_kit.adapters.files import JsonFileStateStore, JsonLinesRunLogStore
from engineer_kit.storage.run_log import RunLogEntry
from engineer_kit.storage.state_store import Watermark


def test_json_file_state_store_round_trip_and_replace(tmp_path):
    path = tmp_path / "_meta" / "state.json"
    store = JsonFileStateStore(path)
    first = Watermark(
        last_run_at=datetime(2026, 8, 10, 12, tzinfo=timezone.utc),
        last_data_date=date(2026, 8, 9),
        cursor_value=None,
    )
    second = Watermark(
        last_run_at=datetime(2026, 8, 12, 12, tzinfo=timezone.utc),
        last_data_date=date(2026, 8, 11),
        cursor_value="cursor-2",
    )

    assert store.get_watermark("orders") is None
    store.set_watermark("orders", first)
    assert store.get_watermark("orders") == first
    store.set_watermark("orders", second)
    assert store.get_watermark("orders") == second

    payload = json.loads(path.read_text(encoding="utf-8"))
    assert list(payload) == ["orders"]
    if os.name != "nt":
        assert path.stat().st_mode & 0o777 == 0o600


def test_json_lines_run_log_keeps_rich_metadata(tmp_path):
    path = tmp_path / "_meta" / "run_log.jsonl"
    store = JsonLinesRunLogStore(path)
    store.record(
        RunLogEntry(
            connector_name="orders",
            started_at=datetime(2026, 8, 12, 12, tzinfo=timezone.utc),
            finished_at=datetime(2026, 8, 12, 12, 1, tzinfo=timezone.utc),
            status="success",
            rows_loaded=42,
            extra_fields_seen=["new_field"],
            run_id="run-123",
            destination="bronze/orders",
            window_start=date(2026, 8, 11),
            window_end=date(2026, 8, 12),
            watermark_after='{"last_data_date":"2026-08-12"}',
        )
    )

    row = json.loads(path.read_text(encoding="utf-8").strip())
    assert row["run_id"] == "run-123"
    assert row["destination"] == "bronze/orders"
    assert row["window_start"] == "2026-08-11"
    assert row["extra_fields_seen"] == ["new_field"]
    if os.name != "nt":
        assert path.stat().st_mode & 0o777 == 0o600


@pytest.mark.skipif(os.name == "nt", reason="POSIX mode bits are not portable to Windows")
def test_metadata_permissions_are_repaired_if_existing_file_is_too_open(tmp_path):
    state_path = tmp_path / "state.json"
    state_path.write_text("{}", encoding="utf-8")
    state_path.chmod(0o644)
    store = JsonFileStateStore(state_path)
    store.set_watermark(
        "orders",
        Watermark(
            last_run_at=datetime(2026, 8, 12, tzinfo=timezone.utc),
            last_data_date=date(2026, 8, 12),
        ),
    )
    assert state_path.stat().st_mode & 0o777 == 0o600
