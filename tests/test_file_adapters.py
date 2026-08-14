import json
import multiprocessing
import os
from datetime import date, datetime, timezone

import pytest

from engineer_kit.adapters.files import JsonFileStateStore, JsonLinesRunLogStore
from engineer_kit.storage.run_log import RunLogEntry
from engineer_kit.storage.state_store import StateConflictError, Watermark


def _watermark(day: int, cursor: str | None = None) -> Watermark:
    return Watermark(
        last_run_at=datetime(2026, 8, day, 12, tzinfo=timezone.utc),
        last_data_date=date(2026, 8, day),
        cursor_value=cursor,
    )


def _cas_worker(path, expected, replacement, ready, start, results):
    store = JsonFileStateStore(path)
    ready.put("ready")
    start.wait(timeout=15)
    try:
        store.compare_and_set_watermark("orders", expected, replacement)
    except StateConflictError:
        results.put("conflict")
    else:
        results.put("ok")


def _jsonl_worker(path, prefix, count, ready, start):
    store = JsonLinesRunLogStore(path)
    ready.put("ready")
    start.wait(timeout=15)
    for index in range(count):
        store.record(
            RunLogEntry(
                connector_name="orders",
                started_at=datetime(2026, 8, 12, 12, tzinfo=timezone.utc),
                finished_at=datetime(2026, 8, 12, 12, 1, tzinfo=timezone.utc),
                status="success",
                rows_loaded=1,
                extra_fields_seen=[],
                run_id=f"{prefix}-{index}",
            )
        )


def test_json_file_state_store_round_trip_and_replace(tmp_path):
    path = tmp_path / "_meta" / "state.json"
    store = JsonFileStateStore(path)
    first = _watermark(9)
    second = _watermark(11, "cursor-2")

    assert store.supports_atomic_compare_and_set is True
    assert store.get_watermark("orders") is None
    store.set_watermark("orders", first)
    assert store.get_watermark("orders") == first
    store.set_watermark("orders", second)
    assert store.get_watermark("orders") == second

    payload = json.loads(path.read_text(encoding="utf-8"))
    assert list(payload) == ["orders"]
    if os.name != "nt":
        assert path.stat().st_mode & 0o777 == 0o600


def test_json_file_state_store_cas_allows_only_one_process_to_advance(tmp_path):
    path = tmp_path / "state.json"
    expected = _watermark(10)
    replacements = (_watermark(11, "a"), _watermark(12, "b"))
    JsonFileStateStore(path).set_watermark("orders", expected)

    context = multiprocessing.get_context("spawn")
    ready = context.Queue()
    results = context.Queue()
    start = context.Event()
    processes = [
        context.Process(
            target=_cas_worker,
            args=(str(path), expected, replacement, ready, start, results),
        )
        for replacement in replacements
    ]
    for process in processes:
        process.start()
    assert [ready.get(timeout=15) for _ in processes] == ["ready", "ready"]
    start.set()
    outcomes = sorted(results.get(timeout=20) for _ in processes)
    for process in processes:
        process.join(timeout=20)
        assert process.exitcode == 0

    assert outcomes == ["conflict", "ok"]
    assert JsonFileStateStore(path).get_watermark("orders") in replacements


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


def test_json_lines_run_log_preserves_all_concurrent_process_records(tmp_path):
    path = tmp_path / "run_log.jsonl"
    context = multiprocessing.get_context("spawn")
    ready = context.Queue()
    start = context.Event()
    count = 25
    processes = [
        context.Process(
            target=_jsonl_worker,
            args=(str(path), prefix, count, ready, start),
        )
        for prefix in ("a", "b")
    ]
    for process in processes:
        process.start()
    assert [ready.get(timeout=15) for _ in processes] == ["ready", "ready"]
    start.set()
    for process in processes:
        process.join(timeout=30)
        assert process.exitcode == 0

    rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
    assert len(rows) == count * 2
    assert len({row["run_id"] for row in rows}) == count * 2


@pytest.mark.skipif(os.name == "nt", reason="POSIX mode bits are not portable to Windows")
def test_metadata_permissions_are_repaired_if_existing_file_is_too_open(tmp_path):
    state_path = tmp_path / "state.json"
    state_path.write_text("{}", encoding="utf-8")
    state_path.chmod(0o644)
    store = JsonFileStateStore(state_path)
    store.set_watermark("orders", _watermark(12))
    assert state_path.stat().st_mode & 0o777 == 0o600
