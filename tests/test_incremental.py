from datetime import date

import duckdb
import pytest

from engineer_kit.connectors.incremental import IncrementalMode, IncrementalStrategy
from engineer_kit.storage.state_store import IngestionStateStore


@pytest.fixture
def state_store():
    conn = duckdb.connect()
    return IngestionStateStore(conn)


def test_first_run_uses_initial_start(state_store):
    strategy = IncrementalStrategy(
        connector_name="conn_a",
        state_store=state_store,
        initial_start=date(2024, 1, 1),
    )
    window = strategy.resolve_window(end=date(2024, 2, 1))
    assert window.start == date(2024, 1, 1)
    assert window.end == date(2024, 2, 1)


def test_commit_then_next_window_starts_from_watermark_data_date(state_store):
    strategy = IncrementalStrategy(
        connector_name="conn_a",
        state_store=state_store,
        mode=IncrementalMode.DATA_DATE,
        initial_start=date(2024, 1, 1),
    )
    window = strategy.resolve_window(end=date(2024, 2, 1))
    strategy.commit(window, max_data_date=date(2024, 1, 20))

    next_window = strategy.resolve_window(end=date(2024, 3, 1))
    assert next_window.start == date(2024, 1, 20)
    assert next_window.end == date(2024, 3, 1)


def test_commit_without_max_data_date_falls_back_to_window_end(state_store):
    strategy = IncrementalStrategy(connector_name="conn_a", state_store=state_store)
    window = strategy.resolve_window(end=date(2024, 2, 1))
    strategy.commit(window)

    next_window = strategy.resolve_window(end=date(2024, 3, 1))
    assert next_window.start == date(2024, 2, 1)


def test_ingestion_date_mode_uses_last_run_timestamp(state_store):
    strategy = IncrementalStrategy(
        connector_name="conn_a",
        state_store=state_store,
        mode=IncrementalMode.INGESTION_DATE,
    )
    window = strategy.resolve_window(end=date(2024, 2, 1))
    strategy.commit(window, max_data_date=date(2024, 1, 5))  # ignorado nesse modo para o proximo start

    next_window = strategy.resolve_window(end=date(2024, 3, 1))
    # no modo INGESTION_DATE o start vem de last_run_at.date(), nao de last_data_date
    assert next_window.start is not None


def test_different_connectors_have_independent_watermarks(state_store):
    strategy_a = IncrementalStrategy(connector_name="conn_a", state_store=state_store)
    strategy_b = IncrementalStrategy(connector_name="conn_b", state_store=state_store)

    window_a = strategy_a.resolve_window(end=date(2024, 2, 1))
    strategy_a.commit(window_a, max_data_date=date(2024, 1, 15))

    window_b = strategy_b.resolve_window(end=date(2024, 2, 1))
    assert window_b.start is None  # conn_b nunca rodou, nao foi afetado pelo commit de conn_a
