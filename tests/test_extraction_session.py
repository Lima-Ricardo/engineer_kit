from datetime import date

import pytest

from engineer_kit import Connector, RestConnector
from engineer_kit.connectors.extraction import (
    DEFAULT_EXTRACTION_BATCH_SIZE,
    ExtractionSession,
    InvalidExtractionBatchSizeError,
)
from engineer_kit.connectors.incremental import IncrementalMode, IncrementalStrategy
from engineer_kit.storage.state_store import StateStore, Watermark


class MemoryStateStore(StateStore):
    def __init__(self):
        self.values = {}

    def get_watermark(self, connector_name: str):
        return self.values.get(connector_name)

    def set_watermark(self, connector_name: str, watermark: Watermark) -> None:
        self.values[connector_name] = watermark


def _session(records, *, batch_size=DEFAULT_EXTRACTION_BATCH_SIZE):
    store = MemoryStateStore()
    incremental = IncrementalStrategy(
        connector_name="orders",
        state_store=store,
        mode=IncrementalMode.INGESTION_DATE,
        initial_start=date(2026, 1, 1),
    )
    window = incremental.resolve_window(date(2026, 1, 31))
    return (
        ExtractionSession(
            window=window,
            records=iter(records),
            incremental=incremental,
            batch_size=batch_size,
        ),
        store,
    )


def test_default_extraction_batch_size_is_25000():
    assert DEFAULT_EXTRACTION_BATCH_SIZE == 25_000


def test_connector_contract_is_platform_neutral_parent():
    assert issubclass(RestConnector, Connector)


def test_normal_iteration_yields_bounded_batches_by_default():
    records = ({"id": str(index)} for index in range(25_001))
    session, _ = _session(records)

    batches = list(session)

    assert [len(batch) for batch in batches] == [25_000, 1]
    assert session.exhausted is True


def test_iter_batches_can_override_default_without_changing_session_default():
    session, _ = _session(({"id": str(i)} for i in range(7)), batch_size=25_000)

    batches = list(session.iter_batches(size=3))

    assert [len(batch) for batch in batches] == [3, 3, 1]
    assert session.batch_size == 25_000


def test_checkpoint_only_advances_after_full_consumption():
    session, store = _session(({"id": str(i)} for i in range(4)), batch_size=2)
    batches = session.iter_batches()

    assert len(next(batches)) == 2
    with pytest.raises(RuntimeError, match="consumida por completo"):
        session.commit()
    assert store.get_watermark("orders") is None

    assert len(next(batches)) == 2
    with pytest.raises(StopIteration):
        next(batches)

    watermark = session.commit()
    assert store.get_watermark("orders") == watermark
    assert session.committed is True


def test_collect_is_explicit_full_materialization():
    session, store = _session(({"id": str(i)} for i in range(5)))

    records = session.collect()

    assert records == [{"id": str(i)} for i in range(5)]
    assert session.exhausted is True
    assert store.get_watermark("orders") is None
    session.commit()
    assert store.get_watermark("orders") is not None


def test_session_is_single_pass():
    session, _ = _session(({"id": str(i)} for i in range(2)))
    assert len(session.collect()) == 2

    with pytest.raises(RuntimeError, match="single-pass"):
        list(session.iter_batches())


def test_abort_never_advances_checkpoint():
    session, store = _session(({"id": "1"},))
    session.abort()

    with pytest.raises(RuntimeError, match="abortada"):
        list(session)
    with pytest.raises(RuntimeError, match="abortada"):
        session.commit()
    assert store.get_watermark("orders") is None


def test_invalid_batch_sizes_are_rejected():
    for value in (0, -1, True, 1.5):
        with pytest.raises(InvalidExtractionBatchSizeError):
            _session([], batch_size=value)
