from datetime import date

from engineer_kit import IncrementalStrategy, StateStore, Watermark


class MemoryStateStore(StateStore):
    """Backend minimo que prova que o incremental nao depende de DuckDB."""

    def __init__(self):
        self.values = {}

    def get_watermark(self, connector_name):
        return self.values.get(connector_name)

    def set_watermark(self, connector_name, watermark):
        self.values[connector_name] = watermark


def test_incremental_strategy_accepts_non_duckdb_state_store():
    store = MemoryStateStore()
    strategy = IncrementalStrategy(
        connector_name="orders",
        state_store=store,
        initial_start=date(2024, 1, 1),
    )

    first = strategy.resolve_window(end=date(2024, 2, 1))
    assert first.start == date(2024, 1, 1)

    strategy.commit(first, max_data_date=date(2024, 1, 20))

    second = strategy.resolve_window(end=date(2024, 3, 1))
    assert second.start == date(2024, 1, 20)
    assert isinstance(store.get_watermark("orders"), Watermark)
