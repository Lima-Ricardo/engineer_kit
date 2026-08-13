from datetime import date

from engineer_kit import RestConnector
from engineer_kit.adapters.files.state_store import JsonFileStateStore


class Response:
    headers = {}

    def json(self):
        return [{"id": 1, "updated_at": "2024-01-15T10:00:00Z", "ignored": "x"}]


class Client:
    def request(self, *args, **kwargs):
        return Response()


def test_select_can_hide_watermark_field_without_breaking_checkpoint(tmp_path):
    state = JsonFileStateStore(tmp_path / "state.json")
    connector = RestConnector(
        base_url="https://example.test/orders",
        pagination=False,
        incremental="updated_at",
        state_store=state,
        initial_start=date(2024, 1, 1),
        select=["id"],
        http_client=Client(),
    )
    records = connector.collect(end=date(2024, 6, 1))
    assert records == [{"id": "1"}]
    assert state.get_watermark("orders").last_data_date == date(2024, 1, 15)
