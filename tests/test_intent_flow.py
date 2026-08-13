from engineer_kit import RestConnector


class _Response:
    headers = {}

    def json(self):
        return [{"id": 1, "amount": 12.5}]


class _Client:
    def request(self, *args, **kwargs):
        return _Response()


def test_managed_duckdb_flow_loads_without_manual_adapters(tmp_path):
    result = RestConnector(
        base_url="https://example.test/orders",
        pagination=False,
        http_client=_Client(),
    ).to(
        "duckdb",
        "bronze.orders",
        path=tmp_path / "analytics.duckdb",
    ).run()

    assert result.success
    assert result.rows_loaded == 1
