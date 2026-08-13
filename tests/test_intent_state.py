from engineer_kit import RestConnector


class _Response:
    headers = {}

    def json(self):
        return [{"id": 1}]


class _Client:
    def request(self, *args, **kwargs):
        return _Response()


def test_managed_incremental_uses_destination_state(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    result = RestConnector(
        base_url="https://example.test/orders",
        pagination=False,
        incremental=True,
        http_client=_Client(),
    ).to(
        "duckdb",
        "bronze.orders",
        path=tmp_path / "analytics.duckdb",
    ).run()

    assert result.success
    assert not (tmp_path / ".engineer_kit" / "state.json").exists()
