from engineer_kit import RestConnector


class Response:
    def __init__(self, value):
        self.value = value
        self.headers = {}

    def json(self):
        return self.value


class Client:
    def __init__(self):
        self.calls = 0
        self.params = []

    def request(self, method, url, params=None, **kwargs):
        pages = [
            {"results": [{"id": 1}], "next_cursor": "abc"},
            {"results": [{"id": 2}], "next_cursor": None},
        ]
        self.params.append(dict(params or {}))
        value = pages[self.calls]
        self.calls += 1
        return Response(value)


def test_minimal_connector_auto_resolves_records_and_cursor():
    client = Client()
    connector = RestConnector(base_url="https://example.test/orders", http_client=client)
    assert connector.collect() == [{"id": "1"}, {"id": "2"}]
    assert connector.name == "orders"
    assert connector.resolved_records_path == "results"
    assert client.params[1] == {"cursor": "abc"}
