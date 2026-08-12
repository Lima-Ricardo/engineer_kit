from datetime import date

import duckdb
import pytest

from engineer_kit.connectors.incremental import IncrementalStrategy
from engineer_kit.connectors.pagination import PageNumberPagination
from engineer_kit.connectors.rest import DateParams, RestConnector
from engineer_kit.storage.state_store import IngestionStateStore


class FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def json(self):
        return self._payload


class FakeHttpClient:
    def __init__(self, pages):
        self._pages = pages
        self.requests = []

    def get(self, url, params=None, **kwargs):
        self.requests.append({"url": url, "params": dict(params or {})})
        page = self._pages[len(self.requests) - 1]
        return FakeResponse(page)


@pytest.fixture
def incremental():
    conn = duckdb.connect()
    return IncrementalStrategy(
        connector_name="fake_api",
        state_store=IngestionStateStore(conn),
        initial_start=date(2024, 1, 1),
    )


def test_extract_paginates_until_incomplete_page_and_stops(incremental):
    fake_http = FakeHttpClient(
        pages=[
            [{"id": 1}, {"id": 2}],
            [{"id": 3}],
        ]
    )
    connector = RestConnector(
        name="fake_api",
        base_url="https://example.test/items",
        incremental=incremental,
        pagination=PageNumberPagination(page_size=2),
        http_client=fake_http,
    )

    records = list(connector.extract(end=date(2024, 2, 1)))

    assert [r["id"] for r in records] == ["1", "2", "3"]  # stringify aplicado
    assert len(fake_http.requests) == 2
    assert fake_http.requests[0]["params"] == {"page": 1, "per_page": 2}
    assert fake_http.requests[1]["params"] == {"page": 2, "per_page": 2}


def test_date_params_are_injected_into_request(incremental):
    fake_http = FakeHttpClient(pages=[[]])
    connector = RestConnector(
        name="fake_api",
        base_url="https://example.test/items",
        incremental=incremental,
        pagination=PageNumberPagination(page_size=100),
        date_params=DateParams(start="since", end="until", date_format="%Y-%m-%d"),
        http_client=fake_http,
    )

    list(connector.extract(end=date(2024, 2, 1)))

    params = fake_http.requests[0]["params"]
    assert params["since"] == "2024-01-01"
    assert params["until"] == "2024-02-01"


def test_records_path_extracts_nested_list(incremental):
    fake_http = FakeHttpClient(pages=[{"results": [{"id": 1}], "next_cursor": None}])
    connector = RestConnector(
        name="fake_api",
        base_url="https://example.test/items",
        incremental=incremental,
        pagination=PageNumberPagination(page_size=100),
        records_path="results",
        http_client=fake_http,
    )

    records = list(connector.extract(end=date(2024, 2, 1)))
    assert [r["id"] for r in records] == ["1"]


def test_commit_watermark_before_extract_raises(incremental):
    connector = RestConnector(
        name="fake_api",
        base_url="https://example.test/items",
        incremental=incremental,
        pagination=PageNumberPagination(page_size=100),
        http_client=FakeHttpClient(pages=[[]]),
    )
    with pytest.raises(RuntimeError):
        connector.commit_watermark()
