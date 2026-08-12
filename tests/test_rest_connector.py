from datetime import date

import duckdb
import pytest

from engineer_kit.connectors.api_connector import InvalidHttpMethodError, MissingDateFieldError
from engineer_kit.connectors.incremental import IncrementalMode, IncrementalStrategy
from engineer_kit.connectors.pagination import (
    LinkHeaderPagination,
    NextUrlPagination,
    PageNumberPagination,
)
from engineer_kit.connectors.rest import DateParams, RestConnector
from engineer_kit.storage.state_store import IngestionStateStore


class FakeResponse:
    def __init__(self, payload, headers=None):
        self._payload = payload
        self.headers = headers or {}

    def json(self):
        return self._payload


class FakeHttpClient:
    def __init__(self, pages, headers_per_page=None):
        self._pages = pages
        self._headers_per_page = headers_per_page or [{}] * len(pages)
        self.requests = []

    def request(self, method, url, params=None, json=None, **kwargs):
        self.requests.append(
            {"method": method, "url": url, "params": dict(params or {}), "json": json}
        )
        index = len(self.requests) - 1
        headers = self._headers_per_page[index] if index < len(self._headers_per_page) else {}
        return FakeResponse(self._pages[index], headers=headers)


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
        method="GET",
        http_client=fake_http,
    )

    records = list(connector.extract(end=date(2024, 2, 1)))

    assert [r["id"] for r in records] == ["1", "2", "3"]  # stringify aplicado
    assert len(fake_http.requests) == 2
    assert fake_http.requests[0]["method"] == "GET"
    assert fake_http.requests[0]["params"] == {"page": 1, "per_page": 2}
    assert fake_http.requests[1]["params"] == {"page": 2, "per_page": 2}


def test_date_params_are_injected_into_request(incremental):
    fake_http = FakeHttpClient(pages=[[]])
    connector = RestConnector(
        name="fake_api",
        base_url="https://example.test/items",
        incremental=incremental,
        pagination=PageNumberPagination(page_size=100),
        method="GET",
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
        method="GET",
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
        method="GET",
        http_client=FakeHttpClient(pages=[[]]),
    )
    with pytest.raises(RuntimeError):
        connector.commit_watermark()


def test_invalid_method_is_rejected(incremental):
    with pytest.raises(InvalidHttpMethodError):
        RestConnector(
            name="fake_api",
            base_url="https://example.test/items",
            incremental=incremental,
            pagination=PageNumberPagination(page_size=100),
            method="DELETE",
            http_client=FakeHttpClient(pages=[[]]),
        )


def test_post_method_sends_payload_as_json_body(incremental):
    fake_http = FakeHttpClient(pages=[[{"id": 1}]])
    connector = RestConnector(
        name="fake_api",
        base_url="https://example.test/search",
        incremental=incremental,
        pagination=PageNumberPagination(page_size=100),
        method="POST",
        static_params={"query": "algo"},
        http_client=fake_http,
    )

    list(connector.extract(end=date(2024, 2, 1)))

    assert fake_http.requests[0]["method"] == "POST"
    assert fake_http.requests[0]["json"]["query"] == "algo"
    assert fake_http.requests[0]["params"] == {}  # nao usa query string quando e POST


def test_link_header_pagination_follows_absolute_next_url(incremental):
    fake_http = FakeHttpClient(
        pages=[[{"id": 1}], [{"id": 2}]],
        headers_per_page=[{"Link": '<https://example.test/items?page=2>; rel="next"'}, {}],
    )
    connector = RestConnector(
        name="fake_api",
        base_url="https://example.test/items",
        incremental=incremental,
        pagination=LinkHeaderPagination(),
        method="GET",
        http_client=fake_http,
    )

    records = list(connector.extract(end=date(2024, 2, 1)))

    assert [r["id"] for r in records] == ["1", "2"]
    assert fake_http.requests[0]["url"] == "https://example.test/items"
    assert fake_http.requests[1]["url"] == "https://example.test/items?page=2"


def test_next_url_pagination_follows_url_from_response_body(incremental):
    fake_http = FakeHttpClient(
        pages=[
            {"results": [{"id": 1}], "next": "https://example.test/items?cursor=abc"},
            {"results": [{"id": 2}], "next": None},
        ]
    )
    connector = RestConnector(
        name="fake_api",
        base_url="https://example.test/items",
        incremental=incremental,
        pagination=NextUrlPagination(),
        method="GET",
        records_path="results",
        http_client=fake_http,
    )

    records = list(connector.extract(end=date(2024, 2, 1)))

    assert [r["id"] for r in records] == ["1", "2"]
    assert fake_http.requests[1]["url"] == "https://example.test/items?cursor=abc"


def test_data_date_mode_without_date_field_is_rejected():
    conn = duckdb.connect()
    with pytest.raises(MissingDateFieldError):
        RestConnector(
            name="fake_api",
            base_url="https://example.test/items",
            state_store=IngestionStateStore(conn),
            incremental_mode=IncrementalMode.DATA_DATE,
            pagination=PageNumberPagination(page_size=100),
            method="GET",
            http_client=FakeHttpClient(pages=[[]]),
        )


def test_ingestion_date_mode_does_not_require_date_field():
    conn = duckdb.connect()
    connector = RestConnector(
        name="fake_api",
        base_url="https://example.test/items",
        state_store=IngestionStateStore(conn),
        incremental_mode=IncrementalMode.INGESTION_DATE,
        pagination=PageNumberPagination(page_size=100),
        method="GET",
        http_client=FakeHttpClient(pages=[[]]),
    )
    assert connector is not None


def test_data_date_mode_tracks_real_max_date_from_records_automatically():
    """O bug que motivou essa mudanca: sem date_field, commit_watermark()
    (chamado sem argumentos, como o Pipeline sempre chama) so tinha
    window.end para usar -- DATA_DATE se comportava igual a
    INGESTION_DATE. Com date_field, a maior data real dos registros e
    rastreada durante extract() e usada automaticamente no commit."""
    conn = duckdb.connect()
    state_store = IngestionStateStore(conn)
    fake_http = FakeHttpClient(
        pages=[
            {
                "results": [
                    {"id": 1, "commit": {"author": {"date": "2024-01-05T10:00:00Z"}}},
                    {"id": 2, "commit": {"author": {"date": "2024-01-15T10:00:00Z"}}},
                ],
                "next": None,
            }
        ]
    )
    connector = RestConnector(
        name="fake_api",
        base_url="https://example.test/items",
        state_store=state_store,
        incremental_mode=IncrementalMode.DATA_DATE,
        initial_start=date(2024, 1, 1),
        date_field="commit.author.date",
        pagination=NextUrlPagination(),
        method="GET",
        records_path="results",
        http_client=fake_http,
    )

    list(connector.extract(end=date(2024, 6, 1)))  # end bem depois da maior data real
    connector.commit_watermark()

    watermark = state_store.get_watermark("fake_api")
    assert watermark.last_data_date == date(2024, 1, 15)  # maior data do dado, nao window.end
