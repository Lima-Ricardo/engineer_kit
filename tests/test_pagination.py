from engineer_kit.connectors.pagination import (
    NEXT_URL_KEY,
    STANDARD_PAGINATION_TYPES,
    AutoPagination,
    CursorPagination,
    LinkHeaderPagination,
    NextUrlPagination,
    NoPagination,
    OffsetPagination,
    PageNumberPagination,
    ParsedPage,
)


def test_no_pagination_never_continues():
    strategy = NoPagination()
    page = ParsedPage(records=[{"a": 1}], raw={"a": 1})
    assert strategy.initial_params() == {}
    assert strategy.next_params(page, {}) is None


def test_page_number_stops_on_incomplete_page():
    strategy = PageNumberPagination(page_size=2)
    params = strategy.initial_params()
    assert params == {"page": 1, "per_page": 2}

    full_page = ParsedPage(records=[{}, {}], raw=None)
    next_params = strategy.next_params(full_page, params)
    assert next_params == {"page": 2, "per_page": 2}

    incomplete_page = ParsedPage(records=[{}], raw=None)
    assert strategy.next_params(incomplete_page, next_params) is None


def test_offset_pagination_advances_by_limit():
    strategy = OffsetPagination(limit=50)
    params = strategy.initial_params()
    full_page = ParsedPage(records=[{}] * 50, raw=None)
    next_params = strategy.next_params(full_page, params)
    assert next_params == {"offset": 50, "limit": 50}

    empty_page = ParsedPage(records=[], raw=None)
    assert strategy.next_params(empty_page, next_params) is None


def test_cursor_pagination_reads_cursor_from_raw_body():
    strategy = CursorPagination(cursor_param="cursor", cursor_field="next_cursor")
    page_with_cursor = ParsedPage(records=[{}], raw={"next_cursor": "abc123"})
    next_params = strategy.next_params(page_with_cursor, {})
    assert next_params == {"cursor": "abc123"}

    page_without_cursor = ParsedPage(records=[{}], raw={"next_cursor": None})
    assert strategy.next_params(page_without_cursor, {}) is None

    page_non_dict_raw = ParsedPage(records=[{}], raw=[1, 2, 3])
    assert strategy.next_params(page_non_dict_raw, {}) is None


def test_link_header_pagination_extracts_next_url_from_header():
    strategy = LinkHeaderPagination()
    page = ParsedPage(
        records=[{}],
        raw=None,
        headers={"Link": '<https://api.test/items?page=2>; rel="next", <https://api.test/items?page=1>; rel="prev"'},
    )
    next_params = strategy.next_params(page, {})
    assert next_params == {NEXT_URL_KEY: "https://api.test/items?page=2"}


def test_link_header_pagination_stops_without_next_rel():
    strategy = LinkHeaderPagination()
    page_no_header = ParsedPage(records=[{}], raw=None, headers={})
    assert strategy.next_params(page_no_header, {}) is None

    page_only_prev = ParsedPage(
        records=[{}], raw=None, headers={"Link": '<https://api.test/items?page=1>; rel="prev"'}
    )
    assert strategy.next_params(page_only_prev, {}) is None


def test_next_url_pagination_reads_url_field_from_body():
    strategy = NextUrlPagination(next_url_field="next")
    page_with_next = ParsedPage(records=[{}], raw={"next": "https://api.test/items?cursor=abc"})
    assert strategy.next_params(page_with_next, {}) == {
        NEXT_URL_KEY: "https://api.test/items?cursor=abc"
    }

    page_without_next = ParsedPage(records=[{}], raw={"next": None})
    assert strategy.next_params(page_without_next, {}) is None


def test_auto_pagination_resolves_cursor_once():
    strategy = AutoPagination()
    first = ParsedPage(records=[{}], raw={"next_cursor": "abc"})
    assert strategy.next_params(first, {}) == {"cursor": "abc"}
    assert strategy.resolved_type == "cursor"

    second = ParsedPage(records=[{}], raw={"next_cursor": None})
    assert strategy.next_params(second, {"cursor": "abc"}) is None
    assert strategy.resolved_type == "cursor"


def test_standard_pagination_types_catalog_lists_all_strategies():
    assert set(STANDARD_PAGINATION_TYPES) == {
        "none",
        "auto",
        "page",
        "offset",
        "cursor",
        "link_header",
        "next_url",
    }
    assert STANDARD_PAGINATION_TYPES["auto"] is AutoPagination
    assert STANDARD_PAGINATION_TYPES["page"] is PageNumberPagination
    assert STANDARD_PAGINATION_TYPES["link_header"] is LinkHeaderPagination
