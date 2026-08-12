from engineer_kit.connectors.pagination import (
    CursorPagination,
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
