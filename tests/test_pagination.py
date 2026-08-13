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
    assert strategy.initial_params() == {}
    assert strategy.next_params(ParsedPage(records=[{}], raw={}), {}) is None


def test_page_and_offset_progress_and_stop():
    page = PageNumberPagination(page_size=2)
    first = page.initial_params()
    assert page.next_params(ParsedPage(records=[{}, {}], raw=None), first)["page"] == 2
    assert page.next_params(ParsedPage(records=[{}], raw=None), first) is None
    offset = OffsetPagination(limit=2)
    first = offset.initial_params()
    assert offset.next_params(ParsedPage(records=[{}, {}], raw=None), first)["offset"] == 2
    assert offset.next_params(ParsedPage(records=[], raw=None), first) is None


def test_cursor_link_and_next_url():
    cursor = CursorPagination()
    assert cursor.next_params(ParsedPage(records=[{}], raw={"next_cursor": "abc"}), {}) == {"cursor": "abc"}
    assert cursor.next_params(ParsedPage(records=[{}], raw={}), {}) is None
    link = LinkHeaderPagination()
    linked = ParsedPage(records=[{}], raw=None, headers={"Link": '<https://api.test/items?page=2>; rel="next"'})
    assert link.next_params(linked, {}) == {NEXT_URL_KEY: "https://api.test/items?page=2"}
    next_url = NextUrlPagination()
    assert next_url.next_params(ParsedPage(records=[{}], raw={"next": "https://api.test/2"}), {}) == {NEXT_URL_KEY: "https://api.test/2"}


def test_standard_catalog_includes_auto_resolution():
    assert set(STANDARD_PAGINATION_TYPES) == {
        "none", "auto", "page", "offset", "cursor", "link_header", "next_url"
    }
    assert STANDARD_PAGINATION_TYPES["auto"] is AutoPagination
    assert STANDARD_PAGINATION_TYPES["page"] is PageNumberPagination
