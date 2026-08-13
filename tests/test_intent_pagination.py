from engineer_kit.connectors.pagination import AutoPagination, CursorPagination, resolve_pagination


def test_pagination_strings_are_case_insensitive():
    assert isinstance(resolve_pagination("Cursor"), CursorPagination)
    assert isinstance(resolve_pagination("AUTO"), AutoPagination)
