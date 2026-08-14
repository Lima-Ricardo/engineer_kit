"""Pagination strategies and ergonomic resolution.

Strict strategy objects remain the runtime contract. Public callers may pass a
string or a small mapping; resolution happens once before the hot path.
"""

from __future__ import annotations

import re
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any

NEXT_URL_KEY = "__next_url__"


@dataclass
class ParsedPage:
    records: list[dict[str, Any]]
    raw: Any
    headers: dict[str, str] = field(default_factory=dict)


def _get_path(value: Any, path: str) -> Any:
    current = value
    for key in path.split("."):
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


class PaginationStrategy(ABC):
    @abstractmethod
    def initial_params(self) -> dict[str, Any]: ...

    @abstractmethod
    def next_params(self, page: ParsedPage, previous_params: dict[str, Any]) -> dict[str, Any] | None: ...


class NoPagination(PaginationStrategy):
    def initial_params(self) -> dict[str, Any]:
        return {}

    def next_params(self, page: ParsedPage, previous_params: dict[str, Any]) -> dict[str, Any] | None:
        return None


class PageNumberPagination(PaginationStrategy):
    def __init__(
        self,
        page_param: str = "page",
        page_size_param: str = "per_page",
        page_size: int = 100,
        start_page: int = 1,
    ) -> None:
        self._page_param = page_param
        self._page_size_param = page_size_param
        self._page_size = page_size
        self._start_page = start_page

    def initial_params(self) -> dict[str, Any]:
        return {self._page_param: self._start_page, self._page_size_param: self._page_size}

    def next_params(self, page: ParsedPage, previous_params: dict[str, Any]) -> dict[str, Any] | None:
        if len(page.records) < self._page_size:
            return None
        return {**previous_params, self._page_param: previous_params[self._page_param] + 1}


class OffsetPagination(PaginationStrategy):
    def __init__(
        self,
        offset_param: str = "offset",
        limit_param: str = "limit",
        limit: int = 100,
        start_offset: int = 0,
    ) -> None:
        self._offset_param = offset_param
        self._limit_param = limit_param
        self._limit = limit
        self._start_offset = start_offset

    def initial_params(self) -> dict[str, Any]:
        return {self._offset_param: self._start_offset, self._limit_param: self._limit}

    def next_params(self, page: ParsedPage, previous_params: dict[str, Any]) -> dict[str, Any] | None:
        if len(page.records) < self._limit:
            return None
        return {**previous_params, self._offset_param: previous_params[self._offset_param] + self._limit}


class CursorPagination(PaginationStrategy):
    def __init__(self, cursor_param: str = "cursor", cursor_field: str = "next_cursor") -> None:
        self._cursor_param = cursor_param
        self._cursor_field = cursor_field

    def initial_params(self) -> dict[str, Any]:
        return {}

    def next_params(self, page: ParsedPage, previous_params: dict[str, Any]) -> dict[str, Any] | None:
        cursor = _get_path(page.raw, self._cursor_field)
        if not cursor:
            return None
        return {**previous_params, self._cursor_param: cursor}


class LinkHeaderPagination(PaginationStrategy):
    _LINK_RE = re.compile(r'<([^>]+)>\s*;\s*rel="next"')

    def __init__(self, header_name: str = "Link") -> None:
        self._header_name = header_name

    def initial_params(self) -> dict[str, Any]:
        return {}

    def next_params(self, page: ParsedPage, previous_params: dict[str, Any]) -> dict[str, Any] | None:
        link_header = page.headers.get(self._header_name) or page.headers.get(self._header_name.lower())
        if not link_header:
            return None
        match = self._LINK_RE.search(link_header)
        return {NEXT_URL_KEY: match.group(1)} if match else None


class NextUrlPagination(PaginationStrategy):
    def __init__(self, next_url_field: str = "next") -> None:
        self._next_url_field = next_url_field

    def initial_params(self) -> dict[str, Any]:
        return {}

    def next_params(self, page: ParsedPage, previous_params: dict[str, Any]) -> dict[str, Any] | None:
        next_url = _get_path(page.raw, self._next_url_field)
        return {NEXT_URL_KEY: next_url} if next_url else None


class AutoPagination(PaginationStrategy):
    """Conservatively resolve next-link/cursor pagination from the first page."""

    _NEXT_URL_PATHS = ("next", "next_url", "links.next", "pagination.next", "paging.next")
    _CURSOR_PATHS = (
        ("next_cursor", "cursor"),
        ("nextCursor", "cursor"),
        ("meta.next_cursor", "cursor"),
        ("pagination.next_cursor", "cursor"),
        ("paging.next_cursor", "cursor"),
        ("continuation_token", "continuation_token"),
        ("next_token", "token"),
        ("nextPageToken", "pageToken"),
    )

    def __init__(self) -> None:
        self._resolved: PaginationStrategy | None = None
        self.resolved_type: str | None = None

    def initial_params(self) -> dict[str, Any]:
        return {}

    def next_params(self, page: ParsedPage, previous_params: dict[str, Any]) -> dict[str, Any] | None:
        if self._resolved is None:
            self._resolved = self._detect(page)
        return self._resolved.next_params(page, previous_params)

    def _detect(self, page: ParsedPage) -> PaginationStrategy:
        link = LinkHeaderPagination()
        if link.next_params(page, {}):
            self.resolved_type = "link_header"
            return link
        for path in self._NEXT_URL_PATHS:
            candidate = _get_path(page.raw, path)
            if isinstance(candidate, str) and candidate:
                self.resolved_type = "next_url"
                return NextUrlPagination(path)
        for path, param in self._CURSOR_PATHS:
            candidate = _get_path(page.raw, path)
            if candidate not in (None, "", False):
                self.resolved_type = "cursor"
                return CursorPagination(cursor_param=param, cursor_field=path)
        self.resolved_type = "none"
        return NoPagination()


STANDARD_PAGINATION_TYPES: dict[str, type[PaginationStrategy]] = {
    "none": NoPagination,
    "auto": AutoPagination,
    "page": PageNumberPagination,
    "offset": OffsetPagination,
    "cursor": CursorPagination,
    "link_header": LinkHeaderPagination,
    "next_url": NextUrlPagination,
}


def _normalize_type(value: str) -> str:
    key = re.sub(r"[^a-z0-9]+", "_", value.strip().lower()).strip("_")
    if key.endswith("_pagination"):
        key = key[: -len("_pagination")]
    elif key.endswith("pagination"):
        key = key[: -len("pagination")].rstrip("_")
    aliases = {
        "": "auto",
        "automatic": "auto",
        "off": "none",
        "false": "none",
        "no": "none",
        "page_number": "page",
        "pagenumber": "page",
        "link": "link_header",
        "linkheader": "link_header",
        "next": "next_url",
        "nexturl": "next_url",
    }
    return aliases.get(key, key)


def _translate_options(kind: str, options: dict[str, Any]) -> dict[str, Any]:
    values = dict(options)
    values.pop("type", None)
    aliases: dict[str, str]
    if kind == "cursor":
        aliases = {"cursor": "cursor_field", "cursor_path": "cursor_field", "param": "cursor_param"}
    elif kind == "page":
        aliases = {"size": "page_size", "param": "page_param", "size_param": "page_size_param"}
    elif kind == "offset":
        aliases = {"size": "limit", "param": "offset_param", "size_param": "limit_param"}
    elif kind == "next_url":
        aliases = {"field": "next_url_field", "path": "next_url_field"}
    elif kind == "link_header":
        aliases = {"header": "header_name"}
    else:
        aliases = {}
    for source, target in aliases.items():
        if source in values and target not in values:
            values[target] = values.pop(source)
    return values


def resolve_pagination(value: PaginationStrategy | str | dict[str, Any] | bool | None) -> PaginationStrategy:
    """Resolve friendly pagination input once, keeping the runtime strongly typed."""
    if isinstance(value, PaginationStrategy):
        return value
    if value is None or value is True:
        return AutoPagination()
    if value is False:
        return NoPagination()
    if isinstance(value, str):
        kind = _normalize_type(value)
        strategy_cls = STANDARD_PAGINATION_TYPES.get(kind)
        if strategy_cls is None:
            valid = ", ".join(sorted(STANDARD_PAGINATION_TYPES))
            raise ValueError(f"pagination '{value}' desconhecida. Use: {valid}.")
        return strategy_cls()
    if isinstance(value, dict):
        kind = _normalize_type(str(value.get("type", "auto")))
        strategy_cls = STANDARD_PAGINATION_TYPES.get(kind)
        if strategy_cls is None:
            valid = ", ".join(sorted(STANDARD_PAGINATION_TYPES))
            raise ValueError(f"pagination.type '{kind}' desconhecido. Use: {valid}.")
        try:
            return strategy_cls(**_translate_options(kind, value))
        except TypeError as exc:
            raise ValueError(f"Parametros de pagination '{kind}' invalidos: {exc}") from exc
    raise TypeError("pagination deve ser string, dict, bool ou PaginationStrategy.")


__all__ = [
    "NEXT_URL_KEY",
    "ParsedPage",
    "PaginationStrategy",
    "NoPagination",
    "AutoPagination",
    "PageNumberPagination",
    "OffsetPagination",
    "CursorPagination",
    "LinkHeaderPagination",
    "NextUrlPagination",
    "STANDARD_PAGINATION_TYPES",
    "resolve_pagination",
]
