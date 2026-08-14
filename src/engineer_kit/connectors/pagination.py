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


def _positive_int(value: Any, *, name: str, allow_zero: bool = False) -> int:
    minimum = 0 if allow_zero else 1
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        comparator = "maior ou igual a zero" if allow_zero else "maior que zero"
        raise ValueError(f"{name} deve ser inteiro {comparator}.")
    return value


class PaginationStrategy(ABC):
    def reset(self) -> None:
        """Reset per-extraction state, if any."""

    @abstractmethod
    def initial_params(self) -> dict[str, Any]: ...

    @abstractmethod
    def next_params(
        self, page: ParsedPage, previous_params: dict[str, Any]
    ) -> dict[str, Any] | None: ...


class NoPagination(PaginationStrategy):
    def initial_params(self) -> dict[str, Any]:
        return {}

    def next_params(
        self, page: ParsedPage, previous_params: dict[str, Any]
    ) -> dict[str, Any] | None:
        return None


class PageNumberPagination(PaginationStrategy):
    """Increment a page number until pagination is exhausted.

    The direct strategy keeps the historical short-page termination behavior for
    backwards compatibility. Intent/declarative resolution opts out by default,
    because some APIs cap the returned page size below the requested value and a
    short-page stop would silently truncate those sources.
    """

    def __init__(
        self,
        page_param: str = "page",
        page_size_param: str = "per_page",
        page_size: int = 100,
        start_page: int = 1,
        stop_on_short_page: bool = True,
    ) -> None:
        self._page_param = str(page_param)
        self._page_size_param = str(page_size_param)
        self._page_size = _positive_int(page_size, name="page_size")
        self._start_page = _positive_int(start_page, name="start_page", allow_zero=True)
        if not isinstance(stop_on_short_page, bool):
            raise TypeError("stop_on_short_page deve ser booleano.")
        self._stop_on_short_page = stop_on_short_page

    def initial_params(self) -> dict[str, Any]:
        return {self._page_param: self._start_page, self._page_size_param: self._page_size}

    def next_params(
        self, page: ParsedPage, previous_params: dict[str, Any]
    ) -> dict[str, Any] | None:
        if not page.records:
            return None
        if self._stop_on_short_page and len(page.records) < self._page_size:
            return None
        return {
            **previous_params,
            self._page_param: previous_params[self._page_param] + 1,
        }


class OffsetPagination(PaginationStrategy):
    """Advance an offset until pagination is exhausted.

    Direct construction preserves the historical short-page stop; intent mode
    defaults to exhaustive pagination for the same reason as page-number mode.
    """

    def __init__(
        self,
        offset_param: str = "offset",
        limit_param: str = "limit",
        limit: int = 100,
        start_offset: int = 0,
        stop_on_short_page: bool = True,
    ) -> None:
        self._offset_param = str(offset_param)
        self._limit_param = str(limit_param)
        self._limit = _positive_int(limit, name="limit")
        self._start_offset = _positive_int(
            start_offset, name="start_offset", allow_zero=True
        )
        if not isinstance(stop_on_short_page, bool):
            raise TypeError("stop_on_short_page deve ser booleano.")
        self._stop_on_short_page = stop_on_short_page

    def initial_params(self) -> dict[str, Any]:
        return {self._offset_param: self._start_offset, self._limit_param: self._limit}

    def next_params(
        self, page: ParsedPage, previous_params: dict[str, Any]
    ) -> dict[str, Any] | None:
        if not page.records:
            return None
        if self._stop_on_short_page and len(page.records) < self._limit:
            return None
        return {
            **previous_params,
            self._offset_param: previous_params[self._offset_param] + self._limit,
        }


class CursorPagination(PaginationStrategy):
    def __init__(
        self, cursor_param: str = "cursor", cursor_field: str = "next_cursor"
    ) -> None:
        self._cursor_param = cursor_param
        self._cursor_field = cursor_field

    def initial_params(self) -> dict[str, Any]:
        return {}

    def next_params(
        self, page: ParsedPage, previous_params: dict[str, Any]
    ) -> dict[str, Any] | None:
        cursor = _get_path(page.raw, self._cursor_field)
        if cursor is None or cursor == "":
            return None
        return {**previous_params, self._cursor_param: cursor}


class LinkHeaderPagination(PaginationStrategy):
    """Follow RFC-style Link headers conservatively."""

    _LINK_PART_RE = re.compile(r"<([^>]+)>\s*(?:;\s*[^,]+)*", re.IGNORECASE)
    _REL_NEXT_RE = re.compile(
        r"(?:^|;)\s*rel\s*=\s*(?:\"[^\"]*\bnext\b[^\"]*\"|'[^']*\bnext\b[^']*'|next)(?:\s*;|\s*$)",
        re.IGNORECASE,
    )

    def __init__(self, header_name: str = "Link") -> None:
        self._header_name = header_name

    def initial_params(self) -> dict[str, Any]:
        return {}

    def next_params(
        self, page: ParsedPage, previous_params: dict[str, Any]
    ) -> dict[str, Any] | None:
        link_header = page.headers.get(self._header_name) or page.headers.get(
            self._header_name.lower()
        )
        if not link_header:
            return None
        for raw_part in _split_link_header(link_header):
            match = self._LINK_PART_RE.match(raw_part.strip())
            if match is None:
                continue
            parameters = raw_part[match.end(1) + 1 :]
            if self._REL_NEXT_RE.search(parameters):
                return {NEXT_URL_KEY: match.group(1)}
        return None


class NextUrlPagination(PaginationStrategy):
    def __init__(self, next_url_field: str = "next") -> None:
        self._next_url_field = next_url_field

    def initial_params(self) -> dict[str, Any]:
        return {}

    def next_params(
        self, page: ParsedPage, previous_params: dict[str, Any]
    ) -> dict[str, Any] | None:
        next_url = _get_path(page.raw, self._next_url_field)
        return {NEXT_URL_KEY: next_url} if isinstance(next_url, str) and next_url else None


class AutoPagination(PaginationStrategy):
    """Conservatively resolve next-link/cursor pagination per extraction."""

    _NEXT_URL_PATHS = (
        "next",
        "next_url",
        "links.next",
        "pagination.next",
        "paging.next",
    )
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

    def reset(self) -> None:
        self._resolved = None
        self.resolved_type = None

    def initial_params(self) -> dict[str, Any]:
        return {}

    def next_params(
        self, page: ParsedPage, previous_params: dict[str, Any]
    ) -> dict[str, Any] | None:
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
            if candidate is not None and candidate != "":
                self.resolved_type = "cursor"
                return CursorPagination(cursor_param=param, cursor_field=path)
        self.resolved_type = "none"
        return NoPagination()


def _split_link_header(value: str) -> list[str]:
    """Split a Link header on commas that are outside angle brackets/quotes."""
    parts: list[str] = []
    start = 0
    in_angle = False
    quote: str | None = None
    for index, char in enumerate(value):
        if quote is not None:
            if char == quote:
                quote = None
            continue
        if char in {"'", '"'}:
            quote = char
        elif char == "<":
            in_angle = True
        elif char == ">":
            in_angle = False
        elif char == "," and not in_angle:
            parts.append(value[start:index])
            start = index + 1
    parts.append(value[start:])
    return parts


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
        aliases = {
            "cursor": "cursor_field",
            "cursor_path": "cursor_field",
            "param": "cursor_param",
        }
    elif kind == "page":
        aliases = {
            "size": "page_size",
            "param": "page_param",
            "size_param": "page_size_param",
            "short_page_ends": "stop_on_short_page",
        }
    elif kind == "offset":
        aliases = {
            "size": "limit",
            "param": "offset_param",
            "size_param": "limit_param",
            "short_page_ends": "stop_on_short_page",
        }
    elif kind == "next_url":
        aliases = {"field": "next_url_field", "path": "next_url_field"}
    elif kind == "link_header":
        aliases = {"header": "header_name"}
    else:
        aliases = {}
    for source, target in aliases.items():
        if source in values and target not in values:
            values[target] = values.pop(source)
    if kind in {"page", "offset"} and "stop_on_short_page" not in values:
        values["stop_on_short_page"] = False
    return values


def resolve_pagination(
    value: PaginationStrategy | str | dict[str, Any] | bool | None,
) -> PaginationStrategy:
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
        if kind in {"page", "offset"}:
            return strategy_cls(stop_on_short_page=False)
        return strategy_cls()
    if isinstance(value, dict):
        kind = _normalize_type(str(value.get("type", "auto")))
        strategy_cls = STANDARD_PAGINATION_TYPES.get(kind)
        if strategy_cls is None:
            valid = ", ".join(sorted(STANDARD_PAGINATION_TYPES))
            raise ValueError(f"pagination.type '{kind}' desconhecido. Use: {valid}.")
        try:
            return strategy_cls(**_translate_options(kind, value))
        except (TypeError, ValueError) as exc:
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
