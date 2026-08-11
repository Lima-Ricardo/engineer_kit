"""Estrategias de paginacao.

Cada estrategia só sabe duas coisas: quais params vao na primeira
requisicao, e — dado o resultado da pagina atual — quais params vao na
proxima (ou None se acabou). Isso e testavel sem rede: basta montar um
ParsedPage na mao.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any


@dataclass
class ParsedPage:
    """Resultado de parsear uma resposta de API: os registros extraidos e o JSON bruto (para ler cursores)."""

    records: list[dict[str, Any]]
    raw: Any


class PaginationStrategy(ABC):
    @abstractmethod
    def initial_params(self) -> dict[str, Any]:
        """Parametros de paginacao da primeira requisicao."""

    @abstractmethod
    def next_params(self, page: ParsedPage, previous_params: dict[str, Any]) -> dict[str, Any] | None:
        """Parametros da proxima pagina, ou None se a paginacao terminou."""


class NoPagination(PaginationStrategy):
    """API devolve tudo numa unica resposta."""

    def initial_params(self) -> dict[str, Any]:
        return {}

    def next_params(self, page: ParsedPage, previous_params: dict[str, Any]) -> dict[str, Any] | None:
        return None


class PageNumberPagination(PaginationStrategy):
    """?page=1&per_page=100 — para quando a pagina vem incompleta (fim dos dados)."""

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
        next_page = previous_params[self._page_param] + 1
        return {**previous_params, self._page_param: next_page}


class OffsetPagination(PaginationStrategy):
    """?offset=0&limit=100 — mesma logica de parada que PageNumberPagination."""

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
        next_offset = previous_params[self._offset_param] + self._limit
        return {**previous_params, self._offset_param: next_offset}


class CursorPagination(PaginationStrategy):
    """Cursor devolvido dentro do corpo da resposta, ex: {"next_cursor": "abc123", "results": [...]}."""

    def __init__(self, cursor_param: str = "cursor", cursor_field: str = "next_cursor") -> None:
        self._cursor_param = cursor_param
        self._cursor_field = cursor_field

    def initial_params(self) -> dict[str, Any]:
        return {}

    def next_params(self, page: ParsedPage, previous_params: dict[str, Any]) -> dict[str, Any] | None:
        if not isinstance(page.raw, dict):
            return None
        cursor = page.raw.get(self._cursor_field)
        if not cursor:
            return None
        return {**previous_params, self._cursor_param: cursor}
