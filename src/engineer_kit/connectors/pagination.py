"""Estrategias de paginacao.

Cada estrategia so sabe duas coisas: quais params vao na primeira
requisicao, e — dado o resultado da pagina atual — quais params vao na
proxima (ou None se acabou). Isso e testavel sem rede: basta montar um
ParsedPage na mao.

`pagination` e sempre um parametro obrigatorio de um conector — nao
existe um tipo padrao "por baixo dos panos", porque o jeito de paginar
muda de API para API e forcar essa escolha explicita evita surpresa.
`STANDARD_PAGINATION_TYPES` lista todo tipo padrao que a biblioteca ja
sabe lidar; para algo fora dessa lista, implemente PaginationStrategy.
"""

from __future__ import annotations

import re
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any

# chave sentinela: quando next_params() devolve um dict contendo essa
# chave, extract() para de montar a proxima requisicao a partir da
# base_url do conector e chama essa URL absoluta diretamente -- e o
# caso de paginacao por header Link ou por um campo "next" na resposta,
# onde a API ja devolve a URL completa da proxima pagina.
NEXT_URL_KEY = "__next_url__"


@dataclass
class ParsedPage:
    """Resultado de parsear uma resposta de API: registros extraidos, o
    JSON bruto (para ler cursores) e os headers da resposta (para
    paginacao por header, ex.: Link)."""

    records: list[dict[str, Any]]
    raw: Any
    headers: dict[str, str] = field(default_factory=dict)


class PaginationStrategy(ABC):
    @abstractmethod
    def initial_params(self) -> dict[str, Any]:
        """Parametros de paginacao da primeira requisicao."""

    @abstractmethod
    def next_params(self, page: ParsedPage, previous_params: dict[str, Any]) -> dict[str, Any] | None:
        """Parametros da proxima pagina, ou None se a paginacao terminou.
        Pode devolver {NEXT_URL_KEY: "https://..."} para pedir que a
        proxima requisicao use essa URL absoluta diretamente."""


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


class LinkHeaderPagination(PaginationStrategy):
    """Paginacao via header HTTP `Link` (RFC 5988), ex.: GitHub, Stripe:
    `Link: <https://api.exemplo.com/items?page=2>; rel="next"`. A API ja
    devolve a URL completa da proxima pagina, entao nada e montado a
    partir da base_url do conector."""

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
        if not match:
            return None
        return {NEXT_URL_KEY: match.group(1)}


class NextUrlPagination(PaginationStrategy):
    """Paginacao via campo de URL completa no corpo da resposta, ex.:
    {"next": "https://api.exemplo.com/items?page=2", "results": [...]}."""

    def __init__(self, next_url_field: str = "next") -> None:
        self._next_url_field = next_url_field

    def initial_params(self) -> dict[str, Any]:
        return {}

    def next_params(self, page: ParsedPage, previous_params: dict[str, Any]) -> dict[str, Any] | None:
        if not isinstance(page.raw, dict):
            return None
        next_url = page.raw.get(self._next_url_field)
        if not next_url:
            return None
        return {NEXT_URL_KEY: next_url}


# Catalogo dos tipos de paginacao padrao que a biblioteca ja resolve.
# O jeito de paginar muda de API para API -- use isto para descobrir
# qual estrategia bate com a documentacao da API que voce esta integrando.
STANDARD_PAGINATION_TYPES: dict[str, type[PaginationStrategy]] = {
    "none": NoPagination,
    "page": PageNumberPagination,
    "offset": OffsetPagination,
    "cursor": CursorPagination,
    "link_header": LinkHeaderPagination,
    "next_url": NextUrlPagination,
}
