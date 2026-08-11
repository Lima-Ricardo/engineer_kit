"""Classe base para conectores.

Toda API nova so precisa implementar `build_request` e `parse_response`
— o loop de paginacao e a leitura da janela incremental sao resolvidos
aqui, sempre do mesmo jeito (Template Method).

Casos comuns (REST/JSON padrao) nao devem herdar disso: usar
`engineer_kit.connectors.rest.RestConnector`. Reserve o subclassing direto
para APIs com comportamento fora do padrao (resposta nao-JSON,
paginacao via header, auth por assinatura etc.).

Importante: `extract()` so itera os registros, nao commita o watermark
sozinho. Quem chama (normalmente o Pipeline) decide o momento certo de
chamar `commit_watermark()` — depois que os registros foram gravados
com sucesso no destino. Se o commit acontecesse automaticamente ao fim
do generator, um loader que falha no meio da escrita ainda correria o
risco de o generator ja ter sido drenado antes do erro aparecer.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import date
from typing import Any, Iterator, Optional, Union

import requests

from engineer_kit.connectors.incremental import IncrementalStrategy, IncrementalWindow
from engineer_kit.connectors.pagination import PaginationStrategy, ParsedPage
from engineer_kit.http.client import HttpClient


class BaseConnector(ABC):
    name: str

    def __init__(
        self,
        http_client: HttpClient,
        pagination: PaginationStrategy,
        incremental: IncrementalStrategy,
    ) -> None:
        self._http = http_client
        self._pagination = pagination
        self._incremental = incremental
        self._current_window: Optional[IncrementalWindow] = None

    @abstractmethod
    def build_request(self, window: IncrementalWindow, page_params: dict[str, Any]) -> dict[str, Any]:
        """Monta os kwargs de HttpClient.get() (url, params, headers) para uma pagina."""

    @abstractmethod
    def parse_response(self, response: requests.Response) -> ParsedPage:
        """Extrai os registros (list[dict], valores como str) e o JSON bruto da resposta."""

    def extract(self, end: Union[date, str] = "today") -> Iterator[dict[str, Any]]:
        self._current_window = self._incremental.resolve_window(end)
        page_params = self._pagination.initial_params()

        while True:
            request_kwargs = self.build_request(self._current_window, page_params)
            response = self._http.get(**request_kwargs)
            page = self.parse_response(response)

            yield from page.records

            page_params = self._pagination.next_params(page, page_params)
            if page_params is None:
                break

    def commit_watermark(self, max_data_date: Optional[date] = None) -> None:
        """Chamar so depois que os registros de extract() foram gravados com sucesso."""
        if self._current_window is None:
            raise RuntimeError("commit_watermark() chamado antes de extract() rodar.")
        self._incremental.commit(self._current_window, max_data_date=max_data_date)
