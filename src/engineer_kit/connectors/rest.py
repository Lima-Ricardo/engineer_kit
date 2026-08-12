"""Conector generico e configuravel para APIs REST/JSON padrao.

Cobre o caso comum sem precisar herdar de APIConnector: autenticacao
via header/query, paginacao por pagina/offset/cursor/link-header/
next-url, filtro de data via dois query params, GET ou POST, e
incremental montado automaticamente a partir de `name` + `state_store`
(sem precisar construir um IncrementalStrategy a parte e repetir o
nome do conector). Para o caso comum, o usuario so instancia esta
classe com os parametros da API — nao escreve codigo novo.

Para APIs com comportamento fora do padrao (resposta nao-JSON, auth
por assinatura HMAC), estenda APIConnector diretamente em vez de
forcar esses casos aqui dentro.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any, Callable, Optional, Union

import requests

from engineer_kit.connectors.api_connector import APIConnector
from engineer_kit.connectors.date_field import DateFieldSpec
from engineer_kit.connectors.incremental import IncrementalMode, IncrementalStrategy, IncrementalWindow
from engineer_kit.connectors.normalize import stringify
from engineer_kit.connectors.pagination import PaginationStrategy, ParsedPage
from engineer_kit.http.auth import AuthStrategy, NoAuth
from engineer_kit.http.client import HttpClient
from engineer_kit.storage.state_store import IngestionStateStore


@dataclass
class DateParams:
    """Nomes dos query params de filtro de data que a API espera."""

    start: Optional[str] = None
    end: Optional[str] = None
    date_format: str = "%Y-%m-%d"


class RestConnector(APIConnector):
    def __init__(
        self,
        name: str,
        base_url: str,
        pagination: PaginationStrategy,
        method: str,
        state_store: Optional[IngestionStateStore] = None,
        incremental_mode: IncrementalMode = IncrementalMode.DATA_DATE,
        initial_start: Optional[date] = None,
        date_field: Optional[DateFieldSpec] = None,
        incremental: Optional[IncrementalStrategy] = None,
        auth: Optional[AuthStrategy] = None,
        date_params: Optional[DateParams] = None,
        static_params: Optional[dict[str, Any]] = None,
        records_path: Optional[Union[Callable[[Any], list[dict]], str]] = None,
        http_client: Optional[HttpClient] = None,
    ) -> None:
        self._base_url = base_url
        self._date_params = date_params or DateParams()
        self._static_params = static_params or {}
        self._records_path = records_path
        http = http_client or HttpClient(auth=auth or NoAuth())
        super().__init__(
            name=name,
            http_client=http,
            pagination=pagination,
            method=method,
            state_store=state_store,
            incremental_mode=incremental_mode,
            initial_start=initial_start,
            date_field=date_field,
            incremental=incremental,
        )

    def build_request(self, window: IncrementalWindow, page_params: dict[str, Any]) -> dict[str, Any]:
        payload = {**self._static_params, **page_params}
        if self._date_params.start and window.start:
            payload[self._date_params.start] = window.start.strftime(self._date_params.date_format)
        if self._date_params.end and window.end:
            payload[self._date_params.end] = window.end.strftime(self._date_params.date_format)

        # GET manda o payload como query string; POST manda como corpo JSON
        # (padrao comum em APIs de busca/paginacao mais complexas).
        if self._method == "POST":
            return {"url": self._base_url, "json": payload}
        return {"url": self._base_url, "params": payload}

    def parse_response(self, response: requests.Response) -> ParsedPage:
        raw = response.json()
        items = self._extract_items(raw)
        records = [stringify(item) for item in items]
        return ParsedPage(records=records, raw=raw, headers=dict(response.headers))

    def _extract_items(self, raw: Any) -> list[dict]:
        if self._records_path is None:
            if isinstance(raw, list):
                return raw
            raise TypeError(
                f"Resposta de '{self.name}' nao e uma lista JSON e nenhum records_path foi "
                "configurado. Passe records_path='campo' ou uma funcao para extrair a lista."
            )
        if callable(self._records_path):
            return self._records_path(raw)
        return raw[self._records_path]
