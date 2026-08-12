"""Conector generico e configuravel para APIs REST/JSON padrao.

Cobre autenticacao, paginacao, filtros de data e incremental sem exigir
que o usuario implemente o loop de extracao. O estado incremental e
recebido pelo contrato `StateStore`, portanto pode viver no backend que
melhor se encaixa no ambiente de execucao.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any, Callable, Optional, Union

import requests

from engineer_kit.connectors.api_connector import APIConnector
from engineer_kit.connectors.date_field import DateFieldSpec
from engineer_kit.connectors.extraction import DEFAULT_EXTRACTION_BATCH_SIZE
from engineer_kit.connectors.incremental import IncrementalMode, IncrementalStrategy, IncrementalWindow
from engineer_kit.connectors.normalize import stringify
from engineer_kit.connectors.pagination import PaginationStrategy, ParsedPage
from engineer_kit.http.auth import AuthStrategy, NoAuth
from engineer_kit.http.client import HttpClient
from engineer_kit.storage.state_store import StateStore


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
        state_store: Optional[StateStore] = None,
        incremental_mode: IncrementalMode = IncrementalMode.DATA_DATE,
        initial_start: Optional[date] = None,
        date_field: Optional[DateFieldSpec] = None,
        incremental: Optional[IncrementalStrategy] = None,
        auth: Optional[AuthStrategy] = None,
        date_params: Optional[DateParams] = None,
        static_params: Optional[dict[str, Any]] = None,
        records_path: Optional[Union[Callable[[Any], list[dict]], str]] = None,
        http_client: Optional[HttpClient] = None,
        extraction_batch_size: int = DEFAULT_EXTRACTION_BATCH_SIZE,
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
            extraction_batch_size=extraction_batch_size,
        )

    def build_request(self, window: IncrementalWindow, page_params: dict[str, Any]) -> dict[str, Any]:
        payload = {**self._static_params, **page_params}
        if self._date_params.start and window.start:
            payload[self._date_params.start] = window.start.strftime(self._date_params.date_format)
        if self._date_params.end and window.end:
            payload[self._date_params.end] = window.end.strftime(self._date_params.date_format)

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
