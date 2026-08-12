"""Classe base para conectores de API.

Toda API nova so precisa implementar `build_request` e `parse_response`
— o loop de paginacao e a leitura da janela incremental sao resolvidos
aqui, sempre do mesmo jeito (Template Method).

Casos comuns (REST/JSON padrao) nao devem herdar disso: usar
`engineer_kit.RestConnector`. Reserve o subclassing direto para APIs
com comportamento fora do padrao (resposta nao-JSON, auth por
assinatura etc.).

O IncrementalStrategy e montado aqui dentro a partir de `name` +
`state_store` -- nao existe mais um objeto separado pra construir e
manter sincronizado com o nome do conector (isso ja causou duplicacao
de configuracao: o mesmo identificador tinha que ser passado duas
vezes). Quem precisa de um IncrementalStrategy customizado ainda pode
passar um pronto via `incremental=`.

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

from engineer_kit.connectors.date_field import DateFieldSpec, extract_date_value
from engineer_kit.connectors.incremental import IncrementalMode, IncrementalStrategy, IncrementalWindow
from engineer_kit.connectors.pagination import NEXT_URL_KEY, PaginationStrategy, ParsedPage
from engineer_kit.http.client import HttpClient
from engineer_kit.storage.state_store import IngestionStateStore

VALID_HTTP_METHODS = ("GET", "POST")


class InvalidHttpMethodError(ValueError):
    """Levantado quando `method` nao e um verbo HTTP suportado."""


class MissingDateFieldError(ValueError):
    """Levantado quando incremental_mode=DATA_DATE e nenhum date_field foi informado."""


class APIConnector(ABC):
    """Base para qualquer conector de API. `method` e `pagination` sao
    obrigatorios de proposito: nao existe um padrao "por baixo dos
    panos" para nenhum dos dois, porque os dois mudam de API para API e
    uma escolha implicita e uma fonte facil de bug silencioso."""

    def __init__(
        self,
        name: str,
        http_client: HttpClient,
        pagination: PaginationStrategy,
        method: str,
        state_store: Optional[IngestionStateStore] = None,
        incremental_mode: IncrementalMode = IncrementalMode.DATA_DATE,
        initial_start: Optional[date] = None,
        date_field: Optional[DateFieldSpec] = None,
        incremental: Optional[IncrementalStrategy] = None,
    ) -> None:
        method = method.upper()
        if method not in VALID_HTTP_METHODS:
            raise InvalidHttpMethodError(
                f"method deve ser um de {VALID_HTTP_METHODS}, recebido '{method}'."
            )

        self.name = name
        self._http = http_client
        self._pagination = pagination
        self._method = method
        self._date_field = date_field
        self._current_window: Optional[IncrementalWindow] = None
        self._max_data_date_seen: Optional[date] = None

        if incremental is not None:
            self._incremental = incremental
        else:
            if state_store is None:
                raise ValueError(
                    "Passe state_store (caso comum, o incremental e montado automaticamente) "
                    "ou incremental=IncrementalStrategy(...) pronto (caso avancado)."
                )
            if incremental_mode is IncrementalMode.DATA_DATE and date_field is None:
                raise MissingDateFieldError(
                    "incremental_mode=DATA_DATE precisa de date_field (o campo de data do "
                    "proprio registro, ex: 'commit.author.date') -- sem isso, o incremental "
                    "nao tem como saber ate onde os dados da API foram atualizados. Use "
                    "IncrementalMode.INGESTION_DATE se so quiser a data da propria execucao."
                )
            self._incremental = IncrementalStrategy(
                connector_name=name,
                state_store=state_store,
                mode=incremental_mode,
                initial_start=initial_start,
            )

    @abstractmethod
    def build_request(self, window: IncrementalWindow, page_params: dict[str, Any]) -> dict[str, Any]:
        """Monta os kwargs de HttpClient.request() (url, params/json,
        headers) para uma pagina."""

    @abstractmethod
    def parse_response(self, response: requests.Response) -> ParsedPage:
        """Extrai os registros (list[dict], valores como str), o JSON
        bruto e os headers da resposta."""

    def extract(self, end: Union[date, str] = "today") -> Iterator[dict[str, Any]]:
        self._current_window = self._incremental.resolve_window(end)
        self._max_data_date_seen = None
        page_params = self._pagination.initial_params()
        next_url: Optional[str] = None

        while True:
            if next_url is not None:
                request_kwargs: dict[str, Any] = {"url": next_url}
            else:
                request_kwargs = self.build_request(self._current_window, page_params)

            response = self._http.request(self._method, **request_kwargs)
            page = self.parse_response(response)

            for record in page.records:
                self._track_max_data_date(record)
                yield record

            next_params = self._pagination.next_params(page, page_params)
            if next_params is None:
                break
            if NEXT_URL_KEY in next_params:
                next_url = next_params[NEXT_URL_KEY]
            else:
                next_url = None
                page_params = next_params

    def _track_max_data_date(self, record: dict[str, Any]) -> None:
        if self._date_field is None:
            return
        seen = extract_date_value(record, self._date_field)
        if seen is None:
            return
        if self._max_data_date_seen is None or seen > self._max_data_date_seen:
            self._max_data_date_seen = seen

    def commit_watermark(self, max_data_date: Optional[date] = None) -> None:
        """Chamar so depois que os registros de extract() foram gravados
        com sucesso. Se `date_field` estiver configurado e nenhum
        `max_data_date` for passado explicitamente, usa automaticamente
        a maior data vista nos registros durante o extract()."""
        if self._current_window is None:
            raise RuntimeError("commit_watermark() chamado antes de extract() rodar.")
        effective_max_date = max_data_date if max_data_date is not None else self._max_data_date_seen
        self._incremental.commit(self._current_window, max_data_date=effective_max_date)
