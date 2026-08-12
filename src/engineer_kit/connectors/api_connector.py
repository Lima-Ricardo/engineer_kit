"""Base class for API connectors with reusable pagination and incrementality."""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import date
from typing import Any, Iterator, Optional, Union

import requests

from engineer_kit.connectors.date_field import DateFieldSpec, extract_date_value
from engineer_kit.connectors.incremental import IncrementalMode, IncrementalStrategy, IncrementalWindow
from engineer_kit.connectors.pagination import NEXT_URL_KEY, PaginationStrategy, ParsedPage
from engineer_kit.http.client import HttpClient
from engineer_kit.storage.state_store import StateStore, Watermark

VALID_HTTP_METHODS = ("GET", "POST")


class InvalidHttpMethodError(ValueError):
    """Raised when ``method`` is not a supported HTTP verb."""


class MissingDateFieldError(ValueError):
    """Raised when DATA_DATE mode has no record date field."""


class APIConnector(ABC):
    """Base for API connectors with reusable pagination and incremental state."""

    def __init__(
        self,
        name: str,
        http_client: HttpClient,
        pagination: PaginationStrategy,
        method: str,
        state_store: Optional[StateStore] = None,
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
                    "Passe state_store ou incremental=IncrementalStrategy(...) pronto."
                )
            if incremental_mode is IncrementalMode.DATA_DATE and date_field is None:
                raise MissingDateFieldError(
                    "incremental_mode=DATA_DATE precisa de date_field. Use "
                    "IncrementalMode.INGESTION_DATE quando o checkpoint for a data da execucao."
                )
            self._incremental = IncrementalStrategy(
                connector_name=name,
                state_store=state_store,
                mode=incremental_mode,
                initial_start=initial_start,
            )

    @property
    def current_window(self) -> IncrementalWindow | None:
        """Incremental window prepared for the current extraction attempt."""
        return self._current_window

    @property
    def max_data_date_seen(self) -> date | None:
        """Largest record date observed in the current extraction stream."""
        return self._max_data_date_seen

    @abstractmethod
    def build_request(
        self, window: IncrementalWindow, page_params: dict[str, Any]
    ) -> dict[str, Any]:
        """Build ``HttpClient.request`` kwargs for one page."""

    @abstractmethod
    def parse_response(self, response: requests.Response) -> ParsedPage:
        """Extract records, raw response and headers."""

    def extract(self, end: Union[date, str] = "today") -> Iterator[dict[str, Any]]:
        """Prepare the incremental window and return a lazy record stream."""
        self._current_window = self._incremental.resolve_window(end)
        self._max_data_date_seen = None
        return self._iter_records(self._current_window)

    def _iter_records(self, window: IncrementalWindow) -> Iterator[dict[str, Any]]:
        page_params = self._pagination.initial_params()
        next_url: Optional[str] = None

        while True:
            if next_url is not None:
                request_kwargs: dict[str, Any] = {"url": next_url}
            else:
                request_kwargs = self.build_request(window, page_params)

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

    def commit_watermark(self, max_data_date: Optional[date] = None) -> Watermark:
        """Confirm and return the checkpoint after the destination confirms the load."""
        if self._current_window is None:
            raise RuntimeError("commit_watermark() chamado antes de extract() rodar.")
        effective_max_date = (
            max_data_date if max_data_date is not None else self._max_data_date_seen
        )
        return self._incremental.commit(
            self._current_window,
            max_data_date=effective_max_date,
        )
