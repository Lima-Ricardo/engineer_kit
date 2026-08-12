"""Base class for API connectors with reusable pagination and incrementality."""

from __future__ import annotations

import json
from abc import abstractmethod
from datetime import date
from typing import Any, Iterator, Optional, Union

import requests

from engineer_kit.connectors.base import Connector
from engineer_kit.connectors.date_field import DateFieldSpec
from engineer_kit.connectors.extraction import (
    DEFAULT_EXTRACTION_BATCH_SIZE,
    ExtractionSession,
    validate_extraction_batch_size,
)
from engineer_kit.connectors.incremental import IncrementalMode, IncrementalStrategy, IncrementalWindow
from engineer_kit.connectors.pagination import NEXT_URL_KEY, PaginationStrategy, ParsedPage
from engineer_kit.http.client import HttpClient
from engineer_kit.storage.state_store import StateStore, Watermark

VALID_HTTP_METHODS = ("GET", "POST")
DEFAULT_MAX_PAGES = 10_000


class InvalidHttpMethodError(ValueError):
    """Raised when ``method`` is not a supported HTTP verb."""


class MissingDateFieldError(ValueError):
    """Raised when DATA_DATE mode has no record date field."""


class PaginationLimitError(RuntimeError):
    """Raised before a broken/malicious API can paginate forever."""


class PaginationLoopError(RuntimeError):
    """Raised when the same pagination request state repeats."""


class APIConnector(Connector):
    """Base for API connectors with reusable pagination and incremental state.

    The preferred public API is :meth:`extract_incremental`, which returns a
    single-pass :class:`ExtractionSession`. Iterating that session yields bounded
    batches (25,000 records by default). The legacy :meth:`extract` record stream
    remains available for compatibility and for low-level consumers.
    """

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
        extraction_batch_size: int = DEFAULT_EXTRACTION_BATCH_SIZE,
        max_pages: int = DEFAULT_MAX_PAGES,
    ) -> None:
        method = method.upper()
        if method not in VALID_HTTP_METHODS:
            raise InvalidHttpMethodError(
                f"method deve ser um de {VALID_HTTP_METHODS}, recebido '{method}'."
            )
        if max_pages <= 0:
            raise ValueError("max_pages deve ser maior que zero.")

        self.name = name
        self._http = http_client
        self._pagination = pagination
        self._method = method
        self._date_field = date_field
        self._extraction_batch_size = validate_extraction_batch_size(extraction_batch_size)
        self._max_pages = max_pages
        self._legacy_session: ExtractionSession | None = None

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
    def extraction_batch_size(self) -> int:
        """Default batch size used by new ExtractionSession objects."""
        return self._extraction_batch_size

    @property
    def max_pages(self) -> int:
        """Maximum pages one extraction attempt may request."""
        return self._max_pages

    @property
    def current_window(self) -> IncrementalWindow | None:
        """Incremental window prepared by the latest legacy extraction attempt."""
        return self._legacy_session.window if self._legacy_session is not None else None

    @property
    def max_data_date_seen(self) -> date | None:
        """Largest record date observed by the latest legacy extraction stream."""
        if self._legacy_session is None:
            return None
        return self._legacy_session.max_data_date_seen

    @abstractmethod
    def build_request(
        self, window: IncrementalWindow, page_params: dict[str, Any]
    ) -> dict[str, Any]:
        """Build ``HttpClient.request`` kwargs for one page."""

    @abstractmethod
    def parse_response(self, response: requests.Response) -> ParsedPage:
        """Extract records, raw response and headers."""

    def extract_incremental(
        self,
        end: Union[date, str] = "today",
        *,
        batch_size: int | None = None,
    ) -> ExtractionSession:
        """Create one streaming-first incremental extraction session.

        Normal iteration yields batches. The checkpoint is not persisted until
        ``session.commit()`` is called after complete consumption and successful
        downstream processing.
        """
        window = self._incremental.resolve_window(end)
        resolved_batch_size = (
            self._extraction_batch_size
            if batch_size is None
            else validate_extraction_batch_size(batch_size)
        )
        return ExtractionSession(
            window=window,
            records=self._iter_records(window),
            incremental=self._incremental,
            date_field=self._date_field,
            batch_size=resolved_batch_size,
        )

    def extract(self, end: Union[date, str] = "today") -> Iterator[dict[str, Any]]:
        """Return the legacy lazy record stream.

        New code should prefer ``extract_incremental()`` and normal session
        iteration so bounded batches are the default user experience.
        """
        session = self.extract_incremental(end)
        self._legacy_session = session
        return session.iter_records()

    @staticmethod
    def _pagination_fingerprint(next_url: str | None, page_params: dict[str, Any]) -> str:
        if next_url is not None:
            # Never put this fingerprint in error messages/logs: next URLs may
            # contain cursor/token values.
            return f"url:{next_url}"
        try:
            serialized = json.dumps(page_params, sort_keys=True, default=str, separators=(",", ":"))
        except (TypeError, ValueError):
            serialized = repr(sorted((str(k), type(v).__name__) for k, v in page_params.items()))
        return f"params:{serialized}"

    def _iter_records(self, window: IncrementalWindow) -> Iterator[dict[str, Any]]:
        page_params = self._pagination.initial_params()
        next_url: Optional[str] = None
        seen_requests: set[str] = set()
        pages_requested = 0

        while True:
            if pages_requested >= self._max_pages:
                raise PaginationLimitError(
                    f"'{self.name}' excedeu max_pages={self._max_pages}; "
                    "interrompendo para evitar loop/custo de requisicoes sem limite."
                )

            fingerprint = self._pagination_fingerprint(next_url, page_params)
            if fingerprint in seen_requests:
                raise PaginationLoopError(
                    f"'{self.name}' repetiu o mesmo estado de paginacao; "
                    "a extracao foi interrompida para evitar loop infinito."
                )
            seen_requests.add(fingerprint)

            if next_url is not None:
                request_kwargs: dict[str, Any] = {"url": next_url}
            else:
                request_kwargs = self.build_request(window, page_params)

            response = self._http.request(self._method, **request_kwargs)
            pages_requested += 1
            page = self.parse_response(response)

            yield from page.records

            next_params = self._pagination.next_params(page, page_params)
            if next_params is None:
                break
            if NEXT_URL_KEY in next_params:
                next_url = next_params[NEXT_URL_KEY]
            else:
                next_url = None
                page_params = next_params

    def commit_watermark(self, max_data_date: Optional[date] = None) -> Watermark:
        """Compatibility wrapper for code that uses ``extract()`` directly."""
        if self._legacy_session is None:
            raise RuntimeError("commit_watermark() chamado antes de extract() rodar.")
        return self._legacy_session.commit(max_data_date=max_data_date)
