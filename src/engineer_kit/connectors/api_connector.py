"""Base class for API connectors with reusable pagination and incrementality."""

from __future__ import annotations

import json
import warnings
from abc import abstractmethod
from datetime import date
from typing import Any, Callable, Iterator, Optional, Sequence, Union
from urllib.parse import urljoin, urlsplit

import requests

from engineer_kit.connectors.base import Connector
from engineer_kit.connectors.date_field import DateFieldSpec
from engineer_kit.connectors.dedup import resolve_primary_key
from engineer_kit.connectors.extraction import (
    DEFAULT_EXTRACTION_BATCH_SIZE,
    ExtractionSession,
    validate_extraction_batch_size,
)
from engineer_kit.connectors.incremental import (
    IncrementalMode,
    IncrementalStrategy,
    IncrementalWindow,
    NoIncrementalStrategy,
)
from engineer_kit.connectors.pagination import NEXT_URL_KEY, PaginationStrategy, ParsedPage
from engineer_kit.http.client import HttpClient
from engineer_kit.profiling.engine import profile_records, resolve_profile_metrics
from engineer_kit.profiling.model import ProfileReport
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


class CrossOriginPaginationError(RuntimeError):
    """Raised before pagination can forward connector auth to another origin."""


def _url_origin(url: str) -> tuple[str, str, int | None]:
    parsed = urlsplit(url)
    scheme = parsed.scheme.lower()
    host = (parsed.hostname or "").lower()
    port = parsed.port
    if port is None:
        port = 443 if scheme == "https" else 80 if scheme == "http" else None
    return scheme, host, port


class APIConnector(Connector):
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
        extraction_batch_size: int = DEFAULT_EXTRACTION_BATCH_SIZE,
        max_pages: int = DEFAULT_MAX_PAGES,
        allow_cross_origin_pagination: bool = False,
        record_transform: Callable[[dict], dict] | None = None,
        primary_key: str | Sequence[str] | None = None,
        dedup: bool | str | Sequence[str] | None = False,
    ) -> None:
        method = method.upper()
        if method not in VALID_HTTP_METHODS:
            raise InvalidHttpMethodError(
                f"method deve ser um de {VALID_HTTP_METHODS}, recebido '{method}'."
            )
        if isinstance(max_pages, bool) or not isinstance(max_pages, int) or max_pages <= 0:
            raise ValueError("max_pages deve ser um inteiro maior que zero.")

        if dedup is None:
            resolved_dedup = False
        elif isinstance(dedup, bool):
            resolved_dedup = dedup
        elif primary_key is None and isinstance(dedup, (str, list, tuple)):
            warnings.warn(
                "dedup=<primary key> esta obsoleto; use primary_key=<...>, dedup=True.",
                DeprecationWarning,
                stacklevel=2,
            )
            primary_key = dedup
            resolved_dedup = True
        else:
            raise TypeError("dedup deve ser booleano; declare a identidade em primary_key.")

        resolved_primary_key = resolve_primary_key(primary_key)
        if resolved_dedup and resolved_primary_key is None:
            raise ValueError(
                "dedup=True exige primary_key. Defina primary_key='id' ou uma chave composta."
            )

        self.name = name
        self._http = http_client
        self._pagination = pagination
        self._method = method
        self._date_field = date_field
        self._record_transform = record_transform
        self._primary_key = resolved_primary_key
        self._dedup_enabled = resolved_dedup
        self._extraction_batch_size = validate_extraction_batch_size(extraction_batch_size)
        self._max_pages = max_pages
        self._allow_cross_origin_pagination = bool(allow_cross_origin_pagination)
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
        return self._extraction_batch_size

    @property
    def max_pages(self) -> int:
        return self._max_pages

    @property
    def primary_key(self) -> tuple[str, ...] | None:
        return self._primary_key

    @property
    def dedup_enabled(self) -> bool:
        return self._dedup_enabled

    @property
    def dedup_keys(self) -> tuple[str, ...] | None:
        return self._primary_key if self._dedup_enabled else None

    @property
    def checkpoint_enabled(self) -> bool:
        return not isinstance(self._incremental, NoIncrementalStrategy)

    @property
    def current_window(self) -> IncrementalWindow | None:
        return self._legacy_session.window if self._legacy_session is not None else None

    @property
    def max_data_date_seen(self) -> date | None:
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
        """Extract normalized records, raw response and headers."""

    def parse_profile_response(self, response: requests.Response) -> ParsedPage:
        return self.parse_response(response)

    def _include_record(self, record: dict[str, Any], window: IncrementalWindow) -> bool:
        return True

    def extract_incremental(
        self,
        end: Union[date, str] = "today",
        *,
        batch_size: int | None = None,
    ) -> ExtractionSession:
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
            record_transform=self._record_transform,
            dedup=self._primary_key if self._dedup_enabled else None,
        )

    def profile(
        self,
        *metrics: str,
        end: Union[date, str] = "today",
        scope: str = "full",
        limit: int | None = None,
        fields: Sequence[str] | None = None,
        key: str | Sequence[str] | None = None,
    ) -> ProfileReport:
        plan = resolve_profile_metrics(metrics)
        window = self._incremental.resolve_window(end)
        records = self._iter_profile_records(window)
        try:
            return profile_records(
                records,
                *plan,
                scope=scope,
                limit=limit,
                fields=fields,
                key=key if key is not None else self._primary_key,
            )
        finally:
            close = getattr(records, "close", None)
            if callable(close):
                close()

    def _iter_profile_records(
        self,
        window: IncrementalWindow,
    ) -> Iterator[dict[str, Any]]:
        pages = self._iter_pages(window, self.parse_profile_response)
        try:
            for page in pages:
                for record in page.records:
                    if not self._include_record(record, window):
                        continue
                    yield self._record_transform(record) if self._record_transform else record
        finally:
            close = getattr(pages, "close", None)
            if callable(close):
                close()

    def extract(self, end: Union[date, str] = "today") -> Iterator[dict[str, Any]]:
        session = self.extract_incremental(end)
        self._legacy_session = session
        return session.iter_records()

    @staticmethod
    def _pagination_fingerprint(next_url: str | None, page_params: dict[str, Any]) -> str:
        if next_url is not None:
            return f"url:{next_url}"
        try:
            serialized = json.dumps(
                page_params,
                sort_keys=True,
                default=str,
                separators=(",", ":"),
            )
        except (TypeError, ValueError):
            serialized = repr(
                sorted((str(key), type(value).__name__) for key, value in page_params.items())
            )
        return f"params:{serialized}"

    def _iter_records(self, window: IncrementalWindow) -> Iterator[dict[str, Any]]:
        for page in self._iter_pages(window, self.parse_response):
            for record in page.records:
                if self._include_record(record, window):
                    yield record

    def _iter_pages(
        self,
        window: IncrementalWindow,
        parser: Callable[[requests.Response], ParsedPage],
    ) -> Iterator[ParsedPage]:
        self._pagination.reset()
        page_params = self._pagination.initial_params()
        next_url: Optional[str] = None
        seen_requests: set[str] = set()
        pages_requested = 0
        initial_origin: tuple[str, str, int | None] | None = None
        current_request_url: str | None = None
        follow_request_extras: dict[str, Any] = {}

        while True:
            if pages_requested >= self._max_pages:
                raise PaginationLimitError(
                    f"'{self.name}' excedeu max_pages={self._max_pages}; "
                    "interrompendo para evitar loop/custo de requisicoes sem limite."
                )

            resolved_next_url: str | None = None
            if next_url is not None:
                if current_request_url is None:
                    raise PaginationLoopError(
                        f"'{self.name}' recebeu next URL antes da primeira requisicao."
                    )
                resolved_next_url = urljoin(current_request_url, str(next_url))

            fingerprint = self._pagination_fingerprint(resolved_next_url, page_params)
            if fingerprint in seen_requests:
                raise PaginationLoopError(
                    f"'{self.name}' repetiu o mesmo estado de paginacao; "
                    "a extracao foi interrompida para evitar loop infinito."
                )
            seen_requests.add(fingerprint)

            if resolved_next_url is not None:
                if (
                    initial_origin is not None
                    and not self._allow_cross_origin_pagination
                    and _url_origin(resolved_next_url) != initial_origin
                ):
                    raise CrossOriginPaginationError(
                        f"'{self.name}' recebeu URL de proxima pagina em outra origem. "
                        "A requisicao foi bloqueada para evitar vazamento de credenciais. "
                        "Use allow_cross_origin_pagination=True somente se a API documentar "
                        "explicitamente esse comportamento."
                    )
                request_kwargs = {**follow_request_extras, "url": resolved_next_url}
            else:
                request_kwargs = self.build_request(window, page_params)

            request_url = str(request_kwargs.get("url") or "")
            if initial_origin is None:
                initial_origin = _url_origin(request_url)
                follow_request_extras = {
                    key: value
                    for key, value in request_kwargs.items()
                    if key not in {"url", "params"}
                }

            response = self._http.request(self._method, **request_kwargs)
            current_request_url = request_url
            pages_requested += 1
            page = parser(response)
            yield page

            next_params = self._pagination.next_params(page, page_params)
            if next_params is None:
                break
            if NEXT_URL_KEY in next_params:
                raw_next = next_params[NEXT_URL_KEY]
                if not isinstance(raw_next, str) or not raw_next.strip():
                    break
                next_url = raw_next
            else:
                next_url = None
                page_params = next_params

    def commit_watermark(self, max_data_date: Optional[date] = None) -> Watermark:
        if self._legacy_session is None:
            raise RuntimeError("commit_watermark() chamado antes de extract() rodar.")
        return self._legacy_session.commit(max_data_date=max_data_date)
