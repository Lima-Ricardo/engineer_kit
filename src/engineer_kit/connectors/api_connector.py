"""Base class for API connectors with reusable pagination and incrementality."""

from __future__ import annotations

import json
import warnings
from abc import abstractmethod
from datetime import date
from typing import Any, Callable, Iterator, Optional, Sequence, Union
from urllib.parse import urlsplit

import requests

from engineer_kit.connectors.base import Connector
from engineer_kit.connectors.date_field import DateFieldSpec
from engineer_kit.connectors.dedup import resolve_dedup_keys
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
        if max_pages <= 0:
            raise ValueError("max_pages deve ser maior que zero.")

        # Temporary compatibility for the unreleased profiling branch: callers
        # that used ``dedup='id'`` or ``dedup=['tenant_id', 'id']`` are migrated
        # to the new orthogonal contract. New code must declare identity with
        # ``primary_key=...`` and policy with ``dedup=True/False``.
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

        resolved_primary_key = resolve_dedup_keys(primary_key)
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
        """Default batch size used by new ExtractionSession objects."""
        return self._extraction_batch_size

    @property
    def max_pages(self) -> int:
        """Maximum pages one extraction attempt may request."""
        return self._max_pages

    @property
    def primary_key(self) -> tuple[str, ...] | None:
        """Declared simple/composite identity, independent from dedup policy."""
        return self._primary_key

    @property
    def dedup_enabled(self) -> bool:
        """Whether extraction suppresses repeated primary-key records."""
        return self._dedup_enabled

    @property
    def dedup_keys(self) -> tuple[str, ...] | None:
        """Compatibility alias for the key used by active deduplication."""
        return self._primary_key if self._dedup_enabled else None

    @property
    def checkpoint_enabled(self) -> bool:
        """Whether successful extraction advances persistent incremental state.

        A no-op incremental strategy still gives callers one uniform
        ``ExtractionSession`` API, but managed destinations must treat those
        executions as independent ad-hoc runs because there is no persistent
        checkpoint transition that can identify a retry safely.
        """
        return not isinstance(self._incremental, NoIncrementalStrategy)

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
        """Extract normalized records, raw response and headers."""

    def parse_profile_response(self, response: requests.Response) -> ParsedPage:
        """Parse records for profiling.

        Connectors whose ingestion parser normalizes values may override this
        hook to preserve native source types for profiling. By default profiling
        uses the regular parsed page, which keeps third-party connectors fully
        compatible.
        """
        return self.parse_response(response)

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
        """Return aggregate profiling/data-quality metrics without persistence.

        No metric selector means a complete profile. Explicit selectors such as
        ``profile("duplicates", "nulls", "missing")`` activate only the
        required aggregators. ``key`` evaluates duplicates by a candidate PK;
        when omitted, a configured ``primary_key`` is reused automatically.
        Profiling never writes a destination and never commits a checkpoint.
        """
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
                    yield self._record_transform(record) if self._record_transform else record
        finally:
            close = getattr(pages, "close", None)
            if callable(close):
                close()

    def extract(self, end: Union[date, str] = "today") -> Iterator[dict[str, Any]]:
        """Return the legacy lazy record stream."""
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
            yield from page.records

    def _iter_pages(
        self,
        window: IncrementalWindow,
        parser: Callable[[requests.Response], ParsedPage],
    ) -> Iterator[ParsedPage]:
        """Yield pages through one shared pagination/safety state machine."""
        page_params = self._pagination.initial_params()
        next_url: Optional[str] = None
        seen_requests: set[str] = set()
        pages_requested = 0
        initial_origin: tuple[str, str, int | None] | None = None

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
                if (
                    initial_origin is not None
                    and not self._allow_cross_origin_pagination
                    and _url_origin(next_url) != initial_origin
                ):
                    raise CrossOriginPaginationError(
                        f"'{self.name}' recebeu URL de proxima pagina em outra origem. "
                        "A requisicao foi bloqueada para evitar vazamento de credenciais. "
                        "Use allow_cross_origin_pagination=True somente se a API documentar "
                        "explicitamente esse comportamento."
                    )
                request_kwargs: dict[str, Any] = {"url": next_url}
            else:
                request_kwargs = self.build_request(window, page_params)

            request_url = str(request_kwargs.get("url") or "")
            if initial_origin is None:
                initial_origin = _url_origin(request_url)

            response = self._http.request(self._method, **request_kwargs)
            pages_requested += 1
            page = parser(response)
            yield page

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