"""Generic REST/JSON connector with an intent-driven happy path."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from time import monotonic
from typing import Any, Callable, Iterator, Optional, Sequence, Union

import requests

from engineer_kit.connectors.api_connector import (
    DEFAULT_MAX_PAGES,
    APIConnector,
    MissingDateFieldError,
)
from engineer_kit.connectors.date_field import DateFieldSpec
from engineer_kit.connectors.extraction import DEFAULT_EXTRACTION_BATCH_SIZE
from engineer_kit.connectors.incremental import (
    IncrementalMode,
    IncrementalStrategy,
    IncrementalWindow,
    NoIncrementalStrategy,
)
from engineer_kit.connectors.intent import (
    FieldSelection,
    as_date,
    infer_name,
    pick_records_path,
    project,
    read_path,
    resolve_select,
)
from engineer_kit.connectors.normalize import stringify
from engineer_kit.connectors.pagination import (
    AutoPagination,
    PaginationStrategy,
    ParsedPage,
    resolve_pagination,
)
from engineer_kit.http.auth import AuthStrategy
from engineer_kit.http.auth_intent import resolve_auth
from engineer_kit.http.client import HttpClient
from engineer_kit.storage.state_store import StateStore, validate_state_key


@dataclass
class DateParams:
    start: Optional[str] = None
    end: Optional[str] = None
    date_format: str = "%Y-%m-%d"


@dataclass(frozen=True)
class ProbeResult:
    """Bounded read-only inspection of one API page.

    A probe never writes a destination and never commits a checkpoint. It is
    intended to be the shared primitive behind CLI/UI Test Connection and
    Response Preview experiences.
    """

    records: list[dict[str, Any]]
    raw: Any
    headers: dict[str, str]
    records_path: str | None
    pagination: str
    status_code: int | None
    latency_ms: float
    response_bytes: int | None


class RestConnector(APIConnector):
    """REST connector whose simple inputs are resolved once before extraction."""

    def __init__(
        self,
        name: str | None = None,
        base_url: str | None = None,
        pagination: PaginationStrategy | str | dict[str, Any] | bool | None = "auto",
        method: str = "GET",
        state_store: Optional[StateStore] = None,
        state_key: str | None = None,
        incremental_mode: IncrementalMode = IncrementalMode.DATA_DATE,
        initial_start: Optional[date | str] = None,
        date_field: Optional[DateFieldSpec] = None,
        incremental: IncrementalStrategy | bool | str | dict[str, Any] | None = None,
        auth: Optional[AuthStrategy | str] = None,
        date_params: Optional[DateParams | dict[str, Any]] = None,
        static_params: Optional[dict[str, Any]] = None,
        params: Optional[dict[str, Any]] = None,
        records_path: Optional[Union[Callable[[Any], list[dict]], str]] = None,
        records: Optional[Union[Callable[[Any], list[dict]], str]] = None,
        select: list[str] | tuple[str, ...] | str | dict[str, str] | None = None,
        primary_key: str | Sequence[str] | None = None,
        dedup: bool | str | Sequence[str] | None = False,
        http_client: Optional[HttpClient] = None,
        extraction_batch_size: int = DEFAULT_EXTRACTION_BATCH_SIZE,
        max_pages: int = DEFAULT_MAX_PAGES,
        allow_cross_origin_pagination: bool = False,
    ) -> None:
        if base_url is None and isinstance(name, str) and name.startswith(("https://", "http://")):
            base_url, name = name, None
        if not base_url:
            raise ValueError("base_url e obrigatorio.")
        if records_path is not None and records is not None:
            raise ValueError("Use records= ou records_path=, nao os dois.")

        resolved_name = name or infer_name(base_url)
        resolved_state_key = validate_state_key(state_key or resolved_name)
        self._base_url = base_url
        self._static_params = {**(static_params or {}), **(params or {})}
        self._records_path = records if records is not None else records_path
        self._resolved_records_path = (
            self._records_path if isinstance(self._records_path, str) else None
        )
        self._select = resolve_select(select)
        self._date_params = self._date_params_from(date_params)
        self._state_key = resolved_state_key

        start = as_date(initial_start)
        mode = incremental_mode
        field = date_field
        runtime_incremental: IncrementalStrategy | None
        auto_state = False

        if isinstance(incremental, NoIncrementalStrategy):
            runtime_incremental = incremental
        elif isinstance(incremental, IncrementalStrategy):
            strategy_state_key = validate_state_key(incremental.state_key)
            if state_key is not None and resolved_state_key != strategy_state_key:
                raise ValueError(
                    "state_key diverge da IncrementalStrategy explicita. "
                    "Use a chave da strategy ou remova state_key da facade."
                )
            resolved_state_key = strategy_state_key
            self._state_key = strategy_state_key
            runtime_incremental = incremental
        elif isinstance(incremental, str):
            field, mode = incremental, IncrementalMode.DATA_DATE
            auto_state = state_store is None
            runtime_incremental, state_store = self._stateful(
                resolved_state_key, state_store, mode, start
            )
        elif isinstance(incremental, dict):
            config = dict(incremental)
            field_value = config.get("field", config.get("date_field"))
            field = str(field_value) if field_value is not None else field
            mode = (
                IncrementalMode(str(config["mode"]).lower())
                if config.get("mode")
                else (IncrementalMode.DATA_DATE if field else IncrementalMode.INGESTION_DATE)
            )
            start = as_date(config.get("initial_start", start))
            if config.get("start_param") or config.get("end_param") or config.get("param"):
                start_param = config.get("start_param", config.get("param"))
                self._date_params = DateParams(
                    start=str(start_param) if start_param else None,
                    end=str(config["end_param"]) if config.get("end_param") else None,
                    date_format=str(
                        config.get("format", config.get("date_format", "%Y-%m-%d"))
                    ),
                )
            if state_store is None and config.get("state_path"):
                state_store = self._local_state(Path(str(config["state_path"])))
            elif state_store is None:
                auto_state = True
            runtime_incremental, state_store = self._stateful(
                resolved_state_key, state_store, mode, start
            )
        elif incremental is True:
            mode = IncrementalMode.DATA_DATE if field else IncrementalMode.INGESTION_DATE
            auto_state = state_store is None
            runtime_incremental, state_store = self._stateful(
                resolved_state_key, state_store, mode, start
            )
        elif incremental is False or state_store is None:
            runtime_incremental = NoIncrementalStrategy()
        else:
            runtime_incremental = None

        if runtime_incremental is None and state_store is not None:
            if mode is IncrementalMode.DATA_DATE and field is None:
                raise MissingDateFieldError(
                    "incremental_mode=DATA_DATE precisa de date_field. Use "
                    "IncrementalMode.INGESTION_DATE quando o checkpoint for a data da execucao."
                )
            runtime_incremental = IncrementalStrategy(
                resolved_state_key,
                state_store,
                mode=mode,
                initial_start=start,
            )

        self._auto_state = auto_state
        self._resolved_incremental_mode = mode
        self._resolved_initial_start = start

        http = http_client or HttpClient(auth=resolve_auth(auth))
        super().__init__(
            name=resolved_name,
            http_client=http,
            pagination=resolve_pagination(pagination),
            method=method,
            state_store=state_store,
            incremental_mode=mode,
            initial_start=start,
            date_field=field,
            incremental=runtime_incremental,
            extraction_batch_size=extraction_batch_size,
            max_pages=max_pages,
            allow_cross_origin_pagination=allow_cross_origin_pagination,
            record_transform=self._project_record if self._select else None,
            primary_key=primary_key,
            dedup=dedup,
        )
        if self._select and self.primary_key:
            emitted = {item.alias for item in self._select}
            missing_keys = [key for key in self.primary_key if key not in emitted]
            if missing_keys:
                raise ValueError(
                    "primary_key deve referenciar colunas emitidas depois de select. "
                    f"PK(s) ausente(s): {', '.join(missing_keys)}."
                )

    @staticmethod
    def _date_params_from(value: DateParams | dict[str, Any] | None) -> DateParams:
        if value is None:
            return DateParams()
        if isinstance(value, DateParams):
            return value
        data = dict(value)
        if "format" in data and "date_format" not in data:
            data["date_format"] = data.pop("format")
        return DateParams(**data)

    @staticmethod
    def _local_state(path: Path) -> StateStore:
        from engineer_kit.adapters.files.state_store import JsonFileStateStore

        return JsonFileStateStore(path)

    @classmethod
    def _stateful(
        cls,
        state_key: str,
        store: StateStore | None,
        mode: IncrementalMode,
        start: date | None,
    ):
        resolved = store or cls._local_state(Path(".engineer_kit") / "state.json")
        return IncrementalStrategy(state_key, resolved, mode=mode, initial_start=start), resolved

    @property
    def needs_auto_state(self) -> bool:
        """Whether managed mode may replace the automatically chosen local state."""
        return self._auto_state

    @property
    def state_key(self) -> str:
        """Stable key used for incremental state; defaults to ``name`` for compatibility."""
        return self._state_key

    def _bind_auto_state_store(self, state_store: StateStore) -> None:
        """Let a managed destination replace only the automatically chosen local state."""
        if not self._auto_state:
            return
        self._incremental = IncrementalStrategy(
            self._state_key,
            state_store,
            mode=self._resolved_incremental_mode,
            initial_start=self._resolved_initial_start,
        )

    @property
    def selected_fields(self) -> tuple[str, ...] | None:
        """Projected output names kept compatible with the 0.2 public property."""
        return tuple(item.alias for item in self._select) if self._select else None

    @property
    def selected_paths(self) -> tuple[str, ...] | None:
        return tuple(item.path for item in self._select) if self._select else None

    @property
    def field_selections(self) -> tuple[FieldSelection, ...] | None:
        return self._select

    @property
    def resolved_records_path(self) -> str | None:
        return self._resolved_records_path

    def build_request(
        self,
        window: IncrementalWindow,
        page_params: dict[str, Any],
    ) -> dict[str, Any]:
        payload = {**self._static_params, **page_params}
        if self._date_params.start and window.start:
            payload[self._date_params.start] = window.start.strftime(
                self._date_params.date_format
            )
        if self._date_params.end and window.end:
            payload[self._date_params.end] = window.end.strftime(self._date_params.date_format)
        return (
            {"url": self._base_url, "json": payload}
            if self._method == "POST"
            else {"url": self._base_url, "params": payload}
        )

    def parse_response(self, response: requests.Response) -> ParsedPage:
        raw = response.json()
        items = self._extract_items(raw)
        return ParsedPage(
            records=[stringify(item) for item in items],
            raw=raw,
            headers=dict(response.headers),
        )

    def parse_profile_response(self, response: requests.Response) -> ParsedPage:
        """Preserve native JSON values for schema and type profiling."""
        raw = response.json()
        return ParsedPage(
            records=self._extract_items(raw),
            raw=raw,
            headers=dict(response.headers),
        )

    def _project_record(self, record: dict[str, Any]) -> dict[str, Any]:
        return project(record, self._select)

    def _extract_items(self, raw: Any) -> list[dict[str, Any]]:
        if callable(self._records_path):
            items = self._records_path(raw)
        elif self._resolved_records_path:
            items = read_path(raw, self._resolved_records_path)
        elif isinstance(raw, list):
            items = raw
        elif isinstance(raw, dict):
            path = pick_records_path(raw)
            if path is None:
                return [raw]
            self._resolved_records_path = path
            items = read_path(raw, path)
        else:
            items = [raw]
        if not isinstance(items, list):
            raise TypeError("records deve apontar para uma lista JSON.")
        return [item if isinstance(item, dict) else {"value": item} for item in items]

    def probe(
        self,
        end: Union[date, str] = "today",
        *,
        limit: int = 25,
    ) -> ProbeResult:
        """Fetch exactly one page for diagnostics without advancing state."""
        if (
            isinstance(limit, bool)
            or not isinstance(limit, int)
            or limit <= 0
            or limit > 1000
        ):
            raise ValueError("probe limit deve ser um inteiro entre 1 e 1000.")

        window = self._incremental.resolve_window(end)
        page_params = self._pagination.initial_params()
        request_kwargs = self.build_request(window, page_params)
        started = monotonic()
        response = self._http.request(self._method, **request_kwargs)
        latency_ms = (monotonic() - started) * 1000.0
        page = self.parse_response(response)

        if isinstance(self._pagination, AutoPagination):
            self._pagination.next_params(page, page_params)

        records = page.records[:limit]
        if self._select:
            records = [self._project_record(record) for record in records]

        content = getattr(response, "content", None)
        response_bytes = len(content) if isinstance(content, (bytes, bytearray)) else None
        status_code = getattr(response, "status_code", None)
        pagination = type(self._pagination).__name__
        if isinstance(self._pagination, AutoPagination) and self._pagination.resolved_type:
            pagination += f" -> {self._pagination.resolved_type}"

        return ProbeResult(
            records=records,
            raw=page.raw,
            headers=page.headers,
            records_path=self._resolved_records_path,
            pagination=pagination,
            status_code=int(status_code) if isinstance(status_code, int) else None,
            latency_ms=latency_ms,
            response_bytes=response_bytes,
        )

    def preview(
        self,
        end: Union[date, str] = "today",
        *,
        limit: int = 25,
    ) -> ProbeResult:
        """Alias for :meth:`probe` used by interactive clients."""
        return self.probe(end=end, limit=limit)

    def collect(self, end: Union[date, str] = "today") -> list[dict[str, Any]]:
        session = self.extract_incremental(end)
        try:
            result = session.collect()
            session.commit()
            return result
        except Exception:
            if not session.committed:
                session.abort()
            raise

    def stream(
        self,
        end: Union[date, str] = "today",
        *,
        batch_size: int | None = None,
    ) -> Iterator[list[dict[str, Any]]]:
        session = self.extract_incremental(end, batch_size=batch_size)
        try:
            yield from session
        except BaseException:
            if not session.committed:
                session.abort()
            raise
        else:
            session.commit()

    def explain(self) -> dict[str, Any]:
        pagination = type(self._pagination).__name__
        if isinstance(self._pagination, AutoPagination) and self._pagination.resolved_type:
            pagination += f" -> {self._pagination.resolved_type}"
        return {
            "name": self.name,
            "method": self._method,
            "base_url": self._base_url,
            "pagination": pagination,
            "records": self._resolved_records_path or "auto",
            "select": [
                {"path": item.path, "alias": item.alias}
                for item in (self._select or ())
            ],
            "primary_key": list(self.primary_key) if self.primary_key else None,
            "dedup": self.dedup_enabled,
            "incremental": type(self._incremental).__name__,
            "state": "destination-auto" if self._auto_state else "explicit-or-disabled",
            "state_key": self._state_key,
            "batch_size": self.extraction_batch_size,
        }

    def to(self, destination: Any, target: str | None = None, **options: Any):
        from engineer_kit.orchestration.flow import ManagedFlow

        return ManagedFlow(self, destination, target=target, options=options)


__all__ = ["DateParams", "ProbeResult", "RestConnector"]