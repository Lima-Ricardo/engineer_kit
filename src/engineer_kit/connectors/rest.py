"""Generic REST/JSON connector with an intent-driven happy path."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Callable, Iterator, Optional, Union

import requests

from engineer_kit.connectors.api_connector import APIConnector, DEFAULT_MAX_PAGES
from engineer_kit.connectors.date_field import DateFieldSpec
from engineer_kit.connectors.extraction import DEFAULT_EXTRACTION_BATCH_SIZE
from engineer_kit.connectors.incremental import IncrementalMode, IncrementalStrategy, IncrementalWindow, NoIncrementalStrategy
from engineer_kit.connectors.intent import as_date, infer_name, pick_records_path, project, read_path, resolve_select
from engineer_kit.connectors.normalize import stringify
from engineer_kit.connectors.pagination import AutoPagination, PaginationStrategy, ParsedPage, resolve_pagination
from engineer_kit.http.auth import AuthStrategy
from engineer_kit.http.auth_intent import resolve_auth
from engineer_kit.http.client import HttpClient
from engineer_kit.storage.state_store import StateStore


@dataclass
class DateParams:
    start: Optional[str] = None
    end: Optional[str] = None
    date_format: str = "%Y-%m-%d"


class RestConnector(APIConnector):
    """REST connector whose simple inputs are resolved once before extraction."""

    def __init__(
        self,
        name: str | None = None,
        base_url: str | None = None,
        pagination: PaginationStrategy | str | dict[str, Any] | bool | None = "auto",
        method: str = "GET",
        state_store: Optional[StateStore] = None,
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
        select: list[str] | tuple[str, ...] | str | None = None,
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
        self._base_url = base_url
        self._static_params = {**(static_params or {}), **(params or {})}
        self._records_path = records if records is not None else records_path
        self._resolved_records_path = self._records_path if isinstance(self._records_path, str) else None
        self._select = resolve_select(select)
        self._date_params = self._date_params_from(date_params)

        start = as_date(initial_start)
        mode = incremental_mode
        field = date_field
        runtime_incremental: IncrementalStrategy | None

        if isinstance(incremental, IncrementalStrategy):
            runtime_incremental = incremental
        elif isinstance(incremental, str):
            field, mode = incremental, IncrementalMode.DATA_DATE
            runtime_incremental, state_store = self._stateful(resolved_name, state_store, mode, start)
        elif isinstance(incremental, dict):
            config = dict(incremental)
            field_value = config.get("field", config.get("date_field"))
            field = str(field_value) if field_value is not None else field
            mode = IncrementalMode(str(config["mode"]).lower()) if config.get("mode") else (IncrementalMode.DATA_DATE if field else IncrementalMode.INGESTION_DATE)
            start = as_date(config.get("initial_start", start))
            if config.get("start_param") or config.get("end_param") or config.get("param"):
                start_param = config.get("start_param", config.get("param"))
                self._date_params = DateParams(
                    start=str(start_param) if start_param else None,
                    end=str(config["end_param"]) if config.get("end_param") else None,
                    date_format=str(config.get("format", config.get("date_format", "%Y-%m-%d"))),
                )
            if state_store is None and config.get("state_path"):
                state_store = self._local_state(Path(str(config["state_path"])))
            runtime_incremental, state_store = self._stateful(resolved_name, state_store, mode, start)
        elif incremental is True:
            mode = IncrementalMode.DATA_DATE if field else IncrementalMode.INGESTION_DATE
            runtime_incremental, state_store = self._stateful(resolved_name, state_store, mode, start)
        elif incremental is False or state_store is None:
            runtime_incremental = NoIncrementalStrategy()
        else:
            runtime_incremental = None

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
    def _stateful(cls, name: str, store: StateStore | None, mode: IncrementalMode, start: date | None):
        resolved = store or cls._local_state(Path(".engineer_kit") / "state.json")
        return IncrementalStrategy(name, resolved, mode=mode, initial_start=start), resolved

    @property
    def selected_fields(self) -> tuple[str, ...] | None:
        return self._select

    @property
    def resolved_records_path(self) -> str | None:
        return self._resolved_records_path

    def build_request(self, window: IncrementalWindow, page_params: dict[str, Any]) -> dict[str, Any]:
        payload = {**self._static_params, **page_params}
        if self._date_params.start and window.start:
            payload[self._date_params.start] = window.start.strftime(self._date_params.date_format)
        if self._date_params.end and window.end:
            payload[self._date_params.end] = window.end.strftime(self._date_params.date_format)
        return {"url": self._base_url, "json": payload} if self._method == "POST" else {"url": self._base_url, "params": payload}

    def parse_response(self, response: requests.Response) -> ParsedPage:
        raw = response.json()
        items = self._extract_items(raw)
        return ParsedPage(
            records=[stringify(project(item, self._select)) for item in items],
            raw=raw,
            headers=dict(response.headers),
        )

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

    def stream(self, end: Union[date, str] = "today", *, batch_size: int | None = None) -> Iterator[list[dict[str, Any]]]:
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
            "select": list(self._select or ()),
            "incremental": type(self._incremental).__name__,
            "batch_size": self.extraction_batch_size,
        }

    def to(self, destination: Any, target: str | None = None, **options: Any):
        from engineer_kit.orchestration.flow import ManagedFlow
        return ManagedFlow(self, destination, target=target, options=options)
