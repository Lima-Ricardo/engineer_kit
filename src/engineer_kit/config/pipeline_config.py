"""Declarative YAML configuration -> backend-agnostic ingestion Pipeline.

The declarative surface mirrors the intent-driven Python API while keeping the
strict typed runtime underneath. Configuration files are versioned, reject
unknown/duplicate keys and keep 0.2 aliases readable for backwards
compatibility.
"""

from __future__ import annotations

import copy
import logging
import os
import re
import warnings
from dataclasses import asdict, dataclass, field
from datetime import date
from pathlib import Path
from typing import Any, Optional, Union

import yaml
from yaml.constructor import ConstructorError

from engineer_kit.adapters.registry import (
    AdapterContext,
    AdapterNotFoundError,
    build_destination,
    build_run_log,
    build_state_store,
    resolve_auto,
)
from engineer_kit.connectors.api_connector import DEFAULT_MAX_PAGES
from engineer_kit.connectors.dedup import resolve_dedup_keys
from engineer_kit.connectors.extraction import DEFAULT_EXTRACTION_BATCH_SIZE
from engineer_kit.connectors.incremental import IncrementalMode
from engineer_kit.connectors.pagination import PaginationStrategy, resolve_pagination
from engineer_kit.connectors.rest import DateParams, RestConnector
from engineer_kit.http.auth import ApiKeyAuth, AuthStrategy, BearerAuth, NoAuth
from engineer_kit.orchestration.pipeline import Pipeline
from engineer_kit.security.secrets import EnvSecretProvider, FileSecretProvider, SecretProvider
from engineer_kit.storage.schema import ColumnSpec, EndpointSchema

logger = logging.getLogger("engineer_kit.config")

CURRENT_PIPELINE_CONFIG_VERSION = 1
MAX_PIPELINE_CONFIG_BYTES = 1024 * 1024
_SECRET_REF_RE = re.compile(r"^\$\{SECRET:([A-Za-z0-9_.-]+)\}$")
_SENSITIVE_OPTION_KEYS = {
    "password",
    "passwd",
    "token",
    "access_token",
    "refresh_token",
    "api_key",
    "apikey",
    "secret",
    "client_secret",
    "aws_access_key_id",
    "aws_secret_access_key",
    "aws_session_token",
    "account_key",
    "sas_token",
    "connection_string",
}

_ROOT_KEYS = {
    "version",
    "name",
    "connector",
    "columns",
    "destination",
    "state",
    "state_store",
    "transform",
    "secrets",
    "run_log",
}
_CONNECTOR_KEYS = {
    "base_url",
    "method",
    "auth",
    "pagination",
    "incremental",
    "date_params",
    "records",
    "records_path",
    "select",
    "params",
    "static_params",
    "state_key",
    "primary_key",
    "dedup",
    "extraction_batch_size",
    "max_pages",
}
_AUTH_KEYS = {"type", "secret_key", "param_name", "location"}
_PAGINATION_KEYS = {
    "type",
    "params",
    "cursor_param",
    "cursor_field",
    "cursor",
    "cursor_path",
    "param",
    "page_param",
    "page_size_param",
    "page_size",
    "start_page",
    "size",
    "size_param",
    "offset_param",
    "limit_param",
    "limit",
    "start_offset",
    "next_url_field",
    "field",
    "path",
    "header_name",
    "header",
}
_INCREMENTAL_KEYS = {"enabled", "mode", "initial_start", "date_field", "field"}
_DATE_PARAM_KEYS = {"start", "end", "format"}
_COLUMN_KEYS = {"name", "dtype"}
_DESTINATION_KEYS = {
    "type",
    "path",
    "schema",
    "batch_size",
    "write_mode",
    "partition_by",
    "options",
}
_STATE_KEYS = {"type", "path", "options"}
_RUN_LOG_KEYS = {"enabled", "type", "path", "options"}
_SECRETS_KEYS = {"type", "path", "allow_inline_values"}
_TRANSFORM_KEYS = {"type", "select"}


class PipelineConfigError(ValueError):
    """Raised when a declarative pipeline is invalid or cannot be built."""


class _UniqueKeySafeLoader(yaml.SafeLoader):
    """Safe loader that refuses duplicate mapping keys instead of last-write-wins."""


def _construct_unique_mapping(loader: yaml.SafeLoader, node, deep: bool = False):
    mapping: dict[Any, Any] = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        if key in mapping:
            raise ConstructorError(
                "while constructing a mapping",
                node.start_mark,
                f"duplicate key: {key!r}",
                key_node.start_mark,
            )
        mapping[key] = loader.construct_object(value_node, deep=deep)
    return mapping


_UniqueKeySafeLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
    _construct_unique_mapping,
)


@dataclass
class SecretsConfig:
    type: str = "env"
    path: Optional[str] = None
    allow_inline_values: bool = False

    def build(self) -> SecretProvider:
        if self.type == "env":
            return EnvSecretProvider()
        if self.type == "file":
            if not self.path:
                raise PipelineConfigError(
                    "secrets.path e obrigatorio quando secrets.type == 'file'."
                )
            return FileSecretProvider(self.path)
        raise PipelineConfigError(
            f"secrets.type '{self.type}' desconhecido (use 'env' ou 'file')."
        )


@dataclass
class AuthConfig:
    type: str = "none"
    secret_key: Optional[str] = None
    param_name: str = "api_key"
    location: str = "query"

    def build(self, secret_provider: SecretProvider) -> AuthStrategy:
        if self.type == "none":
            return NoAuth()
        if self.type == "bearer":
            if not self.secret_key:
                raise PipelineConfigError(
                    "auth.secret_key e obrigatorio quando auth.type == 'bearer'."
                )
            return BearerAuth(secret_provider, self.secret_key)
        if self.type == "api_key":
            if not self.secret_key:
                raise PipelineConfigError(
                    "auth.secret_key e obrigatorio quando auth.type == 'api_key'."
                )
            return ApiKeyAuth(
                secret_provider,
                self.secret_key,
                param_name=self.param_name,
                location=self.location,
            )
        raise PipelineConfigError(
            f"auth.type '{self.type}' desconhecido (use 'none', 'bearer' ou 'api_key')."
        )


@dataclass
class PaginationConfig:
    type: str = "auto"
    params: dict[str, Any] = field(default_factory=dict)

    def build(self) -> PaginationStrategy:
        try:
            return resolve_pagination({"type": self.type, **self.params})
        except (TypeError, ValueError) as exc:
            raise PipelineConfigError(str(exc)) from exc


@dataclass
class IncrementalConfig:
    # Programmatic 0.2 compatibility: explicitly constructing this class still
    # enables incremental mode. ConnectorConfig's default factory disables it.
    enabled: bool = True
    mode: str = "data_date"
    initial_start: Optional[str] = None
    date_field: Optional[str] = None

    def resolve_mode(self) -> IncrementalMode:
        try:
            return IncrementalMode(self.mode)
        except ValueError as exc:
            raise PipelineConfigError(
                f"incremental.mode '{self.mode}' desconhecido "
                "(use 'data_date' ou 'ingestion_date')."
            ) from exc

    def resolve_initial_start(self) -> Optional[date]:
        if not self.initial_start:
            return None
        try:
            return date.fromisoformat(self.initial_start)
        except ValueError as exc:
            raise PipelineConfigError(
                f"incremental.initial_start '{self.initial_start}' deve usar YYYY-MM-DD."
            ) from exc


@dataclass
class DateParamsConfig:
    start: Optional[str] = None
    end: Optional[str] = None
    format: str = "%Y-%m-%d"

    def build(self) -> DateParams:
        return DateParams(start=self.start, end=self.end, date_format=self.format)


@dataclass
class ConnectorConfig:
    base_url: str
    method: str = "GET"
    auth: AuthConfig = field(default_factory=AuthConfig)
    pagination: PaginationConfig = field(default_factory=PaginationConfig)
    incremental: IncrementalConfig = field(
        default_factory=lambda: IncrementalConfig(enabled=False, mode="ingestion_date")
    )
    date_params: DateParamsConfig = field(default_factory=DateParamsConfig)
    records: Optional[str] = None
    select: list[str] | str | dict[str, str] | None = None
    params: dict[str, Any] = field(default_factory=dict)
    state_key: Optional[str] = None
    primary_key: list[str] | str | None = None
    dedup: bool | list[str] | str | None = False
    # 0.2 aliases retained for existing Python configs and the current Local Lab.
    records_path: Optional[str] = None
    static_params: dict[str, Any] = field(default_factory=dict)
    extraction_batch_size: int = DEFAULT_EXTRACTION_BATCH_SIZE
    max_pages: int = DEFAULT_MAX_PAGES

    def __post_init__(self) -> None:
        # Programmatic compatibility for the unreleased intermediate contract
        # where dedup itself carried the PK. Declarative YAML is stricter and
        # requires primary_key + boolean dedup.
        if not isinstance(self.dedup, bool) and self.dedup is not None:
            if self.primary_key is not None:
                raise PipelineConfigError(
                    "connector.dedup deve ser booleano quando primary_key esta definido."
                )
            try:
                legacy_keys = resolve_dedup_keys(self.dedup)
            except (TypeError, ValueError) as exc:
                raise PipelineConfigError(f"connector.dedup invalido: {exc}") from exc
            warnings.warn(
                "ConnectorConfig(dedup=<PK>) esta obsoleto; use primary_key=<PK>, dedup=True.",
                DeprecationWarning,
                stacklevel=2,
            )
            self.primary_key = list(legacy_keys) if legacy_keys else None
            self.dedup = True
        elif self.dedup is None:
            self.dedup = False

        try:
            keys = resolve_dedup_keys(self.primary_key)
        except (TypeError, ValueError) as exc:
            raise PipelineConfigError(f"connector.primary_key invalido: {exc}") from exc
        self.primary_key = list(keys) if keys else None

        if self.dedup and not self.primary_key:
            raise PipelineConfigError(
                "connector.dedup=true exige connector.primary_key."
            )

    def resolved_records(self) -> str | None:
        if self.records and self.records_path and self.records != self.records_path:
            raise PipelineConfigError(
                "connector.records e connector.records_path foram definidos com valores diferentes. "
                "Use apenas connector.records em configs novas."
            )
        return self.records if self.records is not None else self.records_path

    def resolved_params(self) -> dict[str, Any]:
        return {**self.static_params, **self.params}


@dataclass
class ColumnConfig:
    name: str
    dtype: str = "string"


@dataclass
class DestinationConfig:
    """Physical Bronze destination selected through the adapter registry."""

    type: str = "duckdb"
    path: Optional[str] = None
    schema: str = "bronze"
    batch_size: int = 1000
    write_mode: str = "append"
    partition_by: list[str] = field(default_factory=list)
    options: dict[str, Any] = field(default_factory=dict)


@dataclass
class StateConfig:
    """Incremental checkpoint persistence, independent from the data destination."""

    type: str = "auto"
    path: Optional[str] = None
    options: dict[str, Any] = field(default_factory=dict)


@dataclass
class RunLogConfig:
    """Execution audit configuration independent from the destination."""

    enabled: bool = True
    type: str = "auto"
    path: Optional[str] = None
    options: dict[str, Any] = field(default_factory=dict)

    def __bool__(self) -> bool:
        return self.enabled


@dataclass
class TransformConfig:
    """Optional post-ingestion transform used mainly by the local lab."""

    type: str = "none"
    select: Optional[str] = None


@dataclass
class PipelineConfig:
    name: str
    connector: ConnectorConfig
    version: int = CURRENT_PIPELINE_CONFIG_VERSION
    columns: list[ColumnConfig] = field(default_factory=list)
    destination: DestinationConfig = field(default_factory=DestinationConfig)
    state: StateConfig = field(default_factory=StateConfig)
    transform: TransformConfig = field(default_factory=TransformConfig)
    secrets: SecretsConfig = field(default_factory=SecretsConfig)
    run_log: RunLogConfig | bool = field(default_factory=RunLogConfig)

    def __post_init__(self) -> None:
        if isinstance(self.run_log, bool):
            self.run_log = RunLogConfig(enabled=self.run_log)
        if self.version != CURRENT_PIPELINE_CONFIG_VERSION:
            raise PipelineConfigError(
                f"config version {self.version} nao suportada; "
                f"esta versao da biblioteca aceita version={CURRENT_PIPELINE_CONFIG_VERSION}."
            )


def _validate_keys(value: Any, allowed: set[str], path: str) -> None:
    if not isinstance(value, dict):
        raise PipelineConfigError(f"'{path}' deve ser um objeto/mapping.")
    unknown = sorted(str(key) for key in value if key not in allowed)
    if unknown:
        raise PipelineConfigError(
            f"Campo(s) desconhecido(s) em '{path}': {', '.join(unknown)}. "
            "Corrija o nome ou remova a opcao nao suportada."
        )


def _validate_optional_mapping(value: Any, allowed: set[str], path: str) -> None:
    if value is None:
        return
    _validate_keys(value, allowed, path)


def _validate_mapping_value(value: Any, path: str) -> None:
    if value is not None and not isinstance(value, dict):
        raise PipelineConfigError(f"'{path}' deve ser um objeto/mapping.")


def _validate_shape(data: dict[str, Any]) -> None:
    _validate_keys(data, _ROOT_KEYS, "root")
    connector = data.get("connector")
    _validate_keys(connector, _CONNECTOR_KEYS, "connector")

    if "auth" in connector:
        _validate_optional_mapping(connector.get("auth"), _AUTH_KEYS, "connector.auth")

    pagination = connector.get("pagination")
    if pagination is not None and not isinstance(pagination, (bool, str, dict)):
        raise PipelineConfigError("connector.pagination deve ser string, bool ou objeto.")
    if isinstance(pagination, dict):
        _validate_keys(pagination, _PAGINATION_KEYS, "connector.pagination")
        if "params" in pagination:
            _validate_mapping_value(
                pagination.get("params"),
                "connector.pagination.params",
            )

    incremental = connector.get("incremental")
    if incremental is not None and not isinstance(incremental, (bool, str, dict)):
        raise PipelineConfigError("connector.incremental deve ser bool, string ou objeto.")
    if isinstance(incremental, dict):
        _validate_keys(incremental, _INCREMENTAL_KEYS, "connector.incremental")

    if "date_params" in connector:
        _validate_optional_mapping(
            connector.get("date_params"),
            _DATE_PARAM_KEYS,
            "connector.date_params",
        )
    for key in ("params", "static_params"):
        if key in connector:
            _validate_mapping_value(connector.get(key), f"connector.{key}")

    columns = data.get("columns", [])
    if not isinstance(columns, list):
        raise PipelineConfigError("columns deve ser uma lista.")
    for index, column in enumerate(columns):
        _validate_keys(column, _COLUMN_KEYS, f"columns[{index}]")

    destination = data.get("destination")
    if "destination" in data:
        _validate_optional_mapping(destination, _DESTINATION_KEYS, "destination")
    if isinstance(destination, dict) and "options" in destination:
        _validate_mapping_value(destination.get("options"), "destination.options")

    if "state" in data and "state_store" in data:
        raise PipelineConfigError("Use 'state' ou o alias legado 'state_store', nao os dois.")
    state = data.get("state", data.get("state_store"))
    if state is not None and not isinstance(state, (str, dict)):
        raise PipelineConfigError("state deve ser string ou objeto.")
    if isinstance(state, dict):
        _validate_keys(state, _STATE_KEYS, "state")
        if "options" in state:
            _validate_mapping_value(state.get("options"), "state.options")

    secrets = data.get("secrets")
    if "secrets" in data:
        _validate_optional_mapping(secrets, _SECRETS_KEYS, "secrets")

    run_log = data.get("run_log")
    if run_log is not None and not isinstance(run_log, (bool, dict)):
        raise PipelineConfigError("run_log deve ser booleano ou um objeto de configuracao.")
    if isinstance(run_log, dict):
        _validate_keys(run_log, _RUN_LOG_KEYS, "run_log")
        if "options" in run_log:
            _validate_mapping_value(run_log.get("options"), "run_log.options")

    transform = data.get("transform")
    if transform is not None and not isinstance(transform, (str, dict)):
        raise PipelineConfigError("transform deve ser string ou objeto.")
    if isinstance(transform, dict):
        _validate_keys(transform, _TRANSFORM_KEYS, "transform")


def _pagination_from_value(value: Any) -> PaginationConfig:
    if value is None or value is True:
        return PaginationConfig(type="auto")
    if value is False:
        return PaginationConfig(type="none")
    if isinstance(value, str):
        return PaginationConfig(type=value)
    if isinstance(value, dict):
        params = dict(value.get("params") or {})
        params.update(
            {key: nested for key, nested in value.items() if key not in {"type", "params"}}
        )
        return PaginationConfig(type=str(value.get("type", "auto")), params=params)
    raise PipelineConfigError("connector.pagination deve ser string, bool ou objeto.")


def _incremental_from_value(value: Any, *, present: bool) -> IncrementalConfig:
    if not present or value is None or value is False:
        return IncrementalConfig(enabled=False, mode="ingestion_date")
    if value is True:
        return IncrementalConfig(enabled=True, mode="ingestion_date")
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"none", "off", "false", "no"}:
            return IncrementalConfig(enabled=False, mode="ingestion_date")
        return IncrementalConfig(enabled=True, mode="data_date", date_field=value)
    if isinstance(value, dict):
        enabled = _strict_bool(
            value.get("enabled"),
            path="connector.incremental.enabled",
            default=True,
        )
        field_value = value.get("date_field", value.get("field"))
        date_field = str(field_value) if field_value is not None else None
        mode = str(value.get("mode") or ("data_date" if date_field else "ingestion_date"))
        return IncrementalConfig(
            enabled=enabled,
            mode=mode,
            initial_start=(
                str(value["initial_start"]) if value.get("initial_start") else None
            ),
            date_field=date_field,
        )
    raise PipelineConfigError("connector.incremental deve ser bool, string ou objeto.")


def _strict_bool(value: Any, *, path: str, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    raise PipelineConfigError(f"{path} deve ser booleano (true/false).")


def _transform_from_value(value: Any) -> TransformConfig:
    if value is None:
        return TransformConfig()
    if isinstance(value, str):
        return TransformConfig(type=value)
    if isinstance(value, dict):
        return TransformConfig(**value)
    raise PipelineConfigError("transform deve ser string ou objeto.")


def _run_log_from_value(value: Any) -> RunLogConfig:
    if isinstance(value, bool):
        return RunLogConfig(enabled=value)
    if value is None:
        return RunLogConfig()
    if isinstance(value, dict):
        values = dict(value)
        if "enabled" in values:
            values["enabled"] = _strict_bool(
                values.get("enabled"),
                path="run_log.enabled",
                default=True,
            )
        return RunLogConfig(**values)
    raise PipelineConfigError("run_log deve ser booleano ou um objeto de configuracao.")


def _is_secret_ref(value: Any) -> bool:
    return isinstance(value, str) and _SECRET_REF_RE.fullmatch(value) is not None


def _validate_sensitive_mapping(
    value: Any,
    *,
    path: str,
    allow_inline_values: bool,
) -> None:
    if allow_inline_values or not isinstance(value, dict):
        return
    for key, nested in value.items():
        normalized = str(key).strip().lower().replace("-", "_")
        current_path = f"{path}.{key}"
        if (
            normalized in _SENSITIVE_OPTION_KEYS
            and nested not in (None, "")
            and not _is_secret_ref(nested)
        ):
            raise PipelineConfigError(
                f"Valor sensivel inline em '{current_path}' foi recusado. "
                "Use ${SECRET:NOME} e configure secrets.type, ou habilite "
                "secrets.allow_inline_values explicitamente assumindo o risco."
            )
        _validate_sensitive_mapping(
            nested,
            path=current_path,
            allow_inline_values=allow_inline_values,
        )


def _validate_no_inline_secrets(data: dict[str, Any], secrets: SecretsConfig) -> None:
    connector = data.get("connector") or {}
    destination = data.get("destination") or {}
    state = data.get("state", data.get("state_store", {})) or {}
    run_log = data.get("run_log") or {}

    if isinstance(connector, dict):
        for key in ("params", "static_params"):
            _validate_sensitive_mapping(
                connector.get(key) or {},
                path=f"connector.{key}",
                allow_inline_values=secrets.allow_inline_values,
            )
    if isinstance(destination, dict):
        _validate_sensitive_mapping(
            destination.get("options") or {},
            path="destination.options",
            allow_inline_values=secrets.allow_inline_values,
        )
    if isinstance(state, dict):
        _validate_sensitive_mapping(
            state.get("options") or {},
            path="state.options",
            allow_inline_values=secrets.allow_inline_values,
        )
    if isinstance(run_log, dict):
        _validate_sensitive_mapping(
            run_log.get("options") or {},
            path="run_log.options",
            allow_inline_values=secrets.allow_inline_values,
        )


def _resolve_secret_refs(value: Any, provider: SecretProvider) -> Any:
    if isinstance(value, str):
        match = _SECRET_REF_RE.fullmatch(value)
        return provider.get(match.group(1)) if match else value
    if isinstance(value, dict):
        return {
            key: _resolve_secret_refs(nested, provider)
            for key, nested in value.items()
        }
    if isinstance(value, list):
        return [_resolve_secret_refs(nested, provider) for nested in value]
    if isinstance(value, tuple):
        return tuple(_resolve_secret_refs(nested, provider) for nested in value)
    return value


def pipeline_config_from_dict(data: dict[str, Any]) -> PipelineConfig:
    if not isinstance(data, dict):
        raise PipelineConfigError("Configuracao deve ser um mapping.")
    _validate_shape(data)

    try:
        name = data["name"]
        connector_data = data["connector"]
        base_url = connector_data["base_url"]
    except KeyError as exc:
        raise PipelineConfigError(
            f"Campo obrigatorio faltando na configuracao: {exc}"
        ) from exc

    raw_version = data.get("version", CURRENT_PIPELINE_CONFIG_VERSION)
    if isinstance(raw_version, bool) or not isinstance(raw_version, int):
        raise PipelineConfigError("version deve ser um inteiro.")
    version = raw_version
    if version != CURRENT_PIPELINE_CONFIG_VERSION:
        raise PipelineConfigError(
            f"config version {version} nao suportada; "
            f"use version={CURRENT_PIPELINE_CONFIG_VERSION}."
        )

    secrets_data = dict(data.get("secrets") or {})
    if "allow_inline_values" in secrets_data:
        secrets_data["allow_inline_values"] = _strict_bool(
            secrets_data.get("allow_inline_values"),
            path="secrets.allow_inline_values",
            default=False,
        )
    secrets = SecretsConfig(**secrets_data)
    _validate_no_inline_secrets(data, secrets)

    connector = ConnectorConfig(
        base_url=base_url,
        method=connector_data.get("method", "GET"),
        auth=AuthConfig(**(connector_data.get("auth", {}) or {})),
        pagination=_pagination_from_value(connector_data.get("pagination", "auto")),
        incremental=_incremental_from_value(
            connector_data.get("incremental"),
            present="incremental" in connector_data,
        ),
        date_params=DateParamsConfig(**(connector_data.get("date_params", {}) or {})),
        records=connector_data.get("records"),
        select=connector_data.get("select"),
        params=connector_data.get("params") or {},
        state_key=connector_data.get("state_key"),
        primary_key=connector_data.get("primary_key"),
        dedup=_strict_bool(
            connector_data.get("dedup", False),
            path="connector.dedup",
        ),
        records_path=connector_data.get("records_path"),
        static_params=connector_data.get("static_params") or {},
        extraction_batch_size=connector_data.get(
            "extraction_batch_size",
            DEFAULT_EXTRACTION_BATCH_SIZE,
        ),
        max_pages=connector_data.get("max_pages", DEFAULT_MAX_PAGES),
    )
    columns = [
        ColumnConfig(name=column["name"], dtype=column.get("dtype", "string"))
        for column in data.get("columns", [])
    ]
    state_data = data.get("state", data.get("state_store", {})) or {}
    if isinstance(state_data, str):
        state_data = {"type": state_data}

    return PipelineConfig(
        version=version,
        name=name,
        connector=connector,
        columns=columns,
        destination=DestinationConfig(**(data.get("destination", {}) or {})),
        state=StateConfig(**state_data),
        transform=_transform_from_value(data.get("transform")),
        secrets=secrets,
        run_log=_run_log_from_value(data.get("run_log", True)),
    )


def pipeline_config_to_dict(config: PipelineConfig) -> dict[str, Any]:
    run_log = config.run_log
    assert isinstance(run_log, RunLogConfig)
    return {
        "version": config.version,
        "name": config.name,
        "connector": {
            "base_url": config.connector.base_url,
            "method": config.connector.method,
            "auth": asdict(config.connector.auth),
            "pagination": asdict(config.connector.pagination),
            "incremental": asdict(config.connector.incremental),
            "date_params": asdict(config.connector.date_params),
            "records": config.connector.records,
            "select": config.connector.select,
            "params": config.connector.params,
            "state_key": config.connector.state_key,
            "primary_key": config.connector.primary_key,
            "dedup": config.connector.dedup,
            "records_path": config.connector.records_path,
            "static_params": config.connector.static_params,
            "extraction_batch_size": config.connector.extraction_batch_size,
            "max_pages": config.connector.max_pages,
        },
        "columns": [asdict(column) for column in config.columns],
        "destination": asdict(config.destination),
        "state": asdict(config.state),
        "transform": asdict(config.transform),
        "secrets": asdict(config.secrets),
        "run_log": asdict(run_log),
    }


def load_pipeline_config(path: Union[str, Path]) -> PipelineConfig:
    source = Path(path)
    raw = source.read_bytes()
    if len(raw) > MAX_PIPELINE_CONFIG_BYTES:
        raise PipelineConfigError(
            f"Configuracao YAML excede o limite de {MAX_PIPELINE_CONFIG_BYTES} bytes: '{path}'."
        )
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise PipelineConfigError(
            f"Configuracao YAML deve usar UTF-8: '{path}'."
        ) from exc

    # Instantiate our SafeLoader subclass directly rather than calling
    # yaml.load(). This preserves SafeLoader's restricted constructors while
    # letting us reject duplicate mapping keys before building the config.
    loader = _UniqueKeySafeLoader(text)
    try:
        data = loader.get_single_data()
    except yaml.YAMLError as exc:
        raise PipelineConfigError(
            f"Configuracao YAML invalida em '{path}': {exc}"
        ) from exc
    finally:
        loader.dispose()

    if not isinstance(data, dict):
        raise PipelineConfigError(f"Configuracao YAML invalida em '{path}'.")
    return pipeline_config_from_dict(data)


def save_pipeline_config(config: PipelineConfig, path: Union[str, Path]) -> None:
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(
        yaml.safe_dump(
            pipeline_config_to_dict(config),
            sort_keys=False,
            allow_unicode=True,
        ),
        encoding="utf-8",
    )
    if os.name != "nt":
        destination.chmod(0o600)


def list_pipeline_configs(directory: Union[str, Path]) -> list[tuple[Path, PipelineConfig]]:
    directory = Path(directory)
    if not directory.exists():
        return []
    results: list[tuple[Path, PipelineConfig]] = []
    for path in sorted(directory.glob("*.yaml")):
        try:
            results.append((path, load_pipeline_config(path)))
        except Exception as exc:
            logger.warning("Configuracao invalida em '%s', ignorando: %s", path, exc)
    return results


def build_pipeline(config: PipelineConfig, runtime: Any = None) -> Pipeline:
    """Build a Pipeline by resolving state/destination/audit adapters lazily.

    State is only constructed when incrementality is enabled. This makes the
    declarative happy path equivalent to ``RestConnector(base_url=...)`` while
    preserving explicit StateStore configuration for incremental pipelines.
    """
    if config.transform.type not in {"none", "dbt"}:
        raise PipelineConfigError(
            f"transform.type '{config.transform.type}' desconhecido (use 'none' ou 'dbt')."
        )

    run_log = config.run_log
    assert isinstance(run_log, RunLogConfig)

    try:
        secret_provider = config.secrets.build()
        destination_config = copy.deepcopy(config.destination)
        destination_config.options = _resolve_secret_refs(
            destination_config.options,
            secret_provider,
        )
        state_config = copy.deepcopy(config.state)
        state_config.options = _resolve_secret_refs(
            state_config.options,
            secret_provider,
        )
        run_log_config = copy.deepcopy(run_log)
        run_log_config.options = _resolve_secret_refs(
            run_log_config.options,
            secret_provider,
        )
        connector_params = _resolve_secret_refs(
            config.connector.resolved_params(),
            secret_provider,
        )

        context = AdapterContext(
            pipeline_name=config.name,
            runtime=runtime,
            destination_config=destination_config,
        )
        destination = build_destination(
            destination_config.type,
            destination_config,
            context,
        )

        state_store = None
        if config.connector.incremental.enabled:
            state_type = resolve_auto(
                state_config.type,
                destination_type=destination_config.type,
                kind="state",
            )
            state_store = build_state_store(state_type, state_config, context)

        run_log_backend = None
        if run_log_config.enabled:
            run_log_type = resolve_auto(
                run_log_config.type,
                destination_type=destination_config.type,
                kind="run_log",
            )
            run_log_backend = build_run_log(run_log_type, run_log_config, context)

        connector = RestConnector(
            name=config.name,
            base_url=config.connector.base_url,
            state_store=state_store,
            state_key=config.connector.state_key,
            incremental_mode=config.connector.incremental.resolve_mode(),
            initial_start=config.connector.incremental.resolve_initial_start(),
            date_field=config.connector.incremental.date_field,
            incremental=None if config.connector.incremental.enabled else False,
            pagination=config.connector.pagination.build(),
            method=config.connector.method,
            auth=config.connector.auth.build(secret_provider),
            date_params=config.connector.date_params.build(),
            params=connector_params or None,
            records=config.connector.resolved_records(),
            select=config.connector.select,
            primary_key=config.connector.primary_key,
            dedup=config.connector.dedup,
            extraction_batch_size=config.connector.extraction_batch_size,
            max_pages=config.connector.max_pages,
        )
        schema = EndpointSchema(
            columns=[
                ColumnSpec(column.name, dtype=column.dtype)
                for column in config.columns
            ]
        )
        return Pipeline(
            connector=connector,
            schema=schema,
            destination=destination,
            run_log=run_log_config.enabled,
            run_log_store=run_log_backend,
        )
    except PipelineConfigError:
        raise
    except AdapterNotFoundError as exc:
        raise PipelineConfigError(str(exc)) from exc
    except ModuleNotFoundError as exc:
        raise PipelineConfigError(
            _dependency_hint(exc, config.destination.type)
        ) from None
    except ValueError as exc:
        raise PipelineConfigError(str(exc)) from exc


def _dependency_hint(exc: ModuleNotFoundError, destination_type: str) -> str:
    missing = exc.name or "dependencia opcional"
    if missing == "duckdb":
        return 'DuckDB e opcional. Instale `pip install "engineer_kit[duckdb]"` ou `[local]`.'
    if missing == "pyarrow":
        extra = "delta" if destination_type == "delta" else "parquet"
        return f'PyArrow e opcional. Instale `pip install "engineer_kit[{extra}]"`.'
    if missing == "deltalake":
        return 'Delta Lake e opcional. Instale `pip install "engineer_kit[delta]"`.'
    return f"Dependencia opcional ausente: {missing}."


__all__ = [
    "CURRENT_PIPELINE_CONFIG_VERSION",
    "MAX_PIPELINE_CONFIG_BYTES",
    "AuthConfig",
    "ColumnConfig",
    "ConnectorConfig",
    "DateParamsConfig",
    "DestinationConfig",
    "IncrementalConfig",
    "PaginationConfig",
    "PipelineConfig",
    "PipelineConfigError",
    "RunLogConfig",
    "SecretsConfig",
    "StateConfig",
    "TransformConfig",
    "build_pipeline",
    "list_pipeline_configs",
    "load_pipeline_config",
    "pipeline_config_from_dict",
    "pipeline_config_to_dict",
    "save_pipeline_config",
]
