"""Declarative YAML configuration -> backend-agnostic ingestion Pipeline.

Configuration describes concepts (connector, state, destination, audit and
optional transform) rather than hard-coding DuckDB. Concrete implementations
are resolved lazily through the adapter registry.
"""

from __future__ import annotations

import copy
import logging
import os
import re
from dataclasses import asdict, dataclass, field
from datetime import date
from pathlib import Path
from typing import Any, Optional, Union

import yaml

from engineer_kit.adapters.registry import (
    AdapterContext,
    AdapterNotFoundError,
    build_destination,
    build_run_log,
    build_state_store,
    resolve_auto,
)
from engineer_kit.connectors.api_connector import DEFAULT_MAX_PAGES
from engineer_kit.connectors.extraction import DEFAULT_EXTRACTION_BATCH_SIZE
from engineer_kit.connectors.incremental import IncrementalMode
from engineer_kit.connectors.pagination import STANDARD_PAGINATION_TYPES, PaginationStrategy
from engineer_kit.connectors.rest import DateParams, RestConnector
from engineer_kit.http.auth import ApiKeyAuth, AuthStrategy, BearerAuth, NoAuth
from engineer_kit.orchestration.pipeline import Pipeline
from engineer_kit.security.secrets import EnvSecretProvider, FileSecretProvider, SecretProvider
from engineer_kit.storage.schema import ColumnSpec, EndpointSchema

logger = logging.getLogger("engineer_kit.config")

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


class PipelineConfigError(ValueError):
    """Raised when a declarative pipeline is invalid or cannot be built."""


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
                raise PipelineConfigError("secrets.path e obrigatorio quando secrets.type == 'file'.")
            return FileSecretProvider(self.path)
        raise PipelineConfigError(f"secrets.type '{self.type}' desconhecido (use 'env' ou 'file').")


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
                raise PipelineConfigError("auth.secret_key e obrigatorio quando auth.type == 'bearer'.")
            return BearerAuth(secret_provider, self.secret_key)
        if self.type == "api_key":
            if not self.secret_key:
                raise PipelineConfigError("auth.secret_key e obrigatorio quando auth.type == 'api_key'.")
            return ApiKeyAuth(
                secret_provider,
                self.secret_key,
                param_name=self.param_name,
                location=self.location,
            )
        raise PipelineConfigError(f"auth.type '{self.type}' desconhecido (use 'none', 'bearer' ou 'api_key').")


@dataclass
class PaginationConfig:
    type: str = "none"
    params: dict[str, Any] = field(default_factory=dict)

    def build(self) -> PaginationStrategy:
        strategy_cls = STANDARD_PAGINATION_TYPES.get(self.type)
        if strategy_cls is None:
            valid = ", ".join(STANDARD_PAGINATION_TYPES)
            raise PipelineConfigError(f"pagination.type '{self.type}' desconhecido. Use um de: {valid}.")
        try:
            return strategy_cls(**self.params)
        except TypeError as exc:
            raise PipelineConfigError(f"pagination.params invalido para '{self.type}': {exc}") from exc


@dataclass
class IncrementalConfig:
    mode: str = "data_date"
    initial_start: Optional[str] = None
    date_field: Optional[str] = None

    def resolve_mode(self) -> IncrementalMode:
        try:
            return IncrementalMode(self.mode)
        except ValueError as exc:
            raise PipelineConfigError(
                f"incremental.mode '{self.mode}' desconhecido (use 'data_date' ou 'ingestion_date')."
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
    incremental: IncrementalConfig = field(default_factory=IncrementalConfig)
    date_params: DateParamsConfig = field(default_factory=DateParamsConfig)
    records_path: Optional[str] = None
    static_params: dict[str, Any] = field(default_factory=dict)
    extraction_batch_size: int = DEFAULT_EXTRACTION_BATCH_SIZE
    max_pages: int = DEFAULT_MAX_PAGES


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
    columns: list[ColumnConfig] = field(default_factory=list)
    destination: DestinationConfig = field(default_factory=DestinationConfig)
    state: StateConfig = field(default_factory=StateConfig)
    transform: TransformConfig = field(default_factory=TransformConfig)
    secrets: SecretsConfig = field(default_factory=SecretsConfig)
    run_log: RunLogConfig | bool = field(default_factory=RunLogConfig)

    def __post_init__(self) -> None:
        # 0.1 compatibility: PipelineConfig(..., run_log=True/False) still works.
        if isinstance(self.run_log, bool):
            self.run_log = RunLogConfig(enabled=self.run_log)


def _run_log_from_value(value: Any) -> RunLogConfig:
    if isinstance(value, bool):
        return RunLogConfig(enabled=value)
    if value is None:
        return RunLogConfig()
    if isinstance(value, dict):
        return RunLogConfig(**value)
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
        if normalized in _SENSITIVE_OPTION_KEYS and nested not in (None, "") and not _is_secret_ref(nested):
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
        _validate_sensitive_mapping(
            connector.get("static_params") or {},
            path="connector.static_params",
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
        return {key: _resolve_secret_refs(nested, provider) for key, nested in value.items()}
    if isinstance(value, list):
        return [_resolve_secret_refs(nested, provider) for nested in value]
    if isinstance(value, tuple):
        return tuple(_resolve_secret_refs(nested, provider) for nested in value)
    return value


def pipeline_config_from_dict(data: dict[str, Any]) -> PipelineConfig:
    try:
        name = data["name"]
        connector_data = data["connector"]
        base_url = connector_data["base_url"]
    except KeyError as exc:
        raise PipelineConfigError(f"Campo obrigatorio faltando na configuracao: {exc}") from exc

    secrets = SecretsConfig(**(data.get("secrets", {}) or {}))
    _validate_no_inline_secrets(data, secrets)

    connector = ConnectorConfig(
        base_url=base_url,
        method=connector_data.get("method", "GET"),
        auth=AuthConfig(**connector_data.get("auth", {})),
        pagination=PaginationConfig(**connector_data.get("pagination", {})),
        incremental=IncrementalConfig(**connector_data.get("incremental", {})),
        date_params=DateParamsConfig(**connector_data.get("date_params", {})),
        records_path=connector_data.get("records_path"),
        static_params=connector_data.get("static_params") or {},
        extraction_batch_size=connector_data.get(
            "extraction_batch_size", DEFAULT_EXTRACTION_BATCH_SIZE
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
        name=name,
        connector=connector,
        columns=columns,
        destination=DestinationConfig(**(data.get("destination", {}) or {})),
        state=StateConfig(**state_data),
        transform=TransformConfig(**(data.get("transform", {}) or {})),
        secrets=secrets,
        run_log=_run_log_from_value(data.get("run_log", True)),
    )


def pipeline_config_to_dict(config: PipelineConfig) -> dict[str, Any]:
    run_log = config.run_log
    assert isinstance(run_log, RunLogConfig)
    return {
        "name": config.name,
        "connector": {
            "base_url": config.connector.base_url,
            "method": config.connector.method,
            "auth": asdict(config.connector.auth),
            "pagination": asdict(config.connector.pagination),
            "incremental": asdict(config.connector.incremental),
            "date_params": asdict(config.connector.date_params),
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
        raise PipelineConfigError(f"Configuracao YAML deve usar UTF-8: '{path}'.") from exc
    try:
        data = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        raise PipelineConfigError(f"Configuracao YAML invalida em '{path}'.") from exc
    if not isinstance(data, dict):
        raise PipelineConfigError(f"Configuracao YAML invalida em '{path}'.")
    return pipeline_config_from_dict(data)


def save_pipeline_config(config: PipelineConfig, path: Union[str, Path]) -> None:
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(
        yaml.safe_dump(pipeline_config_to_dict(config), sort_keys=False, allow_unicode=True),
        encoding="utf-8",
    )
    # Pipeline YAML should not contain literal secrets by default, but endpoint
    # names, paths and secret-reference names can still be sensitive metadata.
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

    ``runtime`` carries adapter-specific resources when needed. DuckDB uses an
    existing connection; Parquet and Delta do not require a runtime object.
    Secret references in static/adapter options use ``${SECRET:name}`` and are
    resolved only in memory immediately before an adapter/connector is built.
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
            destination_config.options, secret_provider
        )
        state_config = copy.deepcopy(config.state)
        state_config.options = _resolve_secret_refs(state_config.options, secret_provider)
        run_log_config = copy.deepcopy(run_log)
        run_log_config.options = _resolve_secret_refs(run_log_config.options, secret_provider)
        static_params = _resolve_secret_refs(config.connector.static_params, secret_provider)

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
            incremental_mode=config.connector.incremental.resolve_mode(),
            initial_start=config.connector.incremental.resolve_initial_start(),
            date_field=config.connector.incremental.date_field,
            pagination=config.connector.pagination.build(),
            method=config.connector.method,
            auth=config.connector.auth.build(secret_provider),
            date_params=config.connector.date_params.build(),
            static_params=static_params or None,
            records_path=config.connector.records_path,
            extraction_batch_size=config.connector.extraction_batch_size,
            max_pages=config.connector.max_pages,
        )
        schema = EndpointSchema(
            columns=[ColumnSpec(column.name, dtype=column.dtype) for column in config.columns]
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
        raise PipelineConfigError(_dependency_hint(exc, config.destination.type)) from None
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
