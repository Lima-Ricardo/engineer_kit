"""Configuracao declarativa de pipeline (YAML) -> Pipeline de verdade.

Cobre o caso comum (RestConnector + DuckDBLoader) -- o mesmo que ja e
possivel montar em Python, so que descrito em arquivo em vez de codigo.
Serve pra interface web salvar/carregar pipelines sem gerar codigo
Python. Conector customizado (subclasse de APIConnector) continua
sendo Python puro -- este modulo nao tenta cobrir esse caso.
"""

from __future__ import annotations

import logging
from dataclasses import asdict, dataclass, field
from datetime import date
from pathlib import Path
from typing import Any, Optional, Union

import duckdb
import yaml

from engineer_kit.connectors.incremental import IncrementalMode
from engineer_kit.connectors.pagination import STANDARD_PAGINATION_TYPES, PaginationStrategy
from engineer_kit.connectors.rest import DateParams, RestConnector
from engineer_kit.http.auth import ApiKeyAuth, AuthStrategy, BearerAuth, NoAuth
from engineer_kit.orchestration.pipeline import Pipeline
from engineer_kit.security.secrets import EnvSecretProvider, FileSecretProvider, SecretProvider
from engineer_kit.storage.duckdb_loader import DuckDBLoader
from engineer_kit.storage.schema import ColumnSpec, EndpointSchema
from engineer_kit.storage.state_store import IngestionStateStore

logger = logging.getLogger("engineer_kit.config")


class PipelineConfigError(ValueError):
    """Levantado quando uma configuracao de pipeline esta invalida ou incompleta."""


@dataclass
class SecretsConfig:
    type: str = "env"  # "env" ou "file"
    path: Optional[str] = None  # obrigatorio se type == "file"

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
    type: str = "none"  # none | bearer | api_key
    secret_key: Optional[str] = None
    param_name: str = "api_key"
    location: str = "query"  # query | header

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
            return ApiKeyAuth(secret_provider, self.secret_key, param_name=self.param_name, location=self.location)
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
    mode: str = "data_date"  # data_date | ingestion_date
    initial_start: Optional[str] = None  # "YYYY-MM-DD"
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
        return date.fromisoformat(self.initial_start)


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


@dataclass
class ColumnConfig:
    name: str
    dtype: str = "VARCHAR"


@dataclass
class DestinationConfig:
    type: str = "duckdb"
    path: str = "warehouse.duckdb"
    schema: str = "bronze"
    batch_size: int = 1000


@dataclass
class PipelineConfig:
    name: str
    connector: ConnectorConfig
    columns: list[ColumnConfig] = field(default_factory=list)
    destination: DestinationConfig = field(default_factory=DestinationConfig)
    secrets: SecretsConfig = field(default_factory=SecretsConfig)
    run_log: bool = True


def pipeline_config_from_dict(data: dict[str, Any]) -> PipelineConfig:
    try:
        name = data["name"]
        connector_data = data["connector"]
        base_url = connector_data["base_url"]
    except KeyError as exc:
        raise PipelineConfigError(f"Campo obrigatorio faltando na configuracao: {exc}") from exc

    connector = ConnectorConfig(
        base_url=base_url,
        method=connector_data.get("method", "GET"),
        auth=AuthConfig(**connector_data.get("auth", {})),
        pagination=PaginationConfig(**connector_data.get("pagination", {})),
        incremental=IncrementalConfig(**connector_data.get("incremental", {})),
        date_params=DateParamsConfig(**connector_data.get("date_params", {})),
        records_path=connector_data.get("records_path"),
        static_params=connector_data.get("static_params") or {},
    )
    columns = [ColumnConfig(name=c["name"], dtype=c.get("dtype", "VARCHAR")) for c in data.get("columns", [])]
    return PipelineConfig(
        name=name,
        connector=connector,
        columns=columns,
        destination=DestinationConfig(**data.get("destination", {})),
        secrets=SecretsConfig(**data.get("secrets", {})),
        run_log=data.get("run_log", True),
    )


def pipeline_config_to_dict(config: PipelineConfig) -> dict[str, Any]:
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
        },
        "columns": [asdict(c) for c in config.columns],
        "destination": asdict(config.destination),
        "secrets": asdict(config.secrets),
        "run_log": config.run_log,
    }


def load_pipeline_config(path: Union[str, Path]) -> PipelineConfig:
    data = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    return pipeline_config_from_dict(data)


def save_pipeline_config(config: PipelineConfig, path: Union[str, Path]) -> None:
    data = pipeline_config_to_dict(config)
    Path(path).write_text(yaml.dump(data, sort_keys=False, allow_unicode=True), encoding="utf-8")


def list_pipeline_configs(directory: Union[str, Path]) -> list[tuple[Path, PipelineConfig]]:
    """Le todo *.yaml de `directory`. Arquivo invalido e pulado (com
    aviso no log), nao derruba a listagem inteira."""
    directory = Path(directory)
    if not directory.exists():
        return []
    results = []
    for path in sorted(directory.glob("*.yaml")):
        try:
            results.append((path, load_pipeline_config(path)))
        except Exception as exc:
            logger.warning("Configuracao invalida em '%s', ignorando: %s", path, exc)
    return results


def build_pipeline(config: PipelineConfig, conn: duckdb.DuckDBPyConnection) -> Pipeline:
    """Constroi um Pipeline de verdade a partir da config. Qualquer erro
    de validacao (do config em si, ou vindo das classes que ele monta —
    method invalido, date_field faltando etc.) sobe sempre como
    PipelineConfigError, pra quem chama (ex: a UI) so precisar tratar
    um tipo de excecao."""
    if config.destination.type != "duckdb":
        raise PipelineConfigError(
            f"destination.type '{config.destination.type}' nao suportado (so 'duckdb' por enquanto)."
        )

    try:
        secret_provider = config.secrets.build()
        connector = RestConnector(
            name=config.name,
            base_url=config.connector.base_url,
            state_store=IngestionStateStore(conn),
            incremental_mode=config.connector.incremental.resolve_mode(),
            initial_start=config.connector.incremental.resolve_initial_start(),
            date_field=config.connector.incremental.date_field,
            pagination=config.connector.pagination.build(),
            method=config.connector.method,
            auth=config.connector.auth.build(secret_provider),
            date_params=config.connector.date_params.build(),
            static_params=config.connector.static_params or None,
            records_path=config.connector.records_path,
        )
        schema = EndpointSchema(columns=[ColumnSpec(c.name, dtype=c.dtype) for c in config.columns])
        destination = DuckDBLoader(
            conn, schema=config.destination.schema, batch_size=config.destination.batch_size
        )
        return Pipeline(connector=connector, schema=schema, destination=destination, run_log=config.run_log)
    except PipelineConfigError:
        raise
    except ValueError as exc:
        raise PipelineConfigError(str(exc)) from exc
