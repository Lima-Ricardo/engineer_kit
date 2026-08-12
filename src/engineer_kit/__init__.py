"""engineer_kit: reliable API ingestion for analytical destinations.

The top-level package exposes the backend-agnostic core eagerly and loads
optional/local integrations only when the user asks for them. This keeps the
simple ``from engineer_kit import RestConnector`` ergonomics without forcing
DuckDB, Arrow, Delta or dbt into every installation.
"""

from __future__ import annotations

from engineer_kit.connectors.api_connector import (
    APIConnector,
    InvalidHttpMethodError,
    MissingDateFieldError,
    VALID_HTTP_METHODS,
)
from engineer_kit.connectors.date_field import DateFieldSpec, extract_date_value
from engineer_kit.connectors.incremental import (
    IncrementalMode,
    IncrementalStrategy,
    IncrementalWindow,
)
from engineer_kit.connectors.normalize import stringify
from engineer_kit.connectors.pagination import (
    NEXT_URL_KEY,
    STANDARD_PAGINATION_TYPES,
    CursorPagination,
    LinkHeaderPagination,
    NextUrlPagination,
    NoPagination,
    OffsetPagination,
    PageNumberPagination,
    PaginationStrategy,
    ParsedPage,
)
from engineer_kit.connectors.rest import DateParams, RestConnector
from engineer_kit.http.auth import ApiKeyAuth, AuthStrategy, BearerAuth, NoAuth
from engineer_kit.http.client import HttpClient, HttpRequestError, InsecureUrlError
from engineer_kit.orchestration.pipeline import (
    Pipeline,
    PipelineResult,
    PipelineSource,
    StepResult,
)
from engineer_kit.orchestration.scheduler import Scheduler
from engineer_kit.orchestration.trigger import CronTrigger, IntervalTrigger, Trigger
from engineer_kit.security.secrets import (
    EnvSecretProvider,
    FileSecretProvider,
    SecretNotFoundError,
    SecretProvider,
    StaticSecretProvider,
)
from engineer_kit.storage.destination import Destination, LoadResult
from engineer_kit.storage.flatten import flatten_record
from engineer_kit.storage.identifiers import InvalidIdentifierError
from engineer_kit.storage.run_log import RunLogBackend, RunLogEntry
from engineer_kit.storage.schema import ColumnSpec, EndpointSchema
from engineer_kit.storage.state_store import StateStore, Watermark
from engineer_kit.terminal_log import visual_logger
from engineer_kit.transform.dbt_runner import DbtResult, DbtRunner
from engineer_kit.transform.scaffold import (
    generate_sources_yml,
    generate_staging_model,
    write_staging_scaffold,
)

__version__ = "0.1.0"

_CONFIG_EXPORTS = {
    "AuthConfig",
    "ColumnConfig",
    "ConnectorConfig",
    "DateParamsConfig",
    "DestinationConfig",
    "IncrementalConfig",
    "PaginationConfig",
    "PipelineConfig",
    "PipelineConfigError",
    "SecretsConfig",
    "TransformConfig",
    "build_pipeline",
    "list_pipeline_configs",
    "load_pipeline_config",
    "save_pipeline_config",
}

_DUCKDB_EXPORTS = {
    "DuckDBLoader",
    "DuckDBDestination",
    "DEFAULT_BATCH_SIZE",
    "MIN_BATCH_SIZE",
    "MAX_BATCH_SIZE",
    "InvalidBatchSizeError",
    "DuckDBStateStore",
    "IngestionStateStore",
    "DuckDBRunLogStore",
    "RunLogStore",
}

_PARQUET_EXPORTS = {"ParquetDestination"}
_DELTA_EXPORTS = {"DeltaDestination", "DeltaStateStore", "DeltaRunLogStore"}


def __getattr__(name: str):
    if name in _CONFIG_EXPORTS:
        from engineer_kit.config import pipeline_config

        return getattr(pipeline_config, name)

    if name in _PARQUET_EXPORTS:
        try:
            from engineer_kit.adapters.parquet import ParquetDestination
        except ModuleNotFoundError as exc:
            if exc.name == "pyarrow":
                raise ModuleNotFoundError(
                    "Parquet support is optional. Install it with "
                    "`pip install \"engineer_kit[parquet]\"`."
                ) from None
            raise
        return ParquetDestination

    if name in _DELTA_EXPORTS:
        try:
            from engineer_kit.adapters import delta
        except ModuleNotFoundError as exc:
            if exc.name in {"deltalake", "pyarrow"}:
                raise ModuleNotFoundError(
                    "Delta support is optional. Install it with "
                    "`pip install \"engineer_kit[delta]\"`."
                ) from None
            raise
        return getattr(delta, name)

    if name in _DUCKDB_EXPORTS:
        try:
            if name in {
                "DuckDBLoader",
                "DuckDBDestination",
                "DEFAULT_BATCH_SIZE",
                "MIN_BATCH_SIZE",
                "MAX_BATCH_SIZE",
                "InvalidBatchSizeError",
            }:
                from engineer_kit.storage import duckdb_loader

                if name == "DuckDBDestination":
                    return duckdb_loader.DuckDBLoader
                return getattr(duckdb_loader, name)

            if name in {"DuckDBStateStore", "IngestionStateStore"}:
                from engineer_kit.adapters.duckdb.state_store import DuckDBStateStore

                return DuckDBStateStore

            from engineer_kit.adapters.duckdb.run_log import DuckDBRunLogStore

            return DuckDBRunLogStore
        except ModuleNotFoundError as exc:
            if exc.name == "duckdb":
                raise ModuleNotFoundError(
                    "DuckDB support is optional. Install it with "
                    "`pip install \"engineer_kit[duckdb]\"` or use "
                    "`engineer_kit[local]` for DuckDB + dbt."
                ) from None
            raise

    raise AttributeError(name)


__all__ = [
    "__version__",
    "APIConnector",
    "RestConnector",
    "DateParams",
    "InvalidHttpMethodError",
    "MissingDateFieldError",
    "VALID_HTTP_METHODS",
    "DateFieldSpec",
    "extract_date_value",
    "stringify",
    "PipelineConfig",
    "ConnectorConfig",
    "ColumnConfig",
    "DestinationConfig",
    "TransformConfig",
    "SecretsConfig",
    "AuthConfig",
    "PaginationConfig",
    "IncrementalConfig",
    "DateParamsConfig",
    "PipelineConfigError",
    "load_pipeline_config",
    "save_pipeline_config",
    "list_pipeline_configs",
    "build_pipeline",
    "PaginationStrategy",
    "ParsedPage",
    "NoPagination",
    "PageNumberPagination",
    "OffsetPagination",
    "CursorPagination",
    "LinkHeaderPagination",
    "NextUrlPagination",
    "STANDARD_PAGINATION_TYPES",
    "NEXT_URL_KEY",
    "IncrementalMode",
    "IncrementalStrategy",
    "IncrementalWindow",
    "HttpClient",
    "HttpRequestError",
    "InsecureUrlError",
    "AuthStrategy",
    "NoAuth",
    "BearerAuth",
    "ApiKeyAuth",
    "SecretProvider",
    "EnvSecretProvider",
    "StaticSecretProvider",
    "FileSecretProvider",
    "SecretNotFoundError",
    "EndpointSchema",
    "ColumnSpec",
    "Destination",
    "LoadResult",
    "DuckDBLoader",
    "DuckDBDestination",
    "ParquetDestination",
    "DeltaDestination",
    "DEFAULT_BATCH_SIZE",
    "MIN_BATCH_SIZE",
    "MAX_BATCH_SIZE",
    "InvalidBatchSizeError",
    "StateStore",
    "DuckDBStateStore",
    "IngestionStateStore",
    "DeltaStateStore",
    "Watermark",
    "RunLogBackend",
    "DuckDBRunLogStore",
    "RunLogStore",
    "DeltaRunLogStore",
    "RunLogEntry",
    "flatten_record",
    "InvalidIdentifierError",
    "visual_logger",
    "DbtRunner",
    "DbtResult",
    "write_staging_scaffold",
    "generate_sources_yml",
    "generate_staging_model",
    "Pipeline",
    "PipelineSource",
    "PipelineResult",
    "StepResult",
    "Scheduler",
    "Trigger",
    "CronTrigger",
    "IntervalTrigger",
]
