"""engineer_kit: reliable API ingestion for analytical destinations.

The top-level package exposes the backend-agnostic core eagerly and loads
optional integrations only when requested.
"""

from __future__ import annotations

from engineer_kit.adapters.registry import (
    AdapterContext,
    AdapterNotFoundError,
    available_adapters,
    register_destination,
    register_run_log,
    register_state_store,
)
from engineer_kit.connectors.api_connector import (
    DEFAULT_MAX_PAGES,
    APIConnector,
    CrossOriginPaginationError,
    InvalidHttpMethodError,
    MissingDateFieldError,
    PaginationLimitError,
    PaginationLoopError,
    VALID_HTTP_METHODS,
)
from engineer_kit.connectors.base import Connector
from engineer_kit.connectors.date_field import DateFieldSpec, extract_date_value
from engineer_kit.connectors.extraction import (
    DEFAULT_EXTRACTION_BATCH_SIZE,
    ExtractionSession,
    InvalidExtractionBatchSizeError,
    validate_extraction_batch_size,
)
from engineer_kit.connectors.incremental import IncrementalMode, IncrementalStrategy, IncrementalWindow
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
from engineer_kit.http.auth import ApiKeyAuth, AuthStrategy, BearerAuth, InvalidAuthValueError, NoAuth
from engineer_kit.http.client import (
    DEFAULT_MAX_REDIRECTS,
    DEFAULT_MAX_RESPONSE_BYTES,
    HttpClient,
    HttpRequestError,
    InsecureUrlError,
    ResponseTooLargeError,
    UnsafeRedirectError,
    UnsafeUrlError,
)
from engineer_kit.orchestration.pipeline import Pipeline, PipelineResult, PipelineSource, StepResult
from engineer_kit.orchestration.scheduler import Scheduler
from engineer_kit.orchestration.trigger import CronTrigger, IntervalTrigger, Trigger
from engineer_kit.security.redaction import redact_text
from engineer_kit.security.secrets import (
    EnvSecretProvider,
    FileSecretProvider,
    InvalidSecretKeyError,
    SecretNotFoundError,
    SecretProvider,
    SecretTooLargeError,
    StaticSecretProvider,
)
from engineer_kit.storage.destination import Destination, LoadContext, LoadResult, WriteMode
from engineer_kit.storage.flatten import flatten_record
from engineer_kit.storage.identifiers import InvalidIdentifierError
from engineer_kit.storage.run_log import RunLogBackend, RunLogEntry
from engineer_kit.storage.schema import ColumnSpec, EndpointSchema
from engineer_kit.storage.state_store import StateStore, Watermark
from engineer_kit.storage.types import LogicalType, render_sql_type, resolve_logical_type
from engineer_kit.terminal_log import visual_logger
from engineer_kit.transform.dbt_easy import Dbt, discover_dbt_project
from engineer_kit.transform.dbt_runner import DbtResult, DbtRunner
from engineer_kit.transform.scaffold import generate_sources_yml, generate_staging_model, write_staging_scaffold

__version__ = "0.2.0"

_CONFIG_EXPORTS = {
    "AuthConfig", "ColumnConfig", "ConnectorConfig", "DateParamsConfig",
    "DestinationConfig", "IncrementalConfig", "PaginationConfig", "PipelineConfig",
    "PipelineConfigError", "RunLogConfig", "SecretsConfig", "StateConfig",
    "TransformConfig", "build_pipeline", "list_pipeline_configs",
    "load_pipeline_config", "save_pipeline_config",
}
_DUCKDB_EXPORTS = {
    "DuckDBLoader", "DuckDBDestination", "DEFAULT_BATCH_SIZE", "MIN_BATCH_SIZE",
    "MAX_BATCH_SIZE", "InvalidBatchSizeError", "DuckDBStateStore",
    "IngestionStateStore", "DuckDBRunLogStore", "RunLogStore",
}
_PARQUET_EXPORTS = {"ParquetDestination"}
_DELTA_EXPORTS = {"DeltaDestination", "DeltaStateStore", "DeltaRunLogStore"}
_FILE_EXPORTS = {"JsonFileStateStore", "JsonLinesRunLogStore"}


def __getattr__(name: str):
    if name in _CONFIG_EXPORTS:
        from engineer_kit.config import pipeline_config
        return getattr(pipeline_config, name)
    if name in _FILE_EXPORTS:
        from engineer_kit.adapters import files
        return getattr(files, name)
    if name in _PARQUET_EXPORTS:
        try:
            from engineer_kit.adapters.parquet import ParquetDestination
        except ModuleNotFoundError as exc:
            if exc.name == "pyarrow":
                raise ModuleNotFoundError(
                    'Parquet support is optional. Install `pip install "engineer_kit[parquet]"`.'
                ) from None
            raise
        return ParquetDestination
    if name in _DELTA_EXPORTS:
        try:
            from engineer_kit.adapters import delta
        except ModuleNotFoundError as exc:
            if exc.name in {"deltalake", "pyarrow"}:
                raise ModuleNotFoundError(
                    'Delta support is optional. Install `pip install "engineer_kit[delta]"`.'
                ) from None
            raise
        return getattr(delta, name)
    if name in _DUCKDB_EXPORTS:
        try:
            if name in {
                "DuckDBLoader", "DuckDBDestination", "DEFAULT_BATCH_SIZE",
                "MIN_BATCH_SIZE", "MAX_BATCH_SIZE", "InvalidBatchSizeError",
            }:
                from engineer_kit.storage import duckdb_loader
                if name == "DuckDBDestination":
                    return duckdb_loader.DuckDBDestination
                return getattr(duckdb_loader, name)
            if name in {"DuckDBStateStore", "IngestionStateStore"}:
                from engineer_kit.adapters.duckdb.state_store import DuckDBStateStore
                return DuckDBStateStore
            from engineer_kit.adapters.duckdb.run_log import DuckDBRunLogStore
            return DuckDBRunLogStore
        except ModuleNotFoundError as exc:
            if exc.name == "duckdb":
                raise ModuleNotFoundError(
                    'DuckDB support is optional. Install `pip install "engineer_kit[duckdb]"` '
                    'or `pip install "engineer_kit[local]"`.'
                ) from None
            raise
    raise AttributeError(name)


__all__ = [
    "__version__", "Connector", "APIConnector", "RestConnector", "ExtractionSession",
    "DEFAULT_EXTRACTION_BATCH_SIZE", "InvalidExtractionBatchSizeError", "validate_extraction_batch_size",
    "DateParams", "InvalidHttpMethodError", "MissingDateFieldError", "PaginationLimitError",
    "PaginationLoopError", "CrossOriginPaginationError", "DEFAULT_MAX_PAGES", "VALID_HTTP_METHODS",
    "DateFieldSpec", "extract_date_value", "stringify", "PipelineConfig", "ConnectorConfig",
    "ColumnConfig", "DestinationConfig", "StateConfig", "RunLogConfig", "TransformConfig",
    "SecretsConfig", "AuthConfig", "PaginationConfig", "IncrementalConfig", "DateParamsConfig",
    "PipelineConfigError", "load_pipeline_config", "save_pipeline_config", "list_pipeline_configs",
    "build_pipeline", "AdapterContext", "AdapterNotFoundError", "register_destination",
    "register_state_store", "register_run_log", "available_adapters", "PaginationStrategy",
    "ParsedPage", "NoPagination", "PageNumberPagination", "OffsetPagination", "CursorPagination",
    "LinkHeaderPagination", "NextUrlPagination", "STANDARD_PAGINATION_TYPES", "NEXT_URL_KEY",
    "IncrementalMode", "IncrementalStrategy", "IncrementalWindow", "HttpClient", "HttpRequestError",
    "InsecureUrlError", "UnsafeUrlError", "UnsafeRedirectError", "ResponseTooLargeError",
    "DEFAULT_MAX_RESPONSE_BYTES", "DEFAULT_MAX_REDIRECTS", "AuthStrategy", "NoAuth", "BearerAuth",
    "ApiKeyAuth", "InvalidAuthValueError", "SecretProvider", "EnvSecretProvider", "StaticSecretProvider",
    "FileSecretProvider", "SecretNotFoundError", "InvalidSecretKeyError", "SecretTooLargeError",
    "redact_text", "EndpointSchema", "ColumnSpec", "LogicalType", "render_sql_type",
    "resolve_logical_type", "Destination", "LoadContext", "LoadResult", "WriteMode", "DuckDBLoader",
    "DuckDBDestination", "ParquetDestination", "DeltaDestination", "DEFAULT_BATCH_SIZE", "MIN_BATCH_SIZE",
    "MAX_BATCH_SIZE", "InvalidBatchSizeError", "StateStore", "DuckDBStateStore", "IngestionStateStore",
    "DeltaStateStore", "JsonFileStateStore", "Watermark", "RunLogBackend", "DuckDBRunLogStore",
    "RunLogStore", "DeltaRunLogStore", "JsonLinesRunLogStore", "RunLogEntry", "flatten_record",
    "InvalidIdentifierError", "visual_logger", "Dbt", "discover_dbt_project", "DbtRunner", "DbtResult",
    "write_staging_scaffold", "generate_sources_yml", "generate_staging_model", "Pipeline",
    "PipelineSource", "PipelineResult", "StepResult", "Scheduler", "Trigger", "CronTrigger",
    "IntervalTrigger",
]
