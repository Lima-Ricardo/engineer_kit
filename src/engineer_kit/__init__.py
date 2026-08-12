"""engineer_kit: ingestao de APIs para destinos analiticos.

Import direto das classes principais, sem precisar conhecer a
estrutura interna de modulos.
"""

from __future__ import annotations

from engineer_kit.connectors.api_connector import (
    APIConnector,
    InvalidHttpMethodError,
    MissingDateFieldError,
    VALID_HTTP_METHODS,
)
from engineer_kit.connectors.date_field import DateFieldSpec, extract_date_value
from engineer_kit.config.pipeline_config import (
    AuthConfig,
    ColumnConfig,
    ConnectorConfig,
    DateParamsConfig,
    DestinationConfig,
    IncrementalConfig,
    PaginationConfig,
    PipelineConfig,
    PipelineConfigError,
    SecretsConfig,
    build_pipeline,
    list_pipeline_configs,
    load_pipeline_config,
    save_pipeline_config,
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
from engineer_kit.http.auth import ApiKeyAuth, AuthStrategy, BearerAuth, NoAuth
from engineer_kit.http.client import HttpClient, HttpRequestError, InsecureUrlError
from engineer_kit.orchestration.pipeline import Pipeline, PipelineResult, PipelineSource, StepResult
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
from engineer_kit.storage.duckdb_loader import (
    DEFAULT_BATCH_SIZE,
    MAX_BATCH_SIZE,
    MIN_BATCH_SIZE,
    DuckDBLoader,
    InvalidBatchSizeError,
)
from engineer_kit.storage.flatten import flatten_record
from engineer_kit.storage.identifiers import InvalidIdentifierError
from engineer_kit.storage.run_log import (
    DuckDBRunLogStore,
    RunLogBackend,
    RunLogEntry,
    RunLogStore,
)
from engineer_kit.storage.schema import ColumnSpec, EndpointSchema
from engineer_kit.storage.state_store import DuckDBStateStore, IngestionStateStore, StateStore, Watermark
from engineer_kit.terminal_log import visual_logger
from engineer_kit.transform.dbt_runner import DbtResult, DbtRunner
from engineer_kit.transform.scaffold import generate_sources_yml, generate_staging_model, write_staging_scaffold

__version__ = "0.1.0"

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
    "DEFAULT_BATCH_SIZE",
    "MIN_BATCH_SIZE",
    "MAX_BATCH_SIZE",
    "InvalidBatchSizeError",
    "StateStore",
    "DuckDBStateStore",
    "IngestionStateStore",
    "Watermark",
    "RunLogBackend",
    "DuckDBRunLogStore",
    "RunLogStore",
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
