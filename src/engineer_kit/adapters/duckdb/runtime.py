"""Declarative builders for the DuckDB local adapter."""

from __future__ import annotations

from engineer_kit.adapters.duckdb.run_log import DuckDBRunLogStore
from engineer_kit.adapters.duckdb.state_store import DuckDBStateStore
from engineer_kit.storage.destination import WriteMode
from engineer_kit.storage.duckdb_loader import DuckDBDestination


def _connection(context):
    if context.runtime is None:
        raise ValueError(
            "O adapter DuckDB precisa de uma conexao DuckDB no runtime. "
            "Use build_pipeline(config, conn) ou a UI local."
        )
    return context.runtime


def build_destination(config, context):
    return DuckDBDestination(
        _connection(context),
        schema=getattr(config, "schema", "bronze"),
        batch_size=getattr(config, "batch_size", 5000),
        write_mode=WriteMode.parse(getattr(config, "write_mode", "append")),
    )


def build_state_store(config, context):
    return DuckDBStateStore(_connection(context))


def build_run_log(config, context):
    return DuckDBRunLogStore(_connection(context))
