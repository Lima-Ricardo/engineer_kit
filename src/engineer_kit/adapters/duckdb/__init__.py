"""DuckDB adapter package.

Imported only when the DuckDB optional dependency is requested.
"""

from __future__ import annotations


def __getattr__(name: str):
    if name in {"DuckDBStateStore", "IngestionStateStore"}:
        from engineer_kit.adapters.duckdb.state_store import DuckDBStateStore

        return DuckDBStateStore
    if name in {"DuckDBRunLogStore", "RunLogStore"}:
        from engineer_kit.adapters.duckdb.run_log import DuckDBRunLogStore

        return DuckDBRunLogStore
    if name in {"DuckDBLoader", "DuckDBDestination"}:
        from engineer_kit.storage.duckdb_loader import DuckDBLoader

        return DuckDBLoader
    raise AttributeError(name)


__all__ = [
    "DuckDBStateStore",
    "IngestionStateStore",
    "DuckDBRunLogStore",
    "RunLogStore",
    "DuckDBLoader",
    "DuckDBDestination",
]
