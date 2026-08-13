# Destinations: DuckDB, Parquet, and Delta

All three official destinations implement the same conceptual contract, but they are optimized for different environments.

## DuckDB

```bash
pip install "engineer_kit[duckdb]"
```

Best suited for local development, tests, small/medium pipelines, and the Local Lab with dbt-duckdb.

## Parquet

```bash
pip install "engineer_kit[parquet]"
```

Best suited for local or mounted filesystems, Spark staging, simple lakes without a database runtime, and interoperability across analytical engines.

## Delta

```bash
pip install "engineer_kit[delta]"
```

Best suited for Lakehouse architectures, object storage, environments already using Delta, and Delta-based state/audit tables.

## Bronze contract

Official adapters preserve the same metadata model:

```text
declared fields → string/null
_raw            → original record
_extra          → unexpected fields
_run_id         → execution identity
_window_*       → incremental window
_ingestion_key  → retry identity
```

Analytical types declared through `ColumnSpec` are used for staging/casts rather than aggressive Bronze inference.

## `append` vs `overwrite`

Use `append` for normal incremental Bronze. Official adapters safely replace a repeated ingestion of the same checkpoint transition when needed.

Use `overwrite` when the dataset is a full snapshot that should replace the target.

## Partitioning

Parquet and Delta accept partitioning options when supported by the adapter configuration. Avoid unnecessarily high-cardinality partition columns, which can create many tiny files/directories.
