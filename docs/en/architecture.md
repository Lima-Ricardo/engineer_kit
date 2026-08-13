# engineer_kit architecture

`engineer_kit` is a library for **reliable REST API ingestion**. It abstracts repetitive ingestion concerns without trying to replace your Lakehouse, Spark runtime, dbt project, or corporate orchestrator.

## Flow

```text
REST API
   │
   ▼
RestConnector
   │
   ├────────────► StateStore
   │               watermark
   ▼
Destination
   │
   ▼
Bronze
   │
   ├────────────► RunLogBackend
   │
   ▼
Optional transform
   ├── dbt (local lab)
   ├── Spark
   └── SQL / platform
```

`Pipeline` coordinates contracts only. The core does not need to import DuckDB, PyArrow, Delta Lake, or dbt unless those adapters are selected.

## Core contracts

### `RestConnector`

Turns a REST API into an iterator of records and coordinates HTTP, authentication, pagination, incremental windows, and retry policy.

### `StateStore`

Persists incremental checkpoint state:

```python
get_watermark(connector_name) -> Watermark | None
set_watermark(connector_name, watermark) -> None
```

Official implementations are `DuckDBStateStore`, `JsonFileStateStore`, and `DeltaStateStore`. Bronze is **not scanned** to infer the watermark.

### `Destination`

Materializes Bronze:

```python
load(connector_name, endpoint, schema, records) -> LoadResult
```

`load()` stays compatible for third-party adapters. Official adapters also implement contextual loading so the ingestion identity can support idempotent retry.

Official destinations are `DuckDBDestination` (with `DuckDBLoader` compatibility alias), `ParquetDestination`, and `DeltaDestination`.

### `RunLogBackend`

Receives `RunLogEntry` after load attempts. Audit persistence is independent from destination and state. Official backends are DuckDB, JSON Lines, and Delta implementations. Audit-only failure is best effort and does not invalidate data/state already committed.

### `Pipeline`

The orchestrator-facing sequence is:

```text
extract
  ↓
destination transaction
  ↓
checkpoint
  ↓
audit
```

`PipelineResult` exposes run identity, timing, row counts, and step results. `StepResult` carries destination, window, watermarks, schema-drift fields, and `ingestion_key` where available.

## Physical Bronze contract

The goal of Bronze is **capture before interpretation**. Declared API fields are physically stored as string/null by official adapters. `ColumnSpec.dtype` is a logical analytical type used by staging/transformation.

Known logical types:

```text
string
integer
bigint
float
decimal
boolean
date
timestamp
json
```

Legacy SQL type expressions such as `DECIMAL(18, 2)` remain accepted for compatibility.

Internal metadata:

```text
_source
_endpoint
_ingested_at
_run_id
_ingestion_key
_window_start
_window_end
_raw
_extra
```

`_raw` preserves the source record; `_extra` preserves fields that were not declared in the schema.

## Schema drift

```text
API:    A, B, C
Schema: A, B

A → normal column
B → normal column
C → _extra + warning
```

Ingestion continues. API fields do not trigger automatic source-schema mutation.

## Retry and idempotency

Destination and StateStore may live in different systems, so there is no universal distributed transaction. The order intentionally remains:

```text
1. write Bronze
2. commit Destination
3. advance watermark
```

A destination commit followed by a state failure creates a retry window. Official adapters use a deterministic `ingestion_key` derived from connector + incremental window + previous checkpoint. Retrying the same transition therefore reuses the same identity; a later successful checkpoint changes the previous state and produces a new identity.

Physical implementation:

- DuckDB: delete/replace the same `_ingestion_key` within a transaction;
- Parquet: deterministic final file with staged promotion;
- Delta: predicate overwrite for the same `_ingestion_key` within a Delta transaction.

Third-party adapters implementing only `load()` remain compatible with at-least-once semantics. Implement contextual loading to provide equivalent idempotency.

## Write modes

`append` is the default incremental Bronze mode. `overwrite` replaces the target using each adapter's transactional/promotional guarantees. A generic `MERGE` is intentionally not provided because business keys and conflict semantics belong to the domain.

## Adapter registry

Declarative configuration resolves adapters lazily, so optional backend dependencies do not leak into the core installation. Third-party packages can register destination, state, and audit builders. `auto` only resolves known natural pairings (DuckDB→DuckDB, Parquet→file metadata, Delta→Delta) and never silently chooses local metadata for an unknown custom backend.

## Transformation

Transformation is not part of the ingestion transaction. In the Local Lab, dbt runs only after Bronze + watermark succeed. On platforms, the expected boundary is:

```text
engineer_kit → Bronze Delta/Parquet → platform Spark/dbt/SQL
```

## Local UI

The UI is a local development/training lab. It is not a distributed scheduler, enterprise catalog, or replacement for Databricks/Fabric.

## Non-goals

The project does not aim to implement a distributed DAG scheduler, worker cluster, Spark engine, enterprise catalog, proprietary warehouse/Lakehouse, automatic business-rule inference, or a broad database-connector catalog.

The focus remains: **make REST API ingestion predictable, incremental, auditable, and portable**.
