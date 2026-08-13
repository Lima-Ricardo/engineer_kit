# Mental model

Before configuring a pipeline, remember six concepts.

## 1. Connector = how the source is read

`RestConnector` performs requests, authentication, pagination, and turns API pages into records. It does not need to know whether the code runs locally, in a notebook, or on a cluster.

## 2. ExtractionSession = how extraction is delivered

`extract_incremental()` returns a single-pass session:

```python
run = connector.extract_incremental()
for batch in run:
    ...
```

The default extraction batch is 25,000 records. This bounds how many records are delivered to the consumer at once.

## 3. StateStore = where the checkpoint lives

The watermark belongs to ingestion state, not to the destination.

```text
API → extraction → persistence → success → checkpoint
```

If persistence fails, the checkpoint must not advance.

## 4. Destination = where Bronze is materialized

In managed mode, a `Destination` consumes the stream and persists the data. Official implementations are DuckDB, Parquet, and Delta.

## 5. RunLogBackend = audit trail

It records execution identity, row counts, incremental windows, status, and watermarks without coupling `Pipeline` to a specific database.

## 6. Transform = after ingestion

Transformation is deliberately outside the Bronze transaction:

```text
API → Bronze → confirmed checkpoint → dbt/Spark/SQL
```

A downstream model failure should not invalidate a successfully captured Bronze ingestion.

## Managed vs embedded

### Managed

```text
Pipeline
├── Connector
├── Destination
├── StateStore
└── RunLogBackend
```

Use it when you want the library to own persistence and checkpointing.

### Embedded

```text
Connector → ExtractionSession → your code → run.commit()
```

Use it when Spark, Pandas, Polars, or another layer should remain under your project control.
