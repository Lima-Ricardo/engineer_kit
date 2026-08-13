# engineer_kit

**Reliable REST API ingestion for analytics, with streaming, incremental state, and safe checkpoints.**

This documentation assumes you may be using the library for the first time. It starts with the problem and mental model, then moves into concrete configuration and operational details.

<div class="grid cards" markdown>

-   :material-rocket-launch: **I want to start now**

    ---

    Install the package and run your first extraction in a few minutes.

    [:octicons-arrow-right-24: Installation](getting-started/installation.md)

-   :material-pipe: **I want to build a pipeline**

    ---

    Follow a complete API → Bronze → checkpoint example.

    [:octicons-arrow-right-24: First pipeline](getting-started/first-pipeline.md)

-   :material-apache-spark: **I am using Fabric/Databricks**

    ---

    Use only extraction + pagination + incremental state and continue in Spark.

    [:octicons-arrow-right-24: Embedded mode](guides/embedded-mode.md)

-   :material-shield-lock: **I need production guidance**

    ---

    Understand secrets, TLS, resource bounds, logging, and runtime responsibilities.

    [:octicons-arrow-right-24: Security](reference/security.md)

</div>

## The problem the library solves

A REST integration that looks trivial often becomes complex quickly:

```text
GET /orders
   ↓
pagination
   ↓
rate limit / 429
   ↓
authentication
   ↓
incremental window
   ↓
retry
   ↓
schema changed
   ↓
how do I know what was safely persisted?
```

`engineer_kit` centralizes those concerns behind testable contracts while keeping your transformation engine, warehouse, and cloud platform decoupled.

## Main flow

```text
RestConnector
    ↓
ExtractionSession
    ↓
┌───────────────────────────────┐
│ embedded mode                 │
│ your code → Spark/Polars/...  │
└───────────────────────────────┘
              or
┌───────────────────────────────┐
│ managed mode                  │
│ Destination → State → Audit   │
└───────────────────────────────┘
```

## Why streaming-first?

Normal iteration yields at most **25,000 records per batch** by default. Processing can begin before the full API is downloaded, reducing pressure on driver memory.

```python
run = connector.extract_incremental()
for batch in run:
    process(batch)
run.commit()
```

`collect()` is intentionally explicit because it materializes the entire extraction in RAM.

## Where can I use it?

- local Python;
- DuckDB;
- Parquet;
- Delta Lake;
- Databricks;
- Microsoft Fabric;
- workloads running on AWS, GCP, or Azure;
- Airflow, Dagster, cron, or any orchestrator that can execute Python.

Cloud placement does not change the source protocol: REST remains REST.

## Next steps

1. [Install the library](getting-started/installation.md).
2. [Learn the mental model](getting-started/mental-model.md).
3. [Build the first pipeline](getting-started/first-pipeline.md).
4. Choose [managed mode](guides/managed-mode.md) or [embedded mode](guides/embedded-mode.md).
