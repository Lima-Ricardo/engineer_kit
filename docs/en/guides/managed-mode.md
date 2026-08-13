# Managed mode

Managed mode is for cases where `engineer_kit` should coordinate the complete ingestion lifecycle.

```text
Connector
   ↓
Destination
   ↓
StateStore
   ↓
RunLogBackend
```

## When to use it

- simple local jobs;
- Bronze in DuckDB, Parquet, or Delta;
- declarative YAML pipelines;
- checkpointing and audit should be managed by the library.

## Build from YAML

```python
from engineer_kit import build_pipeline, load_pipeline_config

config = load_pipeline_config("pipelines/orders.yaml")
pipeline = build_pipeline(config)
result = pipeline.run()

if not result.success:
    raise RuntimeError(result.steps)
```

DuckDB requires an existing connection for programmatic use:

```python
import duckdb
from engineer_kit import build_pipeline, load_pipeline_config

config = load_pipeline_config("pipelines/orders.yaml")
conn = duckdb.connect("warehouse.duckdb")
try:
    result = build_pipeline(config, conn).run()
finally:
    conn.close()
```

## Transactional order

```text
extract
  ↓
Destination load/commit
  ↓
StateStore checkpoint
  ↓
RunLog audit (best effort)
```

An audit failure does not roll back data/state that were already confirmed. A destination failure prevents checkpoint advancement.

## Write modes

`append` is the default Bronze mode and preserves earlier windows. Repeating the same checkpoint transition is protected by ingestion identity in official adapters.

`overwrite` replaces the complete target.

A generic business-key merge/upsert is intentionally not inferred because that requires explicit domain semantics.
