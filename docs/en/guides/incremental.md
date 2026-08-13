# Incremental state, watermarks, and checkpoints

Incremental ingestion is more than adding `?since=`. The critical question is **when a window is considered safely complete**.

## Main rule

```text
read watermark
    ↓
extract API
    ↓
persist/process
    ↓
success
    ↓
confirm watermark
```

Never commit state before downstream success.

## `data_date`

Use this mode when records contain a reliable timestamp/date for incremental progress.

```python
incremental_mode=IncrementalMode.DATA_DATE
```

The next watermark is derived from the greatest valid date observed in the extraction.

```yaml
incremental:
  mode: data_date
  initial_start: "2026-01-01"
  date_field: updated_at
```

## `ingestion_date`

Use this when the API is filtered by an ingestion window and you do not want the checkpoint derived from a record field.

```yaml
incremental:
  mode: ingestion_date
  initial_start: "2026-01-01"
```

## API date parameters

```yaml
date_params:
  start: updated_from
  end: updated_to
  format: "%Y-%m-%d"
```

These fields only define how the computed window is sent to the API.

## StateStore is separate from Bronze

Checkpoint state can live in the same physical engine as Bronze, but it is a separate contract:

```text
Destination ≠ StateStore
```

Examples:

```text
Bronze → Parquet
State  → local JSON
```

or:

```text
Bronze → Delta
State  → Delta metadata table
```

## Downstream failure in embedded mode

```python
run = connector.extract_incremental()

for batch in run:
    df = transform(batch)
    persist(df)  # failed here

# do not call run.commit()
```

The next execution still sees the previous checkpoint.

## Partial consumption

`ExtractionSession` rejects `commit()` after only part of the stream has been consumed. This prevents confirming a window while unread API pages still exist.

## Idempotent retry in official adapters

The library computes a deterministic ingestion identity for the checkpoint transition. If data persistence succeeds but state persistence fails, retrying the same window should not simply duplicate Bronze.

Physical behavior differs by adapter:

- DuckDB: transactional replacement of the same ingestion;
- Parquet: deterministic staging and safe file promotion;
- Delta: predicate overwrite for the same ingestion identity.

## Late-arriving data

If an API can create old records after the watermark has advanced, a strictly monotonic window may miss them. Add an overlap/lookback margin or API-specific strategy. This is a domain decision and cannot be inferred safely by a generic ingestion library.
