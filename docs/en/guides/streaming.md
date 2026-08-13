# Streaming and batching

`engineer_kit` is **streaming-first**: normal extraction yields bounded batches instead of materializing the complete API in memory.

```python
run = connector.extract_incremental()
for batch in run:
    process(batch)
run.commit()
```

## Default: 25,000 records

```python
DEFAULT_EXTRACTION_BATCH_SIZE = 25_000
```

This is a balanced default, not a hard limit:

```python
run = connector.extract_incremental(batch_size=10_000)
```

## Why 25,000?

It is large enough to avoid excessive overhead in common workloads and conservative enough to reduce the risk of a very large Python list on the driver. With `page_size=1000`, roughly 25 API pages can fill one extraction batch.

## Three independent layers

```text
API pagination size
        ↓
Extraction batch size
        ↓
Destination write batch size
```

Example:

```text
page_size                 1,000
extraction_batch_size    25,000
DuckDB write batch        5,000
```

Twenty-five pages can produce one extraction batch, while the destination can split that batch into five physical writes.

## `collect()`

```python
records = run.collect()
```

Use this only when you deliberately accept full in-memory materialization. It is useful for demos, tests, and small APIs.

## Pure Python

```python
for batch in run:
    for row in batch:
        process(row)
```

## Pandas

```python
import pandas as pd
for batch in run:
    df = pd.DataFrame(batch)
    process(df)
```

## Polars

```python
import polars as pl
for batch in run:
    df = pl.DataFrame(batch)
    process(df)
```

## Spark

```python
for batch in run:
    df = spark.createDataFrame(batch)
    transform_and_write(df)
```

For very large ingestions, avoid creating thousands of tiny DataFrames. Consider staging to Parquet/Delta and let Spark read files natively.

## Memory and cluster cost

Batching can reduce driver RAM pressure, OOM risk, time spent waiting for full materialization, and the amount of work repeated after an early failure. Batches that are too small can instead increase processing calls, commits, scheduler overhead, and small-file problems.

## Rate limiting is not batching

`429`, `Retry-After`, and backoff belong to the HTTP layer. Do not lower `extraction_batch_size` as a substitute for the API's requests/minute policy.
