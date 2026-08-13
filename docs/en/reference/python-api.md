# Python API reference: core contracts

This is a usage-oriented reference. For exact signatures in the installed version, use your IDE or Python `help()`.

## `RestConnector`

Core parameters:

```python
RestConnector(
    name,
    base_url,
    pagination,
    method,
    state_store=None,
    incremental_mode=...,
    initial_start=None,
    date_field=None,
    incremental=None,
    auth=None,
    date_params=None,
    static_params=None,
    records_path=None,
    http_client=None,
    extraction_batch_size=25_000,
    max_pages=...,
    allow_cross_origin_pagination=False,
)
```

### `extract_incremental()`

Returns an `ExtractionSession`.

### `extract()`

Kept for compatibility with the original API. New workloads should prefer `ExtractionSession`.

## `ExtractionSession`

```python
for batch in run:
    ...

run.iter_batches(size=...)
run.collect()
run.commit()
```

The session is single-pass and does not allow partial commit.

## `PaginationStrategy`

```python
initial_params() -> dict
next_params(page, previous_params) -> dict | None
```

## `StateStore`

```python
get_watermark(connector_name)
set_watermark(connector_name, watermark)
```

## `Destination`

The Bronze persistence contract. Adapters may expose additional capabilities, but `Pipeline` depends on the base contract.

## `RunLogBackend`

```python
record(RunLogEntry)
```

## `SecretProvider`

```python
get(name: str) -> str
```

## `Pipeline`

```python
result = pipeline.run()
```

`PipelineResult` contains overall success/status and step results, including run identity, rows, destination, incremental window, and watermarks where available.
