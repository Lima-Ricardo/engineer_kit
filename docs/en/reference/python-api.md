# Python API reference

The public API has two levels: an intent-driven facade for the common case and typed contracts for advanced control.

## `RestConnector`

Happy path:

```python
records = RestConnector(
    base_url=url,
    auth=token,
    pagination="cursor",
    incremental=True,
).collect()
```

Most-used parameters:

```python
RestConnector(
    base_url,
    auth=None,
    pagination="auto",
    incremental=None,
    records=None,
    select=None,
    params=None,
    method="GET",
)
```

Friendly selectors:

- `pagination`: `"auto"`, `"cursor"`, `"page"`, `"offset"`, `"link_header"`, `"next_url"`, `False`, a dict, or `PaginationStrategy`;
- `incremental`: `False`/`None`, `True`, a date-field name, a dict, or `IncrementalStrategy`;
- `auth`: a string for Bearer auth or an explicit `AuthStrategy`;
- `records`: dotted path to the records list, such as `"payload.items"`;
- `select`: fields to keep in the result;
- `params`: static request parameters.

Legacy options such as `name`, `records_path`, `static_params`, `state_store`, `date_params`, `date_field`, `incremental_mode`, and explicit strategy objects remain supported.

### `collect()`

```python
records = connector.collect()
```

Materializes the complete extraction and commits the checkpoint only after collection finishes successfully. Use it for small datasets.

### `stream()`

```python
for batch in connector.stream():
    ...
```

Yields bounded batches and commits only after complete consumption.

### `to()`

```python
result = connector.to(
    "duckdb",
    "bronze.orders",
    path="analytics.duckdb",
).run()
```

Official adapters can be selected by name: `duckdb`, `parquet`, and `delta`. The managed flow resolves initial schema, state, and audit backends once before execution.

### `dbt()` on a managed flow

```python
result = (
    connector
    .to("duckdb", "bronze.orders", path="analytics.duckdb")
    .dbt(select="orders")
    .run()
)
```

The dbt project is discovered from the current directory and its ancestors. `project_dir`, `profiles_dir`, and `target` remain available when needed.

### `explain()`

```python
plan = connector.explain()
```

Returns a safe summary of connector resolution without issuing another HTTP request or exposing the authentication value.

## Advanced pagination

```python
pagination={
    "type": "cursor",
    "cursor": "meta.next_cursor",
    "param": "after",
}
```

You can also pass `CursorPagination`, `PageNumberPagination`, `OffsetPagination`, `LinkHeaderPagination`, `NextUrlPagination`, or your own `PaginationStrategy` directly.

## Advanced incrementality

```python
incremental={
    "field": "updated_at",
    "param": "updated_from",
    "initial_start": "2026-01-01",
}
```

In managed mode, automatically selected local state may be rebound to the destination's natural metadata backend. Explicit `state_store` objects are always respected.

## `ExtractionSession`

The low-level API remains available:

```python
run = connector.extract_incremental()
for batch in run:
    ...
run.commit()
```

Sessions are single-pass and reject partial checkpoint commits.

## Stable contracts

- `PaginationStrategy`: `initial_params()` and `next_params(...)`;
- `StateStore`: `get_watermark(...)` and `set_watermark(...)`;
- `Destination`: Bronze persistence contract;
- `RunLogBackend`: `record(RunLogEntry)`;
- `SecretProvider`: `get(name)`;
- `Pipeline`: `run()`.

The simple facade resolves these contracts; it does not replace them. This preserves extensibility while keeping convenience work outside the execution hot path.
