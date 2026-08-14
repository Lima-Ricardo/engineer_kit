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
    state_key=None,
    dedup=False,
    method="GET",
)
```

Friendly selectors:

- `pagination`: `"auto"`, `"cursor"`, `"page"`, `"offset"`, `"link_header"`, `"next_url"`, `False`, a dict, or `PaginationStrategy`;
- `incremental`: `False`/`None`, `True`, a date-field name, a dict, or `IncrementalStrategy`;
- `auth`: a string for Bearer auth or an explicit `AuthStrategy`;
- `records`: path to the records list, such as `"payload.items"`;
- `select`: a list/string of fields or a `{path: alias}` mapping;
- `params`: static request parameters;
- `state_key`: explicit checkpoint namespace; defaults to the connector name;
- `dedup`: defaults to `False`. With `True`, exact duplicate emitted rows are removed after `select`, using disk-backed temporary fingerprints instead of an unbounded in-memory set.

Declarative paths support objects, explicit array indexes, and quoted keys, for example `items[0].sku` and `payload["odd.key"].value`. Wildcards are intentionally unsupported so selectors cannot change row cardinality implicitly. Alias collisions fail fast and require explicit aliases.

Legacy options such as `name`, `records_path`, `static_params`, `state_store`, `date_params`, `date_field`, `incremental_mode`, and explicit strategy objects remain supported.

### `probe()` / `preview()`

```python
probe = connector.probe(limit=25)
```

Fetches **exactly one page** for diagnostics and returns a `ProbeResult` containing bounded records, the raw payload, headers, resolved records path, detected pagination strategy, HTTP status, latency, and response size when available.

`probe()` and `preview()` are read-only with respect to ingestion state: they do not write a `Destination` and never commit a checkpoint. `auto` pagination detection reuses the page already fetched and does not issue an extra discovery request.

### `profile()` / Data Quality

`profile()` is a first-class operation alongside `collect()` and `stream()`:

```python
report = connector.profile()
print(report)
```

No selectors means a **complete profile**. Request only the metrics you need to avoid unnecessary aggregators:

```python
report = connector.profile(
    "duplicates",
    "nulls",
    "missing",
)
```

Presets are also available:

```python
connector.profile("quality")
connector.profile("statistics")
connector.profile("schema")
```

Field profiling can be narrowed explicitly:

```python
report = connector.profile(
    "nulls",
    "missing",
    fields=["id", "customer.email"],
)
```

The return value is `ProfileReport v1`. The same object supports Python logic, terminal output, and standalone HTML:

```python
text = report.to_text()
html = report.to_html()
quality = report.quality
```

The profiler keeps `missing`, `null`, and empty values distinct. It can also observe JSON paths, native source types, cardinality, and duplicates. A metric that was not computed remains distinct from a computed zero.

Profiling is **aggregate-only**: source values are not stored in the report. Presence, missing/null/empty counts, and type counters use state proportional to the number of observed fields, not source row count. Cardinality is exact while small, then switches to an approximate estimator with an explicit relative-error bound. Exact duplicate detection necessarily needs state proportional to unique rows, so it uses SHA-256 fingerprints in temporary SQLite storage rather than unbounded RAM.

Python defaults to `scope="full"`:

```python
report = connector.profile(scope="sample", limit=10_000)
```

`profile()` **never writes a destination/Bronze table and never commits a checkpoint**. When `dedup=True`, profiling still observes the rows before deduplication so duplicate quality issues remain visible.

The CLI intentionally uses a safer sample default:

```bash
engineer-kit profile-config pipeline.yaml
engineer-kit profile-config pipeline.yaml --metrics duplicates,nulls,missing
engineer-kit profile-config pipeline.yaml --scope full
engineer-kit profile-config pipeline.yaml --html profile.html
```

Local Lab exposes the same `ProfileReport` through its **Data Profile** screen. The UI starts with a 10,000-row sample and requires an explicit `full` selection before scanning the entire configured source.

### `collect()`

```python
records = connector.collect()
```

Materializes the complete extraction and commits the checkpoint only after collection finishes successfully. Use it for small datasets. With `dedup=True`, exact duplicates are suppressed before records are returned.

### `stream()`

```python
for batch in connector.stream():
    ...
```

Yields bounded batches and commits only after complete consumption. `dedup=True` applies to the stream and therefore to managed ingestion that consumes the same `ExtractionSession`.

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

Returns a safe summary of connector resolution without issuing another HTTP request or exposing the authentication value. The summary includes `state_key`, `dedup`, and `select` path/alias pairs.

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

Watermark commits use compare-and-set. If another run advances the same checkpoint after a window was resolved, the stale run receives `StateConflictError` instead of silently overwriting newer state. DuckDB performs the check inside a transaction; the local JSON state backend uses an interprocess lock on POSIX.

## `ExtractionSession`

The low-level API remains available:

```python
run = connector.extract_incremental()
for batch in run:
    ...
run.commit()
```

Sessions are single-pass and reject partial checkpoint commits. Connectors created with `dedup=True` apply the same streaming deduplication before records/batches are emitted.

## `capability_manifest()`

```python
from engineer_kit import capability_manifest

manifest = capability_manifest()
```

Returns serializable metadata describing REST methods, authentication, pagination, incrementality, deduplication, profiling, registered destinations, state stores, run logs, and dbt commands. It is designed for CLI/UI capability discovery without duplicating option lists; typed core contracts remain the execution source of truth.

## Stable contracts

- `PaginationStrategy`: `initial_params()` and `next_params(...)`;
- `StateStore`: `get_watermark(...)`, `set_watermark(...)`, and `compare_and_set_watermark(...)`;
- `Destination`: Bronze persistence contract;
- `RunLogBackend`: `record(RunLogEntry)`;
- `SecretProvider`: `get(name)`;
- `ProfileReport`: versioned profiling/data-quality contract;
- `Pipeline`: `run()`.

The simple facade resolves these contracts; it does not replace them. This preserves extensibility while keeping convenience work outside the execution hot path.
