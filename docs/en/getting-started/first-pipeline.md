# First pipeline, step by step

The primary `engineer_kit` path is **intent-driven**: describe what you know about the source and destination, and let the library resolve adapters, state, audit, pagination, and schema when that can be done safely.

## 1. Install only the destination you need

```bash
pip install "engineer_kit[parquet]"
```

## 2. Start with the minimum

For a simple API, `GET` is already the default, the connector name is derived from the URL, and the records list is detected when there is no ambiguity:

```python
from engineer_kit import RestConnector

records = RestConnector(
    base_url="https://api.example.com/orders",
    auth=token,
).collect()
```

A string passed to `auth` means Bearer auth. In production, `BearerAuth`, `SecretProvider`, and the other explicit contracts remain available when you want to control where the secret comes from.

## 3. Declare only what the API requires

Assume the API uses 1,000-record pages and accepts `updated_from`:

```text
GET /orders?page=1&per_page=1000&updated_from=2026-01-01
```

Response:

```json
{
  "results": [
    {"id": 123, "updated_at": "2026-08-12T10:00:00Z"}
  ]
}
```

You do not need to instantiate pagination or state classes:

```python
connector = RestConnector(
    base_url="https://api.example.com/orders",
    auth=token,
    pagination={"type": "page", "size": 1000},
    incremental={
        "field": "updated_at",
        "param": "updated_from",
        "initial_start": "2026-01-01",
    },
)
```

For a conventional cursor API, this is enough:

```python
pagination="cursor"
```

`Cursor`, `cursor`, and capitalization variants are normalized once before execution.

## 4. Collect, stream, or persist directly

Small dataset:

```python
records = connector.collect()
```

Larger volume with bounded batches:

```python
for batch in connector.stream():
    process(batch)
```

Managed mode, without constructing `Destination`, `StateStore`, or `RunLogBackend` manually:

```python
result = connector.to(
    "parquet",
    "bronze.orders",
    path="./lake",
).run()
```

With an official destination, the managed flow resolves its natural state and audit backends. The checkpoint is still committed only after destination persistence succeeds.

## 5. DuckDB

```python
result = RestConnector(
    base_url="https://api.example.com/orders",
    auth=token,
    pagination="cursor",
    incremental=True,
).to(
    "duckdb",
    "bronze.orders",
    path="analytics.duckdb",
).run()
```

The DuckDB connection, Bronze destination, state, and audit backend are resolved internally. Pass explicit objects only when you need to override the defaults.

## 6. Add dbt after ingestion

When a `dbt_project.yml` exists in the project or a parent directory:

```python
result = (
    connector
    .to("duckdb", "bronze.orders", path="analytics.duckdb")
    .dbt(select="orders")
    .run()
)
```

The dbt project is discovered once. `project_dir`, `profiles_dir`, and `target` remain available as selectors when the environment does not follow the standard layout.

## 7. Control the response without boilerplate

For a nested records list:

```python
records="payload.data.orders"
```

For a small field projection:

```python
select=["id", "customer_id", "amount", "updated_at"]
```

This avoids manual loops whose only purpose is dropping fields you never intended to consume.

## 8. Inspect the plan without opening the implementation

```python
print(connector.explain())
```

`explain()` reports the safe connector resolution without issuing another request and without exposing the authentication value.

## 9. Performance rule

Convenience does not run heuristics per record. Configuration is resolved before extraction; pagination and records-path detection use a response that was already fetched and cache their decision for the remaining execution.

```text
user intent
    ↓
one-time resolution
    ↓
typed runtime objects
    ↓
direct batched hot path
```

## 10. Expert mode is still available

The explicit contracts remain valid:

```python
RestConnector(
    name="orders",
    base_url=url,
    pagination=CursorPagination(
        cursor_param="after",
        cursor_field="meta.next_cursor",
    ),
    auth=BearerAuth(provider, "API_TOKEN"),
    incremental=IncrementalStrategy(...),
)
```

Use that level when the source genuinely needs special control, not as mandatory boilerplate.

## Next steps

- [Authentication](../guides/authentication.md)
- [Pagination](../guides/pagination.md)
- [Incremental state](../guides/incremental.md)
- [Managed mode](../guides/managed-mode.md)
