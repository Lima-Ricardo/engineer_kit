# engineer_kit

[🇧🇷 Português](README.md) · **🇺🇸 English**

> **Intent-driven REST ingestion for analytics — a small surface API over a typed, streaming-first runtime.**

[![CI](https://github.com/Lima-Ricardo/engineer_kit/actions/workflows/ci.yml/badge.svg)](https://github.com/Lima-Ricardo/engineer_kit/actions/workflows/ci.yml)
[![Docs](https://github.com/Lima-Ricardo/engineer_kit/actions/workflows/docs.yml/badge.svg)](https://lima-ricardo.github.io/engineer_kit/en/)
[![Python](https://img.shields.io/badge/python-3.10%20%7C%203.11%20%7C%203.12-blue)](#installation)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

`engineer_kit` abstracts HTTP, authentication, pagination, incrementality, batching, checkpoints, Bronze persistence, destinations, and audit logging. The caller declares **intent**; the library resolves the internal contracts when it can do so safely.

## ⚡ Happy path

```python
from engineer_kit import RestConnector

records = RestConnector(
    base_url=url,
    auth=token,
    pagination="cursor",
    incremental=True,
).collect()
```

No `if/else` to choose pagination, no manual factory, and no need to instantiate `CursorPagination`, `StateStore`, or `Destination` for the common case.

A simple public API can be as small as:

```python
records = RestConnector(
    base_url="https://api.example.com/orders",
).collect()
```

`GET` is the default, the connector name is derived from the URL, the records list is detected when unambiguous, and `pagination="auto"` is conservative.

## 🎯 95% abstraction, 5% selectors

When the source requires details, provide only those details:

```python
connector = RestConnector(
    base_url=url,
    auth=token,
    pagination={"type": "page", "size": 1000},
    incremental={
        "field": "updated_at",
        "param": "updated_from",
        "initial_start": "2026-01-01",
    },
    records="payload.orders",
    select=["id", "customer_id", "amount", "updated_at"],
)
```

The same API still accepts typed objects when advanced control is required.

## 📦 Installation

```bash
pip install engineer-kit
```

Optional extras:

```bash
pip install "engineer-kit[duckdb]"
pip install "engineer-kit[parquet]"
pip install "engineer-kit[delta]"
pip install "engineer-kit[dbt]"
pip install "engineer-kit[ui]"
pip install "engineer-kit[local]"
pip install "engineer-kit[all]"
```

DuckDB, PyArrow, Delta, dbt, and the UI remain lazy optional integrations.

## 📄 Intent-driven pagination

```python
pagination="cursor"
pagination="page"
pagination="offset"
pagination="link_header"
pagination="next_url"
pagination=False
```

Strings are case-insensitive. For non-standard APIs:

```python
pagination={
    "type": "cursor",
    "cursor": "meta.next_cursor",
    "param": "after",
}
```

`auto` reuses a response already fetched by extraction; it does not issue extra discovery requests.

## 🔐 Authentication

A string passed to `auth` means Bearer auth:

```python
auth=token
```

For production, the secret source can remain explicit:

```python
from engineer_kit import BearerAuth, EnvSecretProvider

auth = BearerAuth(EnvSecretProvider(), "API_TOKEN")
```

`FileSecretProvider`, `ApiKeyAuth`, and custom `SecretProvider` implementations remain available.

## ⏱️ Incremental extraction

```python
incremental=True
```

or, when the watermark field is known:

```python
incremental="updated_at"
```

When the source requires a specific incremental filter:

```python
incremental={
    "field": "updated_at",
    "param": "updated_from",
    "initial_start": "2026-01-01",
}
```

The checkpoint advances only after the correct success boundary. `select=` projections do not hide watermark fields from internal checkpoint tracking.

## 🌊 `collect()` and `stream()`

Small dataset:

```python
records = connector.collect()
```

Larger volume:

```python
for batch in connector.stream():
    process(batch)
```

Extraction remains streaming-first; the default extraction batch is 25,000 records.

## 🦆 DuckDB without boilerplate

```python
result = RestConnector(
    base_url=url,
    auth=token,
    pagination="cursor",
    incremental=True,
).to(
    "duckdb",
    "bronze.orders",
    path="analytics.duckdb",
).run()
```

Managed mode resolves the connection, destination, initial schema, compatible state store, and audit backend. `Destination`, `StateStore`, and `RunLogBackend` remain separate contracts internally.

## 🗂️ Parquet and Delta

```python
connector.to("parquet", "bronze.orders", path="./lake").run()
```

```python
connector.to("delta", "bronze.orders", path="s3://bucket/lake").run()
```

## 🔧 Chained dbt

```python
result = (
    connector
    .to("duckdb", "bronze.orders", path="analytics.duckdb")
    .dbt(select="orders")
    .run()
)
```

The dbt project can be discovered from the current directory and its ancestors. `project_dir`, `profiles_dir`, and `target` remain available when needed.

## 🔎 Transparency

```python
print(connector.explain())
```

`explain()` reports connector resolution without issuing another HTTP request or exposing the authentication value.

## 🧠 Performance

Convenience stays in setup, not in the hot path:

```text
simple input
    ↓
one-time resolution
    ↓
cached strategy and paths
    ↓
direct streaming + batches
```

There is no per-record inference. Managed schema inference consumes only a bounded sample and chains that sample back into the same iterator, without fetching the API again.

## 🧩 Expert mode

```python
from engineer_kit import BearerAuth, CursorPagination, RestConnector

connector = RestConnector(
    name="orders",
    base_url=url,
    pagination=CursorPagination(
        cursor_param="after",
        cursor_field="next_cursor",
    ),
    auth=BearerAuth(provider, "API_TOKEN"),
    incremental=custom_incremental_strategy,
    state_store=custom_state_store,
)
```

The ergonomic facade resolves the contracts; it does not replace them.

## 🧱 Architecture

```text
REST API
   ↓
RestConnector
   ↓
ExtractionSession
   ├── embedded → your code / Spark / Pandas / Polars
   └── managed  → Destination → DuckDB / Parquet / Delta
                         ↓
                   safe checkpoint
```

DuckDB is the local reference adapter, not an architectural premise.

## 🛡️ Secure by default

The library keeps secure HTTPS/TLS defaults, secret redaction, response and pagination limits, loop detection, cross-origin and metadata/link-local protections, safe retry policy, filesystem/YAML/subprocess hardening, and CI with Ruff, Bandit, `pip-audit`, package checks, and synthetic stress tests.

Read [`SECURITY.md`](SECURITY.md) before production use.

## 📝 YAML, CLI, and Local Lab

```bash
engineer_kit run-config pipelines/orders.yaml
engineer_kit ui --workspace .
```

The simplified Python API, declarative mode, and UI share the same internal contracts.

## 📚 Documentation

English: **https://lima-ricardo.github.io/engineer_kit/en/**

Português: **https://lima-ricardo.github.io/engineer_kit/**

Start with [First pipeline](https://lima-ricardo.github.io/engineer_kit/en/getting-started/first-pipeline/) and the [Python API reference](https://lima-ricardo.github.io/engineer_kit/en/reference/python-api/).

## 🤝 Contributing and license

Read [`CONTRIBUTING.md`](CONTRIBUTING.md). Security reports should follow [`SECURITY.md`](SECURITY.md), not public issues.

MIT — see [`LICENSE`](LICENSE).
