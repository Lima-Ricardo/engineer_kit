# engineer_kit

[🇧🇷 Português](README.md) · **🇺🇸 English**

> **Intent-driven REST ingestion for analytics — 95% abstraction at the surface, with a typed, streaming-first and secure runtime underneath.**

[![PyPI](https://img.shields.io/pypi/v/engineer-kit)](https://pypi.org/project/engineer-kit/)
[![Python](https://img.shields.io/badge/python-3.10%20%7C%203.11%20%7C%203.12-blue)](#installation)
[![CI](https://github.com/Lima-Ricardo/engineer_kit/actions/workflows/ci.yml/badge.svg)](https://github.com/Lima-Ricardo/engineer_kit/actions/workflows/ci.yml)
[![Docs](https://github.com/Lima-Ricardo/engineer_kit/actions/workflows/docs.yml/badge.svg)](https://lima-ricardo.github.io/engineer_kit/en/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

`engineer_kit` abstracts HTTP, authentication, pagination, incrementality, batching, profiling/Data Quality, primary-key identity, deduplication, checkpoints, Bronze persistence, destinations, and audit logging. You declare **intent**; the library resolves internal contracts when it can do so safely.

## ⚡ Happy path

```python
from engineer_kit import RestConnector

records = RestConnector(
    base_url="https://api.example.com/orders",
).collect()
```

When the source needs more context, provide only the relevant selectors:

```python
connector = RestConnector(
    base_url="https://api.example.com/orders",
    auth=token,
    pagination="cursor",
    incremental="updated_at",
    records="payload.orders",
    select=["id", "customer_id", "amount", "updated_at"],
)
```

No manual factories and no strategy-selection `if/else` for the common path. Typed objects remain available for expert control.

## 🔎 Profile before ingestion

`profile()` lets you inspect the source **before Bronze**, without destination writes and without advancing checkpoints:

```python
report = connector.profile(
    "duplicates",
    "nulls",
    "missing",
    "cardinality",
)

print(report.to_text())
```

With no selectors, profiling computes every supported metric. Presets such as `quality`, `statistics`, and `schema` are also available.

For large sources, the UI defaults to `sample`; Python can explicitly choose `scope="sample"` or `scope="full"`.

## 🔑 Primary key first, dedup second

Identity and policy are independent:

```python
connector = RestConnector(
    base_url="https://api.example.com/customers",
    primary_key="customer_id",
    dedup=False,
)
```

A PK may exist while deduplication is disabled for profiling and identity metadata. Test a candidate before enabling the policy:

```python
report = connector.profile(
    "duplicates",
    "missing",
    "nulls",
    key="customer_id",
)
```

Then, if the identity contract is appropriate:

```python
connector = RestConnector(
    base_url="https://api.example.com/customers",
    primary_key="customer_id",
    dedup=True,
)
```

Composite keys are supported:

```python
primary_key=["tenant_id", "order_id"]
```

With `dedup=True`, the first occurrence wins. When the same PK appears again, **the entire later record** is discarded. Missing, `null`, blank, or non-scalar keys fail ingestion instead of collapsing undefined identities.

## 🖥️ Local Lab

The local UI shares the same contracts as the Python API and YAML.

### Dashboard

![New Local Lab dashboard](https://raw.githubusercontent.com/Lima-Ricardo/engineer_kit/main/docs/assets/ui/dashboard.png)

### Pipeline editor — identity and deduplication are separate

![New pipeline editor with primary key and deduplication](https://raw.githubusercontent.com/Lima-Ricardo/engineer_kit/main/docs/assets/ui/pipeline-editor.png)

### Data Profile — quality and PK validation before Bronze

![New bilingual Data Profile interface](https://raw.githubusercontent.com/Lima-Ricardo/engineer_kit/main/docs/assets/ui/data-profile.png)

Start it with:

```bash
engineer_kit ui --workspace .
```

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

## 🧪 Side-effect-free probe / preview

```python
probe = connector.probe(limit=25)
```

`probe()` / `preview()` read one page for diagnosis, reuse that response for pagination detection, and do not write to a `Destination` or commit a checkpoint.

## 📄 Intent-driven pagination

```python
pagination="cursor"
pagination="page"
pagination="offset"
pagination="link_header"
pagination="next_url"
pagination=False
```

For non-standard APIs:

```python
pagination={
    "type": "cursor",
    "cursor": "meta.next_cursor",
    "param": "after",
}
```

`pagination="auto"` is conservative and reuses a response already fetched by extraction; it does not issue extra discovery requests.

## ⏱️ Incremental extraction and safe checkpoints

```python
incremental="updated_at"
```

or:

```python
incremental={
    "field": "updated_at",
    "param": "updated_from",
    "initial_start": "2026-01-01",
}
```

The checkpoint advances only after the success boundary. `state_key` separates namespaces, and official stores use compare-and-set to reject commits derived from stale state.

## 🌊 Streaming-first

Small dataset:

```python
records = connector.collect()
```

Larger volume:

```python
for batch in connector.stream():
    process(batch)
```

The default extraction batch is 25,000 records. Convenience stays in setup; the hot path remains streaming and bounded.

## 🦆 Managed ingestion

DuckDB:

```python
result = connector.to(
    "duckdb",
    "bronze.orders",
    path="analytics.duckdb",
).run()
```

Parquet and Delta:

```python
connector.to("parquet", "bronze.orders", path="./lake").run()
connector.to("delta", "bronze.orders", path="s3://bucket/lake").run()
```

Optional chained dbt:

```python
result = (
    connector
    .to("duckdb", "bronze.orders", path="analytics.duckdb")
    .dbt(select="orders")
    .run()
)
```

## 📝 YAML and CLI

The declarative contract uses `version: 1`, strict validation, duplicate-YAML-key rejection, and parity with the main Python intents.

```yaml
version: 1
name: customers
connector:
  base_url: https://api.example.com/customers
  records: data
  primary_key: customer_id
  dedup: true
```

```bash
engineer_kit run-config pipelines/customers.yaml
engineer_kit profile-config pipelines/customers.yaml --key customer_id
engineer_kit profile-config pipelines/customers.yaml --html profile.html
```

## 🔎 Transparency and capabilities

```python
print(connector.explain())
```

`explain()` reports connector resolution without another HTTP request or exposing authentication values. `capability_manifest()` exposes a serializable capability contract for CLI/UI surfaces.

## 🧱 Architecture

```text
REST API
   ↓
RestConnector
   ├── probe / preview   → read-only diagnosis
   ├── profile           → Data Quality / PK candidate
   ↓
ExtractionSession
   ├── embedded → your code / Spark / Pandas / Polars
   └── managed  → Destination → DuckDB / Parquet / Delta
                         ↓
                   safe checkpoint
```

DuckDB is the local reference adapter, not an architectural premise.

## 🛡️ Secure by default

Secure HTTPS/TLS defaults, secret redaction, response/pagination limits, cross-origin and metadata/link-local protections, controlled retries, filesystem/YAML/subprocess hardening, reserved Bronze names, explicit alias-collision failures, and CI with Ruff, Bandit, `pip-audit`, package validation, and multi-Python tests.

Read [`SECURITY.md`](SECURITY.md) before production use.

## 📚 Documentation

- English: **https://lima-ricardo.github.io/engineer_kit/en/**
- Português: **https://lima-ricardo.github.io/engineer_kit/**
- [First pipeline](https://lima-ricardo.github.io/engineer_kit/en/getting-started/first-pipeline/)
- [Python API reference](https://lima-ricardo.github.io/engineer_kit/en/reference/python-api/)
- [YAML configuration](https://lima-ricardo.github.io/engineer_kit/en/reference/configuration/)

## 🤝 Contributing and license

Read [`CONTRIBUTING.md`](CONTRIBUTING.md). Security reports should follow [`SECURITY.md`](SECURITY.md), not public issues.

MIT — see [`LICENSE`](LICENSE).
