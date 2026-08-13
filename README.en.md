# engineer_kit

> **Reliable REST API ingestion for analytics — streaming-first, incremental, and backend-agnostic.**

[🇧🇷 Português](README.md) · **🇺🇸 English**

[![CI](https://github.com/Lima-Ricardo/engineer_kit/actions/workflows/ci.yml/badge.svg)](https://github.com/Lima-Ricardo/engineer_kit/actions/workflows/ci.yml)
[![Docs](https://github.com/Lima-Ricardo/engineer_kit/actions/workflows/docs.yml/badge.svg)](https://lima-ricardo.github.io/engineer_kit/en/)
[![Python](https://img.shields.io/badge/python-3.10%20%7C%203.11%20%7C%203.12-blue)](#installation)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Security](https://img.shields.io/badge/security-secure--by--default-success)](SECURITY.md)

`engineer_kit` removes the repetitive and failure-prone parts of API data ingestion: HTTP, authentication, pagination, retries, incremental windows, batching, checkpoints, schema drift, and audit logging. The library can **persist Bronze data for you** or simply **deliver reliable bounded batches to your own Spark/Pandas/Polars code**.

```text
REST API
   │
   ▼
RestConnector ── HTTP / auth / retry / pagination
   │
   ▼
ExtractionSession ── streaming-first, 25,000 records/batch by default
   │
   ├── embedded mode ──► your code ──► Spark / Pandas / Polars / Arrow
   │
   └── managed mode  ──► Destination ──► DuckDB / Parquet / Delta
                              │
                              ▼
                         safe checkpoint
```

## 📰 Project news

### v0.1.0 — public release candidate

The 0.1.0 release candidate consolidates the move from a DuckDB-centered pipeline into a **backend-agnostic, streaming-first ingestion toolkit**:

- platform-neutral `Connector` and `ExtractionSession`;
- extraction batches with a **25,000-record default**;
- explicit, safe checkpointing in embedded mode;
- decoupled `StateStore`, `Destination`, and `RunLogBackend` contracts;
- first-class **DuckDB, Parquet, and Delta Lake** adapters;
- direct use inside **Databricks and Microsoft Fabric**;
- secure-by-default HTTP, secrets, YAML, filesystem, logging, and local UI behavior;
- a visual local lab for learning, configuring, and inspecting pipelines;
- CI on Python 3.10/3.11/3.12, security checks, packaging validation, and synthetic stress tests.

See [`CHANGELOG.md`](CHANGELOG.md) and the [full English documentation](https://lima-ricardo.github.io/engineer_kit/en/).

## 📸 Local Lab / UI

The UI is optional and is designed as a local development and training lab for creating pipelines, understanding contracts, and following executions.

### Dashboard

![engineer_kit dashboard](docs/assets/ui/dashboard.svg)

### Visual pipeline editor

![Pipeline editor](docs/assets/ui/pipeline-editor.svg)

### Architecture and contracts

![Architecture page](docs/assets/ui/architecture.svg)

### Execution and logs

![Pipeline execution](docs/assets/ui/run.svg)

> The preview images use demonstration data, but represent the Local Lab interface and workflow.

## 🚀 Installation

The core stays lightweight and does not install DuckDB, PyArrow, Delta, dbt, or the UI unless requested.

```bash
pip install engineer_kit
```

Install only the extras you need:

```bash
pip install "engineer_kit[duckdb]"    # local DuckDB
pip install "engineer_kit[parquet]"   # Parquet / PyArrow
pip install "engineer_kit[delta]"     # Delta Lake / delta-rs
pip install "engineer_kit[platform]"  # Lakehouse profile
pip install "engineer_kit[ui]"        # local UI
pip install "engineer_kit[dbt]"       # dbt-duckdb
pip install "engineer_kit[local]"     # DuckDB + UI + dbt
pip install "engineer_kit[all]"       # every optional feature
```

The same PyPI package works with `pip`, `pipx`, `uv`, Poetry, and other installers that consume the Python Package Index.

## ⚡ First use: streaming-first extraction

The recommended path does not materialize the complete API response in memory:

```python
from datetime import date

from engineer_kit import IncrementalMode, NoPagination, RestConnector

connector = RestConnector(
    name="customers",
    base_url="https://api.example.com/customers",
    pagination=NoPagination(),
    method="GET",
    incremental_mode=IncrementalMode.INGESTION_DATE,
    initial_start=date(2026, 1, 1),
)

run = connector.extract_incremental()

for batch in run:  # default: up to 25,000 records per batch
    process(batch)

run.commit()
```

`collect()` still exists, but it is an explicit choice for small datasets:

```python
records = connector.extract_incremental().collect()
```

## 🧠 Three sizes that must not be confused

```text
API page size
     ↓
Extraction batch size       default: 25,000
     ↓
Destination write batch     adapter-specific
```

If an API returns 1,000 records per page, an extraction batch may be filled after roughly 25 pages. That is only a practical consequence; **pagination and extraction batching remain independent concerns**.

## 🔐 Authentication and secrets

### Production: environment or mounted file

```python
from engineer_kit import BearerAuth, FileSecretProvider

secrets = FileSecretProvider("/run/secrets")
auth = BearerAuth(secrets, "API_TOKEN")
```

You can also use `EnvSecretProvider`.

### Learning and disposable labs: explicit hardcoded value

```python
from engineer_kit import BearerAuth, StaticSecretProvider

secrets = StaticSecretProvider({"API_TOKEN": "training-only-token"})
auth = BearerAuth(secrets, "API_TOKEN")
```

Hardcoded secrets are intentionally supported for training and disposable tests. For real credentials, prefer mounted files, environment variables, workload identity, or a custom `SecretProvider` connected to the platform secret manager.

## 📄 Pagination strategies

| Strategy | Class | Typical example |
|---|---|---|
| no pagination | `NoPagination` | small endpoint |
| page number | `PageNumberPagination` | `?page=2&per_page=1000` |
| offset | `OffsetPagination` | `?offset=1000&limit=1000` |
| cursor | `CursorPagination` | `next_cursor` in JSON |
| Link header | `LinkHeaderPagination` | GitHub / RFC-style link headers |
| next URL | `NextUrlPagination` | `{"next": "https://..."}` |

For unusual APIs, implement `PaginationStrategy`.

## 🧩 Two official usage modes

### Managed mode

Use managed mode when the library should persist Bronze and commit state for you:

```text
API → Pipeline → Destination → StateStore → RunLogBackend
```

Official adapters:

| Use | Destination | State | Audit |
|---|---|---|---|
| local | `DuckDBDestination` | `DuckDBStateStore` | `DuckDBRunLogStore` |
| files | `ParquetDestination` | `JsonFileStateStore` | `JsonLinesRunLogStore` |
| Lakehouse | `DeltaDestination` | `DeltaStateStore` | `DeltaRunLogStore` |

### Embedded mode

Use embedded mode when you are inside Databricks, Fabric, or another runtime and want `engineer_kit` to own only API extraction + pagination + incremental state:

```python
run = connector.extract_incremental()

for batch in run:
    df = spark.createDataFrame(batch)
    df = transform(df)
    persist(df)

run.commit()  # only after downstream work succeeds
```

The watermark must not advance before downstream persistence is complete.

## 🧱 Stable Bronze by design

Bronze favors **reliable capture** over aggressive type inference:

- declared fields are physically persisted as `string/null` by official adapters;
- analytical types are logical and are applied in staging/transformation;
- `_raw` preserves the original record;
- unexpected fields are retained in `_extra`;
- `_run_id`, `_window_start`, `_window_end`, and `_ingestion_key` provide traceability and safe retries.

## 📝 Declarative YAML pipeline

```yaml
name: orders

connector:
  base_url: https://api.example.com/v1/orders
  method: GET
  extraction_batch_size: 25000
  max_pages: 10000
  auth:
    type: bearer
    secret_key: API_TOKEN
  pagination:
    type: page
    params:
      page_param: page
      page_size_param: per_page
      page_size: 1000
  incremental:
    mode: data_date
    initial_start: "2026-01-01"
    date_field: updated_at
  date_params:
    start: updated_from
    end: updated_to
    format: "%Y-%m-%d"

columns:
  - name: id
    dtype: bigint
  - name: updated_at
    dtype: timestamp

destination:
  type: parquet
  path: ./lake
  schema: bronze
  batch_size: 5000
  write_mode: append

state:
  type: auto

run_log:
  enabled: true
  type: auto

secrets:
  type: env

transform:
  type: none
```

Run it with:

```bash
engineer_kit run-config pipelines/orders.yaml
```

## 🖥️ Local Lab

```bash
pip install "engineer_kit[local]"
engineer_kit ui --workspace .
```

The UI binds to loopback by default, uses authentication, and is intended for development and training. Read the UI guide before considering remote exposure.

## ☁️ Databricks, Fabric, AWS, Google Cloud, and Azure

Cloud is a runtime/storage concern, not a connector type. A REST API remains a `RestConnector` regardless of where the code runs.

```text
Source layer
└── RestConnector

Runtime / storage
├── local → DuckDB / Parquet
├── Databricks → Spark / Delta
├── Microsoft Fabric → Spark / OneLake / Delta
├── AWS → S3 / Delta
├── Google Cloud → GCS / Delta
└── Azure → ADLS / OneLake / Delta
```

This prevents artificial classes such as `AWSRestConnector` or `FabricRestConnector` when the source protocol is unchanged.

## 🛡️ Secure by default

The library includes runtime and supply-chain protections:

- HTTPS required by default and TLS verification enabled;
- secret redaction in logs and errors;
- bounded HTTP response size before parsing;
- cross-origin redirects and pagination blocked by default;
- literal link-local/metadata targets blocked;
- POST retry only with explicit opt-in;
- header injection protection;
- YAML loaded safely, size-bounded, and inline-secret guarded;
- traversal/symlink protection for file-based secrets;
- UI security headers and same-origin controls;
- dbt execution with `shell=False`, timeout, and redaction;
- CI with Ruff, Bandit, `pip-audit`, `pip check`, property tests, packaging checks, and stress tests.

Read [`SECURITY.md`](SECURITY.md) before production use.

## ✅ What CI validates

CI covers:

- Python 3.10, 3.11, and 3.12;
- core-only installation without optional storage backends;
- DuckDB, Parquet, and Delta;
- pagination, incremental state, checkpointing, and idempotent retries;
- embedded and managed modes;
- CLI, YAML, and UI;
- security and property-based tests;
- wheel/sdist build validation;
- synthetic stress: DuckDB 250k, Parquet 250k, Delta 100k records.

## 📚 Documentation

Complete English documentation:

**https://lima-ricardo.github.io/engineer_kit/en/**

Portuguese documentation:

**https://lima-ricardo.github.io/engineer_kit/**

English shortcuts:

- [Installation](https://lima-ricardo.github.io/engineer_kit/en/getting-started/installation/)
- [Mental model](https://lima-ricardo.github.io/engineer_kit/en/getting-started/mental-model/)
- [First pipeline](https://lima-ricardo.github.io/engineer_kit/en/getting-started/first-pipeline/)
- [Authentication and secrets](https://lima-ricardo.github.io/engineer_kit/en/guides/authentication/)
- [Pagination](https://lima-ricardo.github.io/engineer_kit/en/guides/pagination/)
- [Incremental state and watermarks](https://lima-ricardo.github.io/engineer_kit/en/guides/incremental/)
- [Streaming and batching](https://lima-ricardo.github.io/engineer_kit/en/guides/streaming/)
- [Databricks and Fabric](https://lima-ricardo.github.io/engineer_kit/en/guides/embedded-mode/)
- [YAML reference](https://lima-ricardo.github.io/engineer_kit/en/reference/configuration/)
- [Troubleshooting](https://lima-ricardo.github.io/engineer_kit/en/reference/troubleshooting/)

## 🧪 Development

```bash
git clone https://github.com/Lima-Ricardo/engineer_kit.git
cd engineer_kit
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -e ".[dev,all,docs]"
pytest -q
mkdocs serve
```

## 🤝 Contributing

Issues, documentation improvements, and pull requests are welcome. Read [`CONTRIBUTING.md`](CONTRIBUTING.md) first.

## 🔒 Reporting a vulnerability

Do not post tokens, exploit details, or sensitive data in a public issue. Follow [`SECURITY.md`](SECURITY.md).

## 📜 License

MIT — see [`LICENSE`](LICENSE).
