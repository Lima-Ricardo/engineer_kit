# Installation

## Requirements

- Python 3.10, 3.11, or 3.12;
- network access to the API you want to ingest;
- an optional storage backend only if you want managed persistence.

## 1. Create a virtual environment

=== "Linux / macOS"

    ```bash
    python -m venv .venv
    source .venv/bin/activate
    ```

=== "Windows PowerShell"

    ```powershell
    py -m venv .venv
    .\.venv\Scripts\Activate.ps1
    ```

## 2. Install only what you need

Core package:

```bash
pip install engineer_kit
```

The core includes connectors, HTTP, pagination, incremental state, `ExtractionSession`, contracts, and the basic CLI. It **does not** install storage engines.

| Goal | Installation |
|---|---|
| DuckDB | `pip install "engineer_kit[duckdb]"` |
| Parquet | `pip install "engineer_kit[parquet]"` |
| Delta | `pip install "engineer_kit[delta]"` |
| Lakehouse profile | `pip install "engineer_kit[platform]"` |
| Local UI | `pip install "engineer_kit[ui]"` |
| Local dbt | `pip install "engineer_kit[dbt]"` |
| Full local lab | `pip install "engineer_kit[local]"` |
| Every feature | `pip install "engineer_kit[all]"` |

## 3. Verify the installation

```bash
engineer_kit --help
```

```python
import engineer_kit
print(engineer_kit.__version__)
```

## pip, uv, Poetry, and pipx

The official distribution channel is PyPI, so any compatible installer can use the same package:

```bash
uv pip install engineer_kit
poetry add engineer_kit
```

For an isolated CLI installation:

```bash
pipx install "engineer_kit[local]"
```

## Databricks

For embedded mode, the core package is usually sufficient:

```python
%pip install engineer_kit
```

Restart the notebook Python environment if your runtime requires it.

## Microsoft Fabric

```python
%pip install engineer_kit
```

If you want to use `DeltaDestination` directly through delta-rs:

```python
%pip install "engineer_kit[delta]"
```

## `ModuleNotFoundError` for an optional backend

Install the matching extra:

```text
DeltaDestination   → engineer_kit[delta]
ParquetDestination → engineer_kit[parquet]
DuckDBDestination  → engineer_kit[duckdb]
```

See [Troubleshooting](../reference/troubleshooting.md).
