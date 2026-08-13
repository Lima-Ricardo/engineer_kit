# dbt

The dbt integration is optional and deliberately runs after ingestion.

```text
API → Bronze → checkpoint → dbt
```

## Installation

```bash
pip install "engineer_kit[dbt]"
```

Or install the complete local lab:

```bash
pip install "engineer_kit[local]"
```

## Why does dbt run after checkpointing?

A transformation failure should not turn a valid Bronze ingestion into ambiguous state. Bronze + checkpoint are the ingestion transaction; dbt is downstream.

## Scaffold

The library provides helpers for generating `sources.yml` and staging models from the logical schema. Bronze fields are cast in staging to the declared analytical types.

## `--select`

```yaml
transform:
  type: dbt
  select: tag:daily
```

Without `select`, the runner follows the dbt project's default behavior.
