# YAML configuration reference

This page describes the blocks accepted by declarative pipelines.

## Complete structure

```yaml
name: orders
connector: {}
columns: []
destination: {}
state: {}
run_log: {}
secrets: {}
transform: {}
```

## `name`

Logical pipeline identifier. It also participates in state identity and default naming.

## `connector`

| Field | Type | Default | Description |
|---|---|---:|---|
| `base_url` | string | required | absolute API URL |
| `method` | `GET`/`POST` | `GET` | extraction method |
| `records_path` | string/null | null | key containing the record list |
| `static_params` | mapping | `{}` | fixed request parameters |
| `extraction_batch_size` | int | `25000` | records delivered per extraction batch |
| `max_pages` | int | core limit | defensive pagination bound |
| `auth` | object | none | authentication strategy |
| `pagination` | object | conceptually required | page strategy |
| `incremental` | object | data_date | window calculation |
| `date_params` | object | empty | API date-parameter names |

### `auth`

```yaml
auth:
  type: bearer      # none | bearer | api_key
  secret_key: API_TOKEN
  param_name: X-API-Key
  location: header  # query | header
```

### `pagination`

```yaml
pagination:
  type: page  # none | page | offset | cursor | link_header | next_url
  params: {}
```

Parameters depend on the strategy. See [Pagination](../guides/pagination.md).

### `incremental`

```yaml
incremental:
  mode: data_date   # data_date | ingestion_date
  initial_start: "2026-01-01"
  date_field: updated_at
```

### `date_params`

```yaml
date_params:
  start: updated_from
  end: updated_to
  format: "%Y-%m-%d"
```

## `columns`

```yaml
columns:
  - name: id
    dtype: bigint
```

Known logical types:

```text
string integer bigint float decimal boolean date timestamp json
```

## `destination`

```yaml
destination:
  type: parquet
  path: ./lake
  schema: bronze
  batch_size: 5000
  write_mode: append
  partition_by: []
  options: {}
```

`type` can be any registered adapter, including `duckdb`, `parquet`, and `delta`.

## `state`

```yaml
state:
  type: auto
  path: null
  options: {}
```

`auto` resolves the natural state implementation for known destinations.

## `run_log`

```yaml
run_log:
  enabled: true
  type: auto
  path: null
  options: {}
```

Legacy boolean values remain accepted for compatibility.

## `secrets`

```yaml
secrets:
  type: env       # env | file
  path: null
  allow_inline_values: false
```

Secret reference inside options:

```yaml
some_token: ${SECRET:MY_TOKEN}
```

## `transform`

```yaml
transform:
  type: none   # none | dbt
  select: null
```

## File safety

The loader enforces a YAML size bound and uses `yaml.safe_load`. Non-UTF-8 files and invalid structures are rejected.
