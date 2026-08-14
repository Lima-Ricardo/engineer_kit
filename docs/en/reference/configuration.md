# YAML configuration reference

This page describes the blocks accepted by declarative pipelines. The configuration surface mirrors the intent-driven Python API and is validated strictly before execution.

## Happy path

A public, non-incremental API can start with:

```yaml
version: 1
name: orders
connector:
  base_url: https://api.example.com/orders
```

`GET`, `auto` pagination, `dedup: false`, and conservative record-list discovery are the defaults. No `StateStore` is created unless incremental ingestion is enabled.

## Complete structure

```yaml
version: 1
name: orders
connector: {}
columns: []
destination: {}
state: {}
run_log: {}
secrets: {}
transform: {}
```

## `version`

The current configuration format is `1`. The field is optional for files created before versioning was introduced; omitted files are interpreted as version `1`. Unknown versions are rejected rather than partially interpreted.

## `name`

Logical pipeline identifier. For compatibility it is also the default checkpoint key. Use `connector.state_key` when pipelines sharing the same logical name need independent state namespaces.

## `connector`

| Field | Type | Default | Description |
|---|---|---:|---|
| `base_url` | string | required | absolute API URL |
| `method` | `GET`/`POST` | `GET` | extraction method |
| `records` | string/null | auto | path to the record list in JSON |
| `select` | list/string/mapping | null | projected fields; mappings support `path: alias` |
| `params` | mapping | `{}` | fixed API parameters |
| `state_key` | string/null | `name` | explicit checkpoint namespace |
| `primary_key` | string/list/null | null | simple or composite identity of the emitted record |
| `dedup` | bool | `false` | enable deduplication using `primary_key` |
| `extraction_batch_size` | int | `25000` | records delivered per extraction batch |
| `max_pages` | int | core limit | defensive pagination bound |
| `auth` | object | none | authentication strategy |
| `pagination` | string/bool/object | `auto` | pagination strategy |
| `incremental` | bool/string/object | `false` | incremental checkpoint/window behavior |
| `date_params` | object | empty | API date-parameter names |

`records_path` and `static_params` remain readable as `0.2` compatibility aliases. New configurations should use `records` and `params`. If both `records` and `records_path` are supplied with different values, validation fails.

### `records`

```yaml
records: data.orders
```

When omitted, the first response is inspected conservatively for list candidates such as `data`, `results`, `items`, `records`, or nested equivalents. Ambiguous payloads require an explicit path.

### `select`

Simple list:

```yaml
select:
  - id
  - amount
  - customer.id
```

Explicit aliases:

```yaml
select:
  customer.id: customer_id
  totals.net: net_amount
```

Paths support object traversal and explicit indexes, for example `items[0].sku` and `payload["odd.key"].value`. Wildcards are intentionally unsupported because selectors must not change row cardinality implicitly. If two paths normalize to the same output alias, validation fails and explicit aliases are required.

### `primary_key` and `dedup`

Identity and deduplication policy are independent. A PK may be mapped without removing records:

```yaml
connector:
  base_url: https://api.example.com/customers
  primary_key: customer_id
  dedup: false
```

This allows `profile()` to use the configured identity for completeness, null/missing, and duplicate analysis before deduplication is enabled.

Enable deduplication explicitly:

```yaml
connector:
  base_url: https://api.example.com/customers
  primary_key: customer_id
  dedup: true
```

Composite identity:

```yaml
connector:
  base_url: https://api.example.com/orders
  primary_key:
    - tenant_id
    - order_id
  dedup: true
```

`dedup` is strictly boolean and `false` is always the default. `dedup: true` without `primary_key` is rejected. Strings such as `"false"` and intermediate forms such as `dedup: customer_id` or `dedup: [customer_id]` are also rejected in YAML so there is one unambiguous declarative contract.

`primary_key` accepts only a string, a list of strings, or `null`; booleans such as `primary_key: true` and `primary_key: false` are invalid. This prevents dataset identity from being confused with the former idea of switching deduplication on or off.

When deduplication is enabled and a valid PK repeats, the first occurrence wins and **the complete later record is removed**, even if other fields differ. Missing, `null`, blank, array, or object values in any PK component fail ingestion explicitly instead of collapsing records without usable identity.

Identity is evaluated **after `select`**. Therefore projected connectors must reference aliases actually emitted by `select` in `primary_key`.

The runtime stores only SHA-256 fingerprints of the PK in a temporary SQLite database and deletes the file at the end. This avoids an unbounded in-memory set; disk usage grows with the number of unique identities observed.

Use `profile()` to evaluate a candidate key before persisting it or enabling the policy:

```bash
engineer-kit profile-config pipelines/orders.yaml \
  --metrics duplicates,missing,nulls \
  --key tenant_id,order_id
```

Once `primary_key` is configured, `--key` may be omitted and profiling reuses the declared identity even with `dedup: false`.

### `params`

```yaml
params:
  status: open
  region: BR
```

Inline sensitive values remain blocked by default; use `${SECRET:NAME}` references.

### `auth`

```yaml
auth:
  type: bearer      # none | bearer | api_key
  secret_key: API_TOKEN
  param_name: X-API-Key
  location: header  # query | header
```

### `pagination`

Short form:

```yaml
pagination: cursor
```

Supported values:

```text
auto none page offset cursor link_header next_url
```

Pagination can also be disabled with `pagination: false`, or configured with guided options:

```yaml
pagination:
  type: page
  size: 1000
  param: page
  start_page: 1
```

The expert/legacy `params: {}` form remains valid. See [Pagination](../guides/pagination.md).

### `incremental`

Disabled, which is also the default:

```yaml
incremental: false
```

Checkpoint by execution date:

```yaml
incremental: true
```

Known watermark field:

```yaml
incremental: updated_at
```

Explicit form:

```yaml
incremental:
  enabled: true
  mode: data_date   # data_date | ingestion_date
  initial_start: "2026-01-01"
  date_field: updated_at
```

The checkpoint is committed only after the destination confirms the write. Commit also verifies that the watermark used to resolve the extraction window is still current; a stale concurrent run is rejected instead of silently overwriting newer state.

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

Bronze metadata names (`_raw`, `_extra`, `_source`, `_run_id`, `_ingestion_key`, and the other internal fields) are reserved and cannot be declared by a source schema. Duplicate declared columns are rejected as well.

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

`type` can be any registered adapter, including `duckdb`, `parquet`, and `delta`. Physical options are adapter-specific; the runtime keeps `Destination`, `StateStore`, and `RunLogBackend` as separate contracts.

## `state`

```yaml
state:
  type: auto
  path: null
  options: {}
```

This block is materialized only when incremental ingestion is enabled. `auto` resolves the natural state implementation for known destinations. The legacy `state_store` alias is still accepted, but `state` and `state_store` cannot both appear in one file.

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

The short form `transform: dbt` is also accepted.

## Profiling a configuration

The same YAML can be inspected without loading Bronze:

```bash
engineer-kit profile-config pipelines/orders.yaml
engineer-kit profile-config pipelines/orders.yaml --metrics duplicates,nulls,missing
engineer-kit profile-config pipelines/orders.yaml --metrics duplicates,missing,nulls --key customer_id
engineer-kit profile-config pipelines/orders.yaml --scope full
```

When `--key` is omitted, a configured `connector.primary_key` is reused automatically for the `duplicates` metric independently of `dedup`. Without `--key` and without `primary_key`, duplicates are evaluated by complete-row identity.

The command uses an inspection state backend that refuses writes, so profiling cannot advance the configured checkpoint. Local Lab exposes the same operation through the **Data Profile** screen, including candidate-PK testing before persisting identity or enabling deduplication.

## File validation and safety

The loader enforces a YAML size bound, accepts UTF-8 only, and instantiates a loader derived from `yaml.SafeLoader`. Duplicate mapping keys and unknown fields in known blocks are rejected. This prevents typos such as `pagniation:` from being silently ignored.

Configuration validation complements rather than replaces runtime security: HTTPS/TLS, secret redaction, response/pagination limits, and redirect protections remain centralized in the core.
