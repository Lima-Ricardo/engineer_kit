# First pipeline, step by step

We will ingest a fictional orders API, paginate by page number, and persist the Bronze layer as Parquet.

## 1. Install the Parquet extra

```bash
pip install "engineer_kit[parquet]"
```

## 2. Understand the API contract

Assume the API documentation shows:

```text
GET https://api.example.com/orders
?page=1
&per_page=1000
&updated_from=2026-01-01
```

Response:

```json
{
  "results": [
    {"id": 123, "updated_at": "2026-08-12T10:00:00Z"}
  ]
}
```

Identify these facts before writing configuration:

- method: `GET`;
- records list: `results`;
- pagination: `page` + `per_page`;
- incremental filter: `updated_from`;
- record date field: `updated_at`.

## 3. Create `pipelines/orders.yaml`

```yaml
name: orders

connector:
  base_url: https://api.example.com/orders
  method: GET
  records_path: results
  extraction_batch_size: 25000
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
```

## 4. Run it

```bash
engineer_kit run-config pipelines/orders.yaml
```

On the first run, the state starts at `2026-01-01`. Only after Bronze persistence succeeds does the `StateStore` receive the new watermark.

## 5. Run it again

The next execution reads the previous watermark and creates a new incremental window. You do not need to scan the full Bronze table to discover the last processed date.

## 6. What happens on failure?

If the API or destination fails, the previous checkpoint remains unchanged. If destination persistence succeeds but the checkpoint fails, the run is marked as a checkpoint error. Official adapters use deterministic ingestion identity so retrying the same window is safe.

## 7. What if the API adds a field?

The declared schema is not mutated automatically. Unexpected fields are preserved in `_extra`, while `_raw` retains the original record.

## 8. Next steps

- [Authentication](../guides/authentication.md)
- [Pagination](../guides/pagination.md)
- [Incremental state](../guides/incremental.md)
- [Managed mode](../guides/managed-mode.md)
