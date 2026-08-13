# Troubleshooting

## `ModuleNotFoundError` when importing an adapter

Install the corresponding extra:

```text
DuckDB → engineer_kit[duckdb]
Parquet → engineer_kit[parquet]
Delta → engineer_kit[delta]
UI → engineer_kit[ui]
dbt → engineer_kit[dbt]
```

## The response is an object, not a list

Set `records_path`:

```python
records_path="results"
```

For unusual structures, provide a Python record-extraction function to `RestConnector`.

## Pagination stops too early

Verify the API contract. `PageNumberPagination` stops when it receives a page smaller than `page_size`. APIs that always return full pages but signal completion elsewhere require another strategy or custom pagination.

## Loop / max pages

If the API repeats the same cursor/URL, loop protection stops extraction. Fix the strategy/parser instead of blindly increasing `max_pages`.

## `ResponseTooLargeError`

A single API page exceeded the configured HTTP response limit. First reduce the API page size. Increase the response limit only when the expected payload justifies it.

## Plain HTTP on an internal network

Plain HTTP is rejected by default. If a trusted internal network truly requires it, opt in explicitly in `HttpClient`. Prefer TLS whenever possible.

## The watermark did not advance

Check whether the stream was fully consumed, downstream persistence succeeded, `run.commit()` was called in embedded mode, and the `StateStore` points to the intended location.

## Duplicate data after a retry

Official adapters protect retries of the same checkpoint transition through `ingestion_key`. If you implemented a custom Destination or embedded persistence, your downstream write path must provide the idempotency semantics you need.

## Spark is slow with many batches

Avoid creating thousands of tiny DataFrames. Increase extraction batch size or stage to Parquet/Delta and let Spark read the files.

## UI does not start

Install:

```bash
pip install "engineer_kit[ui]"
```

or:

```bash
pip install "engineer_kit[local]"
```

Then verify the port, bind address, and password printed/configured by the CLI.
