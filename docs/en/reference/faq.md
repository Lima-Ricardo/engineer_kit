# FAQ

## Is `iter_batches()` only for Spark?

No. It is a general streaming primitive and works with pure Python, Pandas, Polars, Arrow, DuckDB, or any other consumer.

## Why 25,000 records?

It is a balanced operational default: large enough to avoid excessive overhead and conservative enough to reduce memory materialization. Tune it according to average record size and workload.

## Should `page_size` also be 25,000?

No. Follow the API's documented limits. `page_size` and `extraction_batch_size` are independent.

## Can I hardcode a token?

Yes, with `StaticSecretProvider`, especially for training/labs. For real credentials, prefer environment variables, mounted files, or a secret manager.

## Does the library replace Spark?

No. In embedded mode, the library owns extraction/checkpointing and your code continues with Spark.

## Do I need DuckDB?

No. The core does not depend on DuckDB. You can use extraction only or choose Parquet/Delta.

## Can it run in Fabric or Databricks?

Yes. Install the package in the notebook and use embedded mode or runtime-compatible adapters.

## Why is there no connector per cloud?

Because cloud placement is runtime/storage. The source REST protocol does not change.

## Does it perform automatic merge/upsert?

No generic business key is inferred. Merge semantics belong to the layer that understands the domain.
