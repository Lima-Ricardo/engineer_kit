# Embedded mode: Fabric, Databricks, and your own code

Embedded mode uses `engineer_kit` as a **reliable extraction engine** while leaving transformation and persistence under your control.

## The pattern

```python
run = connector.extract_incremental()

for batch in run:
    process_and_persist(batch)

run.commit()
```

`commit()` is the boundary: it means all downstream work associated with this extraction completed successfully.

## Databricks / PySpark

```python
%pip install engineer_kit
```

```python
run = connector.extract_incremental()

for batch in run:
    df = spark.createDataFrame(batch)
    df = df.dropDuplicates(["id"])
    df.write.format("delta").mode("append").saveAsTable("bronze.orders")

run.commit()
```

## Microsoft Fabric

```python
%pip install engineer_kit
```

```python
run = connector.extract_incremental()

for batch in run:
    df = spark.createDataFrame(batch)
    # your project rules
    write_to_lakehouse(df)

run.commit()
```

## Failure handling

```python
run = connector.extract_incremental()

try:
    for batch in run:
        persist(batch)
except Exception:
    # no commit: the previous watermark remains valid
    raise
else:
    run.commit()
```

## Large volumes

Creating a Spark DataFrame for every batch is simple but can become expensive. For larger workloads:

```text
API
 ↓
ExtractionSession
 ↓
staging Parquet/Delta
 ↓
Spark read
 ↓
distributed transformation
 ↓
checkpoint commit
```

Staging decouples the serial speed of the API from the distributed processing speed of the cluster.

## StateStore in embedded mode

You still need a `StateStore` if incremental progress must survive across runs. Use a runtime-appropriate implementation or inject your own.

## What engineer_kit does not decide here

- Spark partition count;
- cluster size;
- table/catalog naming;
- merge keys;
- Unity Catalog, Fabric Lakehouse, or platform-specific IAM semantics.

Those choices remain with your project and platform.
