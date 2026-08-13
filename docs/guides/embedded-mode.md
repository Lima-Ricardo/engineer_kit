# Embedded mode: Fabric, Databricks e código do usuário

Embedded mode usa o `engineer_kit` como **motor de extração confiável**, sem entregar sua transformação e persistência para a biblioteca.

## O padrão

```python
run = connector.extract_incremental()

for batch in run:
    process_and_persist(batch)

run.commit()
```

O `commit()` é a fronteira. Ele significa: "todo o downstream referente a esta extração terminou com sucesso".

## Databricks / PySpark

```python
%pip install engineer_kit
```

```python
run = connector.extract_incremental()

for batch in run:
    df = spark.createDataFrame(batch)
    df = (
        df
        .dropDuplicates(["id"])
    )
    df.write.format("delta").mode("append").saveAsTable("bronze.orders")

run.commit()
```

## Microsoft Fabric

```python
%pip install engineer_kit
```

O fluxo é o mesmo:

```python
run = connector.extract_incremental()

for batch in run:
    df = spark.createDataFrame(batch)
    # regras do seu projeto
    write_to_lakehouse(df)

run.commit()
```

## Falha

```python
run = connector.extract_incremental()

try:
    for batch in run:
        persist(batch)
except Exception:
    # sem commit: watermark anterior continua válido
    raise
else:
    run.commit()
```

## Volumes grandes

Criar um DataFrame Spark para cada batch é simples, mas pode gerar overhead. Para ingestões grandes:

```text
API
 ↓
ExtractionSession
 ↓
staging Parquet/Delta
 ↓
Spark read
 ↓
transformação distribuída
 ↓
commit checkpoint
```

O staging desacopla a velocidade serial da API do processamento distribuído do cluster.

## StateStore no embedded mode

Você ainda precisa de um `StateStore` se quiser incremental persistente entre runs. Use uma implementação adequada ao runtime ou injete sua própria implementação.

## O que o engineer_kit não faz aqui

- não decide quantas partitions Spark usar;
- não decide cluster size;
- não escolhe tabela/catálogo;
- não decide merge key;
- não assume Unity Catalog, Fabric Lakehouse ou IAM específico.

Essas decisões continuam no seu projeto/plataforma.
