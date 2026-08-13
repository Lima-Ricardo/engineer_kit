# Streaming e batching

`engineer_kit` é **streaming-first**: o fluxo normal entrega batches limitados sem materializar a API inteira.

```python
run = connector.extract_incremental()

for batch in run:
    process(batch)

run.commit()
```

## Default: 25.000 registros

```python
DEFAULT_EXTRACTION_BATCH_SIZE = 25_000
```

É um default equilibrado, não um limite rígido.

```python
run = connector.extract_incremental(batch_size=10_000)
```

## Por que 25.000?

Ele é grande o suficiente para evitar overhead excessivo em workloads comuns e conservador o suficiente para reduzir o risco de uma lista enorme no driver. Em uma API com `page_size=1000`, equivale aproximadamente a 25 páginas antes de entregar um batch completo.

## Três camadas independentes

```text
API pagination size
        ↓
Extraction batch size
        ↓
Destination write batch size
```

Exemplo:

```text
page_size                 1.000
extraction_batch_size    25.000
DuckDB write batch        5.000
```

25 páginas podem gerar um extraction batch, que o destino pode dividir em cinco writes físicos.

## `collect()`

```python
records = run.collect()
```

Use somente quando você aceita materializar tudo em memória. É útil para demos, testes e APIs pequenas.

## Python puro

```python
for batch in run:
    for row in batch:
        process(row)
```

## Pandas

```python
import pandas as pd

for batch in run:
    df = pd.DataFrame(batch)
    process(df)
```

## Polars

```python
import polars as pl

for batch in run:
    df = pl.DataFrame(batch)
    process(df)
```

## Spark

```python
for batch in run:
    df = spark.createDataFrame(batch)
    transform_and_write(df)
```

Para volumes muito altos, evite milhares de DataFrames pequenos. Considere staging em Parquet/Delta e deixe o Spark ler arquivos nativamente.

## Memória e custo do cluster

Batching pode reduzir:

- pressão de RAM no driver;
- risco de OOM;
- tempo em que um cluster/capacity fica esperando a materialização inteira;
- custo de reprocessamento quando o erro aparece cedo.

Batch pequeno demais também custa:

- mais chamadas de processamento;
- mais commits;
- mais overhead de scheduler;
- risco de small files.

## Rate limit não é batching

`429`, `Retry-After` e backoff pertencem à camada HTTP. Não diminua `extraction_batch_size` esperando respeitar requests/minute; configure a estratégia/retry de rede de acordo com a API.
