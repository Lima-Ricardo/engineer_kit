# Extração streaming-first

O `engineer_kit` usa uma política **streaming-first** para extrações incrementais. O objetivo é evitar que o usuário precise materializar uma API inteira em memória antes de começar a processar os dados.

## `ExtractionSession`

O caminho recomendado é:

```python
run = connector.extract_incremental()

for batch in run:
    process(batch)

run.commit()
```

A sessão é single-pass e separa duas responsabilidades:

1. consumir a API de forma incremental e limitada em memória;
2. confirmar o checkpoint somente depois que o consumidor terminar o trabalho downstream.

`run.commit()` só é permitido depois que a sessão foi consumida por completo. Se o processamento falhar antes disso, o watermark não avança.

## Default: 25.000 registros

O tamanho padrão do batch de extração é:

```python
DEFAULT_EXTRACTION_BATCH_SIZE = 25_000
```

O valor é deliberadamente conservador e equilibrado para uso geral. Muitas APIs trabalham com páginas na ordem de centenas ou milhares de registros; uma API com `page_size=1000`, por exemplo, pode preencher um batch de 25.000 após aproximadamente 25 páginas.

Essa relação é apenas ilustrativa. O batch de extração **não depende** do tamanho da página e não interfere na estratégia de paginação.

```text
API page size             1.000
        │
        │ 25 páginas aproximadas
        ▼
Extraction batch         25.000
        │
        ▼
consumidor
```

Se a API devolver menos registros do que o solicitado, o `engineer_kit` simplesmente continua acumulando registros até atingir o batch ou chegar ao fim da extração.

## Três tamanhos diferentes

Não confunda:

```text
API pagination size
        ↓
Extraction batch size
        ↓
Destination write batch size
```

Cada camada otimiza um problema diferente.

### API pagination size

Controlado pela `PaginationStrategy` e limitado pelas regras da própria API.

Exemplo:

```yaml
pagination:
  type: page
  params:
    page_size: 1000
```

### Extraction batch size

Controla quantos registros o consumidor recebe por vez e, portanto, a quantidade de objetos Python materializados simultaneamente.

```yaml
connector:
  extraction_batch_size: 25000
```

ou em Python:

```python
connector = RestConnector(
    ...,
    extraction_batch_size=25_000,
)
```

É possível sobrescrever apenas uma sessão:

```python
run = connector.extract_incremental(batch_size=10_000)
```

### Destination write batch size

É uma decisão interna do adapter de persistência.

Exemplo:

```text
Extraction batch 25.000
        ↓
DuckDB write batch 5.000
        ↓
5 writes internos
```

Isso permite otimizar memória de extração e escrita física separadamente.

## `collect()` é explícito

Para datasets pequenos:

```python
run = connector.extract_incremental()
records = run.collect()
```

`collect()` materializa a extração inteira em memória. Ele existe por conveniência, mas não é o caminho padrão recomendado para workloads médios ou grandes.

## Python, Pandas e Polars

O mesmo batch pode ser consumido por qualquer biblioteca:

```python
for batch in run:
    process(batch)
```

Pandas:

```python
for batch in run:
    df = pandas.DataFrame(batch)
    process(df)
```

Polars:

```python
for batch in run:
    df = polars.DataFrame(batch)
    process(df)
```

O conceito não é específico de Spark.

## Databricks e Microsoft Fabric: embedded mode

Dentro de uma plataforma, o usuário pode instalar apenas o `engineer_kit` e usar a biblioteca para HTTP, paginação e incremental sem entregar a persistência ao `Pipeline`.

```text
API
 ↓
engineer_kit ExtractionSession
 ↓
user code
 ↓
Spark / Pandas / Polars / Arrow
 ↓
Delta / tabela / outro destino
 ↓
run.commit()
```

Exemplo conceitual:

```python
run = connector.extract_incremental()

for batch in run:
    df = spark.createDataFrame(batch)
    transform_and_write(df)

run.commit()
```

O checkpoint só deve ser confirmado depois que o dado downstream estiver persistido com sucesso.

Para volumes muito grandes, criar muitos pequenos DataFrames Spark pode adicionar overhead. Nesses casos, uma estratégia de staging em Parquet/Delta e leitura posterior pelo Spark costuma ser mais apropriada:

```text
API
 ↓
ExtractionSession batches
 ↓
Parquet/Delta staging
 ↓
Spark read
 ↓
transformação distribuída
 ↓
commit checkpoint
```

## Memória, tempo e custo de plataforma

Batching reduz o risco de:

- acumular milhões de `dict` no driver Python;
- OOM por materialização completa;
- manter recursos de cluster/capacity ocupados enquanto o driver acumula a extração inteira;
- reprocessar uma extração completa quando a lógica downstream falha cedo.

Batch menor, porém, não é automaticamente mais barato. Batches excessivamente pequenos podem aumentar:

- overhead de scheduler;
- número de commits;
- número de DataFrames temporários;
- small files em storage distribuído.

Por isso 25.000 é um default, não uma regra fixa. O usuário pode ajustar o valor conforme tamanho médio dos registros, limites da API e comportamento do runtime.

## Rate limit é outra responsabilidade

`ExtractionSession` não deve simular rate limiting através do tamanho do batch.

Regras como:

```text
429 Too Many Requests
Retry-After
backoff
requests/minute
```

pertencem ao `HttpClient` e à política de retry. A paginação continua respeitando o contrato da API independentemente do batch de extração.

## AWS, Google Cloud e Azure

A extração continua idêntica em qualquer runtime. Cloud não é uma subclasse do conector.

```text
Connector
 ├── RestConnector
 └── futuros tipos de source

Storage/runtime
 ├── local
 ├── S3
 ├── GCS
 ├── ADLS/OneLake
 └── Delta/Lakehouse
```

O objetivo é evitar classes duplicadas como `AWSRestConnector`, `GoogleRestConnector` ou `FabricRestConnector` quando o protocolo de origem continua sendo REST. Cloud-specific authentication e storage options pertencem aos adapters e ao ambiente onde o código executa.

## Regra de segurança do checkpoint

A ordem recomendada em embedded mode é sempre:

```text
read checkpoint
 ↓
extract
 ↓
process/write downstream
 ↓
sucesso
 ↓
run.commit()
```

Nunca:

```text
extract
 ↓
commit checkpoint
 ↓
process/write downstream
```

porque uma falha downstream depois do commit poderia fazer a próxima execução pular dados que ainda não foram persistidos.
