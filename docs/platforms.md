# Uso em plataformas de dados

O `engineer_kit` pode entrar em uma plataforma de duas formas:

1. **managed mode**: a biblioteca leva a API até a Bronze através de um `Destination`;
2. **embedded mode**: a biblioteca cuida de HTTP, paginação e incremental, entrega batches ao código do usuário e só confirma o checkpoint depois que o processamento downstream termina.

Ele não substitui Lakehouse, Spark, dbt, SQL Warehouse ou o orquestrador que já existem no ambiente.

Veja também [`streaming.md`](streaming.md) para o contrato `ExtractionSession`, o default de 25.000 registros e a diferença entre paginação, extraction batch e write batch.

## Quatro modos

### 1. DuckDB local

```text
API → engineer_kit → DuckDB Bronze → dbt-duckdb
```

Instalação:

```bash
pip install "engineer_kit[local]"
```

É o modo indicado para desenvolvimento, treinamento, CI, demos e projetos zero-infra.

### 2. Parquet / filesystem montado

```text
API → engineer_kit → Parquet Bronze
                        │
                        ├── JsonFileStateStore
                        └── JsonLinesRunLogStore
```

Instalação:

```bash
pip install "engineer_kit[parquet]"
```

O adapter Parquet escreve em filesystem local ou montado. Ele não tenta implementar autenticação para todos os object stores; para URIs de Lakehouse, o adapter Delta é a fronteira preferida.

### 3. Delta / Lakehouse

```text
API
 ↓
engineer_kit
 ├── DeltaDestination        → bronze/<endpoint>
 ├── DeltaStateStore         → _meta/ingestion_state
 └── DeltaRunLogStore        → _meta/run_log
 ↓
Lakehouse
 ↓
Spark / dbt / SQL / BI
```

Instalação:

```bash
pip install "engineer_kit[delta]"
# alias voltado a ambientes de plataforma
pip install "engineer_kit[platform]"
```

O adapter usa Delta Lake através do pacote `deltalake`/delta-rs e PyArrow. O core continua sem essas dependências.

### 4. Embedded mode

Quando o usuário quer controlar Spark/Delta diretamente, não precisa usar um `Destination` do `engineer_kit`:

```text
API
 ↓
RestConnector
 ↓
ExtractionSession
 ↓
user code
 ↓
Spark / Pandas / Polars / Arrow
 ↓
persistência escolhida pelo usuário
 ↓
run.commit()
```

Exemplo conceitual:

```python
run = connector.extract_incremental()

for batch in run:
    df = spark.createDataFrame(batch)
    # regras e transformações do projeto
    write_result(df)

run.commit()
```

A sessão é streaming-first e usa 25.000 registros por batch por padrão. `collect()` existe para datasets pequenos, mas materializa tudo em memória e portanto não é o caminho recomendado para cargas maiores.

## Connector não é cloud

O contrato pai `Connector` representa a **origem**. `RestConnector` representa REST. Fabric, Databricks, AWS e Google Cloud são runtime/storage, não subclasses do protocolo REST.

```text
Connector
 └── RestConnector

Runtime / storage
 ├── local
 ├── Databricks
 ├── Microsoft Fabric
 ├── AWS
 └── Google Cloud
```

Isso evita duplicar classes como `AWSRestConnector`, `GoogleRestConnector` ou `FabricRestConnector` quando a chamada HTTP continua sendo a mesma.

Cloud-specific credentials, URI schemes e storage options ficam nos adapters ou no runtime da plataforma.

## Databricks

### Managed mode

```text
Databricks Job / notebook
          │
          ▼
      engineer_kit
          │
          ▼
      Delta Bronze
          │
     ┌────┴────┐
     ▼         ▼
   Spark      dbt
```

Exemplo YAML:

```yaml
name: orders
connector:
  base_url: https://api.example.com/orders
  method: GET
  extraction_batch_size: 25000
  pagination:
    type: page
    params:
      page_size: 1000
  incremental:
    mode: ingestion_date

destination:
  type: delta
  path: /Volumes/my_catalog/my_schema/ingestion
  schema: bronze
  batch_size: 5000
  write_mode: append

state:
  type: auto
run_log:
  enabled: true
  type: auto
transform:
  type: none
```

Depois da Bronze, use os recursos nativos da plataforma para transformação/orquestração. O `engineer_kit` não precisa iniciar uma SparkSession para cumprir sua função.

### Embedded mode

```python
run = connector.extract_incremental()

for batch in run:
    df = spark.createDataFrame(batch)
    # transforme e grave no Delta usando as regras do projeto
    persist(df)

run.commit()
```

Para volumes muito grandes, criar muitos DataFrames a partir de objetos Python pode introduzir overhead. Nesses casos, prefira staging em Parquet/Delta e faça o Spark ler o staging de forma nativa.

## Microsoft Fabric

### Managed mode

```text
Fabric notebook / job
       │
       ▼
  engineer_kit
       │
       ▼
Lakehouse Bronze
       │
   ┌───┴────┐
   ▼        ▼
 Spark     SQL
```

Em um notebook com Lakehouse anexado, use um caminho de filesystem/Lakehouse acessível ao processo. O código de ingestão continua igual; o que muda é apenas `destination.path` e, quando necessário, opções de storage/autenticação do ambiente.

O adapter não registra automaticamente tabelas no catálogo específico de cada fornecedor. Essa responsabilidade pertence à plataforma, evitando acoplamento do core a APIs de Databricks ou Fabric.

### Embedded mode

O mesmo `ExtractionSession` pode alimentar lógica do notebook sem exigir que a biblioteca grave a Bronze:

```text
API → ExtractionSession batches → código do notebook → Spark/Delta → commit checkpoint
```

Esse desenho evita materializar a API inteira antes de iniciar o processamento e pode reduzir pressão de memória do driver e tempo improdutivo de cluster/capacity. Batches excessivamente pequenos, porém, também criam overhead; 25.000 é um default ajustável, não uma regra fixa.

## AWS

O core não precisa de um `AWSConnector`. Quando a origem é REST, use `RestConnector` normalmente. Storage AWS deve ser tratado por URI/storage options do adapter escolhido ou pelo filesystem/runtime já configurado.

Exemplo conceitual Delta:

```yaml
destination:
  type: delta
  path: s3://my-bucket/project
  options:
    storage_options: {}
```

Credenciais devem vir do runtime (IAM role, workload identity, secret manager ou variáveis de ambiente), não ser gravadas diretamente no YAML.

## Google Cloud

A mesma regra vale para GCP: o protocolo da origem não muda porque o runtime está no Google Cloud.

Exemplo conceitual Delta:

```yaml
destination:
  type: delta
  path: gs://my-bucket/project
  options:
    storage_options: {}
```

Prefira workload identity/service account fornecida pelo ambiente. A biblioteca não deve duplicar a autenticação cloud dentro do core.

## Azure / OneLake

Para Azure, Fabric e OneLake, URIs/opções são responsabilidades do adapter/runtime:

```yaml
destination:
  type: delta
  path: <lakehouse-or-abfs-uri>
  options:
    storage_options: {}
```

Managed identity/workload identity deve ser preferida quando disponível.

## Storage options e credenciais

`DestinationConfig.options` pode transportar opções técnicas do adapter Delta:

```yaml
destination:
  type: delta
  path: <lakehouse-root>
  options:
    storage_options:
      SOME_NON_SECRET_OPTION: value
```

Para credenciais, prefira mecanismos do ambiente:

- IAM role / workload identity;
- managed identity;
- service principal ou service account via secret manager;
- variáveis de ambiente;
- credenciais já configuradas pelo runtime.

Evite salvar tokens/chaves diretamente em YAML.

`state.options.storage_options` e `run_log.options.storage_options` podem sobrescrever opções do destination quando realmente necessário.

## State e audit independentes

Mesmo em uma plataforma, Destination, StateStore e RunLogBackend não precisam estar no mesmo backend:

```yaml
destination:
  type: delta
  path: /lake/bronze-root

state:
  type: file
  path: /mounted-control/state.json

run_log:
  enabled: true
  type: delta
  path: /lake/control-root
```

Esse desenho é útil para testes e integrações, embora em produção seja comum deixar `state.type: auto` e `run_log.type: auto`.

No embedded mode, ainda é necessário um `StateStore` se o usuário quiser incremental confiável. O que deixa de ser obrigatório é o `Destination` gerenciado pela biblioteca.

## Particionamento

Delta suporta configuração explícita:

```yaml
destination:
  type: delta
  path: /lake/project
  schema: bronze
  partition_by:
    - _window_end
```

Não existe particionamento automático. A escolha depende de volume, padrão de leitura e características da plataforma.

## Retry em plataforma

Managed mode:

```text
Destination transaction
      ↓
StateStore commit
```

Embedded mode:

```text
ExtractionSession
      ↓
user processing/write
      ↓
run.commit()
```

Em ambos os casos, o checkpoint deve ser a última etapa de confirmação de dados. Se o processamento falha antes do commit, a janela permanece disponível para retry.

## Paginação, extraction batch e write batch

São controles independentes:

```text
API page size             1.000
        ↓
Extraction batch         25.000
        ↓
Destination write batch   5.000
```

O primeiro respeita o contrato da API, o segundo limita memória/entrega ao consumidor e o terceiro otimiza a persistência física. Rate limits, `429`, `Retry-After` e backoff pertencem ao `HttpClient`, não ao tamanho do extraction batch.

## Orquestração

No managed mode, o objeto `Pipeline` é a unidade que o orquestrador chama:

```python
result = pipeline.run()
if not result.success:
    raise RuntimeError(result.steps)
```

Também é possível executar um YAML diretamente:

```bash
engineer_kit run-config pipelines/orders.yaml
```

Isso permite integração com:

- Databricks Workflows;
- Fabric Pipelines/notebooks;
- Airflow;
- Dagster;
- cron/GitHub Actions;
- schedulers internos de outras plataformas.

O scheduler local do `engineer_kit` é conveniência, não requisito.

## Testabilidade

A suíte do projeto cobre DuckDB, Parquet e Delta em filesystem local, incluindo:

- schema drift;
- batches;
- `ExtractionSession` single-pass;
- default de 25.000 registros;
- proteção contra checkpoint parcial;
- falha no meio do stream;
- checkpoint posterior ao load;
- retry idempotente da mesma transição de checkpoint;
- execuções bem-sucedidas subsequentes no mesmo dia;
- append/overwrite;
- state e run log separados.

Isso valida os contratos e o formato Delta. Autenticação, paths, IAM/workload identity e catálogo específicos de cada workspace cloud ainda devem ser validados no ambiente onde a lib será implantada.
