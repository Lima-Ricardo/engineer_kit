# Uso em plataformas de dados

O `engineer_kit` entra em uma plataforma como **camada de ingestão da API até a Bronze**. Ele não substitui o Lakehouse, Spark, dbt, SQL Warehouse ou o orquestrador que já existem no ambiente.

## Três modos

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

## Databricks

Padrão recomendado:

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
  pagination:
    type: page
    params:
      page_size: 500
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

## Microsoft Fabric

Padrão recomendado:

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

- managed identity / workload identity;
- service principal disponibilizado por secret manager;
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

A sequência permanece:

```text
Delta transaction
      ↓
StateStore commit
```

Se o commit do StateStore falhar, a mesma **transição de checkpoint** é repetida. `DeltaDestination` usa predicate overwrite da `_ingestion_key` calculada a partir de connector + janela + checkpoint anterior, evitando duplicar o retry. Depois de um checkpoint bem-sucedido, uma execução posterior recebe outra chave mesmo que ocorra no mesmo dia.

## Orquestração

O objeto `Pipeline` é a unidade que o orquestrador chama:

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
- falha no meio do stream;
- checkpoint posterior ao load;
- retry idempotente da mesma transição de checkpoint;
- execuções bem-sucedidas subsequentes no mesmo dia;
- append/overwrite;
- state e run log separados.

Isso valida os contratos e o formato Delta. Autenticação, paths e catálogo específicos de cada workspace cloud ainda devem ser validados no ambiente Databricks/Fabric onde a lib será implantada.
