# Arquitetura do engineer_kit

O `engineer_kit` é uma biblioteca de **ingestão confiável de APIs REST**. O projeto abstrai a parte repetitiva da ingestão sem tentar substituir Lakehouse, Spark, dbt ou um orquestrador corporativo.

## Fluxo

```text
REST API
   │
   ▼
RestConnector
   │
   ├────────────► StateStore
   │               watermark
   ▼
Destination
   │
   ▼
Bronze
   │
   ├────────────► RunLogBackend
   │
   ▼
Transform opcional
   ├── dbt (local lab)
   ├── Spark
   └── SQL / plataforma
```

O `Pipeline` coordena o fluxo, mas só conhece contratos. Ele não importa DuckDB, PyArrow, Delta Lake ou dbt para executar o core.

## Contratos do core

### `RestConnector`

Responsável por transformar uma API REST em um iterator de registros. Centraliza HTTP, autenticação, paginação, janela incremental e retry do cliente HTTP.

### `StateStore`

Persistência do checkpoint incremental:

```python
get_watermark(connector_name) -> Watermark | None
set_watermark(connector_name, watermark) -> None
```

Implementações oficiais:

- `DuckDBStateStore`: modo local/zero-infra;
- `JsonFileStateStore`: filesystem local ou montado;
- `DeltaStateStore`: Lakehouse Delta.

A Bronze **não é consultada** para descobrir o watermark. O estado é uma estrutura pequena e separada.

### `Destination`

Materializa a Bronze:

```python
load(connector_name, endpoint, schema, records) -> LoadResult
```

`load()` permanece estável para adapters de terceiros. Os adapters oficiais também implementam `load_with_context(...)`, que recebe a identidade da execução e permite retry idempotente.

Implementações oficiais:

- `DuckDBDestination` (`DuckDBLoader` continua como alias compatível);
- `ParquetDestination`;
- `DeltaDestination`.

### `RunLogBackend`

Recebe um `RunLogEntry` depois de cada tentativa de carga. Auditoria é independente de Destination e StateStore.

Implementações oficiais:

- `DuckDBRunLogStore`;
- `JsonLinesRunLogStore`;
- `DeltaRunLogStore`.

Falha apenas na auditoria é **best effort**: ela é registrada no log visual, mas não transforma uma ingestão já confirmada em falha artificial.

### `Pipeline`

Unidade atômica do ponto de vista do orquestrador:

```text
extract
  ↓
destination transaction
  ↓
checkpoint
  ↓
audit
```

O `PipelineResult` retorna `run_id`, horários, linhas e resultados de cada source. Cada `StepResult` inclui destino, janela, watermarks, campos extras e `ingestion_key`.

## Contrato físico da Bronze

O objetivo da Bronze é **capturar antes de interpretar**.

Campos declarados são persistidos fisicamente como string/null nos adapters oficiais. O tipo definido em `ColumnSpec.dtype` é um **tipo lógico** para staging/transformação, não uma tentativa de inferir o tipo recebido naquela execução.

Tipos lógicos conhecidos:

```text
string
integer
bigint
float
decimal
boolean
date
timestamp
json
```

Expressões SQL legadas, como `DECIMAL(18, 2)`, continuam aceitas por compatibilidade.

Metadados internos da Bronze:

```text
_source
_endpoint
_ingested_at
_run_id
_ingestion_key
_window_start
_window_end
_raw
_extra
```

`_raw` preserva o registro original. `_extra` preserva campos que chegaram da API mas não fazem parte do schema declarado.

## Schema drift

```text
API: A, B, C
Schema: A, B

A → coluna normal
B → coluna normal
C → _extra + warning
```

A ingestão continua. O schema não sofre `ALTER TABLE` automático para campos da API. Metadados internos da própria biblioteca podem evoluir por migração compatível.

## Retry e idempotência

Destination e StateStore podem estar em sistemas diferentes; portanto não existe uma transação distribuída universal.

A ordem é proposital:

```text
1. grava Bronze
2. confirma a transação do Destination
3. avança watermark
```

Isso impede perda silenciosa de dados. Existe, porém, uma janela em que o Destination pode confirmar e o StateStore falhar. Nesse caso, a próxima execução repete a mesma janela.

Para os adapters oficiais, o `Pipeline` calcula uma `ingestion_key` determinística usando connector + janela incremental. Assim, o retry substitui a representação anterior daquela mesma janela:

- DuckDB: `DELETE _ingestion_key` + INSERT dentro da mesma transação;
- Parquet: arquivo final determinístico por `ingestion_key`, promovido apenas no sucesso;
- Delta: predicate overwrite da mesma `_ingestion_key` em uma transação Delta.

Adapters de terceiros que implementam apenas `load()` continuam funcionando com semântica **at least once**. Para idempotência equivalente, implementam `load_with_context()`.

## Write modes

`WriteMode.APPEND` é o padrão e representa Bronze incremental.

`WriteMode.OVERWRITE` substitui todo o alvo, usando as garantias transacionais/promocionais do adapter.

A biblioteca não oferece um `MERGE` genérico: upsert exige chave de negócio e regras específicas, e não deve ser inferido automaticamente pela camada de ingestão.

## Adapters e registry

A configuração declarativa resolve adapters por um registry lazy. O core não importa dependências opcionais até que o adapter seja selecionado.

Adapters de terceiros podem registrar builders:

```python
from engineer_kit import register_destination

register_destination("my_backend", "my_package.runtime:build_destination")
```

O mesmo mecanismo existe para `StateStore` e `RunLogBackend`.

## Configuração declarativa

Exemplo conceitual:

```yaml
name: orders

connector:
  base_url: https://api.example.com/orders
  method: GET
  pagination:
    type: page
    params:
      page_size: 100
  incremental:
    mode: ingestion_date

destination:
  type: delta
  path: /lakehouse/project
  schema: bronze
  batch_size: 5000
  write_mode: append
  partition_by: []

state:
  type: auto

run_log:
  enabled: true
  type: auto

transform:
  type: none
```

`auto` resolve state/auditoria para o backend natural do destination. Os três componentes continuam configuráveis separadamente.

## Transformação

Transformação não pertence ao `Pipeline` de ingestão.

No local lab, `TransformConfig(type="dbt")` faz o `RunManager` executar `DbtRunner` **depois** que Bronze + watermark foram confirmados.

Em plataformas, o fluxo esperado é:

```text
engineer_kit → Bronze Delta/Parquet → Spark/dbt/SQL da plataforma
```

## Local UI

A UI é um **local lab**, instalada via extra e voltada a aprendizado, desenvolvimento e pequenos projetos locais. Ela usa DuckDB para oferecer navegação e execução zero-infra, mas documenta os mesmos contratos usados pelos adapters Parquet/Delta.

Ela não é um scheduler distribuído, catálogo corporativo ou substituto de Databricks/Fabric.

## Não objetivos

O projeto não pretende implementar:

- DAG scheduler distribuído;
- cluster de workers;
- engine Spark;
- catálogo corporativo;
- Data Warehouse/Lakehouse próprio;
- inferência de regras de negócio;
- conectores de banco em massa.

O foco permanece: **tornar ingestão de APIs previsível, incremental, auditável e portátil**.
