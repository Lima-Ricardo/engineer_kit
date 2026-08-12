# engineer_kit

**Reliable, streaming-first REST API ingestion for analytical destinations.**

`engineer_kit` handles repetitive ingestion mechanics — HTTP, auth, pagination, incremental windows, bounded-memory batching, checkpoints, schema drift and audit — while letting the data platform remain the data platform.

```text
REST API
   │
   ▼
RestConnector
   │
   ▼
ExtractionSession  ─────► batches (default 25,000)
   │
   ├──────────► StateStore / checkpoint
   │
   ▼
Destination (managed mode)  OR  user code (embedded mode)
   │
   ▼
Bronze / Spark / Pandas / Polars / Arrow
   │
   ├──────────► RunLogBackend (managed mode)
   ▼
optional transform
```

The core is backend-agnostic. **DuckDB, Parquet, Delta Lake, dbt and the localhost UI are optional integrations.**

> Portuguese documentation starts at [PT-BR](#pt-br). Detailed architecture: [`docs/architecture.md`](docs/architecture.md). Streaming/batching: [`docs/streaming.md`](docs/streaming.md). Platform guidance: [`docs/platforms.md`](docs/platforms.md).

## What the library owns

- platform-neutral `Connector` source contract;
- REST extraction through `RestConnector` / `APIConnector`;
- explicit pagination strategies;
- streaming-first `ExtractionSession`;
- default extraction batches of **25,000 records**;
- watermark-based incremental loading;
- stable Bronze contract with `_raw` and `_extra`;
- backend-independent `StateStore`, `Destination` and `RunLogBackend` contracts;
- deterministic checkpoint-transition identity for idempotent retries in official destinations;
- declarative YAML pipelines;
- optional dbt/local UI integrations.

## What it does not try to replace

Spark, Databricks, Microsoft Fabric, AWS, Google Cloud, Airflow, Dagster, dbt, a Lakehouse, a warehouse, a catalog or a distributed worker system.

Cloud/runtime is intentionally separate from source protocol. A REST API remains a `RestConnector` whether the code runs locally, on Databricks, Microsoft Fabric, AWS or Google Cloud.

## Built-in adapters

| Mode | Destination | State | Audit | Extra |
|---|---|---|---|---|
| local | `DuckDBDestination` | `DuckDBStateStore` | `DuckDBRunLogStore` | `engineer_kit[duckdb]` |
| files | `ParquetDestination` | `JsonFileStateStore` | `JsonLinesRunLogStore` | `engineer_kit[parquet]` |
| Lakehouse | `DeltaDestination` | `DeltaStateStore` | `DeltaRunLogStore` | `engineer_kit[delta]` |

`DuckDBLoader`, `IngestionStateStore` and `RunLogStore` remain available as compatibility names for the initial API.

## Installation

```bash
# core only — no DuckDB/PyArrow/Delta/dbt/UI
pip install engineer_kit

# choose only what the runtime needs
pip install "engineer_kit[duckdb]"
pip install "engineer_kit[parquet]"
pip install "engineer_kit[delta]"
pip install "engineer_kit[platform]"   # Delta/Lakehouse profile
pip install "engineer_kit[dbt]"
pip install "engineer_kit[ui]"
pip install "engineer_kit[local]"      # DuckDB + dbt + localhost UI
```

For repository development:

```bash
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -e ".[dev,all]"
pytest -q
```

## Streaming-first extraction

The recommended extraction API does **not** materialize the complete API response by default:

```python
run = connector.extract_incremental()

for batch in run:
    process(batch)

run.commit()
```

Normal iteration yields `list[dict]` batches. The official default is:

```python
DEFAULT_EXTRACTION_BATCH_SIZE = 25_000
```

`collect()` is deliberately explicit:

```python
records = run.collect()  # complete extraction in RAM; use for small datasets
```

The session is single-pass and the checkpoint cannot be committed until the stream has been consumed completely.

### Three independent sizes

```text
API pagination size
        ↓
Extraction batch size        default 25,000
        ↓
Destination write batch size adapter-specific
```

For example, an API that returns 1,000 records per page may fill one default extraction batch after roughly 25 pages. This is an illustration, not a coupling: pagination follows the API contract while extraction batching limits the consumer-facing in-memory unit.

Rate limits (`429`, `Retry-After`, backoff, requests/minute) belong to the HTTP/retry layer, not to `ExtractionSession`.

See [`docs/streaming.md`](docs/streaming.md).

## Managed mode

Use `Pipeline` when `engineer_kit` should own Bronze persistence:

```text
API
 ↓
ExtractionSession record stream
 ↓
Destination transaction
 ↓
StateStore checkpoint
 ↓
RunLogBackend audit
```

The destination may internally split the stream into smaller write batches. For example, a 25,000-record extraction batch and a 5,000-record destination batch are separate concerns.

## Embedded mode

Inside Databricks, Microsoft Fabric or any Python runtime, you can use only extraction + pagination + incremental state and keep Spark/persistence under application control:

```python
run = connector.extract_incremental()

for batch in run:
    df = spark.createDataFrame(batch)
    # project-specific transformations and persistence
    persist(df)

run.commit()
```

The safe order is:

```text
read checkpoint
      ↓
extract batches
      ↓
user processing / persistence
      ↓
success
      ↓
run.commit()
```

If downstream processing fails, do not commit; the watermark remains unchanged. For very large Spark workloads, staging batches to Parquet/Delta and then letting Spark read them natively may be more efficient than creating many small DataFrames from Python objects.

## Declarative pipeline

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

columns:
  - name: id
    dtype: bigint
  - name: created_at
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

transform:
  type: none
```

For Parquet/Delta, no database runtime is required:

```python
from engineer_kit import build_pipeline, load_pipeline_config

config = load_pipeline_config("pipelines/orders.yaml")
result = build_pipeline(config).run()

if not result.success:
    raise RuntimeError(result.steps)
```

DuckDB uses an existing connection supplied by the caller:

```python
import duckdb
from engineer_kit import build_pipeline, load_pipeline_config

config = load_pipeline_config("pipelines/orders.yaml")
conn = duckdb.connect("warehouse.duckdb")
result = build_pipeline(config, conn).run()
conn.close()
```

Or execute YAML directly from the CLI:

```bash
engineer_kit run-config pipelines/orders.yaml
engineer_kit adapters
```

`run-config` opens `destination.path` for DuckDB (falling back to `warehouse.duckdb`) and needs no database runtime object for Parquet/Delta.

## Bronze contract

Official destinations persist declared API fields as strings/null. The declared `dtype` is a **logical analytical type** used by staging/transform tooling rather than a type inferred from every API response.

Known logical types:

```text
string · integer · bigint · float · decimal · boolean · date · timestamp · json
```

Every Bronze row also carries:

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

Unexpected API fields are preserved in `_extra` and reported; they do not trigger automatic source-schema mutations.

## Incremental reliability

Managed mode:

```text
extract
  ↓
Destination transaction
  ↓
StateStore checkpoint
  ↓
RunLogBackend audit
```

Embedded mode:

```text
extract batches
  ↓
user processing/persistence
  ↓
ExtractionSession.commit()
```

If destination/downstream persistence fails, the checkpoint does not advance. If managed destination persistence succeeds but the state checkpoint fails, the same checkpoint transition is retried.

Official destinations receive a deterministic `ingestion_key` derived from **connector + incremental window + checkpoint-before**. A retry after a state failure therefore gets the same key and replaces the previous representation instead of duplicating it. Once the checkpoint succeeds, the checkpoint-before changes, so a later successful run receives another key even if it occurs on the same calendar day.

- DuckDB: transactional delete/rewrite of that ingestion key;
- Parquet: deterministic final file promoted only after success;
- Delta: predicate overwrite in a Delta transaction.

Third-party destinations implementing only `Destination.load()` remain compatible with at-least-once semantics. They can implement `load_with_context()` to use the same retry identity.

## Write modes

`append` is the default Bronze mode. `overwrite` replaces the target using the adapter's transactional/staging guarantees.

A generic merge/upsert is intentionally not guessed by the ingestion layer because it requires explicit business keys and semantics.

## Schema drift

```text
Declared: A, B
API sends: A, B, C

A → normal column
B → normal column
C → _extra + warning
```

The original record is retained in `_raw`.

## dbt

`dbt` is optional and **not part of the ingestion transaction**.

The localhost runtime executes:

```text
Pipeline.run()
   ↓
Bronze + watermark confirmed
   ↓
DbtRunner.run()
```

Generated staging models cast Bronze strings using the logical types declared in `ColumnSpec`. Business rules, joins, tests and materializations remain explicit dbt code.

## Local web UI

The web UI is a **local learning/development lab**, not a production control plane.

```bash
pip install "engineer_kit[local]"
engineer_kit ui --workspace .
```

It provides:

- pipeline form;
- API page-size, extraction-batch and destination-write-batch controls;
- live execution logs;
- DuckDB data browser;
- dbt model view;
- visual Source → Extraction → State → Destination → Transform flow;
- architecture/documentation pages explaining managed and embedded modes.

The visual editor deliberately targets the DuckDB local runtime. Parquet/Delta pipelines and platform embedded mode use the same Python/YAML contracts and are documented inside the UI.

## Databricks / Microsoft Fabric / AWS / Google Cloud

Managed platform boundary:

```text
API → engineer_kit → Delta/Parquet Bronze → platform Spark/dbt/SQL
```

Embedded platform boundary:

```text
API → engineer_kit batches → user/platform code → persistence → checkpoint
```

The library does not start or replace Spark and does not create one REST connector subclass per cloud. Run the Python extraction from the platform's job/notebook/orchestrator and use paths/storage options available to that runtime.

See [`docs/platforms.md`](docs/platforms.md) for Databricks/Fabric patterns, AWS/GCP/Azure storage boundaries, storage options, state/audit layout and current test boundaries.

## Extending adapters

Built-in adapters are resolved lazily through a registry. Custom packages can register their own destination/state/audit builders:

```python
from engineer_kit import register_destination

register_destination("company_lake", "company_ingestion.runtime:build_destination")
```

Equivalent functions exist for state and audit backends. `auto` only resolves known natural relationships (DuckDB→DuckDB, Parquet→file metadata, Delta→Delta); a custom destination must register or explicitly select compatible state/audit backends.

Custom source protocols can derive from the platform-neutral `Connector` contract without knowing the runtime or storage backend.

## Security

- HTTPS is enforced by the HTTP client unless explicitly configured otherwise;
- API credentials are resolved through `SecretProvider` implementations;
- the UI never asks users to paste secret values into pipeline YAML;
- use managed/workload identity, environment variables or the platform's secret manager for Lakehouse credentials;
- SQL identifiers are validated/quoted before dynamic identifier use;
- data values are parameterized rather than interpolated into SQL.

## Tests and CI

CI validates:

- core import with no DuckDB/PyArrow/Delta installed;
- Python 3.10 / 3.11 / 3.12;
- `ExtractionSession` 25k default, overrides, single-pass behavior and partial-checkpoint protection;
- DuckDB, Parquet and Delta adapters;
- checkpoint failure and retry idempotency;
- successful same-day runs after checkpoint advancement;
- schema drift and batch behavior;
- local UI and declarative CLI;
- dbt optional-extra smoke test;
- synthetic streaming stress tests;
- Ruff, Bandit and dependency audit;
- wheel/sdist build validation.

Local Delta tests validate the Delta format and adapter contract. Cloud-specific authentication, catalog registration and workspace paths must still be verified in the target cloud workspace.

## License

MIT — see [`LICENSE`](LICENSE).

---

# PT-BR

## O que é o engineer_kit

`engineer_kit` é uma biblioteca Python para transformar APIs REST em uma **camada de ingestão confiável, portátil e streaming-first**.

O objetivo não é criar um novo Airflow, Databricks ou Fabric. O objetivo é remover o código repetitivo que aparece antes da Bronze:

- requests/auth;
- paginação;
- incremental;
- watermark/checkpoint;
- retry;
- flattening;
- schema drift;
- batches;
- auditoria;
- persistência da Bronze quando desejada.

A plataforma continua cuidando daquilo que ela já faz bem: Spark, catálogo, transformação, governança, jobs e consumo.

## Arquitetura

```text
                         engineer_kit core
                               │
        ┌──────────────────────┼──────────────────────┐
        │                      │                      │
    Connector          ExtractionSession         StateStore
        │                      │                      │
 RestConnector          batches 25k default      checkpoint
        │                      │                      │
        └──────────────────────┼──────────────────────┘
                               │
               ┌───────────────┴───────────────┐
               │                               │
          managed mode                    embedded mode
               │                               │
         Destination                      código do usuário
      ┌────┼─────┐                   Spark/Pandas/Polars
   DuckDB Parquet Delta                        │
               │                               │
               └───────────────┬───────────────┘
                               ▼
                          dados persistidos
```

Detalhes: [`docs/architecture.md`](docs/architecture.md), [`docs/streaming.md`](docs/streaming.md) e [`docs/platforms.md`](docs/platforms.md).

## Streaming-first e default de 25.000

O caminho padrão recomendado é:

```python
run = connector.extract_incremental()

for batch in run:
    processar(batch)

run.commit()
```

Cada iteração entrega até **25.000 registros por padrão**. Isso evita transformar a memória do processo Python no buffer de toda a API.

Para datasets pequenos, existe materialização explícita:

```python
records = run.collect()
```

`collect()` traz a extração completa para RAM; por isso não é o comportamento padrão.

### Página da API, batch de extração e batch de escrita são diferentes

```text
API page size             1.000  (exemplo)
        ↓
Extraction batch         25.000  (default)
        ↓
Destination write batch   5.000  (exemplo)
```

Uma API que devolve 1.000 registros por página pode preencher aproximadamente 25 páginas para formar um batch de 25.000, mas esses valores não são acoplados. A paginação respeita a documentação da API; o extraction batch controla memória/entrega; o write batch otimiza o adapter físico.

Rate limit, `429`, `Retry-After` e backoff pertencem ao cliente HTTP/retry.

## Instalação por capacidade

```bash
pip install engineer_kit             # core
pip install "engineer_kit[duckdb]"  # local sem UI/dbt
pip install "engineer_kit[parquet]" # arquivos Bronze
pip install "engineer_kit[delta]"   # Lakehouse Delta
pip install "engineer_kit[platform]"# perfil Lakehouse/Delta
pip install "engineer_kit[local]"   # DuckDB + UI + dbt
```

Nenhuma dessas integrações é necessária para importar o core.

## DuckDB continua fazendo sentido

Sim — como **adapter zero-infra**.

```text
API → engineer_kit → DuckDB → dbt
```

É ótimo para desenvolvimento, aprendizado, CI e projetos locais. Ele deixou de ser uma premissa arquitetural.

## Managed mode em plataforma

```text
API → engineer_kit → Delta Bronze → Databricks/Fabric/Spark/dbt/SQL
```

ou:

```text
API → engineer_kit → Parquet Bronze → lake/filesystem montado
```

O mesmo `RestConnector` e `Pipeline` continuam válidos. O que muda é o adapter.

## Embedded mode em Fabric/Databricks

O usuário pode instalar a biblioteca e usar apenas extração + paginação + incremental:

```python
run = connector.extract_incremental()

for batch in run:
    df = spark.createDataFrame(batch)
    # lógica do projeto
    persistir(df)

run.commit()
```

Assim `engineer_kit` não precisa controlar o Spark nem a escrita final. O checkpoint só avança depois que o usuário confirmar que o downstream terminou com sucesso.

Para volumes muito grandes, staging em Parquet/Delta seguido de `spark.read` pode ser mais eficiente do que criar muitos DataFrames a partir de objetos Python.

## AWS e Google Cloud

Não existe necessidade de `AWSRestConnector` ou `GoogleRestConnector` quando a fonte continua sendo REST.

```text
Connector / origem
└── RestConnector

Runtime / storage
├── local
├── Databricks
├── Microsoft Fabric
├── AWS / S3
├── Google Cloud / GCS
└── Azure / ADLS / OneLake
```

Cloud-specific credentials, URIs e `storage_options` pertencem ao adapter/runtime, não ao protocolo de origem.

Veja [`docs/platforms.md`](docs/platforms.md).

## Estado incremental não depende da Bronze

O watermark fica em `StateStore`, não em `SELECT MAX(...)` sobre a tabela Bronze.

```text
StateStore
├── DuckDBStateStore
├── JsonFileStateStore
└── DeltaStateStore
```

Assim a leitura do checkpoint permanece pequena e previsível mesmo quando a Bronze cresce.

## Destination, State e Audit são separados

```yaml
destination:
  type: delta

state:
  type: auto

run_log:
  enabled: true
  type: auto
```

`auto` usa o backend natural conhecido do destination, mas cada parte pode ser configurada independentemente. Um adapter customizado não cai silenciosamente em arquivos locais: ele deve registrar ou selecionar state/audit compatíveis.

## Bronze e tipos

A Bronze prioriza captura. Campos declarados ficam em string e a tipagem analítica é explícita:

```yaml
columns:
  - name: amount
    dtype: decimal
  - name: created_at
    dtype: timestamp
```

O staging/dbt pode transformar isso no tipo físico apropriado. Se a API adicionar um campo novo, ele vai para `_extra` sem derrubar a carga.

## Confiabilidade

No managed mode, o watermark só avança depois da transação da Bronze.

No embedded mode, `ExtractionSession.commit()` exige que o stream tenha sido consumido completamente e deve ser chamado somente depois da persistência downstream.

Os adapters oficiais também tornam o retry idempotente usando `_ingestion_key`. A chave identifica a transição de checkpoint (connector + janela + checkpoint anterior): uma falha de state reutiliza a mesma chave; depois de um checkpoint bem-sucedido, uma nova execução recebe outra chave mesmo no mesmo dia.

## CLI declarativa

Além de pipelines Python, um YAML pode ser executado diretamente:

```bash
engineer_kit run-config pipelines/orders.yaml
engineer_kit adapters
```

Isso facilita jobs em Databricks/Fabric, CI e outros orquestradores sem exigir um módulo Python intermediário.

## Interface localhost

A interface é opcional e propositalmente local:

```bash
pip install "engineer_kit[local]"
engineer_kit ui --workspace .
```

Ela serve para aprender, montar e observar pipelines. A tela de arquitetura mostra `Connector`, `RestConnector`, `ExtractionSession`, `StateStore`, `Destination`, `RunLogBackend`, os três níveis de batching, tipos lógicos, retry, embedded mode e dbt.

## Transformação

`Pipeline` termina na ingestão. dbt é uma integração pós-ingestão no local lab. Em ambientes robustos, a transformação pode ser executada pela própria plataforma.

## Escopo

O foco continua sendo **API ingestion**. A biblioteca não quer se transformar em um novo orquestrador, Lakehouse ou framework de infraestrutura.

## Desenvolvimento

```bash
pip install -e ".[dev,all]"
pytest -q
ruff check src tests
bandit -q -r src/engineer_kit -ll
```

O CI também verifica o core sem backends opcionais, Python 3.10–3.12, adapters, `ExtractionSession`, UI, dbt extra, stress sintético, segurança e build do pacote.
