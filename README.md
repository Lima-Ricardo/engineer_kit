# engineer_kit

**Reliable REST API ingestion for analytical destinations.**

`engineer_kit` handles the repetitive ingestion mechanics — HTTP, auth, pagination, incremental windows, schema drift, batching, checkpoints and audit — while letting the data platform remain the data platform.

```text
REST API
   │
   ▼
RestConnector
   │
   ├──────────► StateStore
   ▼
Destination
   │
   ▼
Bronze
   │
   ├──────────► RunLogBackend
   ▼
optional transform
```

The core is backend-agnostic. **DuckDB, Parquet, Delta Lake, dbt and the localhost UI are optional integrations.**

> Portuguese documentation starts at [PT-BR](#pt-br). Detailed architecture: [`docs/architecture.md`](docs/architecture.md). Platform guidance: [`docs/platforms.md`](docs/platforms.md).

## What the library owns

- REST extraction through `RestConnector` / `APIConnector`;
- explicit pagination strategies;
- watermark-based incremental loading;
- stable Bronze contract with `_raw` and `_extra`;
- bounded-memory batch writes;
- backend-independent `StateStore`, `Destination` and `RunLogBackend` contracts;
- deterministic ingestion identity for idempotent retries in official destinations;
- declarative YAML pipelines;
- optional dbt/local UI integrations.

## What it does not try to replace

Spark, Databricks, Microsoft Fabric, Airflow, Dagster, dbt, a Lakehouse, a warehouse, a catalog or a distributed worker system.

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

## Declarative pipeline

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

```text
extract
  ↓
Destination transaction
  ↓
StateStore checkpoint
  ↓
RunLogBackend audit
```

If destination persistence fails, the checkpoint does not advance. If destination persistence succeeds but the state checkpoint fails, the same window is retried.

Official destinations receive a deterministic `ingestion_key` for that connector/window, so the retry replaces the previous representation of the same window instead of duplicating it:

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
- live execution logs;
- DuckDB data browser;
- dbt model view;
- visual Source → State → Destination → Transform flow;
- architecture/documentation pages explaining the same core contracts.

The visual editor deliberately targets the DuckDB local runtime. Parquet/Delta pipelines use the same Python/YAML contracts and are documented inside the UI.

## Databricks / Microsoft Fabric / Lakehouse

The intended platform boundary is:

```text
API → engineer_kit → Delta/Parquet Bronze → platform Spark/dbt/SQL
```

The library does not start or replace Spark. Run the Python ingestion from the platform's job/notebook/orchestrator and point the destination to a path available to that runtime.

See [`docs/platforms.md`](docs/platforms.md) for Databricks/Fabric patterns, storage options, state/audit layout and current test boundaries.

## Extending adapters

Built-in adapters are resolved lazily through a registry. Custom packages can register their own destination/state/audit builders:

```python
from engineer_kit import register_destination

register_destination("company_lake", "company_ingestion.runtime:build_destination")
```

Equivalent functions exist for state and audit backends.

## Security

- HTTPS is enforced by the HTTP client unless explicitly configured otherwise;
- API credentials are resolved through `SecretProvider` implementations;
- the UI never asks users to paste secret values into pipeline YAML;
- use managed/workload identity, environment variables or the platform's secret manager for Lakehouse credentials;
- SQL identifiers are validated before use.

## Tests and CI

CI validates:

- core import with no DuckDB/PyArrow/Delta installed;
- Python 3.10 / 3.11 / 3.12;
- DuckDB, Parquet and Delta adapters;
- checkpoint failure and retry idempotency;
- schema drift and batch behavior;
- local UI;
- Ruff, Bandit and dependency audit;
- wheel/sdist build validation.

Local Delta tests validate the Delta format and adapter contract. Cloud-specific authentication, catalog registration and workspace paths must still be verified in the target Databricks/Fabric environment.

## License

MIT — see [`LICENSE`](LICENSE).

---

# PT-BR

## O que é o engineer_kit

`engineer_kit` é uma biblioteca Python para transformar APIs REST em uma **camada de ingestão confiável e portátil**.

O objetivo não é criar um novo Airflow, Databricks ou Fabric. O objetivo é remover o código repetitivo que aparece antes da Bronze:

- requests/auth;
- paginação;
- incremental;
- watermark;
- retry;
- flattening;
- schema drift;
- batches;
- auditoria;
- persistência da Bronze.

A plataforma continua cuidando daquilo que ela já faz bem: Spark, catálogo, transformação, governança, jobs e consumo.

## Arquitetura

```text
                         engineer_kit core
                               │
          ┌────────────────────┼────────────────────┐
          │                    │                    │
     RestConnector         StateStore          Destination
          │                    │                    │
          │              ┌─────┼─────┐       ┌─────┼─────┐
          │              │     │     │       │     │     │
          │           DuckDB  File  Delta  DuckDB Parquet Delta
          │                    │                    │
          └────────────────────┴────────────────────┘
                               │
                               ▼
                             Bronze
                               │
                         transformação
                            opcional
```

Detalhes: [`docs/architecture.md`](docs/architecture.md).

## Instalação por capacidade

```bash
pip install engineer_kit             # core
pip install "engineer_kit[duckdb]"  # local sem UI/dbt
pip install "engineer_kit[parquet]" # arquivos Bronze
pip install "engineer_kit[delta]"   # Lakehouse Delta
pip install "engineer_kit[local]"   # DuckDB + UI + dbt
```

Nenhuma dessas integrações é necessária para importar o core.

## DuckDB continua fazendo sentido

Sim — como **adapter zero-infra**.

```text
API → engineer_kit → DuckDB → dbt
```

É ótimo para desenvolvimento, aprendizado, CI e projetos locais. Ele deixou de ser uma premissa arquitetural.

## Em plataforma

```text
API → engineer_kit → Delta Bronze → Databricks/Fabric/Spark/dbt/SQL
```

ou:

```text
API → engineer_kit → Parquet Bronze → lake/filesystem montado
```

O mesmo `RestConnector` e `Pipeline` continuam válidos. O que muda é o adapter.

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

`auto` usa o backend natural do destination, mas cada parte pode ser configurada independentemente.

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

O watermark só avança depois da transação da Bronze.

Os adapters oficiais também tornam o retry da mesma janela idempotente usando `_ingestion_key`, reduzindo o risco clássico de duplicação quando a escrita terminou mas a persistência do checkpoint falhou.

## Interface localhost

A interface é opcional e propositalmente local:

```bash
pip install "engineer_kit[local]"
engineer_kit ui --workspace .
```

Ela serve para aprender, montar e observar pipelines. A tela de arquitetura mostra `RestConnector`, `StateStore`, `Destination`, `RunLogBackend`, tipos lógicos, retry e dbt.

## Transformação

`Pipeline` termina na ingestão. dbt é uma integração pós-ingestão no local lab. Em ambientes robustos, a transformação pode ser executada pela própria plataforma.

## Escopo

O foco continua sendo **API ingestion**. A biblioteca não quer se transformar em um novo orquestrador, Lakehouse ou framework de infraestrutura.

## Desenvolvimento

```bash
pip install -e ".[dev,all]"
pytest -q
ruff check src tests
```

O CI também verifica que o core continua importável sem os backends opcionais.
