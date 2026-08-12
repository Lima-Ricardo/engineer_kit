# engineer_kit

*[Português abaixo](#engineer_kit-pt-br) / Portuguese version below.*

Python library for API ingestion: **Python (extraction) + dbt (transformation)**, both executed on top of DuckDB — you write connector config and dbt models, never raw DuckDB SQL yourself.

The goal is to be the easy-to-use bridge between a REST API and an analytics pipeline, without hiding what's happening underneath: explicitly declared schema, watermark-based incremental loading, batched writes, everything auditable in bronze.

## Why it exists

- **Connectors** (`APIConnector`/`RestConnector`) abstract `requests`, pagination, HTTP method and the incremental window — you only write what changes from one API to another (endpoint, auth, date format).
- **Python writes the raw load (bronze)**, already flattened, with everything as `VARCHAR` by default, using DuckDB as the execution engine underneath (never exposed directly — you configure `DuckDBLoader`, not raw SQL). It never breaks on schema drift: any new field the API sends goes into an `_extra` column, with a log warning — the load never fails because of it. Writes happen in batches, not all at once.
- **dbt does the real transformation** (staging → silver → gold), also running on DuckDB via the `dbt-duckdb` adapter. Staging (type casting) is generated automatically from the declared schema; business rules are still written by hand — dbt is the only place transformation logic lives.
- **Visual log + run table**: every load shows a terminal progress bar and records start/end/status/row count in `_meta.run_log` inside DuckDB itself — queryable from dbt like any other source.

DuckDB itself is never a third thing you manage — it's the engine both Python and dbt run on, invisible the same way `libpq` is invisible when you say "I use Postgres."

## Installation

```bash
python -m venv venv
venv/Scripts/activate   # or source venv/bin/activate on Linux/Mac
pip install -e ".[dev]"
```

License: MIT (see [`LICENSE`](LICENSE)).

## Quickstart

Import directly from the package — no need to know which submodule each class lives in, `import pandas as pd`-style:

```python
from engineer_kit import (
    ColumnSpec, DateParams, DuckDBLoader, EndpointSchema, IncrementalMode,
    IngestionStateStore, NoAuth, PageNumberPagination,
    Pipeline, RestConnector,
)
```

See [`examples/github_commits.py`](examples/github_commits.py) for a full pipeline running against the public GitHub API. Summary:

```python
from datetime import date, timedelta
import duckdb

conn = duckdb.connect("warehouse.duckdb")

connector = RestConnector(
    name="github_commits",
    base_url="https://api.github.com/repos/psf/requests/commits",
    state_store=IngestionStateStore(conn),  # the incremental strategy is built automatically from "name" above
    incremental_mode=IncrementalMode.DATA_DATE,
    initial_start=date.today() - timedelta(days=30),
    date_field="commit.author.date",  # date field in the raw API JSON (required in DATA_DATE mode)
    pagination=PageNumberPagination(page_size=20),  # every other parameter already has a sensible default
    method="GET",  # required: GET or POST, no implicit default
    auth=NoAuth(),
    date_params=DateParams(start="since", end="until", date_format="%Y-%m-%dT%H:%M:%SZ"),
)

schema = EndpointSchema.from_names(["sha", "commit_author_name", "commit_author_date", "commit_message"])

pipeline = Pipeline(
    connector=connector,
    schema=schema,
    destination=DuckDBLoader(conn, schema="bronze", batch_size=1000),  # 1000 records per batch
    # run_log=True is the default: records each run in _meta.run_log automatically
)

result = pipeline.run()
```

For more than one connector in the same pipeline, use `sources=[PipelineSource(connector=..., schema=...), ...]` instead of `connector=`/`schema=`.

After running the pipeline, generate the dbt staging from the same schema and run it:

```python
from engineer_kit import write_staging_scaffold, DbtRunner

write_staging_scaffold("dbt_project", {"github_commits": schema}, bronze_schema="bronze")
DbtRunner(project_dir="dbt_project").run()
```

## Pagination

`pagination` is always required when creating a connector — there's no implicit type "under the hood," because this changes from API to API. `STANDARD_PAGINATION_TYPES` lists every standard type already supported:

| Type | Class | When to use |
|---|---|---|
| `page` | `PageNumberPagination` | `?page=1&per_page=100` |
| `offset` | `OffsetPagination` | `?offset=0&limit=100` |
| `cursor` | `CursorPagination` | cursor returned in the response body |
| `link_header` | `LinkHeaderPagination` | HTTP `Link` header (RFC 5988) — GitHub, Stripe |
| `next_url` | `NextUrlPagination` | full URL of the next page in a body field |
| `none` | `NoPagination` | API returns everything in a single response |

Every strategy already has sensible defaults for the most common parameter names — you'll usually only tune `page_size`.

## HTTP method

`method` is also required (`"GET"` or `"POST"`, validated) — no implicit default. On `POST`, the payload (date + pagination params) goes as a JSON body instead of a query string, for APIs that require search via `POST`.

## Incremental: `date_field`

Under `incremental_mode=IncrementalMode.DATA_DATE`, `date_field` is required: it's the dot-separated path to the date field **in the raw API JSON**, before flattening — e.g. `"commit.author.date"`. Note the separator here is `.`, different from the `_` used in the already-flattened column names in the schema (`commit_author_date`) — these are two different points in the pipeline (one reads the API's raw response, the other describes the already-flattened bronze table).

With `date_field` configured, the connector automatically tracks the highest date seen in each run and uses it in the watermark — no need to pass anything manually to `commit_watermark()`. Without `date_field`, `DATA_DATE` would have no way to know the data's real date and would silently fall back to the same behavior as `INGESTION_DATE`; that's why the library requires the field instead of silently ignoring it.

For non-standard cases, `date_field` also accepts a function: `date_field=lambda record: record["some_computed_field"]`.

You no longer need to build a separate `IncrementalStrategy`: `RestConnector` takes `state_store`/`incremental_mode`/`initial_start`/`date_field` directly and builds the incremental strategy internally, using the connector's own `name` — without duplicating that identifier in two places. Anyone who needs a custom `IncrementalStrategy` can still pass a ready-made one via `incremental=`.

## Architecture

Two things you write — a connector (Python) and dbt models — both executing on DuckDB, which you never touch directly:

```
Connector (RestConnector : APIConnector)
  ├─ PaginationStrategy   (page/offset/cursor/link_header/next_url/none)
  ├─ IncrementalStrategy  (watermark read/written in IngestionStateStore)
  └─ HttpClient           (retry+backoff, HTTPS required, auth via SecretProvider)
        ↓ extract() -> Iterator[dict]  (everything already as string)
DuckDBLoader (implements Destination)
  ├─ flatten_record()     (flattens nested JSON, full-path column names)
  ├─ EndpointSchema       (schema declared by hand — columns outside it go to _extra)
  ├─ writes in `batch_size` chunks (default 1000; never materializes everything in memory)
  ├─ progress bar (tqdm) + visual log (loguru) during the write
  └─ bronze.<endpoint>    (DuckDB table: schema columns + _source/_endpoint/_ingested_at/_raw/_extra)
        ↓
dbt (generated scaffold + hand-written models)
  ├─ models/staging/stg_<endpoint>.sql   (generated: type casting)
  ├─ models/silver/*.sql                 (hand-written: business rules)
  └─ models/gold/*.sql                   (hand-written: denormalization)
        ↓
Pipeline -> _meta.run_log (optional, via RunLogStore)
         -> Scheduler (thin wrapper over APScheduler) / CLI (`engineer_kit run module:attribute`)
```

### Design decisions worth explaining

- **Everything comes out of the connector as a string.** Prevents schema drift at the source from breaking ingestion — proper typing happens in dbt staging, deliberately, not automatically.
- **Schema is declared, not inferred.** The loader never runs `ALTER TABLE` on its own. A field outside the declared schema becomes an entry in `_extra` (JSON) plus a warning — the load never fails because of it.
- **Flattening is done in Python, not with DuckDB's native `unnest`.** DuckDB's `unnest(recursive := true)` resolves field-name collisions (e.g. `commit.author.date` vs `commit.committer.date`) by order of appearance, not by path — unpredictable. The flatten here always names by full path (`commit_author_date`), so it never collides.
- **`DATA_DATE` requires `date_field`.** Without it, the incremental logic has no way to know the data's real date — and before this check existed, `DATA_DATE` silently behaved exactly like `INGESTION_DATE`, because nothing extracted the record's own date. Forcing the field prevents that kind of silent bug.
- **Batched writes, not everything at once.** `DuckDBLoader` consumes the record generator in `batch_size` slices (default 1000), writing each one and releasing memory — an extraction of millions of records doesn't need to fit in memory before the first row is written.
- **The watermark only advances after the load succeeds.** `commit_watermark()` is an explicit call, separate from `extract()` — a failure partway through redoes the same window on the next run, without duplicating or losing data.
- **Method and pagination are always explicit, never implicit.** Neither has a hidden "default" behavior — it's one of the things that varies most from API to API, and an implicit choice here is an easy source of silent bugs.
- **The library doesn't replace an orchestrator or dbt.** `Pipeline` is the atomic unit that any external orchestrator (Airflow, cron, GitHub Actions) can call via the CLI; the built-in `Scheduler` is only for those without (or who don't want) an external orchestrator.

## Visual log and run table

During writes, the terminal shows a progress bar (tqdm, with elapsed time and record count) and a narrative log (loguru) of each connector's start/end/success/error. If a `RunLogStore` is passed to `Pipeline`, the same information — connector, start, end, status, row count and new columns outside the schema — gets recorded in `_meta.run_log` in DuckDB, queryable from dbt.

This visual log (loguru) is separate from the library's internal technical logging (standard `logging`, used in `HttpClient`/auditing) — they serve different purposes: one is for human reading in the terminal, the other is what the security tests audit (see below).

## Security

- Secrets always go through a `SecretProvider`: `EnvSecretProvider` (env vars), `FileSecretProvider` (reads from a file — one file per secret in a directory, Docker/Kubernetes-secrets style, or a single file; re-reads on every call, so rotating the file takes effect without restarting the process), or `StaticSecretProvider` (hardcoded in memory — a deliberate choice for internal scripts/controlled environments, never commit a real secret to a shared repo).
- HTTPS is required by default (`allow_http=True` must be explicit).
- Schema/table/column names are validated against a safe identifier pattern before entering any dynamically-built SQL — including files generated for dbt.
- No log or error message ever exposes a secret, even when authentication is done via a query param: the URL used in logs and exception messages is always the pre-auth version, or redacted.
- See `tests/test_http_client.py` for the tests that lock in this behavior.

## Running the tests

```bash
venv/Scripts/python.exe -m pytest tests/ -v
```

## Running the end-to-end example

```bash
venv/Scripts/python.exe examples/github_commits.py
```

Extracts recent commits from `psf/requests`, loads them into bronze in batches, generates dbt staging, materializes a silver model (`commits_daily_summary`) and records the run in `_meta.run_log` — all against the public GitHub API, no token needed for this example's volume.

## Scope

Focused on **API ingestion**. Database sources and cloud warehouse destinations (Redshift, Snowflake, Data Lake) were deliberately left out — not due to a technical limitation, but because joining data across different database engines in a single query is a query-federation problem (Trino/Presto), not something a Python abstraction layer solves.

---

<a name="engineer_kit-pt-br"></a>
# engineer_kit (PT-BR)

*[English version above](#engineer_kit).*

Biblioteca Python para ingestão de APIs: **Python (extração) + dbt (transformação)**, os dois executando em cima do DuckDB — você escreve configuração de conector e modelo dbt, nunca SQL de DuckDB direto.

O objetivo é ser a ponte fácil de usar entre uma API REST e um pipeline analítico, sem esconder o que está acontecendo por baixo: schema declarado explicitamente, incremental por watermark, gravação em blocos, tudo auditável no bronze.

## Por que existe

- **Conectores** (`APIConnector`/`RestConnector`) abstraem `requests`, paginação, método HTTP e janela incremental — você só escreve o que muda de uma API pra outra (endpoint, auth, formato de data).
- **O Python grava a carga bruta (bronze)**, já desaninhada, com tudo como `VARCHAR` por padrão, usando o DuckDB como motor de execução por baixo (nunca exposto direto — você configura o `DuckDBLoader`, não escreve SQL). Nunca quebra por schema drift: campo novo que a API mandar vai para uma coluna `_extra`, com aviso no log — a carga nunca falha por isso. A gravação acontece em blocos, não tudo de uma vez.
- **O dbt faz a transformação de verdade** (staging → silver → gold), também rodando em cima do DuckDB via o adapter `dbt-duckdb`. O staging (cast de tipo) é gerado automaticamente a partir do schema declarado; as regras de negócio continuam sendo escritas à mão — o dbt é o único lugar onde mora lógica de transformação.
- **Log visual + tabela de execução**: cada carga mostra uma barra de progresso no terminal e registra início/fim/status/quantidade em `_meta.run_log` no próprio DuckDB — consultável pelo dbt como qualquer outra fonte.

O DuckDB nunca é uma terceira coisa que você gerencia — é o motor sobre o qual Python e dbt rodam, invisível do mesmo jeito que o `libpq` é invisível quando você diz "eu uso Postgres".

## Instalação

```bash
python -m venv venv
venv/Scripts/activate   # ou source venv/bin/activate no Linux/Mac
pip install -e ".[dev]"
```

Licença: MIT (ver [`LICENSE`](LICENSE)).

## Quickstart

Import direto do pacote — sem precisar saber em qual submódulo cada classe mora, no estilo `import pandas as pd`:

```python
from engineer_kit import (
    ColumnSpec, DateParams, DuckDBLoader, EndpointSchema, IncrementalMode,
    IngestionStateStore, NoAuth, PageNumberPagination,
    Pipeline, RestConnector,
)
```

Veja [`examples/github_commits.py`](examples/github_commits.py) para um pipeline completo rodando contra a API pública do GitHub. Resumo:

```python
from datetime import date, timedelta
import duckdb

conn = duckdb.connect("warehouse.duckdb")

connector = RestConnector(
    name="github_commits",
    base_url="https://api.github.com/repos/psf/requests/commits",
    state_store=IngestionStateStore(conn),  # o incremental e montado automaticamente a partir do "name" acima
    incremental_mode=IncrementalMode.DATA_DATE,
    initial_start=date.today() - timedelta(days=30),
    date_field="commit.author.date",  # campo de data no JSON bruto da API (obrigatorio em modo DATA_DATE)
    pagination=PageNumberPagination(page_size=20),  # todos os outros parametros ja tem padrao sensato
    method="GET",  # obrigatorio: GET ou POST, sem valor implicito
    auth=NoAuth(),
    date_params=DateParams(start="since", end="until", date_format="%Y-%m-%dT%H:%M:%SZ"),
)

schema = EndpointSchema.from_names(["sha", "commit_author_name", "commit_author_date", "commit_message"])

pipeline = Pipeline(
    connector=connector,
    schema=schema,
    destination=DuckDBLoader(conn, schema="bronze", batch_size=1000),  # 1000 registros por bloco
    # run_log=True e o padrao: registra cada execucao em _meta.run_log automaticamente
)

result = pipeline.run()
```

Para mais de um conector no mesmo pipeline, use `sources=[PipelineSource(connector=..., schema=...), ...]` em vez de `connector=`/`schema=`.

Depois de rodar o pipeline, gere o staging do dbt a partir do mesmo schema e rode:

```python
from engineer_kit import write_staging_scaffold, DbtRunner

write_staging_scaffold("dbt_project", {"github_commits": schema}, bronze_schema="bronze")
DbtRunner(project_dir="dbt_project").run()
```

## Paginação

`pagination` é sempre obrigatório na criação de um conector — não existe um tipo "por baixo dos panos", porque isso muda de API para API. `STANDARD_PAGINATION_TYPES` lista todo tipo padrão já suportado:

| Tipo | Classe | Quando usar |
|---|---|---|
| `page` | `PageNumberPagination` | `?page=1&per_page=100` |
| `offset` | `OffsetPagination` | `?offset=0&limit=100` |
| `cursor` | `CursorPagination` | cursor devolvido no corpo da resposta |
| `link_header` | `LinkHeaderPagination` | header HTTP `Link` (RFC 5988) — GitHub, Stripe |
| `next_url` | `NextUrlPagination` | URL completa da próxima página num campo do corpo |
| `none` | `NoPagination` | API devolve tudo numa única resposta |

Cada estratégia já tem valores padrão sensatos para os nomes de parâmetro mais comuns — normalmente você só ajusta o `page_size`.

## Método HTTP

`method` também é obrigatório (`"GET"` ou `"POST"`, validado) — sem default implícito. Em `POST`, o payload (params de data + paginação) vai como corpo JSON em vez de query string, para as APIs que exigem busca via `POST`.

## Incremental: `date_field`

Em `incremental_mode=IncrementalMode.DATA_DATE`, `date_field` é obrigatório: é o caminho (separado por ponto) até o campo de data **no JSON bruto da API**, antes do flatten — ex.: `"commit.author.date"`. Note que o separador aqui é `.`, diferente do `_` usado nos nomes de coluna já achatados no schema (`commit_author_date`) — são dois pontos diferentes do pipeline (um lê a resposta crua da API, o outro descreve a tabela já achatada no bronze).

Com `date_field` configurado, o conector rastreia automaticamente a maior data vista em cada execução e usa isso no watermark — sem precisar passar nada manualmente em `commit_watermark()`. Sem `date_field`, `DATA_DATE` não teria como saber a data real dos dados e cairia para o mesmo comportamento de `INGESTION_DATE`; por isso a biblioteca exige o campo em vez de silenciosamente ignorá-lo.

Para casos fora do padrão, `date_field` também aceita uma função: `date_field=lambda record: record["algum_campo_calculado"]`.

Não precisa mais montar um `IncrementalStrategy` separado: `RestConnector` recebe `state_store`/`incremental_mode`/`initial_start`/`date_field` diretamente e monta o incremental internamente, usando o próprio `name` do conector — sem duplicar esse identificador em dois lugares. Quem precisar de um `IncrementalStrategy` customizado ainda pode passar um pronto via `incremental=`.

## Arquitetura

Duas coisas que você escreve — um conector (Python) e modelos dbt — os dois executando em cima do DuckDB, que você nunca toca direto:

```
Connector (RestConnector : APIConnector)
  ├─ PaginationStrategy   (page/offset/cursor/link_header/next_url/none)
  ├─ IncrementalStrategy  (watermark lido/escrito na IngestionStateStore)
  └─ HttpClient           (retry+backoff, HTTPS obrigatório, auth via SecretProvider)
        ↓ extract() -> Iterator[dict]  (tudo já como string)
DuckDBLoader (implementa Destination)
  ├─ flatten_record()     (achata JSON aninhado, nomes por caminho completo)
  ├─ EndpointSchema       (schema declarado a mão — colunas fora dele vão para _extra)
  ├─ grava em blocos de `batch_size` (padrão 1000; nunca materializa tudo em memória)
  ├─ barra de progresso (tqdm) + log visual (loguru) durante a gravação
  └─ bronze.<endpoint>    (tabela DuckDB: colunas do schema + _source/_endpoint/_ingested_at/_raw/_extra)
        ↓
dbt (scaffold gerado + modelos escritos a mão)
  ├─ models/staging/stg_<endpoint>.sql   (gerado: cast de tipo)
  ├─ models/silver/*.sql                 (escrito a mão: regra de negócio)
  └─ models/gold/*.sql                   (escrito a mão: desnormalização)
        ↓
Pipeline -> _meta.run_log (opcional, via RunLogStore)
         -> Scheduler (wrapper fino sobre APScheduler) / CLI (`engineer_kit run modulo:atributo`)
```

### Decisões de design que valem explicar

- **Tudo sai como string do conector.** Evita que schema drift na origem quebre a ingestão — a tipagem certa acontece no staging do dbt, de forma deliberada, não automática.
- **Schema é declarado, não inferido.** O loader nunca faz `ALTER TABLE` sozinho. Campo fora do schema declarado vira uma entrada em `_extra` (JSON) e um aviso — a carga nunca falha por causa disso.
- **Flatten é feito em Python, não com o `unnest` nativo do DuckDB.** O `unnest(recursive := true)` do DuckDB resolve colisão de nome de campo por ordem de aparição, não por caminho — imprevisível. O flatten aqui sempre nomeia pelo caminho completo (`commit_author_date`), então nunca colide.
- **`DATA_DATE` exige `date_field`.** Sem isso, o incremental não tem como saber a data real dos dados — e antes dessa checagem existir, `DATA_DATE` silenciosamente se comportava igual a `INGESTION_DATE`, porque nada extraía a data do registro. Forçar o campo evita esse tipo de bug silencioso.
- **Gravação em blocos, não tudo de uma vez.** `DuckDBLoader` consome o gerador de registros em fatias de `batch_size` (padrão 1000), gravando cada uma e liberando a memória — uma extração de milhões de registros não precisa caber tudo em memória antes da primeira linha ser gravada.
- **O watermark só avança depois que a carga tem sucesso.** `commit_watermark()` é uma chamada explícita e separada de `extract()` — uma falha no meio do caminho refaz a mesma janela no próximo run, sem duplicar nem perder dado.
- **Método e paginação são sempre explícitos, nunca implícitos.** Nenhum dos dois tem comportamento "por padrão" escondido — é um dos pontos que mais varia de API para API, e uma escolha implícita aqui é fonte fácil de bug silencioso.
- **A biblioteca não substitui orquestrador nem o dbt.** `Pipeline` é a unidade atômica que qualquer orquestrador externo (Airflow, cron, GitHub Actions) pode chamar via CLI; o `Scheduler` embutido é só para quem não tem/quer um orquestrador externo.

## Log visual e tabela de execução

Durante a gravação, o terminal mostra uma barra de progresso (tqdm, com cronômetro e contagem de registros) e um log narrativo (loguru) de início/fim/sucesso/erro de cada conector. Se um `RunLogStore` for passado ao `Pipeline`, a mesma informação — conector, início, fim, status, quantidade de registros e colunas novas fora do schema — fica registrada em `_meta.run_log` no DuckDB, consultável pelo dbt.

Esse log visual (loguru) é separado do logging técnico interno da biblioteca (`logging` padrão, usado em `HttpClient`/auditoria) — são propósitos diferentes: um é para leitura humana no terminal, o outro é o que os testes de segurança auditam (ver seção abaixo).

## Segurança

- Segredos sempre passam por um `SecretProvider`: `EnvSecretProvider` (variável de ambiente), `FileSecretProvider` (lê de um arquivo — um arquivo por segredo numa pasta, estilo Docker/Kubernetes secrets, ou um arquivo único; relê a cada chamada, então rotacionar o arquivo vale sem reiniciar o processo), ou `StaticSecretProvider` (hardcoded em memória — escolha deliberada para script interno/ambiente controlado, nunca comite um segredo real num repositório compartilhado).
- HTTPS é obrigatório por padrão (`allow_http=True` precisa ser explícito).
- Nomes de schema/tabela/coluna são validados contra um identificador seguro antes de entrar em qualquer SQL montado dinamicamente — inclusive nos arquivos gerados para o dbt.
- Nenhum log ou mensagem de erro expõe segredo, mesmo quando a autenticação é feita via query param: a URL usada em log e em mensagens de exceção é sempre a versão pré-autenticação ou redigida.
- Ver `tests/test_http_client.py` para os testes que travam esses comportamentos.

## Rodando os testes

```bash
venv/Scripts/python.exe -m pytest tests/ -v
```

## Rodando o exemplo end-to-end

```bash
venv/Scripts/python.exe examples/github_commits.py
```

Extrai commits recentes de `psf/requests`, carrega no bronze em blocos, gera o staging do dbt, materializa um modelo de silver (`commits_daily_summary`) e registra a execução em `_meta.run_log` — tudo contra a API pública do GitHub, sem precisar de token para o volume desse exemplo.

## Escopo

Focado em **ingestão de APIs**. Fontes de banco de dados e destinos de warehouse em nuvem (Redshift, Snowflake, Data Lake) foram deliberadamente deixados de fora — não por limitação técnica, mas porque juntar dados de motores de banco diferentes numa mesma query é um problema de federação de query (Trino/Presto), não algo que uma camada de abstração em Python resolve.
