# engineer_kit

Biblioteca Python para ingestão de APIs: **conectores → DuckDB (bronze) → dbt (silver/gold)**.

O objetivo é ser a ponte fácil de usar entre uma API REST e um pipeline analítico, sem esconder o que está acontecendo por baixo: schema declarado explicitamente, incremental por watermark, tudo auditável no bronze.

## Por que existe

- **Conectores** abstraem `requests`, paginação e janela incremental — você só escreve o que muda de uma API pra outra (endpoint, auth, formato de data).
- **DuckDB** guarda a carga bruta (bronze) já desaninhada, com tudo como `VARCHAR` por padrão. Nunca quebra por schema drift: campo novo que a API mandar vai para uma coluna `_extra`, com um aviso no log — a carga nunca falha por isso.
- **dbt** faz a transformação de verdade (staging → silver → gold). O staging (cast de tipo) é gerado automaticamente a partir do schema declarado; as regras de negócio continuam sendo escritas à mão.

## Instalação

```bash
python -m venv venv
venv/Scripts/activate   # ou source venv/bin/activate no Linux/Mac
pip install -e ".[dev]"
```

## Quickstart

Veja [`examples/github_commits.py`](examples/github_commits.py) para um pipeline completo rodando contra a API pública do GitHub. Resumo:

```python
from datetime import date, timedelta

import duckdb

from engineer_kit.connectors.incremental import IncrementalMode, IncrementalStrategy
from engineer_kit.connectors.pagination import PageNumberPagination
from engineer_kit.connectors.rest import DateParams, RestConnector
from engineer_kit.http.auth import NoAuth
from engineer_kit.orchestration.pipeline import Pipeline, PipelineSource
from engineer_kit.storage.duckdb_loader import DuckDBLoader
from engineer_kit.storage.schema import EndpointSchema
from engineer_kit.storage.state_store import IngestionStateStore

conn = duckdb.connect("warehouse.duckdb")
state_store = IngestionStateStore(conn)

incremental = IncrementalStrategy(
    connector_name="github_commits",
    state_store=state_store,
    mode=IncrementalMode.DATA_DATE,
    initial_start=date.today() - timedelta(days=30),
)

connector = RestConnector(
    name="github_commits",
    base_url="https://api.github.com/repos/psf/requests/commits",
    incremental=incremental,
    pagination=PageNumberPagination(page_param="page", page_size_param="per_page", page_size=20),
    auth=NoAuth(),
    date_params=DateParams(start="since", end="until", date_format="%Y-%m-%dT%H:%M:%SZ"),
)

schema = EndpointSchema.from_names(["sha", "commit_author_name", "commit_author_date", "commit_message"])

pipeline = Pipeline(
    sources=[PipelineSource(connector=connector, schema=schema)],
    destination=DuckDBLoader(conn, schema="bronze"),
)

result = pipeline.run()
```

Depois de rodar o pipeline, gere o staging do dbt a partir do mesmo schema e rode:

```python
from engineer_kit.transform.scaffold import write_staging_scaffold
from engineer_kit.transform.dbt_runner import DbtRunner

write_staging_scaffold("dbt_project", {"github_commits": schema}, bronze_schema="bronze")
DbtRunner(project_dir="dbt_project").run()
```

## Arquitetura

```
Connector (RestConnector)
  ├─ PaginationStrategy   (Page/Offset/Cursor/NoPagination)
  ├─ IncrementalStrategy  (watermark lido/escrito na IngestionStateStore)
  └─ HttpClient           (retry+backoff, HTTPS obrigatório, auth via SecretProvider)
        ↓ extract() -> Iterator[dict]  (tudo já como string)
DuckDBLoader (implementa Destination)
  ├─ flatten_record()     (achata JSON aninhado, nomes por caminho completo)
  ├─ EndpointSchema       (schema declarado a mão — colunas fora dele vão para _extra)
  └─ bronze.<endpoint>    (tabela DuckDB: colunas do schema + _source/_endpoint/_ingested_at/_raw/_extra)
        ↓
dbt (scaffold gerado + modelos escritos a mão)
  ├─ models/staging/stg_<endpoint>.sql   (gerado: cast de tipo)
  ├─ models/silver/*.sql                 (escrito a mão: regra de negócio)
  └─ models/gold/*.sql                   (escrito a mão: desnormalização)
        ↓
Pipeline -> Scheduler (wrapper fino sobre APScheduler) / CLI (`engineer_kit run modulo:atributo`)
```

### Decisões de design que valem explicar

- **Tudo sai como string do conector.** Evita que schema drift na origem quebre a ingestão — a tipagem certa acontece no staging do dbt, de forma deliberada, não automática.
- **Schema é declarado, não inferido.** O loader nunca faz `ALTER TABLE` sozinho. Campo fora do schema declarado vira uma entrada em `_extra` (JSON) e um aviso no log — a carga nunca falha por causa disso, e cabe ao engenheiro decidir quando promover aquele campo a coluna de verdade.
- **Flatten é feito em Python, não com o `unnest` nativo do DuckDB.** O `unnest(recursive := true)` do DuckDB resolve colisão de nome de campo (ex.: `commit.author.date` vs `commit.committer.date`) por ordem de aparição, não por caminho — imprevisível. O flatten aqui sempre nomeia pelo caminho completo (`commit_author_date`), então nunca colide.
- **O watermark só avança depois que a carga tem sucesso.** `commit_watermark()` é uma chamada explícita e separada de `extract()` — uma falha no meio do caminho refaz a mesma janela no próximo run, sem duplicar nem perder dado.
- **A biblioteca não substitui orquestrador nem o dbt.** `Pipeline` é a unidade atômica que qualquer orquestrador externo (Airflow, cron, GitHub Actions) pode chamar via CLI; o `Scheduler` embutido é só para quem não tem/quer um orquestrador externo.

## Segurança

- Segredos nunca são hardcoded: passam por um `SecretProvider` (`EnvSecretProvider` por padrão).
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

Extrai commits recentes de `psf/requests`, carrega no bronze, gera o staging do dbt e materializa um modelo de silver (`commits_daily_summary`) — tudo contra a API pública do GitHub, sem precisar de token para o volume desse exemplo.

## Escopo

Focado em **ingestão de APIs**. Fontes de banco de dados e destinos de warehouse em nuvem (Redshift, Snowflake, Data Lake) foram deliberadamente deixados de fora — não por limitação técnica, mas porque juntar dados de motores de banco diferentes numa mesma query é um problema de federação de query (Trino/Presto), não algo que uma camada de abstração em Python resolve.
