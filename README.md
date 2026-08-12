# engineer_kit

Biblioteca Python para ingestão de APIs: **conectores → DuckDB (bronze) → dbt (silver/gold)**.

O objetivo é ser a ponte fácil de usar entre uma API REST e um pipeline analítico, sem esconder o que está acontecendo por baixo: schema declarado explicitamente, incremental por watermark, gravação em blocos, tudo auditável no bronze.

## Por que existe

- **Conectores** (`APIConnector`/`RestConnector`) abstraem `requests`, paginação, método HTTP e janela incremental — você só escreve o que muda de uma API pra outra (endpoint, auth, formato de data).
- **DuckDB** guarda a carga bruta (bronze) já desaninhada, com tudo como `VARCHAR` por padrão. Nunca quebra por schema drift: campo novo que a API mandar vai para uma coluna `_extra`, com aviso no log — a carga nunca falha por isso. A gravação acontece em blocos, não tudo de uma vez.
- **dbt** faz a transformação de verdade (staging → silver → gold). O staging (cast de tipo) é gerado automaticamente a partir do schema declarado; as regras de negócio continuam sendo escritas à mão.
- **Log visual + tabela de execução**: cada carga mostra uma barra de progresso no terminal e registra início/fim/status/quantidade em `_meta.run_log` no próprio DuckDB — consultável pelo dbt como qualquer outra fonte.

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
    Pipeline, PipelineSource, RestConnector, RunLogStore,
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
    sources=[PipelineSource(connector=connector, schema=schema)],
    destination=DuckDBLoader(conn, schema="bronze", batch_size=1000),  # 1000 registros por bloco
    run_log_store=RunLogStore(conn),  # opcional: registra cada execucao em _meta.run_log
)

result = pipeline.run()
```

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

Extrai commits recentes de `psf/requests`, carrega no bronze em blocos, gera o staging do dbt, materializa um modelo de silver (`commits_daily_summary`) e registra a execução em `_meta.run_log` — tudo contra a API pública do GitHub, sem precisar de token para o volume desse exemplo.

## Escopo

Focado em **ingestão de APIs**. Fontes de banco de dados e destinos de warehouse em nuvem (Redshift, Snowflake, Data Lake) foram deliberadamente deixados de fora — não por limitação técnica, mas porque juntar dados de motores de banco diferentes numa mesma query é um problema de federação de query (Trino/Presto), não algo que uma camada de abstração em Python resolve.
