"""Exemplo end-to-end real: conector do GitHub (commits) -> DuckDB
(bronze) -> dbt (silver).

Roda contra a API publica do GitHub de verdade. Sem token, o rate
limit e 60 requisicoes/hora — suficiente para este exemplo. Para mais,
defina GITHUB_TOKEN no ambiente e troque `auth=NoAuth()` por
`auth=BearerAuth(EnvSecretProvider(), "GITHUB_TOKEN")` abaixo.

Rodar a partir da raiz do projeto:
    venv/Scripts/python.exe examples/github_commits.py
"""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import duckdb

from engineer_kit import (
    ColumnSpec,
    DateParams,
    DbtRunner,
    Destination,
    DuckDBLoader,
    EndpointSchema,
    IncrementalMode,
    IngestionStateStore,
    NoAuth,
    PageNumberPagination,
    Pipeline,
    PipelineSource,
    RestConnector,
    RunLogStore,
    write_staging_scaffold,
)

DB_PATH = "warehouse.duckdb"
OWNER, REPO = "psf", "requests"

# Schema declarado a mao: a maioria fica VARCHAR (padrao), duas colunas
# de data ganham override de tipo porque sabemos que o dbt vai conseguir
# fazer o CAST direto (testado: DuckDB aceita ISO8601 com "Z" em CAST).
COMMITS_SCHEMA = EndpointSchema(
    columns=[
        ColumnSpec("sha"),
        ColumnSpec("commit_author_name"),
        ColumnSpec("commit_author_email"),
        ColumnSpec("commit_author_date", dtype="TIMESTAMP"),
        ColumnSpec("commit_committer_name"),
        ColumnSpec("commit_committer_email"),
        ColumnSpec("commit_committer_date", dtype="TIMESTAMP"),
        ColumnSpec("commit_message"),
        ColumnSpec("parents"),
        ColumnSpec("html_url"),
    ]
)


def build_pipeline(conn: duckdb.DuckDBPyConnection) -> Pipeline:
    connector = RestConnector(
        name="github_commits",
        base_url=f"https://api.github.com/repos/{OWNER}/{REPO}/commits",
        state_store=IngestionStateStore(conn),
        incremental_mode=IncrementalMode.DATA_DATE,
        initial_start=date.today() - timedelta(days=30),
        date_field="commit.author.date",  # caminho na resposta bruta da API, antes do flatten
        pagination=PageNumberPagination(page_size=20),
        method="GET",
        auth=NoAuth(),
        date_params=DateParams(start="since", end="until", date_format="%Y-%m-%dT%H:%M:%SZ"),
    )
    destination: Destination = DuckDBLoader(conn, schema="bronze", batch_size=1000)
    return Pipeline(
        sources=[PipelineSource(connector=connector, schema=COMMITS_SCHEMA)],
        destination=destination,
        run_log_store=RunLogStore(conn),
    )


def main() -> None:
    conn = duckdb.connect(DB_PATH)
    pipeline = build_pipeline(conn)
    result = pipeline.run()
    for step in result.steps:
        print(f"[extract+load] {step.connector_name}: {step.rows_loaded} linha(s) -- erro: {step.error}")
    conn.close()  # fecha antes do dbt abrir o mesmo arquivo, DuckDB nao aceita dois writers

    written = write_staging_scaffold(
        "dbt_project", {"github_commits": COMMITS_SCHEMA}, bronze_schema="bronze"
    )
    print("[dbt scaffold] arquivos gerados:", written)

    dbt = DbtRunner(
        project_dir="dbt_project",
        target="dev",
        env={"ENGINEER_KIT_DUCKDB_PATH": str(Path(DB_PATH).resolve())},
    )
    dbt_result = dbt.run()
    print("[dbt run] sucesso:", dbt_result.success)
    if not dbt_result.success:
        print(dbt_result.output)


if __name__ == "__main__":
    main()
