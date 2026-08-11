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

from engineer_kit.connectors.incremental import IncrementalMode, IncrementalStrategy
from engineer_kit.connectors.pagination import PageNumberPagination
from engineer_kit.connectors.rest import DateParams, RestConnector
from engineer_kit.http.auth import NoAuth
from engineer_kit.orchestration.pipeline import Pipeline, PipelineSource
from engineer_kit.storage.destination import Destination
from engineer_kit.storage.duckdb_loader import DuckDBLoader
from engineer_kit.storage.schema import ColumnSpec, EndpointSchema
from engineer_kit.storage.state_store import IngestionStateStore
from engineer_kit.transform.dbt_runner import DbtRunner
from engineer_kit.transform.scaffold import write_staging_scaffold

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
    state_store = IngestionStateStore(conn)
    incremental = IncrementalStrategy(
        connector_name="github_commits",
        state_store=state_store,
        mode=IncrementalMode.DATA_DATE,
        initial_start=date.today() - timedelta(days=30),
    )
    connector = RestConnector(
        name="github_commits",
        base_url=f"https://api.github.com/repos/{OWNER}/{REPO}/commits",
        incremental=incremental,
        pagination=PageNumberPagination(page_param="page", page_size_param="per_page", page_size=20),
        auth=NoAuth(),
        date_params=DateParams(start="since", end="until", date_format="%Y-%m-%dT%H:%M:%SZ"),
    )
    destination: Destination = DuckDBLoader(conn, schema="bronze")
    return Pipeline(
        sources=[PipelineSource(connector=connector, schema=COMMITS_SCHEMA)],
        destination=destination,
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
