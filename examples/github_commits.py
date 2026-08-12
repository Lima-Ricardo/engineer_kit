"""End-to-end local example: GitHub REST API -> DuckDB Bronze -> optional dbt.

Install the local profile first:

    pip install -e ".[local]"

The example intentionally uses the new explicit adapter names. Compatibility
aliases such as DuckDBLoader/IngestionStateStore remain supported.
"""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import duckdb

from engineer_kit import (
    ColumnSpec,
    DateParams,
    DbtRunner,
    DuckDBDestination,
    DuckDBStateStore,
    EndpointSchema,
    IncrementalMode,
    NoAuth,
    PageNumberPagination,
    Pipeline,
    RestConnector,
    write_staging_scaffold,
)

DB_PATH = "warehouse.duckdb"
OWNER, REPO = "psf", "requests"

COMMITS_SCHEMA = EndpointSchema(
    columns=[
        ColumnSpec("sha"),
        ColumnSpec("commit_author_name"),
        ColumnSpec("commit_author_email"),
        ColumnSpec("commit_author_date", dtype="timestamp"),
        ColumnSpec("commit_committer_name"),
        ColumnSpec("commit_committer_email"),
        ColumnSpec("commit_committer_date", dtype="timestamp"),
        ColumnSpec("commit_message"),
        ColumnSpec("parents"),
        ColumnSpec("html_url"),
    ]
)


def make_pipeline(conn: duckdb.DuckDBPyConnection) -> Pipeline:
    connector = RestConnector(
        name="github_commits",
        base_url=f"https://api.github.com/repos/{OWNER}/{REPO}/commits",
        state_store=DuckDBStateStore(conn),
        incremental_mode=IncrementalMode.DATA_DATE,
        initial_start=date.today() - timedelta(days=30),
        date_field="commit.author.date",
        pagination=PageNumberPagination(page_size=20),
        method="GET",
        auth=NoAuth(),
        date_params=DateParams(
            start="since",
            end="until",
            date_format="%Y-%m-%dT%H:%M:%SZ",
        ),
    )
    return Pipeline(
        connector=connector,
        schema=COMMITS_SCHEMA,
        destination=DuckDBDestination(conn, schema="bronze", batch_size=1000),
    )


def main() -> None:
    conn = duckdb.connect(DB_PATH)
    result = make_pipeline(conn).run()
    for step in result.steps:
        print(
            "[ingestion]",
            step.connector_name,
            step.status,
            step.rows_loaded,
            "rows",
            "window=",
            (step.window_start, step.window_end),
            "destination=",
            step.destination,
        )
    conn.close()

    if not result.success:
        raise RuntimeError(result.steps)

    written = write_staging_scaffold(
        "dbt_project",
        {"github_commits": COMMITS_SCHEMA},
        bronze_schema="bronze",
        dialect="duckdb",
    )
    print("[dbt scaffold]", written)

    dbt_result = DbtRunner(
        project_dir="dbt_project",
        target="dev",
        env={"ENGINEER_KIT_DUCKDB_PATH": str(Path(DB_PATH).resolve())},
    ).run()
    print("[dbt run] success:", dbt_result.success)
    if not dbt_result.success:
        print(dbt_result.output)


if __name__ == "__main__":
    main()
