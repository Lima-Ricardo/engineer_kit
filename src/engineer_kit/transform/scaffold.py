"""Generate dbt staging scaffolds from the declared endpoint contract."""

from __future__ import annotations

from pathlib import Path

import yaml

from engineer_kit.storage.identifiers import validate_identifier
from engineer_kit.storage.schema import EndpointSchema


def generate_sources_yml(
    bronze_schema: str,
    endpoints: dict[str, EndpointSchema],
    source_name: str = "bronze",
) -> str:
    validate_identifier(bronze_schema, "Schema bronze")
    validate_identifier(source_name, "Nome da source dbt")
    for endpoint in endpoints:
        validate_identifier(endpoint, "Nome de endpoint")

    doc = {
        "version": 2,
        "sources": [
            {
                "name": source_name,
                "schema": bronze_schema,
                "tables": [{"name": endpoint} for endpoint in endpoints],
            }
        ],
    }
    return yaml.dump(doc, sort_keys=False, allow_unicode=True)


def generate_staging_model(
    endpoint: str,
    schema: EndpointSchema,
    source_name: str = "bronze",
    dialect: str = "duckdb",
) -> str:
    """Generate the mechanical Bronze -> staging cast layer.

    Logical types declared in ``ColumnSpec`` are rendered for the requested
    dialect. Business rules remain outside the generated staging model.
    """
    validate_identifier(endpoint, "Nome de endpoint")
    validate_identifier(source_name, "Nome da source dbt")
    select_lines = [
        f'    "{column.name}"::{column.sql_type(dialect)} as {column.name}'
        for column in schema.columns
    ]
    select_lines += [
        '    "_source" as _source',
        '    "_endpoint" as _endpoint',
        '    "_ingested_at" as _ingested_at',
    ]
    columns_sql = ",\n".join(select_lines)
    # This function emits a dbt model as text; it does not execute SQL. Source
    # and endpoint identifiers are validated above before template generation.
    return (
        f"-- generated from the declared engineer_kit schema for '{endpoint}'.\n"  # nosec B608
        f"-- business rules belong in silver/gold; this layer only casts Bronze strings.\n"
        f"select\n{columns_sql}\n"
        f"from {{{{ source('{source_name}', '{endpoint}') }}}}\n"
    )


def write_staging_scaffold(
    dbt_project_dir: str,
    endpoints: dict[str, EndpointSchema],
    bronze_schema: str = "bronze",
    dialect: str = "duckdb",
) -> list[str]:
    """Write sources.yml plus one generated ``stg_<endpoint>.sql`` per endpoint."""
    staging_dir = Path(dbt_project_dir) / "models" / "staging"
    staging_dir.mkdir(parents=True, exist_ok=True)

    written: list[str] = []
    sources_path = staging_dir / "sources.yml"
    sources_path.write_text(generate_sources_yml(bronze_schema, endpoints), encoding="utf-8")
    written.append(str(sources_path))

    for endpoint, schema in endpoints.items():
        model_path = staging_dir / f"stg_{endpoint}.sql"
        model_path.write_text(
            generate_staging_model(
                endpoint,
                schema,
                source_name=bronze_schema,
                dialect=dialect,
            ),
            encoding="utf-8",
        )
        written.append(str(model_path))

    return written
