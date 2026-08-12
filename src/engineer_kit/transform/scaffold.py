"""Gera sources.yml e um modelo de staging a partir do schema declarado
de cada endpoint.

Isso elimina o trabalho manual repetitivo de descrever no dbt uma
tabela que a lib ja sabe descrever (o schema foi declarado em Python,
na EndpointSchema). As regras de negocio de silver/gold continuam
manuais — este gerador so cobre staging, que e so um cast de string
para o tipo certo.
"""

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


def generate_staging_model(endpoint: str, schema: EndpointSchema, source_name: str = "bronze") -> str:
    validate_identifier(endpoint, "Nome de endpoint")
    validate_identifier(source_name, "Nome da source dbt")
    select_lines = [f'    "{col.name}"::{col.dtype} as {col.name}' for col in schema.columns]
    select_lines += [
        '    "_source" as _source',
        '    "_endpoint" as _endpoint',
        '    "_ingested_at" as _ingested_at',
    ]
    columns_sql = ",\n".join(select_lines)
    return (
        f"-- gerado automaticamente a partir do schema declarado em Python "
        f"para o endpoint '{endpoint}'.\n"
        f"-- revise os tipos (::TIPO) manualmente antes de considerar isto pronto para producao.\n"
        f"select\n{columns_sql}\n"
        f"from {{{{ source('{source_name}', '{endpoint}') }}}}\n"
    )


def write_staging_scaffold(
    dbt_project_dir: str,
    endpoints: dict[str, EndpointSchema],
    bronze_schema: str = "bronze",
) -> list[str]:
    """Escreve sources.yml + um stg_<endpoint>.sql por endpoint em
    <dbt_project_dir>/models/staging/. Sobrescreve o que tiver sido
    gerado antes — nao e para editar esses arquivos a mao; crie modelos
    de silver/gold que leem deles em vez disso."""
    staging_dir = Path(dbt_project_dir) / "models" / "staging"
    staging_dir.mkdir(parents=True, exist_ok=True)

    written = []
    sources_path = staging_dir / "sources.yml"
    sources_path.write_text(generate_sources_yml(bronze_schema, endpoints), encoding="utf-8")
    written.append(str(sources_path))

    for endpoint, schema in endpoints.items():
        model_path = staging_dir / f"stg_{endpoint}.sql"
        model_path.write_text(generate_staging_model(endpoint, schema, source_name=bronze_schema), encoding="utf-8")
        written.append(str(model_path))

    return written
