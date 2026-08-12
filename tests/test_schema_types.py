from engineer_kit.storage.schema import ColumnSpec, EndpointSchema
from engineer_kit.storage.types import LogicalType, render_sql_type
from engineer_kit.transform.scaffold import generate_staging_model


def test_logical_types_render_per_dialect():
    assert ColumnSpec("name").logical_type is LogicalType.STRING
    assert ColumnSpec("created_at", "timestamp").sql_type("duckdb") == "TIMESTAMP"
    assert ColumnSpec("created_at", "timestamp").sql_type("spark") == "TIMESTAMP"
    assert ColumnSpec("payload", "json").sql_type("spark") == "STRING"
    assert render_sql_type("VARCHAR", "duckdb") == "VARCHAR"


def test_legacy_parameterized_sql_type_remains_supported():
    column = ColumnSpec("amount", "DECIMAL(18, 2)")
    assert column.logical_type is None
    assert column.sql_type("duckdb") == "DECIMAL(18, 2)"


def test_dbt_staging_renders_logical_type_instead_of_physical_bronze_type():
    schema = EndpointSchema(
        [ColumnSpec("id", "bigint"), ColumnSpec("created_at", "timestamp")]
    )
    sql = generate_staging_model("orders", schema, dialect="duckdb")
    assert '"id"::BIGINT as id' in sql
    assert '"created_at"::TIMESTAMP as created_at' in sql
