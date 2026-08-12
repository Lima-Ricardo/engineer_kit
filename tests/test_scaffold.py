import pytest

from engineer_kit.storage.identifiers import InvalidIdentifierError
from engineer_kit.storage.schema import EndpointSchema
from engineer_kit.transform.scaffold import (
    generate_sources_yml,
    generate_staging_model,
    write_staging_scaffold,
)


@pytest.fixture
def schema():
    return EndpointSchema.from_names(["id", "name"])


def test_generate_sources_yml_lists_all_endpoints(schema):
    yml = generate_sources_yml("bronze", {"commits": schema, "issues": schema})
    assert "commits" in yml
    assert "issues" in yml
    assert "schema: bronze" in yml


def test_generate_staging_model_casts_declared_columns(schema):
    sql = generate_staging_model("commits", schema)
    assert '"id"::VARCHAR as id' in sql
    assert "source('bronze', 'commits')" in sql


def test_malicious_endpoint_name_is_rejected_not_interpolated(schema):
    with pytest.raises(InvalidIdentifierError):
        generate_staging_model("commits'); DROP TABLE bronze.commits; --", schema)


def test_path_traversal_endpoint_name_is_rejected(schema, tmp_path):
    with pytest.raises(InvalidIdentifierError):
        write_staging_scaffold(str(tmp_path), {"../../evil": schema})


def test_write_staging_scaffold_creates_expected_files(schema, tmp_path):
    written = write_staging_scaffold(str(tmp_path), {"commits": schema})
    assert len(written) == 2
    for path in written:
        assert (tmp_path / "models" / "staging").exists()
