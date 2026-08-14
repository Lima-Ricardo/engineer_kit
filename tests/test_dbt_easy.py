from engineer_kit.transform.dbt_easy import discover_dbt_project


def test_discover_dbt_project(tmp_path):
    project = tmp_path / "project"
    project.mkdir()
    (project / "dbt_project.yml").write_text("name: demo\n", encoding="utf-8")
    assert discover_dbt_project(project) == project.resolve()
