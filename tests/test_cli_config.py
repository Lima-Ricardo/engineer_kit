from pathlib import Path

import responses
from typer.testing import CliRunner

from engineer_kit.cli import app


runner = CliRunner()


def test_adapters_command_lists_builtin_backends():
    result = runner.invoke(app, ["adapters"])
    assert result.exit_code == 0
    assert "destination: delta, duckdb, parquet" in result.output
    assert "state_store: delta, duckdb, file, parquet" in result.output


@responses.activate
def test_run_config_executes_parquet_pipeline_without_duckdb_runtime(tmp_path):
    config_path = tmp_path / "pipeline.yaml"
    config_path.write_text(
        f"""
name: cli_events
connector:
  base_url: https://example.test/events
  method: GET
  pagination:
    type: none
  incremental:
    mode: ingestion_date
columns:
  - name: id
    dtype: bigint
destination:
  type: parquet
  path: {str(tmp_path / 'lake')!r}
  schema: bronze
  batch_size: 100
state:
  type: auto
run_log:
  enabled: true
  type: auto
""".strip(),
        encoding="utf-8",
    )
    responses.get("https://example.test/events", json=[{"id": 1}], status=200)

    result = runner.invoke(app, ["run-config", str(config_path)])

    assert result.exit_code == 0, result.output
    assert "success=True" in result.output
    assert "cli_events: 1 linha(s) -- OK" in result.output
    assert list((tmp_path / "lake" / "bronze" / "cli_events").glob("*.parquet"))
    assert (tmp_path / "lake" / "_meta" / "ingestion_state.json").exists()
    assert (tmp_path / "lake" / "_meta" / "run_log.jsonl").exists()


def test_run_config_reports_invalid_yaml(tmp_path):
    path = Path(tmp_path) / "broken.yaml"
    path.write_text("name: only_name\n", encoding="utf-8")

    result = runner.invoke(app, ["run-config", str(path)])

    assert result.exit_code == 1
    assert "Falha ao executar configuracao" in result.output
