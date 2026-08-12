"""Thin command-line entry points around engineer_kit primitives."""

from __future__ import annotations

import importlib
import logging
import sys
from pathlib import Path

import typer

from engineer_kit.adapters.registry import available_adapters
from engineer_kit.orchestration.pipeline import Pipeline, PipelineResult
from engineer_kit.orchestration.scheduler import Scheduler
from engineer_kit.orchestration.trigger import CronTrigger

app = typer.Typer(add_completion=False)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

_LOOPBACK_HOSTS = {"127.0.0.1", "localhost", "::1"}


def _load_pipeline(target: str) -> Pipeline:
    if ":" not in target:
        raise typer.BadParameter(
            "Use o formato 'modulo.caminho:atributo' (ex: pipelines.commits:pipeline)"
        )
    module_name, attr_name = target.split(":", 1)
    sys.path.insert(0, ".")
    module = importlib.import_module(module_name)
    pipeline = getattr(module, attr_name)
    if not isinstance(pipeline, Pipeline):
        raise typer.BadParameter(f"'{target}' nao e uma instancia de Pipeline.")
    return pipeline


def _print_result(result: PipelineResult) -> None:
    typer.echo(f"run_id={result.run_id} | rows={result.rows_loaded} | success={result.success}")
    for step in result.steps:
        status = "OK" if step.success else f"ERRO ({step.status}): {step.error}"
        window = (
            f" | window={step.window_start}..{step.window_end}"
            if step.window_start or step.window_end
            else ""
        )
        destination = f" | destination={step.destination}" if step.destination else ""
        typer.echo(
            f"{step.connector_name}: {step.rows_loaded} linha(s) -- {status}{window}{destination}"
        )


def _validate_ui_exposure(
    host: str,
    username: str,
    password: str,
    *,
    allow_remote: bool,
) -> None:
    """Require an explicit, non-default opt-in before non-loopback binding."""
    if host in _LOOPBACK_HOSTS:
        return
    if not allow_remote:
        raise ValueError(
            "A UI e um lab local. Para bind fora de localhost, passe --allow-remote "
            "explicitamente e coloque-a atras de TLS/reverse proxy."
        )
    if username == "admin" or password == "admin":
        raise ValueError(
            "Exposicao remota recusa as credenciais padrao. Defina "
            "ENGINEER_KIT_UI_USER e ENGINEER_KIT_UI_PASSWORD (ou --username/--password)."
        )


@app.command()
def run(target: str) -> None:
    """Roda uma Pipeline Python uma unica vez. TARGET: modulo.caminho:atributo."""
    result = _load_pipeline(target).run()
    _print_result(result)
    if not result.success:
        raise typer.Exit(code=1)


@app.command("run-config")
def run_config(path: Path) -> None:
    """Roda diretamente um pipeline YAML, sem exigir um modulo Python intermediario."""
    from engineer_kit.config.pipeline_config import build_pipeline, load_pipeline_config

    try:
        config = load_pipeline_config(path)
        if config.destination.type == "duckdb":
            try:
                import duckdb
            except ImportError:
                typer.echo('DuckDB e opcional: pip install "engineer_kit[duckdb]"')
                raise typer.Exit(code=1) from None

            warehouse_path = config.destination.path or "warehouse.duckdb"
            conn = duckdb.connect(warehouse_path)
            try:
                result = build_pipeline(config, conn).run()
            finally:
                conn.close()
        else:
            result = build_pipeline(config).run()
    except typer.Exit:
        raise
    except Exception as exc:
        typer.echo(f"Falha ao executar configuracao: {exc}")
        raise typer.Exit(code=1) from None

    _print_result(result)
    if not result.success:
        raise typer.Exit(code=1)


@app.command("adapters")
def adapters_command() -> None:
    """Lista os adapters declarativos registrados no processo atual."""
    adapters = available_adapters()
    for kind, names in adapters.items():
        typer.echo(f"{kind}: {', '.join(names) if names else '-'}")


@app.command()
def schedule(
    target: str,
    cron: str = typer.Option(..., help="Expressao cron, ex: '0 3 * * *'"),
) -> None:
    """Agenda uma Pipeline Python para rodar recorrentemente. Bloqueia o processo."""
    pipeline = _load_pipeline(target)
    scheduler = Scheduler()
    scheduler.schedule(pipeline, CronTrigger(cron), job_id=target)
    scheduler.start()


@app.command()
def ui(
    workspace: str = typer.Option(
        ".", help="Pasta do workspace: pipelines/*.yaml, warehouse.duckdb, dbt_project/."
    ),
    host: str = typer.Option("127.0.0.1", help="Endereco para bind."),
    port: int = typer.Option(8000, min=1, max=65535, help="Porta."),
    username: str = typer.Option("admin", envvar="ENGINEER_KIT_UI_USER"),
    password: str = typer.Option("admin", envvar="ENGINEER_KIT_UI_PASSWORD"),
    allow_remote: bool = typer.Option(
        False,
        "--allow-remote",
        help="Opt-in explicito para bind fora de localhost; use somente atras de TLS/reverse proxy.",
    ),
) -> None:
    """Sobe o lab web local: pipelines, dados e modelos dbt."""
    try:
        _validate_ui_exposure(
            host,
            username,
            password,
            allow_remote=allow_remote,
        )
    except ValueError as exc:
        raise typer.BadParameter(str(exc), param_hint="--host") from None

    try:
        import uvicorn

        from engineer_kit.ui.app import create_app
    except ImportError as exc:
        typer.echo('A interface web precisa de dependencias extras: pip install "engineer_kit[ui]"')
        raise typer.Exit(code=1) from exc

    if host not in _LOOPBACK_HOSTS:
        typer.echo(
            "ATENCAO: bind remoto habilitado explicitamente. HTTP Basic nao cifra credenciais; "
            "termine TLS em um reverse proxy e restrinja o acesso de rede."
        )

    web_app = create_app(workspace_dir=workspace, username=username, password=password)
    display_host = f"[{host}]" if ":" in host and not host.startswith("[") else host
    typer.echo(f"Subindo em http://{display_host}:{port} (usuario: {username})")
    uvicorn.run(web_app, host=host, port=port)


if __name__ == "__main__":
    app()
