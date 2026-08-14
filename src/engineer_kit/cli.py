"""Thin command-line entry points around engineer_kit primitives."""

from __future__ import annotations

import importlib
import logging
import secrets
import sys
from pathlib import Path

import typer

from engineer_kit.adapters.registry import available_adapters
from engineer_kit.orchestration.pipeline import Pipeline, PipelineResult
from engineer_kit.orchestration.scheduler import Scheduler
from engineer_kit.orchestration.trigger import CronTrigger
from engineer_kit.security.redaction import redact_text

app = typer.Typer(add_completion=False)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

_LOOPBACK_HOSTS = {"127.0.0.1", "localhost", "::1"}
_MIN_REMOTE_PASSWORD_LENGTH = 12


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
        error = redact_text(step.error) if step.error else None
        status = "OK" if step.success else f"ERRO ({step.status}): {error}"
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
    if len(password) < _MIN_REMOTE_PASSWORD_LENGTH:
        raise ValueError(
            f"Senha de UI remota deve ter pelo menos {_MIN_REMOTE_PASSWORD_LENGTH} caracteres."
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
        typer.echo(f"Falha ao executar configuracao: {redact_text(exc)}")
        raise typer.Exit(code=1) from None

    _print_result(result)
    if not result.success:
        raise typer.Exit(code=1)


@app.command("profile-config")
def profile_config(
    path: Path,
    metrics: str = typer.Option(
        "",
        "--metrics",
        "-m",
        help="Metricas separadas por virgula. Vazio executa o perfil completo.",
    ),
    key: str = typer.Option(
        "",
        "--key",
        help=(
            "PK candidata para a metrica duplicates. Use virgula para chave composta. "
            "Se omitida, reutiliza connector.primary_key quando configurado."
        ),
    ),
    scope: str = typer.Option(
        "sample",
        help="Escopo: sample (padrao seguro no CLI) ou full.",
    ),
    limit: int = typer.Option(
        10_000,
        min=1,
        help="Maximo de registros no modo sample.",
    ),
    html: Path | None = typer.Option(
        None,
        "--html",
        help="Tambem grava o mesmo ProfileReport como HTML standalone.",
    ),
) -> None:
    """Perfila uma fonte YAML sem carregar Bronze nem avancar checkpoint."""
    from engineer_kit.config.pipeline_config import load_pipeline_config
    from engineer_kit.profiling.config import connector_from_config

    normalized_scope = scope.strip().lower()
    if normalized_scope not in {"sample", "full"}:
        raise typer.BadParameter("scope deve ser 'sample' ou 'full'.", param_hint="--scope")
    selectors = tuple(item.strip() for item in metrics.split(",") if item.strip())
    candidate_key = [item.strip() for item in key.split(",") if item.strip()] or None
    resolved_limit = limit if normalized_scope == "sample" else None

    try:
        config = load_pipeline_config(path)
        connector = connector_from_config(config)
        report = connector.profile(
            *selectors,
            scope=normalized_scope,
            limit=resolved_limit,
            key=candidate_key,
        )
    except Exception as exc:
        typer.echo(f"Falha ao gerar data profile: {redact_text(exc)}")
        raise typer.Exit(code=1) from None

    typer.echo(report.to_text(), nl=False)
    if html is not None:
        html.parent.mkdir(parents=True, exist_ok=True)
        html.write_text(report.to_html(), encoding="utf-8")
        typer.echo(f"HTML: {html}")


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
    password: str | None = typer.Option(
        None,
        envvar="ENGINEER_KIT_UI_PASSWORD",
        help="Senha HTTP Basic. No localhost, uma senha aleatoria e gerada se omitida.",
    ),
    allow_remote: bool = typer.Option(
        False,
        "--allow-remote",
        help="Opt-in explicito para bind fora de localhost; use somente atras de TLS/reverse proxy.",
    ),
) -> None:
    """Sobe o lab web local: pipelines, dados e modelos dbt."""
    if password is None:
        if host not in _LOOPBACK_HOSTS:
            raise typer.BadParameter(
                "Exposicao remota exige ENGINEER_KIT_UI_PASSWORD/--password explicita.",
                param_hint="--password",
            )
        password = secrets.token_urlsafe(24)
        typer.echo(
            "Senha temporaria da UI local (nao persistida; muda a cada inicializacao): "
            f"{password}"
        )

    try:
        _validate_ui_exposure(host, username, password, allow_remote=allow_remote)
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
