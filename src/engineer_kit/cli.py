"""CLI fina sobre Pipeline.

`engineer_kit run <modulo:atributo>` executa uma Pipeline uma vez;
`engineer_kit schedule <modulo:atributo> --cron "0 3 * * *"` agenda.
`<modulo:atributo>` aponta para uma variavel Pipeline no codigo do
usuario (ex.: "pipelines.commits:pipeline") — a CLI nunca sabe o que
tem dentro do pipeline, so o executa. Isso e o que permite qualquer
orquestrador externo (Airflow, cron, GitHub Actions) chamar a mesma
unidade sem precisar conhecer o codigo Python por dentro.
"""

from __future__ import annotations

import importlib
import logging
import sys

import typer

from engineer_kit.orchestration.pipeline import Pipeline
from engineer_kit.orchestration.scheduler import Scheduler
from engineer_kit.orchestration.trigger import CronTrigger

app = typer.Typer(add_completion=False)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")


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


@app.command()
def run(target: str) -> None:
    """Roda uma Pipeline uma unica vez. TARGET: modulo.caminho:atributo."""
    pipeline = _load_pipeline(target)
    result = pipeline.run()
    for step in result.steps:
        status = "OK" if step.error is None else f"ERRO: {step.error}"
        typer.echo(f"{step.connector_name}: {step.rows_loaded} linha(s) -- {status}")
    if not result.success:
        raise typer.Exit(code=1)


@app.command()
def schedule(
    target: str,
    cron: str = typer.Option(..., help="Expressao cron, ex: '0 3 * * *'"),
) -> None:
    """Agenda uma Pipeline para rodar recorrentemente. Bloqueia o processo."""
    pipeline = _load_pipeline(target)
    scheduler = Scheduler()
    scheduler.schedule(pipeline, CronTrigger(cron), job_id=target)
    scheduler.start()


if __name__ == "__main__":
    app()
