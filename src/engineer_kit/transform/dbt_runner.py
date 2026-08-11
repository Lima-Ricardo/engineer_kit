"""Invoca o dbt para materializar silver/gold a partir do bronze.

So dispara o comando via subprocess (mais estavel entre versoes do dbt
do que acoplar na API interna `dbt.cli.main`, que muda com frequencia
entre releases) — as regras de transformacao (joins, desnormalizacao)
ficam nos modelos .sql do projeto dbt, escritos pelo engenheiro.
"""

from __future__ import annotations

import logging
import os
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

logger = logging.getLogger("engineer_kit.dbt")


@dataclass
class DbtResult:
    success: bool
    output: str


def _default_dbt_executable() -> str:
    """Acha o dbt sem depender do venv estar ativado: primeiro tenta o
    PATH (respeita venv ativado ou instalacao global), depois olha o
    mesmo diretorio do interpretador Python em uso (venv/Scripts/dbt.exe
    -- o caso comum quando o script roda via `venv/Scripts/python.exe`
    sem ativar o venv antes)."""
    found = shutil.which("dbt")
    if found:
        return found
    suffix = ".exe" if sys.platform == "win32" else ""
    candidate = Path(sys.executable).parent / f"dbt{suffix}"
    if candidate.exists():
        return str(candidate)
    return "dbt"  # deixa falhar com uma mensagem clara do proprio SO se nao existir


class DbtRunner:
    def __init__(
        self,
        project_dir: str,
        profiles_dir: Optional[str] = None,
        target: str = "dev",
        dbt_executable: Optional[str] = None,
        env: Optional[dict[str, str]] = None,
    ) -> None:
        self._project_dir = project_dir
        self._profiles_dir = profiles_dir or project_dir
        self._target = target
        self._dbt_executable = dbt_executable or _default_dbt_executable()
        # variaveis extras passadas ao subprocesso do dbt -- usar para
        # caminhos absolutos referenciados via env_var() no profiles.yml,
        # em vez de path relativo (a resolucao de relativo no dbt-duckdb
        # depende do cwd de onde o dbt foi chamado, nao do profiles.yml,
        # o que e uma fonte facil de bug silencioso).
        self._env = env or {}

    def run(self, select: Optional[str] = None) -> DbtResult:
        args = [
            self._dbt_executable,
            "run",
            "--project-dir",
            self._project_dir,
            "--profiles-dir",
            self._profiles_dir,
            "--target",
            self._target,
        ]
        if select:
            args += ["--select", select]
        return self._invoke(args)

    def _invoke(self, args: list[str]) -> DbtResult:
        logger.info("Executando: %s", " ".join(args))
        completed = subprocess.run(
            args, capture_output=True, text=True, env={**os.environ, **self._env}
        )
        success = completed.returncode == 0
        output = completed.stdout + completed.stderr
        if not success:
            logger.error("dbt falhou:\n%s", output)
        return DbtResult(success=success, output=output)
