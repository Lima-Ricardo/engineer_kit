"""Invoca o dbt para materializar silver/gold a partir do bronze.

So dispara o comando via subprocess (mais estavel entre versoes do dbt
do que acoplar na API interna `dbt.cli.main`, que muda com frequencia
entre releases) — as regras de transformacao (joins, desnormalizacao)
ficam nos modelos .sql do projeto dbt, escritos pelo engenheiro.
"""

from __future__ import annotations

import logging
import subprocess
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger("engineer_kit.dbt")


@dataclass
class DbtResult:
    success: bool
    output: str


class DbtRunner:
    def __init__(self, project_dir: str, profiles_dir: Optional[str] = None, target: str = "dev") -> None:
        self._project_dir = project_dir
        self._profiles_dir = profiles_dir or project_dir
        self._target = target

    def run(self, select: Optional[str] = None) -> DbtResult:
        args = [
            "dbt",
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
        completed = subprocess.run(args, capture_output=True, text=True)
        success = completed.returncode == 0
        output = completed.stdout + completed.stderr
        if not success:
            logger.error("dbt falhou:\n%s", output)
        return DbtResult(success=success, output=output)
