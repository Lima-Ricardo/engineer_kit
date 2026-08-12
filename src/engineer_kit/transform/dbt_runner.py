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

from engineer_kit.security.redaction import redact_text

logger = logging.getLogger("engineer_kit.dbt")
DEFAULT_DBT_TIMEOUT_SECONDS = 60 * 60


@dataclass
class DbtResult:
    success: bool
    output: str


def _default_dbt_executable() -> str:
    """Acha o dbt sem depender do venv estar ativado."""
    found = shutil.which("dbt")
    if found:
        return found
    suffix = ".exe" if sys.platform == "win32" else ""
    candidate = Path(sys.executable).parent / f"dbt{suffix}"
    if candidate.exists():
        return str(candidate)
    return "dbt"


class DbtRunner:
    def __init__(
        self,
        project_dir: str,
        profiles_dir: Optional[str] = None,
        target: str = "dev",
        dbt_executable: Optional[str] = None,
        env: Optional[dict[str, str]] = None,
        timeout_seconds: float = DEFAULT_DBT_TIMEOUT_SECONDS,
    ) -> None:
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds deve ser maior que zero.")
        self._project_dir = str(Path(project_dir).resolve())
        self._profiles_dir = str(Path(profiles_dir or project_dir).resolve())
        self._target = target
        self._dbt_executable = dbt_executable or _default_dbt_executable()
        self._env = env or {}
        self._timeout_seconds = timeout_seconds

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
        logger.info("Executando dbt: %s", redact_text(" ".join(args)))
        try:
            completed = subprocess.run(
                args,
                capture_output=True,
                text=True,
                env={**os.environ, **self._env},
                stdin=subprocess.DEVNULL,
                shell=False,
                check=False,
                timeout=self._timeout_seconds,
            )
        except subprocess.TimeoutExpired as exc:
            partial = (exc.stdout or "") + (exc.stderr or "")
            output = redact_text(
                f"dbt excedeu timeout de {self._timeout_seconds:g}s.\n{partial}"
            )
            logger.error("dbt falhou: %s", output)
            return DbtResult(success=False, output=output)

        success = completed.returncode == 0
        output = redact_text(completed.stdout + completed.stderr)
        if not success:
            logger.error("dbt falhou:\n%s", output)
        return DbtResult(success=success, output=output)
