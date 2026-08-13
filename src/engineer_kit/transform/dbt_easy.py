"""Zero-config facade over the stable DbtRunner."""

from __future__ import annotations

from pathlib import Path

from engineer_kit.transform.dbt_runner import DbtResult, DbtRunner


def discover_dbt_project(start: str | Path | None = None) -> Path:
    origin = Path(start or Path.cwd()).resolve()
    for parent in (origin, *origin.parents):
        for candidate in (parent, parent / "dbt_project"):
            if (candidate / "dbt_project.yml").is_file():
                return candidate
    raise FileNotFoundError("dbt_project.yml nao encontrado; passe project_dir quando necessario.")


class Dbt:
    """Simple dbt facade; project/profile discovery happens only once."""

    def __init__(
        self,
        project_dir: str | None = None,
        *,
        profiles_dir: str | None = None,
        target: str = "dev",
    ) -> None:
        project = discover_dbt_project(project_dir) if project_dir is None else Path(project_dir).resolve()
        if profiles_dir is None:
            home = Path.home() / ".dbt"
            profiles = project if (project / "profiles.yml").is_file() else home
        else:
            profiles = Path(profiles_dir).resolve()
        self._runner = DbtRunner(str(project), profiles_dir=str(profiles), target=target)

    def run(self, select: str | None = None) -> DbtResult:
        return self._runner.run(select=select)


__all__ = ["Dbt", "discover_dbt_project"]
