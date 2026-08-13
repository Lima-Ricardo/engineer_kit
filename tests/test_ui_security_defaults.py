import pytest

from engineer_kit.cli import _validate_ui_exposure
from engineer_kit.ui.app import create_app


def test_programmatic_ui_requires_explicit_password(tmp_path):
    with pytest.raises(ValueError, match="password explicita"):
        create_app(workspace_dir=str(tmp_path))


def test_programmatic_ui_rejects_empty_username(tmp_path):
    with pytest.raises(ValueError, match="username nao vazio"):
        create_app(workspace_dir=str(tmp_path), username="", password="safe-local-password")


def test_ui_workspace_children_cannot_escape_workspace(tmp_path):
    with pytest.raises(ValueError, match="dentro do workspace"):
        create_app(
            workspace_dir=str(tmp_path),
            pipelines_dirname="../outside",
            password="safe-local-password",
        )


def test_remote_ui_requires_reasonable_password_length():
    with pytest.raises(ValueError, match="pelo menos 12"):
        _validate_ui_exposure(
            "0.0.0.0",
            "lab-user",
            "short",
            allow_remote=True,
        )
