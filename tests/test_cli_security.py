import pytest

from engineer_kit.cli import _validate_ui_exposure


def test_loopback_ui_keeps_zero_config_local_experience():
    _validate_ui_exposure("127.0.0.1", "admin", "admin", allow_remote=False)
    _validate_ui_exposure("localhost", "admin", "admin", allow_remote=False)
    _validate_ui_exposure("::1", "admin", "admin", allow_remote=False)


def test_remote_ui_requires_explicit_opt_in():
    with pytest.raises(ValueError, match="--allow-remote"):
        _validate_ui_exposure("0.0.0.0", "safe-user", "safe-password", allow_remote=False)


def test_remote_ui_rejects_default_credentials_even_with_opt_in():
    with pytest.raises(ValueError, match="credenciais padrao"):
        _validate_ui_exposure("0.0.0.0", "admin", "safe-password", allow_remote=True)
    with pytest.raises(ValueError, match="credenciais padrao"):
        _validate_ui_exposure("0.0.0.0", "safe-user", "admin", allow_remote=True)


def test_remote_ui_accepts_explicit_non_default_credentials():
    _validate_ui_exposure(
        "0.0.0.0",
        "lab-user",
        "long-non-default-password",
        allow_remote=True,
    )
