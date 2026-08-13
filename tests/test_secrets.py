import pytest

from engineer_kit.security.secrets import (
    EnvSecretProvider,
    FileSecretProvider,
    InvalidSecretKeyError,
    SecretNotFoundError,
    StaticSecretProvider,
)


def test_env_secret_provider_reads_environment_variable(monkeypatch):
    monkeypatch.setenv("MY_TOKEN", "abc123")
    assert EnvSecretProvider().get("MY_TOKEN") == "abc123"


def test_env_secret_provider_raises_when_missing(monkeypatch):
    monkeypatch.delenv("MISSING_TOKEN", raising=False)
    with pytest.raises(SecretNotFoundError):
        EnvSecretProvider().get("MISSING_TOKEN")


def test_static_secret_provider_returns_configured_value():
    provider = StaticSecretProvider({"MY_TOKEN": "xyz"})
    assert provider.get("MY_TOKEN") == "xyz"


def test_static_secret_provider_raises_when_missing():
    provider = StaticSecretProvider({})
    with pytest.raises(SecretNotFoundError):
        provider.get("MISSING")


def test_static_secret_provider_copies_input_mapping():
    values = {"TOKEN": "original"}
    provider = StaticSecretProvider(values)
    values["TOKEN"] = "mutated"
    assert provider.get("TOKEN") == "original"


def test_file_secret_provider_single_file_ignores_key(tmp_path):
    token_file = tmp_path / "token.txt"
    token_file.write_text("meu-token-secreto\n", encoding="utf-8")

    provider = FileSecretProvider(token_file)
    assert provider.get("qualquer_coisa") == "meu-token-secreto"
    assert provider.get("../ignored-in-single-file-mode") == "meu-token-secreto"


def test_file_secret_provider_directory_mode_uses_key_as_filename(tmp_path):
    (tmp_path / "GITHUB_TOKEN").write_text("token-do-github", encoding="utf-8")
    (tmp_path / "SLACK_TOKEN").write_text("token-do-slack", encoding="utf-8")

    provider = FileSecretProvider(tmp_path)
    assert provider.get("GITHUB_TOKEN") == "token-do-github"
    assert provider.get("SLACK_TOKEN") == "token-do-slack"


@pytest.mark.parametrize(
    "key",
    ["../outside", "../../outside", "subdir/token", r"..\outside", "", ".", ".."],
)
def test_file_secret_provider_directory_mode_rejects_path_traversal(tmp_path, key):
    provider = FileSecretProvider(tmp_path)
    with pytest.raises(InvalidSecretKeyError):
        provider.get(key)


def test_file_secret_provider_raises_when_file_missing(tmp_path):
    provider = FileSecretProvider(tmp_path)
    with pytest.raises(SecretNotFoundError):
        provider.get("NAO_EXISTE")


def test_file_secret_provider_rereads_on_every_call_no_cache(tmp_path):
    token_file = tmp_path / "token.txt"
    token_file.write_text("valor-1", encoding="utf-8")
    provider = FileSecretProvider(token_file)

    assert provider.get("x") == "valor-1"
    token_file.write_text("valor-2-rotacionado", encoding="utf-8")
    assert provider.get("x") == "valor-2-rotacionado"
