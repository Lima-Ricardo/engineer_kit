# Publicação e releases

Esta página é voltada ao mantenedor.

## Canal oficial

A distribuição Python oficial é o **PyPI**. `pip`, `uv`, Poetry e `pipx` consomem esse mesmo índice, portanto não exigem uploads separados.

Cada tag `vX.Y.Z` executa `.github/workflows/release.yml`:

```text
tag
 ↓
build wheel/sdist
 ↓
twine check + smoke install
 ↓
PyPI Trusted Publishing (OIDC)
 ↓
GitHub Release + arquivos
```

## Configuração única do PyPI

No PyPI, crie um Trusted Publisher para:

```text
Owner: Lima-Ricardo
Repository: engineer_kit
Workflow: release.yml
Environment: pypi
```

Para a primeira publicação, use o recurso de *pending trusted publisher* se o projeto ainda não existir no PyPI. Não armazene API token de PyPI no repositório se Trusted Publishing estiver disponível.

## Criar a release 0.1.0

Depois que `main` estiver verde e a versão em `pyproject.toml` for `0.1.0`:

```bash
git tag -s v0.1.0 -m "engineer_kit 0.1.0"
git push origin v0.1.0
```

Se você não usa assinatura Git local, uma tag anotada também funciona, mas assinatura é recomendada para releases.

## Conda / conda-forge

Conda-forge não é apenas um segundo upload do wheel PyPI. Ele usa um feedstock/recipe revisado pela comunidade. A ordem recomendada é:

1. publicar no PyPI;
2. validar o sdist público;
3. enviar recipe para `conda-forge/staged-recipes`;
4. após aprovação, updates passam a ser automatizados pelo feedstock.

Até existir feedstock, documente PyPI como canal oficial.
