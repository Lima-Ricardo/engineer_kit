# Publishing and releases

This page is for maintainers.

## Official channel

The official Python distribution channel is **PyPI**. `pip`, `uv`, Poetry, and `pipx` consume the same index, so they do not require separate uploads.

Every `vX.Y.Z` tag runs `.github/workflows/release.yml`:

```text
tag
 ↓
build wheel/sdist
 ↓
twine check + smoke install
 ↓
PyPI Trusted Publishing (OIDC)
 ↓
GitHub Release + artifacts
```

## One-time PyPI setup

Create a Trusted Publisher for:

```text
Owner: Lima-Ricardo
Repository: engineer_kit
Workflow: release.yml
Environment: pypi
```

For the first release, use a pending trusted publisher if the project does not yet exist. Do not store a long-lived PyPI API token in the repository when Trusted Publishing is available.

## Create release 0.1.0

After `main` is green and `pyproject.toml` contains version `0.1.0`:

```bash
git tag -s v0.1.0 -m "engineer_kit 0.1.0"
git push origin v0.1.0
```

An annotated tag also works when local signing is unavailable, although signed release tags are recommended.

## Conda / conda-forge

Conda-forge is not a second upload of the PyPI wheel. It uses a community-reviewed feedstock/recipe. Recommended sequence:

1. publish to PyPI;
2. validate the public sdist;
3. submit a recipe to `conda-forge/staged-recipes`;
4. after approval, updates are automated by the feedstock.

Until a feedstock exists, document PyPI as the official channel.
