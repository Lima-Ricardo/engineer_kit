# Security policy

`engineer_kit` handles API credentials, external HTTP traffic, incremental state and analytical data. Security is therefore part of the runtime contract and CI, not only deployment documentation.

## Security defaults

The supported defaults are intentionally conservative:

- HTTPS is required unless `allow_http=True` is explicitly selected.
- credentials embedded in URLs are rejected;
- request/query secret values and response bodies are not written to normal HTTP logs;
- redirects to a different origin are blocked by default;
- API-provided `next`/`Link` pagination URLs cannot switch origin by default, preventing automatic credential forwarding;
- cloud metadata/link-local literal IP targets are blocked by default;
- HTTP response pages have a bounded in-memory size;
- pagination has a maximum page count and loop detection;
- authentication header names/values reject control-character injection;
- YAML uses `safe_load`, has a size limit and rejects obvious inline credential fields by default;
- local state, audit and generated YAML files use restrictive file permissions where the operating system supports them;
- pipeline/audit/UI error text is redacted before persistence/display;
- the optional web UI binds to loopback by default, generates a temporary password when launched from the CLI without one, adds browser security headers and blocks cross-site mutation requests;
- remote UI exposure requires explicit opt-in, non-default credentials and is expected to sit behind TLS/network access controls;
- dbt execution uses `shell=False`, has a timeout and redacts captured output;
- CI runs tests across supported Python versions, Ruff, Bandit, dependency audit, package validation and synthetic stress tests;
- third-party GitHub Actions used by CI are pinned to reviewed commit SHAs;
- Dependabot monitors Python and GitHub Actions dependencies.

## Secrets: production and training

Both external secrets and hardcoded training values are supported deliberately.

### Recommended for real workloads

Use a provider that keeps the value outside source code:

```python
from engineer_kit import BearerAuth, FileSecretProvider

secrets = FileSecretProvider("/run/secrets")
auth = BearerAuth(secrets, "API_TOKEN")
```

`EnvSecretProvider` is also supported. Platform-specific secret-manager adapters can implement the same `SecretProvider` contract.

Declarative configs can reference values without storing them literally:

```yaml
connector:
  static_params:
    api_key: ${SECRET:API_KEY}

secrets:
  type: file
  path: /run/secrets
```

### Supported for study, examples and disposable labs

Hardcoded values remain available through `StaticSecretProvider`:

```python
from engineer_kit import BearerAuth, StaticSecretProvider

secrets = StaticSecretProvider({"TOKEN": "training-only-token"})
auth = BearerAuth(secrets, "TOKEN")
```

This is intentionally convenient for learning. Do not commit real credentials. Declarative inline values that look sensitive are rejected by default; a lab can explicitly set `secrets.allow_inline_values: true` when the risk is understood.

## Trust boundaries

`engineer_kit` treats these as trusted operator inputs:

- Python modules explicitly imported by `engineer_kit run`;
- custom adapters registered programmatically;
- dbt projects selected by the operator;
- local filesystem destinations selected by the operator.

Those mechanisms execute/read operator-controlled code or paths and are not sandboxes.

For environments that execute pipeline YAML supplied by untrusted users, enforce an additional runtime boundary: restricted filesystem permissions, network egress policy, isolated credentials and a container/job identity with least privilege.

## Cloud credentials

Prefer workload identity / managed identity / IAM roles over long-lived access keys whenever the platform supports them. If explicit object-store credentials are necessary, pass them through secret references or a custom `SecretProvider`; avoid storing them in Git, notebook source or CI logs.

## Reporting a vulnerability

Do not publish credentials, exploit payloads or sensitive data in a public issue. Prefer GitHub's private vulnerability reporting / Security Advisory channel for the repository when available. Otherwise contact the maintainer privately through the repository owner's GitHub profile and include the affected version, reproduction steps and expected impact.

## Scope limits

Static analysis and regression tests reduce risk but do not replace deployment controls. TLS termination, IAM, network egress, secret-manager policy, workspace permissions and cloud storage ACLs remain responsibilities of the runtime where the library is installed.
