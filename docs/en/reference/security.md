# Security

The complete vulnerability policy is in [`SECURITY.md`](https://github.com/Lima-Ricardo/engineer_kit/blob/main/SECURITY.md).

## Important defaults

### Network

- HTTPS required by default;
- TLS verification is not silently disabled;
- credentials in URLs are rejected;
- cross-origin redirects are blocked;
- cross-origin pagination URLs are blocked;
- literal link-local/metadata targets are blocked;
- HTTP response size is bounded;
- pagination has page limits and loop detection;
- POST retry requires explicit opt-in.

### Secrets

- logs and errors are redacted;
- query values are not normally logged;
- header injection is rejected;
- file secrets cannot escape the configured directory through traversal/symlink tricks;
- inline YAML secrets are rejected by default.

### Filesystem / UI

- sensitive local files receive restrictive permissions where supported;
- paths derived from identifiers are sanitized/isolated;
- the UI uses security headers and same-origin validation;
- remote UI exposure requires explicit opt-in.

### Supply chain

CI runs:

```text
Ruff
Bandit
pip-audit
pip check
pytest + property tests
package build/twine check
stress
```

Third-party GitHub Actions are pinned by commit SHA.

## Library boundaries

The library does not replace IAM, network egress policy, bucket/Lakehouse ACLs, TLS/reverse proxy controls, a corporate secret manager, or isolation for untrusted Python code.

If YAML comes from untrusted users, run the workload under a least-privilege identity with isolated filesystem and network access.
