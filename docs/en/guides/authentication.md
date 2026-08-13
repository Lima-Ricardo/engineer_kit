# Authentication and secrets

A connector should not need to know where a token is stored. It receives an `AuthStrategy`, and that strategy reads values through a `SecretProvider`.

```text
SecretProvider → AuthStrategy → HttpClient → API
```

## No authentication

```python
from engineer_kit import NoAuth

auth = NoAuth()
```

## Bearer token

### Production: environment variable

```bash
export API_TOKEN='...'
```

```python
from engineer_kit import BearerAuth, EnvSecretProvider

secrets = EnvSecretProvider()
auth = BearerAuth(secrets, "API_TOKEN")
```

### Production: mounted file

```text
/run/secrets/
└── API_TOKEN
```

```python
from engineer_kit import BearerAuth, FileSecretProvider

secrets = FileSecretProvider("/run/secrets")
auth = BearerAuth(secrets, "API_TOKEN")
```

The provider reads the file when requested, which is useful when mounted secrets are rotated.

### Learning: explicit hardcoded value

```python
from engineer_kit import BearerAuth, StaticSecretProvider

secrets = StaticSecretProvider({"API_TOKEN": "training-only-token"})
auth = BearerAuth(secrets, "API_TOKEN")
```

This is supported for training, testing, and disposable notebooks. Do not commit real credentials to Git.

## API key in query string

```python
from engineer_kit import ApiKeyAuth, EnvSecretProvider

secrets = EnvSecretProvider()
auth = ApiKeyAuth(secrets, "API_KEY", param_name="key", location="query")
```

Conceptually:

```text
GET /resource?key=<secret>
```

The value is redacted from normal logs.

## API key in a header

```python
auth = ApiKeyAuth(
    secrets,
    "API_KEY",
    param_name="X-API-Key",
    location="header",
)
```

## Secrets in YAML

The `auth` block stores the secret name, not the secret value:

```yaml
connector:
  auth:
    type: bearer
    secret_key: API_TOKEN

secrets:
  type: env
```

Mounted files:

```yaml
secrets:
  type: file
  path: /run/secrets
```

Other configuration values, including storage options, can reference secrets:

```yaml
destination:
  options:
    access_token: ${SECRET:LAKE_TOKEN}
```

## Hardcoded values in YAML

Values that look like credentials are blocked by default. A disposable lab must opt in explicitly:

```yaml
secrets:
  type: env
  allow_inline_values: true
```

Only use this when you accept that the value is stored in the file.

## Custom SecretProvider

Implement `SecretProvider.get(name) -> str` to integrate Azure Key Vault, AWS Secrets Manager, Google Secret Manager, Databricks Secrets, Fabric/Key Vault, or another corporate vault without changing the connector.

## Related protections

- CR/LF/NUL values in authentication headers are rejected;
- credentials embedded directly in URLs are rejected;
- redirects to another origin are blocked by default;
- query values and tokens are not emitted in normal HTTP logs.
