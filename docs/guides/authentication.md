# Autenticação e secrets

O connector nunca deve precisar conhecer onde o token está armazenado. Ele recebe uma `AuthStrategy`, e a estratégia lê o valor de um `SecretProvider`.

```text
SecretProvider → AuthStrategy → HttpClient → API
```

## Sem autenticação

```python
from engineer_kit import NoAuth

auth = NoAuth()
```

## Bearer token

### Produção: variável de ambiente

```bash
export API_TOKEN='...'
```

```python
from engineer_kit import BearerAuth, EnvSecretProvider

secrets = EnvSecretProvider()
auth = BearerAuth(secrets, "API_TOKEN")
```

### Produção: arquivo montado

Estrutura:

```text
/run/secrets/
└── API_TOKEN
```

```python
from engineer_kit import BearerAuth, FileSecretProvider

secrets = FileSecretProvider("/run/secrets")
auth = BearerAuth(secrets, "API_TOKEN")
```

O provider lê o arquivo quando o valor é solicitado, o que facilita rotação de secret montado.

### Estudo: hardcoded explícito

```python
from engineer_kit import BearerAuth, StaticSecretProvider

secrets = StaticSecretProvider({
    "API_TOKEN": "training-only-token"
})
auth = BearerAuth(secrets, "API_TOKEN")
```

Esse caminho é suportado para treinamento, teste e notebooks descartáveis. Não coloque credenciais reais em Git.

## API key em query string

```python
from engineer_kit import ApiKeyAuth, EnvSecretProvider

secrets = EnvSecretProvider()
auth = ApiKeyAuth(
    secrets,
    "API_KEY",
    param_name="key",
    location="query",
)
```

Resulta conceitualmente em:

```text
GET /resource?key=<secret>
```

O valor é redigido dos logs normais.

## API key em header

```python
auth = ApiKeyAuth(
    secrets,
    "API_KEY",
    param_name="X-API-Key",
    location="header",
)
```

## Secrets no YAML

O YAML não armazena o valor do token no bloco `auth`; apenas o nome:

```yaml
connector:
  auth:
    type: bearer
    secret_key: API_TOKEN

secrets:
  type: env
```

Para arquivo:

```yaml
secrets:
  type: file
  path: /run/secrets
```

Outras opções de config, como `storage_options`, podem referenciar secrets em memória:

```yaml
destination:
  options:
    access_token: ${SECRET:LAKE_TOKEN}
```

## Hardcoded no YAML

Valores que parecem credenciais são bloqueados por padrão. Para um laboratório descartável, o opt-in é explícito:

```yaml
secrets:
  type: env
  allow_inline_values: true
```

Use isso somente quando você entende que o valor ficará no arquivo.

## Criando seu próprio SecretProvider

Implemente o contrato `SecretProvider` e faça `get(name)` devolver uma string. Isso permite integrar Key Vault, AWS Secrets Manager, Google Secret Manager, Databricks Secrets, Fabric/Key Vault ou qualquer cofre corporativo sem alterar o connector.

## Proteções relacionadas

- valores com CR/LF/NUL em headers de autenticação são rejeitados;
- credenciais embutidas diretamente na URL são rejeitadas;
- redirects para outra origem são bloqueados por padrão;
- query values e tokens não aparecem nos logs HTTP normais.
