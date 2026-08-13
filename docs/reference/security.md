# Segurança

A política completa e o canal de vulnerabilidades estão em [`SECURITY.md`](https://github.com/Lima-Ricardo/engineer_kit/blob/main/SECURITY.md).

## Defaults relevantes

### Rede

- HTTPS obrigatório por padrão;
- TLS verification não é desativado silenciosamente;
- credenciais na URL são rejeitadas;
- redirect cross-origin bloqueado;
- próxima URL de paginação cross-origin bloqueada;
- alvos link-local/metadata literal são bloqueados;
- resposta HTTP tem limite de tamanho;
- paginação tem limite/loop detection;
- retry de POST exige opt-in.

### Secrets

- logs e erros são redigidos;
- query values não são logados normalmente;
- header injection é rejeitada;
- arquivo de secret não pode escapar do diretório configurado por traversal/symlink;
- secrets inline em YAML são recusados por padrão.

### Filesystem / UI

- arquivos locais sensíveis recebem permissões restritivas quando suportado;
- paths derivados de IDs são sanitizados/isolados;
- UI usa security headers e validações same-origin;
- exposição remota exige opt-in.

### Supply chain

CI executa:

```text
Ruff
Bandit
pip-audit
pip check
pytest + property tests
package build/twine check
stress
```

Actions de terceiros são pinadas por commit SHA.

## Limites da biblioteca

A lib não substitui:

- IAM;
- network egress policy;
- ACL do bucket/Lakehouse;
- TLS/reverse proxy do ambiente;
- secret manager corporativo;
- isolamento de código Python não confiável.

Se YAML vier de usuários não confiáveis, execute o job com identidade de mínimo privilégio e filesystem/network isolados.
