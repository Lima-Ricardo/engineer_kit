# Primeiro pipeline, passo a passo

O caminho principal do `engineer_kit` é **intent-driven**: você informa o que sabe sobre a origem e o destino; a biblioteca resolve adapters, estado, auditoria, paginação e schema quando isso pode ser feito com segurança.

## 1. Instale somente o destino que você vai usar

```bash
pip install "engineer_kit[parquet]"
```

## 2. Comece pelo mínimo

Para uma API simples, `GET` já é o método padrão, o nome do conector é derivado da URL e a lista de registros é detectada quando não há ambiguidade:

```python
from engineer_kit import RestConnector

records = RestConnector(
    base_url="https://api.example.com/orders",
    auth=token,
).collect()
```

Uma string em `auth` representa Bearer auth. Para produção, os objetos `BearerAuth`, `SecretProvider` e demais contratos continuam disponíveis quando você quiser controlar a origem do segredo explicitamente.

## 3. Declare somente o que a API exige

Suponha que a API use páginas de 1.000 registros e aceite `updated_from`:

```text
GET /orders?page=1&per_page=1000&updated_from=2026-01-01
```

A resposta é:

```json
{
  "results": [
    {"id": 123, "updated_at": "2026-08-12T10:00:00Z"}
  ]
}
```

Você não precisa instanciar classes de paginação ou estado:

```python
connector = RestConnector(
    base_url="https://api.example.com/orders",
    auth=token,
    pagination={"type": "page", "size": 1000},
    incremental={
        "field": "updated_at",
        "param": "updated_from",
        "initial_start": "2026-01-01",
    },
)
```

Para cursor comum, basta:

```python
pagination="cursor"
```

`Cursor`, `cursor` e outras variações de capitalização são normalizadas uma vez antes da execução.

## 4. Colete, faça streaming ou grave diretamente

Dataset pequeno:

```python
records = connector.collect()
```

Volume maior, com batches limitados:

```python
for batch in connector.stream():
    process(batch)
```

Managed mode, sem construir `Destination`, `StateStore` ou `RunLogBackend` manualmente:

```python
result = connector.to(
    "parquet",
    "bronze.orders",
    path="./lake",
).run()
```

Ao usar um destino oficial, o managed flow resolve o backend natural de estado e auditoria. O checkpoint continua sendo confirmado somente depois de a carga no destino terminar com sucesso.

## 5. Se quiser DuckDB

```python
result = RestConnector(
    base_url="https://api.example.com/orders",
    auth=token,
    pagination="cursor",
    incremental=True,
).to(
    "duckdb",
    "bronze.orders",
    path="analytics.duckdb",
).run()
```

A conexão, destino Bronze, estado e auditoria DuckDB são resolvidos internamente. Passe objetos explícitos apenas quando precisar substituir os defaults.

## 6. Se quiser dbt depois da ingestão

Quando houver um `dbt_project.yml` no projeto ou em um diretório pai:

```python
result = (
    connector
    .to("duckdb", "bronze.orders", path="analytics.duckdb")
    .dbt(select="orders")
    .run()
)
```

O projeto dbt é descoberto uma vez. `project_dir`, `profiles_dir` e `target` continuam disponíveis como seletores quando o ambiente não segue o layout padrão.

## 7. Controle a resposta sem boilerplate

Se a lista estiver aninhada:

```python
records="payload.data.orders"
```

Se você quiser somente algumas colunas:

```python
select=["id", "customer_id", "amount", "updated_at"]
```

Isso evita loops manuais apenas para remover campos que você não pretende consumir.

## 8. Entenda sem abrir a implementação

```python
print(connector.explain())
```

`explain()` mostra a resolução segura do conector sem executar uma nova chamada e sem exibir o valor da autenticação.

## 9. Regra de performance

A conveniência não roda heurísticas por registro. Resolução de configuração acontece antes da extração; detecção de paginação e do caminho de registros usa a resposta que já foi buscada e é cacheada para as próximas páginas daquela execução.

```text
intenção do usuário
      ↓
resolução única
      ↓
objetos tipados
      ↓
hot path direto e em batches
```

## 10. Modo avançado continua disponível

Nada impede o uso explícito dos contratos internos:

```python
RestConnector(
    name="orders",
    base_url=url,
    pagination=CursorPagination(
        cursor_param="after",
        cursor_field="meta.next_cursor",
    ),
    auth=BearerAuth(provider, "API_TOKEN"),
    incremental=IncrementalStrategy(...),
)
```

Use esse nível quando a API realmente exigir controle especial; não como boilerplate obrigatório.

## Próximos passos

- [Autenticação](../guides/authentication.md)
- [Paginação](../guides/pagination.md)
- [Incremental](../guides/incremental.md)
- [Managed mode](../guides/managed-mode.md)
