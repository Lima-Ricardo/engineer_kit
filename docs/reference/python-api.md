# Referência Python: API pública

A API pública tem dois níveis: uma fachada intent-driven para o caso comum e os contratos tipados para controle avançado.

## `RestConnector`

Happy path:

```python
records = RestConnector(
    base_url=url,
    auth=token,
    pagination="cursor",
    incremental=True,
).collect()
```

Parâmetros mais usados:

```python
RestConnector(
    base_url,
    auth=None,
    pagination="auto",
    incremental=None,
    records=None,
    select=None,
    params=None,
    method="GET",
)
```

Seletores simples aceitos:

- `pagination`: `"auto"`, `"cursor"`, `"page"`, `"offset"`, `"link_header"`, `"next_url"`, `False`, dict ou `PaginationStrategy`;
- `incremental`: `False`/`None`, `True`, nome do campo de data, dict ou `IncrementalStrategy`;
- `auth`: string para Bearer ou `AuthStrategy` explícita;
- `records`: caminho pontuado para a lista de registros, por exemplo `"payload.items"`;
- `select`: campos que devem permanecer no resultado;
- `params`: parâmetros estáticos da request.

As opções legadas `name`, `records_path`, `static_params`, `state_store`, `date_params`, `date_field`, `incremental_mode` e os objetos de estratégia continuam suportados.

### `collect()`

```python
records = connector.collect()
```

Materializa a extração completa e confirma o checkpoint somente depois da coleta terminar com sucesso. Indicado para conjuntos pequenos.

### `stream()`

```python
for batch in connector.stream():
    ...
```

Entrega batches limitados e confirma o checkpoint somente depois do consumo completo.

### `to()`

```python
result = connector.to(
    "duckdb",
    "bronze.orders",
    path="analytics.duckdb",
).run()
```

Adapters oficiais podem ser escolhidos por nome: `duckdb`, `parquet` e `delta`. O managed flow resolve schema inicial, state e audit backends compatíveis uma vez antes da execução.

### `dbt()` no managed flow

```python
result = (
    connector
    .to("duckdb", "bronze.orders", path="analytics.duckdb")
    .dbt(select="orders")
    .run()
)
```

O projeto dbt é descoberto a partir do diretório atual e ancestrais. `project_dir`, `profiles_dir` e `target` permanecem disponíveis quando necessários.

### `explain()`

```python
plan = connector.explain()
```

Retorna um resumo seguro da resolução do conector, sem executar outra chamada HTTP e sem retornar o valor de autenticação.

## Paginação avançada

```python
pagination={
    "type": "cursor",
    "cursor": "meta.next_cursor",
    "param": "after",
}
```

Ou use diretamente `CursorPagination`, `PageNumberPagination`, `OffsetPagination`, `LinkHeaderPagination`, `NextUrlPagination` ou uma implementação própria de `PaginationStrategy`.

## Incremental avançado

```python
incremental={
    "field": "updated_at",
    "param": "updated_from",
    "initial_start": "2026-01-01",
}
```

Em managed mode, um state store escolhido automaticamente pode ser substituído pelo backend natural do destino. Quando `state_store` é passado explicitamente, ele é respeitado.

## `ExtractionSession`

A API de baixo nível permanece disponível:

```python
run = connector.extract_incremental()
for batch in run:
    ...
run.commit()
```

A sessão é single-pass e recusa commit parcial.

## Contratos estáveis

- `PaginationStrategy`: `initial_params()` e `next_params(...)`;
- `StateStore`: `get_watermark(...)` e `set_watermark(...)`;
- `Destination`: contrato de persistência Bronze;
- `RunLogBackend`: `record(RunLogEntry)`;
- `SecretProvider`: `get(name)`;
- `Pipeline`: `run()`.

A fachada simples resolve esses contratos; não os substitui. Isso mantém extensibilidade e evita custo de abstração dentro do hot path.
