# Referência Python: principais contratos

Esta é uma referência orientada ao uso. Para descobrir assinaturas exatas na versão instalada, use `help()`/IDE.

## `RestConnector`

Parâmetros centrais:

```python
RestConnector(
    name,
    base_url,
    pagination,
    method,
    state_store=None,
    incremental_mode=...,
    initial_start=None,
    date_field=None,
    incremental=None,
    auth=None,
    date_params=None,
    static_params=None,
    records_path=None,
    http_client=None,
    extraction_batch_size=25_000,
    max_pages=...,
    allow_cross_origin_pagination=False,
)
```

### `extract_incremental()`

Retorna `ExtractionSession`.

### `extract()`

Mantido como caminho de compatibilidade da API inicial. Novos workloads devem preferir `ExtractionSession`.

## `ExtractionSession`

Operações principais:

```python
for batch in run:
    ...
```

```python
run.iter_batches(size=...)
```

```python
run.collect()
```

```python
run.commit()
```

A sessão é single-pass e não permite commit parcial.

## `PaginationStrategy`

Contrato:

```python
initial_params() -> dict
next_params(page, previous_params) -> dict | None
```

## `StateStore`

```python
get_watermark(connector_name)
set_watermark(connector_name, watermark)
```

## `Destination`

Contrato de persistência da Bronze. Adapters podem expor recursos extras, mas o `Pipeline` depende somente do contrato base.

## `RunLogBackend`

```python
record(RunLogEntry)
```

## `SecretProvider`

```python
get(name: str) -> str
```

## `Pipeline`

```python
result = pipeline.run()
```

`PipelineResult` contém sucesso/status e resultados das etapas, incluindo identidade de run, linhas, destino, janela e watermarks quando disponíveis.
