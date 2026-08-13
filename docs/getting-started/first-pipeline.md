# Primeiro pipeline, passo a passo

Vamos consumir uma API fictícia de pedidos, paginada por número de página, e salvar em Parquet.

## 1. Instale o extra Parquet

```bash
pip install "engineer_kit[parquet]"
```

## 2. Entenda a API

Suponha que a documentação diga:

```text
GET https://api.example.com/orders
?page=1
&per_page=1000
&updated_from=2026-01-01
```

E a resposta:

```json
{
  "results": [
    {"id": 123, "updated_at": "2026-08-12T10:00:00Z"}
  ]
}
```

Antes de escrever código, identifique:

- método: `GET`;
- lista: `results`;
- paginação: `page` + `per_page`;
- filtro incremental: `updated_from`;
- campo de data do registro: `updated_at`.

## 3. Crie `pipelines/orders.yaml`

```yaml
name: orders

connector:
  base_url: https://api.example.com/orders
  method: GET
  records_path: results
  extraction_batch_size: 25000
  pagination:
    type: page
    params:
      page_param: page
      page_size_param: per_page
      page_size: 1000
  incremental:
    mode: data_date
    initial_start: "2026-01-01"
    date_field: updated_at
  date_params:
    start: updated_from

columns:
  - name: id
    dtype: bigint
  - name: updated_at
    dtype: timestamp

destination:
  type: parquet
  path: ./lake
  schema: bronze
  batch_size: 5000
  write_mode: append

state:
  type: auto

run_log:
  enabled: true
  type: auto

secrets:
  type: env
```

## 4. Execute

```bash
engineer_kit run-config pipelines/orders.yaml
```

No primeiro run, o estado começa em `2026-01-01`. Depois de a Bronze ser confirmada, o `StateStore` grava o novo watermark.

## 5. Execute novamente

A segunda execução lê o watermark anterior e monta uma nova janela incremental. Você não precisa consultar a Bronze inteira para descobrir a última data.

## 6. O que acontece se falhar?

Se a API ou o destino falhar:

```text
checkpoint anterior permanece
```

Se o destino persistir, mas o checkpoint falhar, o run é marcado como erro de checkpoint. Os adapters oficiais usam uma identidade determinística de ingestão para tornar o retry da mesma janela seguro.

## 7. E se a API adicionar um campo?

O schema declarado não é alterado automaticamente. O campo inesperado é preservado em `_extra`, e o registro original continua em `_raw`.

## 8. Próximos passos

- [Autenticação](../guides/authentication.md)
- [Paginação](../guides/pagination.md)
- [Incremental](../guides/incremental.md)
- [Managed mode](../guides/managed-mode.md)
