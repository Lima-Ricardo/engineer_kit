# Referência de configuração YAML

Esta página descreve os blocos aceitos pelo pipeline declarativo.

## Estrutura completa

```yaml
name: orders
connector: {}
columns: []
destination: {}
state: {}
run_log: {}
secrets: {}
transform: {}
```

## `name`

Identificador lógico do pipeline. Também participa da chave de estado e de nomes padrão.

## `connector`

| Campo | Tipo | Default | Descrição |
|---|---|---:|---|
| `base_url` | string | obrigatório | URL absoluta da API |
| `method` | `GET`/`POST` | `GET` | método da extração |
| `records_path` | string/null | null | chave onde está a lista no JSON |
| `static_params` | mapping | `{}` | parâmetros fixos |
| `extraction_batch_size` | int | `25000` | registros por batch entregue |
| `max_pages` | int | limite do core | limite defensivo de páginas |
| `auth` | object | none | estratégia de autenticação |
| `pagination` | object | obrigatório conceitualmente | estratégia de páginas |
| `incremental` | object | data_date | cálculo da janela |
| `date_params` | object | vazio | nomes dos parâmetros de data |

### `auth`

```yaml
auth:
  type: bearer      # none | bearer | api_key
  secret_key: API_TOKEN
  param_name: X-API-Key
  location: header  # query | header
```

### `pagination`

```yaml
pagination:
  type: page  # none | page | offset | cursor | link_header | next_url
  params: {}
```

Os parâmetros dependem da estratégia. Veja [Paginação](../guides/pagination.md).

### `incremental`

```yaml
incremental:
  mode: data_date   # data_date | ingestion_date
  initial_start: "2026-01-01"
  date_field: updated_at
```

### `date_params`

```yaml
date_params:
  start: updated_from
  end: updated_to
  format: "%Y-%m-%d"
```

## `columns`

```yaml
columns:
  - name: id
    dtype: bigint
```

Tipos lógicos conhecidos:

```text
string integer bigint float decimal boolean date timestamp json
```

## `destination`

```yaml
destination:
  type: parquet
  path: ./lake
  schema: bronze
  batch_size: 5000
  write_mode: append
  partition_by: []
  options: {}
```

`type` pode ser um adapter registrado, incluindo `duckdb`, `parquet` e `delta`.

## `state`

```yaml
state:
  type: auto
  path: null
  options: {}
```

`auto` escolhe a implementação natural para o destino quando disponível.

## `run_log`

```yaml
run_log:
  enabled: true
  type: auto
  path: null
  options: {}
```

Por compatibilidade, booleanos antigos continuam aceitos.

## `secrets`

```yaml
secrets:
  type: env       # env | file
  path: null
  allow_inline_values: false
```

Referência em opções:

```yaml
some_token: ${SECRET:MY_TOKEN}
```

## `transform`

```yaml
transform:
  type: none   # none | dbt
  select: null
```

## Limite do arquivo

O loader limita o tamanho do YAML e usa `yaml.safe_load`. Arquivos não UTF-8 ou estruturas inválidas são rejeitados.
