# Referência de configuração YAML

Esta página descreve os blocos aceitos pelo pipeline declarativo. A configuração usa a mesma superfície orientada a intenção da API Python e é validada de forma estrita antes da execução.

## Happy path

Uma API pública sem incremental pode começar assim:

```yaml
version: 1
name: orders
connector:
  base_url: https://api.example.com/orders
```

`GET`, paginação `auto`, `dedup: false` e detecção conservadora da lista de registros são os defaults. Nenhum `StateStore` é criado quando incremental não foi habilitado.

## Estrutura completa

```yaml
version: 1
name: orders
connector: {}
columns: []
destination: {}
state: {}
run_log: {}
secrets: {}
transform: {}
```

## `version`

A versão atual do formato é `1`. O campo é opcional para arquivos anteriores à introdução do versionamento; quando omitido, `1` é assumido. Versões desconhecidas são recusadas em vez de interpretadas parcialmente.

## `name`

Identificador lógico do pipeline. Por compatibilidade ele também é a chave padrão do checkpoint. Use `connector.state_key` quando pipelines com o mesmo nome lógico precisarem de namespaces de estado distintos.

## `connector`

| Campo | Tipo | Default | Descrição |
|---|---|---:|---|
| `base_url` | string | obrigatório | URL absoluta da API |
| `method` | `GET`/`POST` | `GET` | método da extração |
| `records` | string/null | auto | caminho da lista no JSON |
| `select` | list/string/mapping | null | campos projetados; mapping permite `path: alias` |
| `params` | mapping | `{}` | parâmetros fixos da API |
| `state_key` | string/null | `name` | namespace explícito do checkpoint |
| `primary_key` | string/list/null | null | identidade simples ou composta do registro emitido |
| `dedup` | bool | `false` | ativa deduplicação usando `primary_key` |
| `extraction_batch_size` | int | `25000` | registros por batch entregue |
| `max_pages` | int | limite do core | limite defensivo de páginas |
| `auth` | object | none | estratégia de autenticação |
| `pagination` | string/bool/object | `auto` | estratégia de páginas |
| `incremental` | bool/string/object | `false` | checkpoint/janela incremental |
| `date_params` | object | vazio | nomes dos parâmetros de data |

`records_path` e `static_params` continuam sendo lidos como aliases de compatibilidade da `0.2`; configs novas devem preferir `records` e `params`. Se `records` e `records_path` forem definidos com valores diferentes, a configuração é recusada.

### `records`

```yaml
records: data.orders
```

Quando omitido, o primeiro payload é usado para detectar de forma conservadora uma lista como `data`, `results`, `items`, `records` ou caminhos aninhados. Ambiguidade exige um caminho explícito.

### `select`

Lista simples:

```yaml
select:
  - id
  - amount
  - customer.id
```

Aliases explícitos:

```yaml
select:
  customer.id: customer_id
  totals.net: net_amount
```

Paths aceitam navegação por objetos e índices explícitos, por exemplo `items[0].sku` e `payload["odd.key"].value`. Wildcards não são usados porque não é seguro alterar cardinalidade implicitamente. Se dois paths produzirem o mesmo alias normalizado, a configuração falha e exige aliases explícitos.

### `primary_key` e `dedup`

A identidade e a política de deduplicação são independentes. É possível mapear uma PK sem remover registros:

```yaml
connector:
  base_url: https://api.example.com/customers
  primary_key: customer_id
  dedup: false
```

Isso permite que `profile()` use a identidade configurada para avaliar completude, nulls/missing e duplicatas antes de a deduplicação ser ativada.

Para ativar a deduplicação:

```yaml
connector:
  base_url: https://api.example.com/customers
  primary_key: customer_id
  dedup: true
```

Chave composta:

```yaml
connector:
  base_url: https://api.example.com/orders
  primary_key:
    - tenant_id
    - order_id
  dedup: true
```

`dedup` é estritamente booleano e `false` é sempre o default. `dedup: true` sem `primary_key` é recusado. Strings como `"false"` e formas antigas como `dedup: customer_id` ou `dedup: [customer_id]` também são recusadas no YAML para evitar dois contratos concorrentes.

Quando a deduplicação está ativa e uma PK válida reaparece, a primeira ocorrência vence e **o registro inteiro posterior é removido**, mesmo que outros campos tenham valores diferentes. Valores ausentes, `null`, blank, arrays ou objetos em qualquer componente da PK fazem a ingestão falhar explicitamente em vez de colapsar registros sem identidade.

A identidade é avaliada **depois de `select`**. Portanto, quando existe projeção, os campos de `primary_key` devem ser aliases efetivamente emitidos pelo `select`.

O runtime guarda somente fingerprints SHA-256 da PK em SQLite temporário e remove o arquivo ao final. Isso evita um conjunto sem limite em RAM; o uso de disco cresce com a quantidade de identidades únicas observadas.

Use `profile()` para avaliar uma chave candidata antes de persistir ou ativar a política:

```bash
engineer-kit profile-config pipelines/orders.yaml \
  --metrics duplicates,missing,nulls \
  --key tenant_id,order_id
```

Depois que `primary_key` estiver configurada, `--key` pode ser omitido e o profile reutiliza a identidade declarada, mesmo com `dedup: false`.

### `params`

```yaml
params:
  status: open
  region: BR
```

Valores sensíveis inline continuam bloqueados por padrão; use referências `${SECRET:NOME}`.

### `auth`

```yaml
auth:
  type: bearer      # none | bearer | api_key
  secret_key: API_TOKEN
  param_name: X-API-Key
  location: header  # query | header
```

### `pagination`

Forma curta:

```yaml
pagination: cursor
```

Valores suportados:

```text
auto none page offset cursor link_header next_url
```

Também é possível desligar com `pagination: false` ou usar opções guiadas:

```yaml
pagination:
  type: page
  size: 1000
  param: page
  start_page: 1
```

A forma expert/legada com `params: {}` continua válida. Veja [Paginação](../guides/pagination.md).

### `incremental`

Desligado, que também é o default:

```yaml
incremental: false
```

Checkpoint pela data de execução:

```yaml
incremental: true
```

Campo de watermark conhecido:

```yaml
incremental: updated_at
```

Forma explícita:

```yaml
incremental:
  enabled: true
  mode: data_date   # data_date | ingestion_date
  initial_start: "2026-01-01"
  date_field: updated_at
```

O checkpoint só é confirmado depois da escrita bem-sucedida no destino. O commit também verifica se o watermark que originou a janela ainda é o estado atual; uma execução concorrente obsoleta é recusada em vez de sobrescrever silenciosamente um checkpoint mais novo.

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

Os nomes de metadata da Bronze (`_raw`, `_extra`, `_source`, `_run_id`, `_ingestion_key` e demais campos internos) são reservados e não podem ser declarados como colunas da origem. Colunas duplicadas também são rejeitadas.

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

`type` pode ser qualquer adapter registrado, incluindo `duckdb`, `parquet` e `delta`. As opções físicas são específicas do adapter; o runtime mantém os contratos de `Destination`, `StateStore` e `RunLogBackend` separados.

## `state`

```yaml
state:
  type: auto
  path: null
  options: {}
```

Esse bloco só é materializado quando incremental está ativo. `auto` escolhe a implementação natural para o destino quando disponível. O alias legado `state_store` continua aceito, mas `state` e `state_store` não podem coexistir no mesmo arquivo.

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

A forma curta `transform: dbt` também é aceita.

## Profiling a partir da configuração

O mesmo YAML pode ser inspecionado sem carregar Bronze:

```bash
engineer-kit profile-config pipelines/orders.yaml
engineer-kit profile-config pipelines/orders.yaml --metrics duplicates,nulls,missing
engineer-kit profile-config pipelines/orders.yaml --metrics duplicates,missing,nulls --key customer_id
engineer-kit profile-config pipelines/orders.yaml --scope full
```

Quando `--key` é omitido, uma `connector.primary_key` já configurada é reutilizada automaticamente para a métrica `duplicates`, independentemente de `dedup`. Sem `--key` e sem `primary_key`, duplicatas são avaliadas pela linha completa.

O comando usa um `StateStore` de inspeção que nunca aceita writes; portanto o profile não avança o checkpoint configurado. O Local Lab expõe a mesma operação na tela **Data Profile**, inclusive para testar uma PK candidata antes de persistir a identidade ou ativar dedup.

## Validação e segurança do arquivo

O loader limita o tamanho do YAML, aceita apenas UTF-8 e usa um loader derivado de `yaml.SafeLoader`. Chaves duplicadas são recusadas, assim como campos desconhecidos em blocos conhecidos. Isso evita erros silenciosos como `pagniation:` ser ignorado por causa de um typo.

A validação não substitui a segurança do runtime: HTTPS/TLS, redaction de secrets, limites de resposta/paginação e proteção de redirects continuam centralizados no core.