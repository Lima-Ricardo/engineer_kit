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
    state_key=None,
    dedup=False,
    method="GET",
)
```

Seletores simples aceitos:

- `pagination`: `"auto"`, `"cursor"`, `"page"`, `"offset"`, `"link_header"`, `"next_url"`, `False`, dict ou `PaginationStrategy`;
- `incremental`: `False`/`None`, `True`, nome do campo de data, dict ou `IncrementalStrategy`;
- `auth`: string para Bearer ou `AuthStrategy` explícita;
- `records`: caminho para a lista de registros, por exemplo `"payload.items"`;
- `select`: lista/string de campos ou mapping `{path: alias}`;
- `params`: parâmetros estáticos da request;
- `state_key`: namespace explícito do checkpoint; por default usa o nome do connector;
- `dedup`: `False` por padrão; passe uma PK simples (`"customer_id"`) ou composta (`["tenant_id", "order_id"]`) para remover registros completos cuja identidade já apareceu.

`dedup=True` é recusado porque não declara identidade. A primeira ocorrência de uma PK vence. Chave ausente, `null`, blank ou não escalar é erro de ingestão. A deduplicação acontece depois de `select`; quando há projeção, as chaves de dedup devem usar os aliases emitidos.

Paths declarativos aceitam objetos, índices de arrays e chaves entre aspas, por exemplo `items[0].sku` e `payload["odd.key"].value`. Wildcards não são usados para evitar mudança implícita de cardinalidade. Colisões de aliases são recusadas e exigem alias explícito.

As opções legadas `name`, `records_path`, `static_params`, `state_store`, `date_params`, `date_field`, `incremental_mode` e os objetos de estratégia continuam suportados.

### `probe()` / `preview()`

```python
probe = connector.probe(limit=25)
```

Faz **uma única página** para diagnóstico e devolve um `ProbeResult` com registros limitados, payload bruto, headers, caminho de registros resolvido, estratégia de paginação detectada, status HTTP, latência e tamanho da resposta quando disponível.

`probe()` e `preview()` são read-only em relação ao runtime de ingestão: não escrevem em `Destination` e não confirmam checkpoint. A detecção de paginação `auto` reutiliza a página já buscada e não dispara uma segunda request apenas para inferência.

### `profile()` / Data Quality

`profile()` é uma operação de primeira classe no mesmo nível de `collect()` e `stream()`:

```python
report = connector.profile()
print(report)
```

Sem seletores significa **perfil completo**. Para calcular somente o necessário:

```python
report = connector.profile(
    "duplicates",
    "nulls",
    "missing",
)
```

Presets também podem ser usados:

```python
connector.profile("quality")
connector.profile("statistics")
connector.profile("schema")
```

E a análise por campo pode ser limitada:

```python
report = connector.profile(
    "nulls",
    "missing",
    fields=["id", "customer.email"],
)
```

#### Validando uma PK candidata

Esse é um dos usos principais do profiling. Antes de configurar dedup, teste a identidade candidata:

```python
report = connector.profile(
    "duplicates",
    "missing",
    "nulls",
    key="customer_id",
)

print(report.duplicates.key_fields)
print(report.duplicates.duplicate_rows)
print(report.duplicates.invalid_key_rows)
```

Chave composta:

```python
report = connector.profile(
    "duplicates",
    key=["tenant_id", "order_id"],
)
```

O profile não assume que o nome `id` torna um campo PK. Ele mede o contrato observado: completude da chave, valores inválidos e violações de unicidade. A decisão de declarar essa identidade em `dedup=` continua com o usuário.

Se o connector já foi criado com:

```python
connector = RestConnector(
    base_url=url,
    dedup=["customer_id"],
)
```

então `connector.profile("duplicates")` reutiliza essa PK automaticamente. Sem `key=` e sem dedup configurado, a métrica `duplicates` compara registros completos.

O retorno é um `ProfileReport v1`. O mesmo objeto alimenta código Python, relatório terminal e HTML:

```python
text = report.to_text()
html = report.to_html()
quality = report.quality
```

O profiler diferencia `missing`, `null` e valores vazios. Também observa paths JSON, tipos nativos, cardinalidade e duplicatas quando essas métricas são solicitadas. Métrica não calculada permanece distinta de resultado zero.

O profiling é **aggregate-only**: valores reais não entram no relatório. Para fontes grandes, contagens, presença, missing/null/empty e tipos usam estado proporcional ao número de campos observados, não ao número total de linhas. Cardinalidade permanece exata enquanto pequena e muda para estimativa aproximada com erro declarado quando cresce. Detecção exata de duplicatas/PKs exige estado proporcional às identidades únicas, portanto usa fingerprints SHA-256 em SQLite temporário em disco em vez de RAM sem limite.

Por padrão no Python, `scope="full"` percorre a fonte configurada inteira. Para uma leitura limitada:

```python
report = connector.profile(scope="sample", limit=10_000)
```

`profile()` **não grava Destination/Bronze e não confirma checkpoint**. Quando o connector possui `dedup=[...]`, o profiler observa o conjunto antes da remoção e reutiliza a PK configurada para a análise de duplicatas.

A CLI usa um default mais conservador:

```bash
engineer-kit profile-config pipeline.yaml
engineer-kit profile-config pipeline.yaml --metrics duplicates,nulls,missing
engineer-kit profile-config pipeline.yaml --metrics duplicates,missing,nulls --key customer_id
engineer-kit profile-config pipeline.yaml --scope full
engineer-kit profile-config pipeline.yaml --html profile.html
```

No Local Lab, a tela **Data Profile** usa o mesmo `ProfileReport`; começa em sample de 10.000 registros, permite informar uma PK candidata e exige seleção explícita de `full` para percorrer toda a fonte.

### `collect()`

```python
records = connector.collect()
```

Materializa a extração completa e confirma o checkpoint somente depois da coleta terminar com sucesso. Indicado para conjuntos pequenos. Com `dedup=["customer_id"]`, a primeira ocorrência de cada PK é mantida e os registros completos posteriores com a mesma chave são removidos.

### `stream()`

```python
for batch in connector.stream():
    ...
```

Entrega batches limitados e confirma o checkpoint somente depois do consumo completo. A mesma regra de PK configurada em `dedup` se aplica ao stream e, consequentemente, à ingestão gerenciada que consome a mesma `ExtractionSession`.

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

Retorna um resumo seguro da resolução do conector, sem executar outra chamada HTTP e sem retornar o valor de autenticação. O resumo inclui `state_key`, a lista de PKs em `dedup` quando ativa e os pares path/alias de `select`.

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

O commit do watermark usa compare-and-set: se outro run avançar o mesmo checkpoint depois que a janela foi resolvida, a execução obsoleta recebe `StateConflictError` em vez de sobrescrever silenciosamente o estado mais novo. DuckDB implementa esse check dentro de transação; o state JSON local usa lock interprocess em POSIX.

## `ExtractionSession`

A API de baixo nível permanece disponível:

```python
run = connector.extract_incremental()
for batch in run:
    ...
run.commit()
```

A sessão é single-pass e recusa commit parcial. Quando o connector possui uma PK em `dedup`, a sessão aplica a mesma deduplicação streaming antes de emitir registros/batches.

## `capability_manifest()`

```python
from engineer_kit import capability_manifest

manifest = capability_manifest()
```

Retorna metadata serializável sobre métodos REST, autenticação, paginação, incremental, dedup por PK, profiling, destinations registrados, state stores, run logs e comandos dbt. O objetivo é permitir que CLI/UI descubram capabilities sem manter listas duplicadas; a execução continua definida pelos contratos tipados do core.

## Contratos estáveis

- `PaginationStrategy`: `initial_params()` e `next_params(...)`;
- `StateStore`: `get_watermark(...)`, `set_watermark(...)` e `compare_and_set_watermark(...)`;
- `Destination`: contrato de persistência Bronze;
- `RunLogBackend`: `record(RunLogEntry)`;
- `SecretProvider`: `get(name)`;
- `ProfileReport`: contrato versionado de profiling/data quality;
- `Pipeline`: `run()`.

A fachada simples resolve esses contratos; não os substitui. Isso mantém extensibilidade e evita custo de abstração dentro do hot path.
