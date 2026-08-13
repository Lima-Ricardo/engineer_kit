# Managed mode

Managed mode é para quando o `engineer_kit` deve coordenar a ingestão completa.

```text
Connector
   ↓
Destination
   ↓
StateStore
   ↓
RunLogBackend
```

## Quando usar

- job local simples;
- Bronze em DuckDB, Parquet ou Delta;
- pipeline declarativo YAML;
- você quer checkpoint e auditoria gerenciados pela biblioteca.

## Construindo a partir de YAML

```python
from engineer_kit import build_pipeline, load_pipeline_config

config = load_pipeline_config("pipelines/orders.yaml")
pipeline = build_pipeline(config)
result = pipeline.run()

if not result.success:
    raise RuntimeError(result.steps)
```

DuckDB precisa de uma conexão existente quando usado programaticamente:

```python
import duckdb
from engineer_kit import build_pipeline, load_pipeline_config

config = load_pipeline_config("pipelines/orders.yaml")
conn = duckdb.connect("warehouse.duckdb")
try:
    result = build_pipeline(config, conn).run()
finally:
    conn.close()
```

## Ordem transacional

```text
extract
  ↓
Destination load/commit
  ↓
StateStore checkpoint
  ↓
RunLog audit (best effort)
```

Uma falha de auditoria não desfaz dados/checkpoint já confirmados. Uma falha de destination impede o avanço do checkpoint.

## Write modes

`append` é o default da Bronze e preserva janelas anteriores. O retry da mesma janela é protegido pela identidade de ingestão.

`overwrite` substitui o alvo inteiro.

Merge/upsert por chave de negócio não é inventado automaticamente porque exige semântica explícita do domínio.
