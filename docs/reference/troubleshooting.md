# Troubleshooting

## `ModuleNotFoundError` ao importar um adapter

Instale o extra correspondente:

```text
DuckDB → engineer_kit[duckdb]
Parquet → engineer_kit[parquet]
Delta → engineer_kit[delta]
UI → engineer_kit[ui]
dbt → engineer_kit[dbt]
```

## A resposta é um objeto, não uma lista

Configure `records_path`:

```python
records_path="results"
```

Para estrutura incomum, passe uma função Python ao `RestConnector`.

## O pipeline para cedo na paginação

Confirme o contrato da API. `PageNumberPagination` encerra quando recebe uma página menor que `page_size`. APIs que sempre devolvem páginas completas mas indicam fim por outro campo exigem outra estratégia/customização.

## Loop/max pages

Se a API repete o mesmo cursor/URL, a proteção de loop interrompe a extração. Corrija a estratégia ou o parser; não aumente `max_pages` sem entender a causa.

## `ResponseTooLargeError`

Uma única página excedeu o limite HTTP configurado. Primeiro reduza o page size da API. Só aumente o limite de resposta se o payload esperado justificar.

## HTTP em rede interna

HTTP puro é recusado por padrão. Se a rede é confiável e isso é realmente necessário, faça opt-in explícito no `HttpClient`. Prefira TLS sempre que possível.

## O watermark não avançou

Verifique:

1. o stream foi consumido completamente?
2. o destination/downstream terminou sem erro?
3. `run.commit()` foi chamado no embedded mode?
4. o `StateStore` está persistindo no local esperado?

## Dados duplicados depois de retry

Adapters oficiais protegem retry da mesma janela por `ingestion_key`. Se você implementou um Destination próprio ou embedded persistence, sua lógica downstream precisa ser idempotente para o padrão de falha que você aceita.

## Spark está lento com muitos batches

Não crie milhares de DataFrames pequenos. Aumente o extraction batch ou faça staging Parquet/Delta e depois leia com Spark.

## UI não abre

Confirme que instalou:

```bash
pip install "engineer_kit[ui]"
```

ou:

```bash
pip install "engineer_kit[local]"
```

Depois verifique porta, bind e senha exibida/definida pela CLI.
