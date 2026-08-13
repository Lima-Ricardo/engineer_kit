# engineer_kit

**Ingestão confiável de APIs REST para analytics, com streaming, incremental e checkpoint seguro.**

Esta documentação parte do princípio de que você pode estar usando a biblioteca pela primeira vez. Ela explica primeiro o problema, depois o modelo mental, e só então entra nos detalhes de configuração.

<div class="grid cards" markdown>

-   :material-rocket-launch: **Quero começar agora**

    ---

    Instale o pacote e faça sua primeira extração em poucos minutos.

    [:octicons-arrow-right-24: Instalação](getting-started/installation.md)

-   :material-pipe: **Quero montar um pipeline**

    ---

    Veja um exemplo completo de API → Bronze → checkpoint.

    [:octicons-arrow-right-24: Primeiro pipeline](getting-started/first-pipeline.md)

-   :material-apache-spark: **Estou no Fabric/Databricks**

    ---

    Use apenas extração + paginação + incremental e continue com Spark.

    [:octicons-arrow-right-24: Embedded mode](guides/embedded-mode.md)

-   :material-shield-lock: **Preciso usar em produção**

    ---

    Entenda secrets, TLS, limites, logs e responsabilidades do runtime.

    [:octicons-arrow-right-24: Segurança](reference/security.md)

</div>

## O problema que a biblioteca resolve

Uma integração REST aparentemente simples costuma ganhar complexidade rapidamente:

```text
GET /orders
   ↓
paginação
   ↓
rate limit / 429
   ↓
autenticação
   ↓
janela incremental
   ↓
retry
   ↓
schema mudou
   ↓
como saber até onde foi persistido?
```

`engineer_kit` centraliza essas preocupações em contratos testáveis e mantém sua transformação/warehouse desacoplados.

## Fluxo principal

```text
RestConnector
    ↓
ExtractionSession
    ↓
┌───────────────────────────────┐
│ embedded mode                 │
│ seu código → Spark/Polars/... │
└───────────────────────────────┘
              ou
┌───────────────────────────────┐
│ managed mode                  │
│ Destination → State → Audit   │
└───────────────────────────────┘
```

## Por que streaming-first?

A iteração padrão entrega no máximo **25.000 registros por batch**. Você começa a processar antes de terminar de baixar a API inteira e reduz pressão de memória no driver.

```python
run = connector.extract_incremental()
for batch in run:
    process(batch)
run.commit()
```

`collect()` é propositalmente explícito porque materializa tudo em RAM.

## Onde posso usar?

- Python local;
- DuckDB;
- Parquet;
- Delta Lake;
- Databricks;
- Microsoft Fabric;
- workloads em AWS, GCP ou Azure;
- Airflow/Dagster/cron/qualquer orquestrador que execute Python.

A cloud não altera o tipo de source: REST continua sendo REST.

## Próximos passos

1. [Instale a biblioteca](getting-started/installation.md).
2. [Entenda o modelo mental](getting-started/mental-model.md).
3. [Faça o primeiro pipeline](getting-started/first-pipeline.md).
4. Escolha [managed mode](guides/managed-mode.md) ou [embedded mode](guides/embedded-mode.md).
