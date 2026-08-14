# engineer_kit

**🇧🇷 Português** · [🇺🇸 English](README.en.md)

> **Ingestão REST intent-driven para analytics — poucas linhas na superfície, runtime tipado e streaming-first por baixo.**

[![CI](https://github.com/Lima-Ricardo/engineer_kit/actions/workflows/ci.yml/badge.svg)](https://github.com/Lima-Ricardo/engineer_kit/actions/workflows/ci.yml)
[![Docs](https://github.com/Lima-Ricardo/engineer_kit/actions/workflows/docs.yml/badge.svg)](https://lima-ricardo.github.io/engineer_kit/)
[![Python](https://img.shields.io/badge/python-3.10%20%7C%203.11%20%7C%203.12-blue)](#instalação)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

`engineer_kit` abstrai HTTP, autenticação, paginação, incremental, batching, checkpoint, Bronze, destinos e auditoria. O usuário declara **intenção**; a biblioteca resolve os contratos internos quando isso pode ser feito com segurança.

## ⚡ Happy path

```python
from engineer_kit import RestConnector

records = RestConnector(
    base_url=url,
    auth=token,
    pagination="cursor",
    incremental=True,
).collect()
```

Sem `if/else` para selecionar paginação, sem factory manual e sem precisar instanciar `CursorPagination`, `StateStore` ou `Destination` no caso comum.

Uma API pública simples pode ser apenas:

```python
records = RestConnector(
    base_url="https://api.example.com/orders",
).collect()
```

`GET` é o padrão, o nome do connector é derivado da URL, a lista de registros é detectada quando não há ambiguidade e `pagination="auto"` é conservador.

## 🎯 95% abstração, 5% seletores

Quando a API exige detalhes, informe somente esses detalhes:

```python
connector = RestConnector(
    base_url=url,
    auth=token,
    pagination={"type": "page", "size": 1000},
    incremental={
        "field": "updated_at",
        "param": "updated_from",
        "initial_start": "2026-01-01",
    },
    records="payload.orders",
    select=["id", "customer_id", "amount", "updated_at"],
)
```

A mesma API aceita objetos tipados quando você precisa de controle avançado.

## 📦 Instalação

```bash
pip install engineer-kit
```

Extras opcionais:

```bash
pip install "engineer-kit[duckdb]"
pip install "engineer-kit[parquet]"
pip install "engineer-kit[delta]"
pip install "engineer-kit[dbt]"
pip install "engineer-kit[ui]"
pip install "engineer-kit[local]"
pip install "engineer-kit[all]"
```

DuckDB, PyArrow, Delta, dbt e a UI continuam opcionais e lazy.

## 📄 Paginação por intenção

```python
pagination="cursor"
pagination="page"
pagination="offset"
pagination="link_header"
pagination="next_url"
pagination=False
```

Strings são case-insensitive. Para APIs fora do padrão:

```python
pagination={
    "type": "cursor",
    "cursor": "meta.next_cursor",
    "param": "after",
}
```

O modo `auto` usa uma resposta que a extração já buscou; não dispara requests extras só para descobrir paginação.

## 🔐 Autenticação

Uma string em `auth` significa Bearer:

```python
auth=token
```

Em produção, você pode manter a origem do segredo explícita:

```python
from engineer_kit import BearerAuth, EnvSecretProvider

auth = BearerAuth(EnvSecretProvider(), "API_TOKEN")
```

`FileSecretProvider`, `ApiKeyAuth` e `SecretProvider` customizado continuam disponíveis.

## ⏱️ Incremental

```python
incremental=True
```

ou, se o campo de watermark é conhecido:

```python
incremental="updated_at"
```

Quando a origem exige filtro incremental específico:

```python
incremental={
    "field": "updated_at",
    "param": "updated_from",
    "initial_start": "2026-01-01",
}
```

O checkpoint só avança depois do boundary de sucesso adequado. Projeções com `select=` não escondem o campo de watermark da lógica interna de checkpoint.

## 🌊 `collect()` e `stream()`

Dataset pequeno:

```python
records = connector.collect()
```

Volume maior:

```python
for batch in connector.stream():
    process(batch)
```

A extração permanece streaming-first; o extraction batch padrão é de 25.000 registros.

## 🦆 DuckDB sem boilerplate

```python
result = RestConnector(
    base_url=url,
    auth=token,
    pagination="cursor",
    incremental=True,
).to(
    "duckdb",
    "bronze.orders",
    path="analytics.duckdb",
).run()
```

O managed flow resolve conexão, destino, schema inicial, state store e auditoria compatíveis. `Destination`, `StateStore` e `RunLogBackend` continuam separados internamente.

## 🗂️ Parquet e Delta

```python
connector.to("parquet", "bronze.orders", path="./lake").run()
```

```python
connector.to("delta", "bronze.orders", path="s3://bucket/lake").run()
```

## 🔧 dbt encadeado

```python
result = (
    connector
    .to("duckdb", "bronze.orders", path="analytics.duckdb")
    .dbt(select="orders")
    .run()
)
```

O projeto dbt pode ser descoberto a partir do diretório atual e seus ancestrais. `project_dir`, `profiles_dir` e `target` permanecem disponíveis quando necessários.

## 🔎 Transparência

```python
print(connector.explain())
```

`explain()` mostra a resolução do connector sem fazer nova chamada HTTP e sem expor o valor de autenticação.

## 🧠 Performance

A conveniência fica no setup, não no hot path:

```text
input simples
    ↓
resolução única
    ↓
strategy e paths cacheados
    ↓
streaming + batches diretos
```

Não há heurística por registro. A inferência inicial de schema em managed mode usa uma amostra limitada e devolve essa amostra ao mesmo iterador, sem refazer a API.

## 🧩 Modo expert

```python
from engineer_kit import BearerAuth, CursorPagination, RestConnector

connector = RestConnector(
    name="orders",
    base_url=url,
    pagination=CursorPagination(
        cursor_param="after",
        cursor_field="next_cursor",
    ),
    auth=BearerAuth(provider, "API_TOKEN"),
    incremental=custom_incremental_strategy,
    state_store=custom_state_store,
)
```

A fachada simples resolve os contracts; não os substitui.

## 🧱 Arquitetura

```text
REST API
   ↓
RestConnector
   ↓
ExtractionSession
   ├── embedded → seu código / Spark / Pandas / Polars
   └── managed  → Destination → DuckDB / Parquet / Delta
                         ↓
                  checkpoint seguro
```

DuckDB é o adapter local de referência, não uma premissa arquitetural.

## 🛡️ Segurança por padrão

A biblioteca mantém HTTPS/TLS seguro, redaction de secrets, limites de resposta e paginação, detecção de loops, proteção cross-origin e metadata/link-local, política segura de retries, hardening de filesystem/YAML/subprocess e CI com Ruff, Bandit, `pip-audit`, package checks e stress sintético.

Leia [`SECURITY.md`](SECURITY.md) antes de produção.

## 📝 YAML, CLI e Local Lab

```bash
engineer_kit run-config pipelines/orders.yaml
engineer_kit ui --workspace .
```

A API Python simplificada, o modo declarativo e a UI compartilham os mesmos contracts internos.

## 📚 Documentação

Português: **https://lima-ricardo.github.io/engineer_kit/**

English: **https://lima-ricardo.github.io/engineer_kit/en/**

Veja [Primeiro pipeline](https://lima-ricardo.github.io/engineer_kit/getting-started/first-pipeline/) e [Referência Python](https://lima-ricardo.github.io/engineer_kit/reference/python-api/).

## 🤝 Contribuição e licença

Leia [`CONTRIBUTING.md`](CONTRIBUTING.md). Vulnerabilidades devem seguir [`SECURITY.md`](SECURITY.md), não issues públicas.

MIT — veja [`LICENSE`](LICENSE).
