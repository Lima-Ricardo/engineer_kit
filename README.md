# engineer_kit

**🇧🇷 Português** · [🇺🇸 English](README.en.md)

> **Ingestão REST intent-driven para analytics — 95% de abstração na superfície, runtime tipado, streaming-first e seguro por baixo.**

[![PyPI](https://img.shields.io/pypi/v/engineer-kit)](https://pypi.org/project/engineer-kit/)
[![Python](https://img.shields.io/badge/python-3.10%20%7C%203.11%20%7C%203.12-blue)](#instalação)
[![CI](https://github.com/Lima-Ricardo/engineer_kit/actions/workflows/ci.yml/badge.svg)](https://github.com/Lima-Ricardo/engineer_kit/actions/workflows/ci.yml)
[![Docs](https://github.com/Lima-Ricardo/engineer_kit/actions/workflows/docs.yml/badge.svg)](https://lima-ricardo.github.io/engineer_kit/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

`engineer_kit` abstrai HTTP, autenticação, paginação, incremental, batching, profiling/Data Quality, identidade por primary key, deduplicação, checkpoint, Bronze, destinos e auditoria. Você declara **intenção**; a biblioteca resolve contratos internos quando isso pode ser feito com segurança.

## ⚡ Happy path

```python
from engineer_kit import RestConnector

records = RestConnector(
    base_url="https://api.example.com/orders",
).collect()
```

Quando a API precisa de mais contexto, informe apenas os seletores relevantes:

```python
connector = RestConnector(
    base_url="https://api.example.com/orders",
    auth=token,
    pagination="cursor",
    incremental="updated_at",
    records="payload.orders",
    select=["id", "customer_id", "amount", "updated_at"],
)
```

Sem factories manuais e sem `if/else` para escolher estratégia no caso comum. Objetos tipados continuam disponíveis no modo expert.

## 🔎 Profile antes de ingerir

`profile()` permite entender a fonte **antes da Bronze**, sem gravar destino e sem avançar checkpoint:

```python
report = connector.profile(
    "duplicates",
    "nulls",
    "missing",
    "cardinality",
)

print(report.to_text())
```

Sem seletores, o profile calcula todas as métricas suportadas. Também existem presets como `quality`, `statistics` e `schema`.

Para fontes grandes, a UI usa `sample` por padrão; Python pode usar `scope="sample"` ou `scope="full"` explicitamente.

## 🔑 Primary key primeiro, dedup depois

Identidade e política são independentes:

```python
connector = RestConnector(
    base_url="https://api.example.com/customers",
    primary_key="customer_id",
    dedup=False,
)
```

A PK pode existir com dedup desligado para profiling e metadata. Antes de ativá-la como política, teste uma candidata:

```python
report = connector.profile(
    "duplicates",
    "missing",
    "nulls",
    key="customer_id",
)
```

Depois, se a identidade estiver adequada:

```python
connector = RestConnector(
    base_url="https://api.example.com/customers",
    primary_key="customer_id",
    dedup=True,
)
```

Chaves compostas também são suportadas:

```python
primary_key=["tenant_id", "order_id"]
```

Com `dedup=True`, a primeira ocorrência vence. Se a mesma PK reaparecer, **o registro inteiro posterior** é descartado. PK ausente, `null`, blank ou não escalar interrompe a ingestão em vez de colapsar identidades indefinidas.

## 🖥️ Local Lab

A UI local compartilha os mesmos contratos da API Python e do YAML.

### Dashboard

![Local Lab dashboard](https://raw.githubusercontent.com/Lima-Ricardo/engineer_kit/main/docs/assets/ui/dashboard.svg)

### Pipeline editor — identidade e deduplicação separadas

![Pipeline editor with primary key and dedup](https://raw.githubusercontent.com/Lima-Ricardo/engineer_kit/main/docs/assets/ui/pipeline-editor.svg)

### Data Profile — qualidade e validação de PK antes da Bronze

![Data Profile interface](https://raw.githubusercontent.com/Lima-Ricardo/engineer_kit/main/docs/assets/ui/data-profile.svg)

Inicie com:

```bash
engineer_kit ui --workspace .
```

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

DuckDB, PyArrow, Delta, dbt e a UI permanecem opcionais e lazy.

## 🧪 Probe / preview sem side effects

```python
probe = connector.probe(limit=25)
```

`probe()` / `preview()` leem uma única página para diagnóstico, reutilizam essa resposta na detecção de paginação e não escrevem em `Destination` nem confirmam checkpoint.

## 📄 Paginação por intenção

```python
pagination="cursor"
pagination="page"
pagination="offset"
pagination="link_header"
pagination="next_url"
pagination=False
```

Para APIs fora do padrão:

```python
pagination={
    "type": "cursor",
    "cursor": "meta.next_cursor",
    "param": "after",
}
```

`pagination="auto"` é conservador e reutiliza uma resposta já buscada pela extração; não faz requests extras só para descoberta.

## ⏱️ Incremental e checkpoint seguro

```python
incremental="updated_at"
```

ou:

```python
incremental={
    "field": "updated_at",
    "param": "updated_from",
    "initial_start": "2026-01-01",
}
```

O checkpoint só avança depois do boundary de sucesso. `state_key` permite separar namespaces e os stores oficiais usam compare-and-set para recusar commits derivados de estado obsoleto.

## 🌊 Streaming-first

Dataset pequeno:

```python
records = connector.collect()
```

Volume maior:

```python
for batch in connector.stream():
    process(batch)
```

O extraction batch padrão é de 25.000 registros. A conveniência fica no setup; o hot path continua orientado a streaming e batches limitados.

## 🦆 Managed ingestion

DuckDB:

```python
result = connector.to(
    "duckdb",
    "bronze.orders",
    path="analytics.duckdb",
).run()
```

Parquet e Delta:

```python
connector.to("parquet", "bronze.orders", path="./lake").run()
connector.to("delta", "bronze.orders", path="s3://bucket/lake").run()
```

Transformação dbt opcional:

```python
result = (
    connector
    .to("duckdb", "bronze.orders", path="analytics.duckdb")
    .dbt(select="orders")
    .run()
)
```

## 📝 YAML e CLI

O contrato declarativo possui `version: 1`, validação estrita, rejeição de chaves YAML duplicadas e paridade com os principais intents Python.

```yaml
version: 1
name: customers
connector:
  base_url: https://api.example.com/customers
  records: data
  primary_key: customer_id
  dedup: true
```

```bash
engineer_kit run-config pipelines/customers.yaml
engineer_kit profile-config pipelines/customers.yaml --key customer_id
engineer_kit profile-config pipelines/customers.yaml --html profile.html
```

## 🔎 Transparência e capacidades

```python
print(connector.explain())
```

`explain()` mostra a resolução sem nova chamada HTTP e sem expor autenticação. `capability_manifest()` fornece uma representação serializável das capacidades para CLI/UI.

## 🧱 Arquitetura

```text
REST API
   ↓
RestConnector
   ├── probe / preview   → diagnóstico read-only
   ├── profile           → Data Quality / PK candidate
   ↓
ExtractionSession
   ├── embedded → seu código / Spark / Pandas / Polars
   └── managed  → Destination → DuckDB / Parquet / Delta
                         ↓
                 checkpoint seguro
```

DuckDB é o adapter local de referência, não uma premissa arquitetural.

## 🛡️ Segurança por padrão

HTTPS/TLS seguro, redaction de secrets, limites de resposta/paginação, proteção cross-origin e metadata/link-local, retries controlados, hardening de filesystem/YAML/subprocess, nomes Bronze reservados, colisões de alias explícitas e CI com Ruff, Bandit, `pip-audit`, package checks e testes multi-Python.

Leia [`SECURITY.md`](SECURITY.md) antes de produção.

## 📚 Documentação

- Português: **https://lima-ricardo.github.io/engineer_kit/**
- English: **https://lima-ricardo.github.io/engineer_kit/en/**
- [Primeiro pipeline](https://lima-ricardo.github.io/engineer_kit/getting-started/first-pipeline/)
- [Referência Python](https://lima-ricardo.github.io/engineer_kit/reference/python-api/)
- [Configuração YAML](https://lima-ricardo.github.io/engineer_kit/reference/configuration/)

## 🤝 Contribuição e licença

Leia [`CONTRIBUTING.md`](CONTRIBUTING.md). Vulnerabilidades devem seguir [`SECURITY.md`](SECURITY.md), não issues públicas.

MIT — veja [`LICENSE`](LICENSE).
