# Destinos: DuckDB, Parquet e Delta

Os três destinos implementam o mesmo contrato conceitual, mas cada formato tem um uso ideal.

## DuckDB

Instalação:

```bash
pip install "engineer_kit[duckdb]"
```

Melhor para:

- desenvolvimento local;
- testes;
- pipelines pequenos/médios;
- local lab + dbt-duckdb.

## Parquet

```bash
pip install "engineer_kit[parquet]"
```

Melhor para:

- filesystem local/montado;
- staging para Spark;
- lake simples sem runtime de banco;
- interoperabilidade com engines analíticos.

## Delta

```bash
pip install "engineer_kit[delta]"
```

Melhor para:

- Lakehouse;
- storage object;
- ambientes que já usam Delta;
- state/audit também em tabelas Delta.

## Contrato Bronze

Os adapters oficiais seguem o mesmo contrato:

```text
campos declarados → string/null
_raw              → registro original
_extra            → campos inesperados
_run_id           → execução
_window_*         → janela incremental
_ingestion_key    → identidade de retry
```

A tipagem analítica declarada em `ColumnSpec` é usada para staging/casts, não para inferir agressivamente a Bronze.

## `append` vs `overwrite`

Use `append` para Bronze incremental normal. O adapter substitui de forma segura uma ingestão repetida da mesma janela quando necessário.

Use `overwrite` quando o dataset inteiro é uma fotografia e deve substituir o alvo.

## Particionamento

Parquet/Delta aceitam opções de particionamento quando suportadas pela configuração do adapter. Escolha colunas de partição com cuidado: cardinalidade alta demais gera muitos arquivos/diretórios pequenos.
