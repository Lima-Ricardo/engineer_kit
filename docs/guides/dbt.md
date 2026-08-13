# dbt

A integração dbt é opcional e deliberadamente pós-ingestão.

```text
API → Bronze → checkpoint → dbt
```

## Instalação

```bash
pip install "engineer_kit[dbt]"
```

Ou o laboratório completo:

```bash
pip install "engineer_kit[local]"
```

## Por que dbt roda depois do checkpoint?

Porque uma falha de transformação não deve transformar uma ingestão Bronze válida em um estado ambíguo. A biblioteca considera Bronze + checkpoint a transação de ingestão; dbt é downstream.

## Scaffold

A biblioteca expõe helpers para gerar `sources.yml` e staging a partir do schema lógico. Os campos da Bronze são castados no staging para os tipos analíticos declarados.

## `--select`

Na configuração local:

```yaml
transform:
  type: dbt
  select: tag:daily
```

Sem `select`, o runner usa o comportamento padrão do projeto dbt.
