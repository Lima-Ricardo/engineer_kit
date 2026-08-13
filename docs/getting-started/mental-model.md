# Modelo mental

Antes de configurar qualquer pipeline, guarde seis conceitos.

## 1. Connector = como ler a origem

`RestConnector` sabe fazer requests, autenticar, paginar e transformar cada página em registros.

Ele **não precisa saber** se o código roda em notebook, servidor local ou cluster.

## 2. ExtractionSession = como entregar a extração

`extract_incremental()` devolve uma sessão single-pass.

```python
run = connector.extract_incremental()
for batch in run:
    ...
```

O batch padrão é 25.000 registros. Isso limita o volume entregue ao consumidor por vez.

## 3. StateStore = onde fica o checkpoint

O watermark não pertence ao destino. Ele pertence ao estado da ingestão.

```text
API → extração → persistência → sucesso → checkpoint
```

Se a persistência falhar, o checkpoint não deve avançar.

## 4. Destination = onde fica a Bronze

No managed mode, um `Destination` recebe o stream e materializa os dados.

Implementações oficiais:

- DuckDB;
- Parquet;
- Delta.

## 5. RunLogBackend = auditoria

Registra execução, linhas, janela, status e watermarks sem acoplar o `Pipeline` a um banco específico.

## 6. Transform = depois da ingestão

Transformação não faz parte da transação da Bronze.

```text
API → Bronze → checkpoint confirmado → dbt/Spark/SQL
```

Isso evita perder o estado de uma ingestão bem-sucedida só porque um modelo downstream falhou.

## Managed vs embedded

### Managed

```text
Pipeline
├── Connector
├── Destination
├── StateStore
└── RunLogBackend
```

Use quando quer que a biblioteca cuide da persistência.

### Embedded

```text
Connector → ExtractionSession → seu código → run.commit()
```

Use quando Spark, Pandas, Polars ou outra camada deve continuar controlada pelo seu projeto.
