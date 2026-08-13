# Incremental, watermark e checkpoint

Incremental não é somente adicionar `?since=`. O ponto mais importante é **quando confirmar que uma janela terminou**.

## Regra principal

```text
ler watermark
    ↓
extrair API
    ↓
persistir/processar
    ↓
sucesso
    ↓
confirmar watermark
```

Nunca confirme antes do downstream.

## `data_date`

Use quando o registro contém uma data confiável para incremental.

```python
incremental_mode=IncrementalMode.DATA_DATE
```

O novo watermark é derivado da maior data válida encontrada na extração.

Exemplo de YAML:

```yaml
incremental:
  mode: data_date
  initial_start: "2026-01-01"
  date_field: updated_at
```

## `ingestion_date`

Use quando a API é filtrada pela janela de ingestão e você não quer derivar o checkpoint de um campo do registro.

```yaml
incremental:
  mode: ingestion_date
  initial_start: "2026-01-01"
```

## Parâmetros de data da API

```yaml
date_params:
  start: updated_from
  end: updated_to
  format: "%Y-%m-%d"
```

Eles só definem como a janela calculada é enviada à API.

## StateStore separado da Bronze

O checkpoint pode estar no mesmo engine físico do destino, mas é um contrato diferente.

```text
Destination ≠ StateStore
```

Isso permite, por exemplo:

```text
Bronze → Parquet
State  → JSON local
```

ou:

```text
Bronze → Delta
State  → Delta metadata table
```

## Falha downstream no embedded mode

```python
run = connector.extract_incremental()

for batch in run:
    df = transform(batch)
    persist(df)  # falhou aqui

# não chamar run.commit()
```

Na próxima execução, a janela ainda está pendente.

## Consumo parcial

`ExtractionSession` não permite `commit()` depois de consumir apenas parte do stream. Isso evita confirmar uma janela quando ainda havia páginas por ler.

## Retry idempotente dos adapters oficiais

A biblioteca gera uma identidade determinística de ingestão para a transição de checkpoint. Se os dados foram persistidos e o checkpoint falhou, repetir a mesma janela não deve simplesmente duplicar a Bronze.

A implementação física varia:

- DuckDB: substituição transacional da mesma ingestão;
- Parquet: staging/promoção segura;
- Delta: overwrite por predicado da ingestão.

## Late arriving data

Se a API pode inserir registros antigos depois do watermark, uma janela estritamente monotônica pode não capturá-los. Nesse caso, adote uma margem/overlap na estratégia incremental ou uma regra específica da API. Essa é uma decisão de domínio e não pode ser inferida genericamente pela biblioteca.
