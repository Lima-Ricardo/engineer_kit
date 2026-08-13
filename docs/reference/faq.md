# FAQ

## `iter_batches()` é só para Spark?

Não. É uma primitive geral de streaming e funciona com Python, Pandas, Polars, Arrow, DuckDB ou qualquer consumidor.

## Por que 25.000 registros?

É um default operacional equilibrado. Ele reduz materialização de memória sem criar batches excessivamente pequenos. Ajuste conforme tamanho médio do registro e workload.

## `page_size` deve ser 25.000?

Não. Respeite o limite/documentação da API. `page_size` e `extraction_batch_size` são independentes.

## Posso hardcodar token?

Sim, com `StaticSecretProvider`, especialmente para estudo/lab. Para credencial real, prefira environment, arquivo ou secret manager.

## A biblioteca substitui Spark?

Não. Em embedded mode ela termina a responsabilidade na extração/checkpoint e você continua com Spark.

## Preciso de DuckDB?

Não. O core não depende de DuckDB. Você pode usar somente extração ou escolher Parquet/Delta.

## Posso rodar no Fabric ou Databricks?

Sim. Instale o pacote no notebook e use embedded mode ou adapters compatíveis com o runtime.

## Por que não existe connector por cloud?

Porque cloud é runtime/storage. O protocolo REST da origem não muda.

## A biblioteca faz merge/upsert automaticamente?

Não infere chave de negócio. Isso evita comportamento destrutivo ou semanticamente incorreto. Defina merge na camada que conhece o domínio.
