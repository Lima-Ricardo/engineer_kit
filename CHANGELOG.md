# Changelog

Todas as mudanças relevantes do projeto serão documentadas aqui.

O formato segue a ideia de Keep a Changelog e o versionamento usa SemVer quando aplicável.

## [Unreleased]

## [0.3.1] - 2026-08-14

### Documentation

- README principal e versão em inglês atualizados para refletir as capacidades públicas da linha `0.3.x`: `probe()`/`preview()`, profiling/Data Quality, PK candidata, `primary_key`, dedup por PK, checkpoint seguro e Local Lab.
- Novas imagens da interface mostram o dashboard, o editor com identidade/deduplicação separadas e a tela Data Profile antes da Bronze.
- README usa URLs absolutas para os assets de UI para que as imagens também renderizem na descrição longa do PyPI.

### Packaging

- Metadata do PyPI passa a destacar profiling, Data Quality, primary key e deduplicação na descrição e keywords.
- Patch sem alteração de comportamento do runtime; a versão `0.3.1` existe para atualizar os artefatos/metadata imutáveis já publicados como `0.3.0`.

## [0.3.0] - 2026-08-14

### Added

- Paridade declarativa com a superfície intent-driven: YAML aceita `records`, `select`, `params`, paginação curta e incremental opcional.
- `version: 1` para o formato de configuração, com recusa explícita de versões desconhecidas.
- `RestConnector.probe()` / `preview()` para Test Connection e preview de uma página sem destination write ou commit de checkpoint.
- `RestConnector.profile()` e `ProfileReport v1` para profiling/data quality agregado antes da Bronze, com seletores de métricas, presets, filtros de campos e modos full/sample.
- Profiling de `duplicates`, `nulls`, `missing`, `empty`, tipos JSON nativos, cardinalidade, schema/path map e contagem de registros.
- Análise de PK candidata via `profile(..., key=...)`, incluindo duplicatas por chave e registros com PK ausente/null/blank/não escalar.
- Relatório de profiling/Data Quality no terminal, HTML standalone, CLI `profile-config` e Local Lab.
- `primary_key` como identidade simples ou composta independente da política de deduplicação, disponível em Python, YAML e Local Lab.
- `dedup=False` por padrão e `dedup=True` opt-in para remover registros inteiros repetidos pela `primary_key`; a primeira ocorrência vence.
- Deduplicação exata disk-backed por fingerprints SHA-256 da PK em SQLite temporário, compartilhada por `collect()`, `stream()` e managed ingestion.
- Paths declarativos com índices de arrays e chaves entre aspas, além de aliases explícitos em `select`.
- `state_key` para namespaces de checkpoint independentes do nome lógico do connector.
- `capability_manifest()` serializável para descoberta de capacidades por CLI/UI, incluindo `primary_key`, profiling e política de dedup.
- `StateStore.compare_and_set_watermark()` e `StateConflictError` para recusar commits derivados de checkpoints obsoletos.

### Changed

- O happy path YAML sem `incremental` não cria mais um `StateStore`, alinhando-se a `RestConnector(base_url=...)`.
- Profiling preserva os tipos JSON nativos mesmo quando a ingestão normaliza valores para o contrato Bronze atual.
- Métricas de Data Quality não calculadas permanecem semanticamente distintas de resultados calculados com valor zero.
- Cardinalidade de profiling é exata enquanto limitada e passa a estimativa HyperLogLog com erro relativo declarado quando cresce.
- Identidade e deduplicação passam a ser ortogonais: `primary_key="customer_id", dedup=False` mapeia identidade sem remover linhas; `dedup=True` exige `primary_key` explícita.
- `profile("duplicates")` reutiliza `primary_key` configurada mesmo quando dedup está desligado, permitindo validar a identidade antes de ativar a política.
- Dedup por PK é aplicado depois de `select`; quando existe projeção, `primary_key` deve referenciar aliases emitidos. Quando a PK reaparece, o registro inteiro posterior é removido.
- DuckDB faz compare-and-set do checkpoint dentro de uma transação.
- O StateStore JSON usa lock interprocess em POSIX para serializar leitura/compare/write local.
- `__version__` passa a vir do metadata do pacote instalado, eliminando a duplicação manual com `pyproject.toml`.
- A referência YAML e a referência Python PT/EN foram atualizadas para os novos contratos.

### Performance

- Contadores de presença, missing/null/empty e tipos do profiler mantêm estado proporcional aos paths/campos observados, não ao total de registros.
- A deduplicação mantém apenas fingerprints de PK em disco e confirma transações SQLite periodicamente, evitando materialização de registros e um set de hashes ilimitado em RAM.
- Métricas não solicitadas por `profile(...)` não ativam seus agregadores.

### Security

- YAML usa loader derivado de `yaml.SafeLoader`, rejeita chaves duplicadas e campos desconhecidos em blocos conhecidos.
- `connector.dedup` é estritamente booleano no YAML; `true` exige `connector.primary_key`, e strings/listas são recusadas para evitar contratos ambíguos.
- `connector.primary_key` aceita somente string/lista/null; booleanos como `true`/`false` são recusados para não confundir identidade com política de deduplicação.
- Uma deduplicação ativa com PK ausente/null/blank/não escalar interrompe a ingestão em vez de colapsar identidades indefinidas.
- O `ProfileReport` é aggregate-only e seus renderers não persistem valores reais da fonte; HTML faz escaping de paths/warnings.
- Colunas reservadas da Bronze (`_raw`, `_extra`, `_source`, `_run_id`, `_ingestion_key` e demais metadata internos) não podem ser declaradas pela origem.
- Flattening e seleção falham explicitamente quando paths distintos colidem no mesmo nome de coluna/alias, evitando sobrescrita silenciosa de dados.

### Compatibility

- `collect()` e `stream()` preservam seus tipos de retorno; profiling é uma operação separada e opt-in.
- `dedup=False` preserva o comportamento anterior sem custo do tracker temporário.
- A forma programática intermediária e ainda não publicada `dedup=<PK>` é convertida para `primary_key=<PK>, dedup=True` com `DeprecationWarning`; YAML novo aceita apenas o contrato separado.
- `records_path`, `static_params`, `state_store` e as formas declarativas da `0.2` continuam aceitos.
- Objetos programáticos `IncrementalConfig(...)` continuam habilitando incremental por padrão; o novo default não incremental aplica-se ao happy path declarativo quando esse bloco é omitido.

## [0.2.0] - 2026-08-13

### Added

- API pública intent-driven para reduzir boilerplate sem simplificar o core tipado.
- `RestConnector(...).collect()` como caminho de conveniência para datasets pequenos.
- `RestConnector(...).stream()` para consumo em batches limitados.
- Seletores de paginação por string, bool ou dict, incluindo `pagination="cursor"` e modo `auto` conservador.
- Autenticação Bearer simplificada com `auth=<string>` mantendo `AuthStrategy`/`SecretProvider` para controle explícito.
- Descoberta automática do nome do conector e da lista de registros quando não há ambiguidade.
- `records=` para caminhos aninhados e `select=` para projeção de campos sem loops manuais.
- Incremental simplificado com `True`, nome do campo ou dict de seletores.
- `connector.to("duckdb" | "parquet" | "delta", ...).run()` para managed ingestion sem construção manual de adapters.
- Inferência inicial de schema a partir de uma amostra limitada, sem materializar toda a extração.
- Resolução automática do state store e do run log naturais do destino no managed flow.
- `connector.explain()` para inspecionar a resolução sem nova chamada HTTP e sem expor autenticação.
- Fachada dbt com descoberta de projeto e encadeamento `.dbt(...).run()` após ingestão.
- Documentação bilíngue atualizada para o fluxo 95% abstração / 5% configuração explícita.

### Performance

- Resolução de strings/dicts acontece uma vez antes do hot path.
- Paginação automática reutiliza a primeira resposta real; não dispara requests de descoberta.
- Caminho de registros e estratégia de paginação são cacheados durante a execução.
- Schema inference usa somente um prefixo limitado do stream e recoloca a amostra no mesmo iterador.
- Streaming, batching e connection pooling existentes permanecem no caminho de produção.

### Compatibility

- Objetos `PaginationStrategy`, `IncrementalStrategy`, `AuthStrategy`, `StateStore`, `Destination` e `RunLogBackend` continuam aceitos diretamente.
- `extract_incremental()` e `ExtractionSession` permanecem disponíveis para workloads que precisam controlar explicitamente o checkpoint.
- Opções legadas como `records_path`, `static_params`, `date_field` e `state_store` continuam suportadas.

## [0.1.0] - 2026-08-13

### Added

- `Connector` como contrato de source independente de plataforma.
- `ExtractionSession` streaming-first com batch padrão de 25.000 registros.
- Embedded mode com `run.commit()` explícito após processamento downstream.
- `StateStore`, `Destination` e `RunLogBackend` desacoplados.
- Adapters DuckDB, Parquet e Delta Lake.
- Adapter registry lazy para extensões de terceiros.
- Tipos lógicos de schema e contrato Bronze com `_raw`/`_extra`.
- YAML backend-agnostic e CLI `run-config` / `adapters`.
- Local Lab UI e integração opcional com dbt.
- Segurança por padrão em HTTP, secrets, YAML, filesystem, subprocess e UI.
- CI multi-Python, análise de segurança, dependency audit, package validation e stress sintético.
- Documentação completa via MkDocs/GitHub Pages.

### Security

- HTTPS/TLS seguros por padrão.
- Bloqueio de credential-in-URL e header injection.
- Redaction de secrets em logs/erros.
- Limite de response body e paginação.
- Proteção cross-origin em redirects/next URLs.
- Proteção contra traversal/symlink em secrets e paths de adapters.
- Actions de CI pinadas por SHA e Dependabot.
