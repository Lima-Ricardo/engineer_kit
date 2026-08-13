# Changelog

Todas as mudanças relevantes do projeto serão documentadas aqui.

O formato segue a ideia de Keep a Changelog e o versionamento usa SemVer quando aplicável.

## [0.1.0] - Unreleased

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
