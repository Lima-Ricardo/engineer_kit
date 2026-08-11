# engineer_kit

Biblioteca Python para ingestão de APIs: conectores -> DuckDB -> dbt.

- **Conectores**: classes que abstraem `requests`, paginação e incremental.
- **DuckDB**: camada bronze, JSON desaninhado automaticamente.
- **dbt**: staging gerado automaticamente a partir do bronze; silver/gold escritos por você.

Ver `examples/` para um pipeline completo rodando contra uma API real.
