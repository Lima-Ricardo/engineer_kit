# Instalação

## Requisitos

- Python 3.10, 3.11 ou 3.12;
- acesso de rede à API que será consumida;
- um backend opcional apenas se você quiser persistência gerenciada.

## 1. Crie um ambiente virtual

=== "Linux / macOS"

    ```bash
    python -m venv .venv
    source .venv/bin/activate
    ```

=== "Windows PowerShell"

    ```powershell
    py -m venv .venv
    .\.venv\Scripts\Activate.ps1
    ```

## 2. Instale somente o que precisa

Core:

```bash
pip install engineer_kit
```

O core contém connectors, HTTP, paginação, incremental, `ExtractionSession`, contracts e CLI básica. Ele **não** instala engines de storage.

Extras:

| Objetivo | Instalação |
|---|---|
| DuckDB | `pip install "engineer_kit[duckdb]"` |
| Parquet | `pip install "engineer_kit[parquet]"` |
| Delta | `pip install "engineer_kit[delta]"` |
| Lakehouse | `pip install "engineer_kit[platform]"` |
| UI local | `pip install "engineer_kit[ui]"` |
| dbt local | `pip install "engineer_kit[dbt]"` |
| laboratório completo | `pip install "engineer_kit[local]"` |
| todos os recursos | `pip install "engineer_kit[all]"` |

## 3. Verifique a instalação

```bash
engineer_kit --help
```

E no Python:

```python
import engineer_kit
print(engineer_kit.__version__)
```

## pip, uv, Poetry e pipx

O pacote é distribuído pelo PyPI. Portanto você pode usar qualquer instalador compatível com o índice:

```bash
uv pip install engineer_kit
```

```bash
poetry add engineer_kit
```

Para usar somente a CLI em ambiente isolado:

```bash
pipx install "engineer_kit[local]"
```

## Databricks

Em notebook, instale o perfil que realmente precisa. Para embedded mode, normalmente o core é suficiente:

```python
%pip install engineer_kit
```

Reinicie o Python do notebook se o runtime exigir.

## Microsoft Fabric

No notebook:

```python
%pip install engineer_kit
```

Se você for usar `DeltaDestination` diretamente pelo `delta-rs`, instale o extra:

```python
%pip install "engineer_kit[delta]"
```

## Erro `ModuleNotFoundError` para backend opcional

Isso geralmente significa que você importou um adapter sem instalar o extra correspondente. Exemplo:

```text
DeltaDestination → engineer_kit[delta]
ParquetDestination → engineer_kit[parquet]
DuckDBDestination → engineer_kit[duckdb]
```

Veja [Troubleshooting](../reference/troubleshooting.md).
