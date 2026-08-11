"""Contrato que qualquer destino (DuckDB hoje; Redshift, Data Lake,
Snowflake depois) precisa cumprir. O Pipeline so conhece esta
interface — nunca fala com DuckDBLoader ou qualquer implementacao
concreta diretamente. Isso e o que permite trocar ou adicionar um
destino sem tocar na orquestracao.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Iterable

from engineer_kit.storage.schema import EndpointSchema


@dataclass
class LoadResult:
    table: str
    rows_loaded: int
    extra_fields_seen: list[str]


class Destination(ABC):
    @abstractmethod
    def load(
        self,
        connector_name: str,
        endpoint: str,
        schema: EndpointSchema,
        records: Iterable[dict[str, Any]],
    ) -> LoadResult:
        """Grava os registros no destino, seguindo o schema declarado.
        Campos fora do schema devem ser preservados (ex: numa coluna
        _extra) e sinalizados via log — nunca descartados em silencio
        nem motivo de falha da carga."""
