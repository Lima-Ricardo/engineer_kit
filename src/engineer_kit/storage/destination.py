"""Contratos de persistencia de dados produzidos pela ingestao.

O Pipeline conhece apenas :class:`Destination`. Implementacoes concretas
podem gravar em DuckDB, Parquet, Delta/Lakehouse ou qualquer outro
backend sem alterar a orquestracao.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Iterable

from engineer_kit.storage.schema import EndpointSchema

if TYPE_CHECKING:
    from engineer_kit.storage.run_log import RunLogBackend


@dataclass(frozen=True)
class LoadResult:
    table: str
    rows_loaded: int
    extra_fields_seen: list[str]


class Destination(ABC):
    """Porta de escrita usada pelo Pipeline.

    O usuario escolhe onde a Bronze vive; a extracao nao precisa conhecer
    detalhes do engine ou da plataforma.
    """

    @abstractmethod
    def load(
        self,
        connector_name: str,
        endpoint: str,
        schema: EndpointSchema,
        records: Iterable[dict[str, Any]],
    ) -> LoadResult:
        """Grava registros seguindo o schema declarado.

        Campos fora do schema devem ser preservados e sinalizados, nunca
        descartados silenciosamente nem usados como motivo para falhar a
        carga inteira.
        """

    def default_run_log_backend(self) -> "RunLogBackend | None":
        """Retorna o backend de auditoria natural deste destino, se houver.

        O metodo e opcional para manter o caso simples ergonomico: o
        DuckDB pode registrar execucoes automaticamente, enquanto um
        destino customizado continua livre para nao oferecer auditoria ou
        para receber `run_log_store=` explicitamente no Pipeline.
        """
        return None
