"""Une conector(es) -> destino em uma unidade atomica: e essa unidade
que um scheduler (nosso ou externo, tipo Airflow) chama para rodar uma
carga completa.

O watermark de cada conector so e commitado depois que o load no
destino teve sucesso — uma falha no meio do caminho refaz a mesma
janela no proximo run, sem duplicar nem perder dado. Uma fonte falhar
nao impede as outras de rodar: um problema numa API nao deveria travar
o resto do pipeline.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

from engineer_kit.connectors.base import BaseConnector
from engineer_kit.storage.destination import Destination
from engineer_kit.storage.schema import EndpointSchema

logger = logging.getLogger("engineer_kit.pipeline")


@dataclass
class PipelineSource:
    connector: BaseConnector
    schema: EndpointSchema


@dataclass
class StepResult:
    connector_name: str
    rows_loaded: int
    error: Optional[str] = None


@dataclass
class PipelineResult:
    steps: list[StepResult]

    @property
    def success(self) -> bool:
        return all(step.error is None for step in self.steps)


class Pipeline:
    def __init__(self, sources: list[PipelineSource], destination: Destination) -> None:
        self._sources = sources
        self._destination = destination

    def run(self) -> PipelineResult:
        return PipelineResult(steps=[self._run_step(source) for source in self._sources])

    def _run_step(self, source: PipelineSource) -> StepResult:
        connector = source.connector
        try:
            records = connector.extract()
            result = self._destination.load(
                connector_name=connector.name,
                endpoint=connector.name,
                schema=source.schema,
                records=records,
            )
            connector.commit_watermark()
            logger.info("Conector '%s': %d linha(s) carregada(s).", connector.name, result.rows_loaded)
            return StepResult(connector_name=connector.name, rows_loaded=result.rows_loaded)
        except Exception as exc:
            logger.exception(
                "Conector '%s' falhou -- watermark NAO avancado, proximo run refaz a mesma janela.",
                connector.name,
            )
            return StepResult(connector_name=connector.name, rows_loaded=0, error=str(exc))
