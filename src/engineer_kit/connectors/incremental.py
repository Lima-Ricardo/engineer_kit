"""Resolve janelas incrementais usando um StateStore independente de backend.

Dois modos:
- DATA_DATE: usa a data do proprio dado (ex: updated_at do registro).
- INGESTION_DATE: usa a data da ultima execucao concluida com sucesso.

A estrategia conhece apenas o contrato StateStore. O estado pode viver
em DuckDB localmente, em uma tabela Delta/Lakehouse ou em qualquer
implementacao fornecida pelo usuario.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from enum import Enum
from typing import Optional, Union

from engineer_kit.storage.state_store import StateStore, Watermark


class IncrementalMode(str, Enum):
    DATA_DATE = "data_date"
    INGESTION_DATE = "ingestion_date"


@dataclass
class IncrementalWindow:
    start: Optional[date]
    end: date


class IncrementalStrategy:
    def __init__(
        self,
        connector_name: str,
        state_store: StateStore,
        mode: IncrementalMode = IncrementalMode.DATA_DATE,
        initial_start: Optional[date] = None,
    ) -> None:
        self._connector_name = connector_name
        self._state_store = state_store
        self._mode = mode
        self._initial_start = initial_start

    def resolve_window(self, end: Union[date, str] = "today") -> IncrementalWindow:
        resolved_end = date.today() if end == "today" else end
        watermark = self._state_store.get_watermark(self._connector_name)

        if watermark is None:
            return IncrementalWindow(start=self._initial_start, end=resolved_end)

        if self._mode is IncrementalMode.DATA_DATE:
            start = watermark.last_data_date
        else:
            start = watermark.last_run_at.date()

        return IncrementalWindow(start=start, end=resolved_end)

    def commit(self, window: IncrementalWindow, max_data_date: Optional[date] = None) -> None:
        """Avanca o checkpoint depois que o destino confirmou a carga.

        A estrategia nao conhece o destino dos dados. Essa separacao
        permite usar o mesmo incremental em DuckDB, Parquet, Delta ou
        plataformas de Lakehouse sem alterar o conector.
        """
        watermark = Watermark(
            last_run_at=datetime.now(timezone.utc),
            last_data_date=max_data_date or window.end,
            cursor_value=None,
        )
        self._state_store.set_watermark(self._connector_name, watermark)
