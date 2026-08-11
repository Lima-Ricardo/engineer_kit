"""Decide qual janela de datas pedir da API a cada execucao, usando o
watermark salvo no IngestionStateStore.

Dois modos:
- DATA_DATE: usa a data do proprio dado (ex: updated_at do registro).
  Simples, mas um registro que chega atrasado (updated_at antigo, so
  agora inserido na origem) fica fora da janela se ela ja passou.
- INGESTION_DATE: sempre busca "desde a ultima vez que rodei com sucesso",
  independente da data do dado. Nunca perde registro atrasado, mas nao
  serve para ressincronizar um periodo especifico da origem.

A escolha e por conector, nao global — depende de como aquela API
especifica se comporta.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from enum import Enum
from typing import Optional, Union

from engineer_kit.storage.state_store import IngestionStateStore, Watermark


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
        state_store: IngestionStateStore,
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
            # primeira execucao: usa o start inicial configurado (ou None = "desde sempre")
            return IncrementalWindow(start=self._initial_start, end=resolved_end)

        if self._mode is IncrementalMode.DATA_DATE:
            start = watermark.last_data_date
        else:
            start = watermark.last_run_at.date()

        return IncrementalWindow(start=start, end=resolved_end)

    def commit(self, window: IncrementalWindow, max_data_date: Optional[date] = None) -> None:
        """So deve ser chamado depois que o load no DuckDB teve sucesso —
        garante que uma falha no meio do caminho refaz a mesma janela no
        proximo run, em vez de pular dados."""
        watermark = Watermark(
            last_run_at=datetime.now(timezone.utc),
            last_data_date=max_data_date or window.end,
            cursor_value=None,
        )
        self._state_store.set_watermark(self._connector_name, watermark)
