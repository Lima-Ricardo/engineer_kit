"""Platform-neutral connector contract.

Connectors describe how records are extracted from a source. They do not know
where the code is running (local, Fabric, Databricks, AWS, GCP) or where data
will be persisted. Platform/storage concerns belong to adapters.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import date
from typing import Union

from engineer_kit.connectors.extraction import ExtractionSession


class Connector(ABC):
    """Base contract for streaming incremental sources."""

    name: str

    @abstractmethod
    def extract_incremental(
        self,
        end: Union[date, str] = "today",
        *,
        batch_size: int | None = None,
    ) -> ExtractionSession:
        """Create a single-pass extraction session for one source window."""


__all__ = ["Connector"]
