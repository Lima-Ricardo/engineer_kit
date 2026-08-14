"""Build inspection-only connectors from declarative pipeline configuration."""

from __future__ import annotations

from engineer_kit.config.pipeline_config import PipelineConfig, _resolve_secret_refs
from engineer_kit.connectors.rest import RestConnector
from engineer_kit.storage.state_store import StateStore, Watermark


class InspectionStateStore(StateStore):
    """Empty state backend that refuses writes.

    A profile launched from Local Lab/CLI should evaluate the configured source
    window without mutating production/local checkpoints. ``RestConnector``
    starts from declarative ``initial_start`` when this store returns None.
    """

    def get_watermark(self, connector_name: str) -> Watermark | None:
        return None

    def set_watermark(self, connector_name: str, watermark: Watermark) -> None:
        raise RuntimeError("profiling inspection state is read-only")


def connector_from_config(config: PipelineConfig) -> RestConnector:
    """Return a connector suitable for probe/profile, with no writable state."""
    provider = config.secrets.build()
    incremental = config.connector.incremental
    state_store = InspectionStateStore() if incremental.enabled else None
    params = _resolve_secret_refs(config.connector.resolved_params(), provider)

    return RestConnector(
        name=config.name,
        base_url=config.connector.base_url,
        method=config.connector.method,
        auth=config.connector.auth.build(provider),
        pagination=config.connector.pagination.build(),
        incremental=None if incremental.enabled else False,
        state_store=state_store,
        state_key=config.connector.state_key,
        incremental_mode=incremental.resolve_mode(),
        initial_start=incremental.resolve_initial_start(),
        date_field=incremental.date_field,
        date_params=config.connector.date_params.build(),
        params=params or None,
        records=config.connector.resolved_records(),
        select=config.connector.select,
        primary_key=config.connector.primary_key,
        dedup=config.connector.dedup,
        extraction_batch_size=config.connector.extraction_batch_size,
        max_pages=config.connector.max_pages,
    )


__all__ = ["InspectionStateStore", "connector_from_config"]