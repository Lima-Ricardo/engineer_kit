"""Streaming Parquet destination for the portable Bronze layer."""

from __future__ import annotations

import hashlib
import logging
import shutil
from pathlib import Path
from typing import Any, Iterable
from uuid import uuid4

import pyarrow.parquet as pq

from engineer_kit.adapters._arrow import bronze_arrow_schema, rows_to_table
from engineer_kit.storage.batching import DEFAULT_BATCH_SIZE, iter_in_batches, validate_batch_size
from engineer_kit.storage.bronze import build_bronze_rows
from engineer_kit.storage.destination import (
    Destination,
    LoadContext,
    LoadResult,
    WriteMode,
)
from engineer_kit.storage.identifiers import validate_identifier
from engineer_kit.storage.schema import EndpointSchema
from engineer_kit.terminal_log import visual_logger

logger = logging.getLogger("engineer_kit.storage.parquet")


def _path_token(value: str) -> str:
    """Return a filesystem-safe deterministic token for an ingestion identity."""
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:24]


class ParquetDestination(Destination):
    """Write bounded-memory Parquet Bronze files.

    APPEND writes one file per ingestion window. Its final filename uses a
    deterministic token derived from ``ingestion_key`` supplied by Pipeline,
    so retrying a window atomically replaces that window's previous file instead
    of duplicating it. Staging paths use an internal random token, so even a
    caller-supplied/reused ``run_id`` cannot escape the base path or collide
    with another concurrent attempt. OVERWRITE stages a complete replacement
    directory before promotion.
    """

    def __init__(
        self,
        base_path: str | Path,
        batch_size: int = DEFAULT_BATCH_SIZE,
        compression: str = "snappy",
        write_mode: WriteMode | str = WriteMode.APPEND,
    ) -> None:
        self._base_path = Path(base_path)
        self._batch_size = validate_batch_size(batch_size)
        self._compression = compression
        self._write_mode = WriteMode.parse(write_mode)

    @property
    def write_mode(self) -> WriteMode:
        return self._write_mode

    def load(
        self,
        connector_name: str,
        endpoint: str,
        schema: EndpointSchema,
        records: Iterable[dict[str, Any]],
    ) -> LoadResult:
        return self._load(
            connector_name,
            endpoint,
            schema,
            records,
            LoadContext.adhoc(connector_name),
        )

    def load_with_context(
        self,
        connector_name: str,
        endpoint: str,
        schema: EndpointSchema,
        records: Iterable[dict[str, Any]],
        context: LoadContext,
    ) -> LoadResult:
        return self._load(connector_name, endpoint, schema, records, context)

    def _load(
        self,
        connector_name: str,
        endpoint: str,
        schema: EndpointSchema,
        records: Iterable[dict[str, Any]],
        context: LoadContext,
    ) -> LoadResult:
        endpoint_name = validate_identifier(endpoint, "endpoint")
        endpoint_dir = self._base_path / endpoint_name
        staging_root = self._base_path / ".engineer_kit_staging" / endpoint_name
        staging_root.mkdir(parents=True, exist_ok=True)

        arrow_schema = bronze_arrow_schema(schema)
        total_rows = 0
        all_extra_fields: set[str] = set()
        staging_token = uuid4().hex
        ingestion_token = _path_token(context.ingestion_key)

        if self._write_mode is WriteMode.OVERWRITE:
            staged_dir = staging_root / f"replace-{staging_token}"
            staged_dir.mkdir(parents=True, exist_ok=False)
            temp_path = staged_dir / f"part-{staging_token}.parquet"
            final_path = temp_path
        else:
            endpoint_dir.mkdir(parents=True, exist_ok=True)
            temp_path = staging_root / f"{staging_token}.parquet.tmp"
            final_path = endpoint_dir / f"part-{ingestion_token}.parquet"

        writer: pq.ParquetWriter | None = None
        try:
            for batch in iter_in_batches(records, self._batch_size):
                rows, extra_fields = build_bronze_rows(
                    connector_name,
                    endpoint_name,
                    schema,
                    batch,
                    context=context,
                )
                all_extra_fields.update(extra_fields)
                table = rows_to_table(rows, schema)
                if writer is None:
                    writer = pq.ParquetWriter(
                        temp_path,
                        arrow_schema,
                        compression=self._compression,
                        flavor="spark",
                    )
                writer.write_table(table, row_group_size=len(rows))
                total_rows += len(rows)

            if writer is not None:
                writer.close()
                writer = None

            if self._write_mode is WriteMode.OVERWRITE:
                self._promote_overwrite(staged_dir, endpoint_dir, staging_token)
            elif total_rows:
                temp_path.replace(final_path)
            else:
                # Successful retry of the same window with no rows means the
                # previous committed representation of that window is stale.
                final_path.unlink(missing_ok=True)
        except Exception:
            if writer is not None:
                writer.close()
            if self._write_mode is WriteMode.OVERWRITE:
                shutil.rmtree(staged_dir, ignore_errors=True)
            else:
                temp_path.unlink(missing_ok=True)
            raise

        self._report_extra_fields(connector_name, endpoint_name, all_extra_fields)
        visual_logger.success(
            "'{}': {} registros gravados em Parquet: {}",
            connector_name,
            total_rows,
            endpoint_dir,
        )
        return LoadResult(
            table=str(endpoint_dir),
            rows_loaded=total_rows,
            extra_fields_seen=sorted(all_extra_fields),
        )

    @staticmethod
    def _promote_overwrite(staged_dir: Path, endpoint_dir: Path, staging_token: str) -> None:
        backup = endpoint_dir.with_name(f".{endpoint_dir.name}.backup-{staging_token}")
        had_previous = endpoint_dir.exists()
        try:
            if had_previous:
                endpoint_dir.replace(backup)
            staged_dir.replace(endpoint_dir)
        except Exception:
            if not endpoint_dir.exists() and backup.exists():
                backup.replace(endpoint_dir)
            raise
        else:
            if backup.exists():
                shutil.rmtree(backup)

    @staticmethod
    def _report_extra_fields(
        connector_name: str, endpoint_name: str, extra_fields: set[str]
    ) -> None:
        if not extra_fields:
            return
        logger.warning(
            "Endpoint '%s': campos fora do schema preservados em _extra: %s",
            endpoint_name,
            sorted(extra_fields),
        )
        visual_logger.warning(
            "'{}': {} coluna(s) nova(s) preservada(s) em _extra: {}",
            connector_name,
            len(extra_fields),
            sorted(extra_fields),
        )


__all__ = ["ParquetDestination"]
