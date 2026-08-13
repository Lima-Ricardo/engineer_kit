"""URI helpers shared by Delta adapters."""

from __future__ import annotations

from pathlib import Path


def join_table_uri(base_uri: str | Path, *parts: str) -> str:
    """Join local paths and object-store URIs without changing their scheme."""
    base = str(base_uri)
    if "://" in base:
        return "/".join([base.rstrip("/"), *(part.strip("/") for part in parts)])
    return str(Path(base).joinpath(*parts))
