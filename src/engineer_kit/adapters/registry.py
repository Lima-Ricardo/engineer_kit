"""Lazy adapter registry used by declarative pipeline configuration.

The core does not import optional backends. Registry entries point to small
builder callables that are imported only when a user selects that adapter.
Third-party packages can register their own builders at runtime.
"""

from __future__ import annotations

from dataclasses import dataclass
from importlib import import_module
from typing import Any, Callable


class AdapterNotFoundError(ValueError):
    """Raised when a declarative config names an unregistered adapter."""


@dataclass(frozen=True)
class AdapterContext:
    """Runtime resources shared while building one declarative pipeline."""

    pipeline_name: str
    runtime: Any = None
    destination_config: Any = None


Builder = Callable[[Any, AdapterContext], Any]
BuilderRef = Builder | str

_DESTINATIONS: dict[str, BuilderRef] = {}
_STATE_STORES: dict[str, BuilderRef] = {}
_RUN_LOGS: dict[str, BuilderRef] = {}


def _key(name: str) -> str:
    return name.strip().lower()


def _load(ref: BuilderRef) -> Builder:
    if callable(ref):
        return ref
    module_name, symbol = ref.split(":", 1)
    return getattr(import_module(module_name), symbol)


def _register(registry: dict[str, BuilderRef], name: str, builder: BuilderRef) -> None:
    registry[_key(name)] = builder


def register_destination(name: str, builder: BuilderRef) -> None:
    _register(_DESTINATIONS, name, builder)


def register_state_store(name: str, builder: BuilderRef) -> None:
    _register(_STATE_STORES, name, builder)


def register_run_log(name: str, builder: BuilderRef) -> None:
    _register(_RUN_LOGS, name, builder)


def _build(registry: dict[str, BuilderRef], kind: str, name: str, config: Any, context: AdapterContext):
    key = _key(name)
    ref = registry.get(key)
    if ref is None:
        available = ", ".join(sorted(registry)) or "nenhum"
        raise AdapterNotFoundError(
            f"Adapter de {kind} '{name}' nao registrado. Disponiveis: {available}."
        )
    return _load(ref)(config, context)


def build_destination(name: str, config: Any, context: AdapterContext):
    return _build(_DESTINATIONS, "destination", name, config, context)


def build_state_store(name: str, config: Any, context: AdapterContext):
    return _build(_STATE_STORES, "state_store", name, config, context)


def build_run_log(name: str, config: Any, context: AdapterContext):
    return _build(_RUN_LOGS, "run_log", name, config, context)


def resolve_auto(name: str, *, destination_type: str, kind: str) -> str:
    """Resolve ``auto`` to the destination's natural persistence backend."""
    if _key(name) != "auto":
        return _key(name)
    destination_key = _key(destination_type)
    registry = _STATE_STORES if kind == "state" else _RUN_LOGS
    if destination_key in registry:
        return destination_key
    if "file" in registry:
        return "file"
    raise AdapterNotFoundError(
        f"Nao existe adapter automatico de {kind} para destination '{destination_type}'."
    )


def available_adapters() -> dict[str, tuple[str, ...]]:
    return {
        "destination": tuple(sorted(_DESTINATIONS)),
        "state_store": tuple(sorted(_STATE_STORES)),
        "run_log": tuple(sorted(_RUN_LOGS)),
    }


# Built-ins remain lazy: these modules are not imported until selected.
register_destination("duckdb", "engineer_kit.adapters.duckdb.runtime:build_destination")
register_state_store("duckdb", "engineer_kit.adapters.duckdb.runtime:build_state_store")
register_run_log("duckdb", "engineer_kit.adapters.duckdb.runtime:build_run_log")

register_destination("parquet", "engineer_kit.adapters.parquet.runtime:build_destination")
register_state_store("parquet", "engineer_kit.adapters.parquet.runtime:build_state_store")
register_run_log("parquet", "engineer_kit.adapters.parquet.runtime:build_run_log")

register_state_store("file", "engineer_kit.adapters.files.runtime:build_state_store")
register_run_log("file", "engineer_kit.adapters.files.runtime:build_run_log")

register_destination("delta", "engineer_kit.adapters.delta.runtime:build_destination")
register_state_store("delta", "engineer_kit.adapters.delta.runtime:build_state_store")
register_run_log("delta", "engineer_kit.adapters.delta.runtime:build_run_log")
