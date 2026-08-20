"""Source plugin registry."""

from __future__ import annotations
from typing import Type, TYPE_CHECKING

if TYPE_CHECKING:
    from bid_crawler.sources.base import BaseSource

_REGISTRY: dict[str, Type["BaseSource"]] = {}


def register(source_id: str):
    """Decorator that registers a source class by its ID."""
    def decorator(cls: Type["BaseSource"]) -> Type["BaseSource"]:
        _REGISTRY[source_id] = cls
        return cls
    return decorator


def get_source_class(source_id: str) -> Type["BaseSource"]:
    """Return the source class for the given ID, importing lazily."""
    if source_id not in _REGISTRY:
        _load_all()
    if source_id not in _REGISTRY:
        raise KeyError(f"Unknown source: {source_id!r}. Available: {list(_REGISTRY)}")
    return _REGISTRY[source_id]


def list_sources() -> list[str]:
    """Return registered source IDs (triggers lazy import)."""
    _load_all()
    return list(_REGISTRY)


def _load_all() -> None:
    """Import all source modules so they self-register, then map generic classes."""
    from bid_crawler.sources import (  # noqa: F401
        sam_gov_source,
        texas_esbd_source,
        bidnet_source,
        opengov_source,
        fort_worth_bonfire_source,
        dallas_bonfire_source,
        bonfire_source,
        ionwave_source,
        bidsync_source,
    )
    from bid_crawler.sources.bonfire_source import BonfireSource
    from bid_crawler.sources.ionwave_source import IonWaveSource

    for _tid in _BONFIRE_TENANT_IDS:
        if _tid not in _REGISTRY:
            _REGISTRY[_tid] = BonfireSource

    for _tid in _IONWAVE_TENANT_IDS:
        if _tid not in _REGISTRY:
            _REGISTRY[_tid] = IonWaveSource


# Bonfire tenants configured via YAML extras
_BONFIRE_TENANT_IDS = [
    "dallas_isd_bonfire",
    "richardson_isd_bonfire",
    "rockwall_isd_bonfire",
]

# Ion Wave tenants configured via YAML extras
_IONWAVE_TENANT_IDS = [
    "ionwave_arlington_isd",
    "ionwave_mesquite_isd",
    "ionwave_tarrant_county",
    "ionwave_irving",
    "ionwave_plano",
    "ionwave_fwisd",
    "ionwave_nwisd",
    "ionwave_red_oak_isd",
    "ionwave_terrell_isd",
    "ionwave_melissa_isd",
    "ionwave_tips",
]
