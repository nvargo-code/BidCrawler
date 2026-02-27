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
        # Lazy-load all source modules so plugins self-register
        from bid_crawler.sources import (  # noqa: F401
            sam_gov_source,
            texas_esbd_source,
            bidnet_source,
            opengov_source,
        )
    if source_id not in _REGISTRY:
        raise KeyError(f"Unknown source: {source_id!r}. Available: {list(_REGISTRY)}")
    return _REGISTRY[source_id]


def list_sources() -> list[str]:
    """Return registered source IDs (triggers lazy import)."""
    get_source_class.__module__  # trigger import
    # Force all registrations
    from bid_crawler.sources import (  # noqa: F401
        sam_gov_source,
        texas_esbd_source,
        bidnet_source,
        opengov_source,
    )
    return list(_REGISTRY)
