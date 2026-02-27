"""Load transformed bids into DuckDB via upsert."""

from __future__ import annotations
import logging
from typing import Any

from bid_crawler.db import BidDB

logger = logging.getLogger(__name__)


def load_bids(db: BidDB, bids: list[dict[str, Any]]) -> tuple[int, int]:
    """
    Upsert bids into the database.
    Returns (inserted_count, updated_count).
    """
    if not bids:
        return 0, 0
    inserted, updated = db.upsert_bids(bids)
    logger.info("Loaded %d bids: %d new, %d updated", len(bids), inserted, updated)
    return inserted, updated
