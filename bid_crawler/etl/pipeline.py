"""ETL pipeline orchestrator."""

from __future__ import annotations
import logging
from datetime import datetime, timezone
from typing import Any, Optional

from bid_crawler.config import CrawlerConfig, CriteriaConfig, SourceConfig
from bid_crawler.db import BidDB
from bid_crawler.matcher import BidMatcher
from bid_crawler.sources import get_source_class
from bid_crawler.etl.transformer import transform_bid
from bid_crawler.etl.loader import load_bids

logger = logging.getLogger(__name__)


class PipelineResult:
    def __init__(self, source_id: str):
        self.source_id = source_id
        self.fetched = 0
        self.matched = 0
        self.inserted = 0
        self.updated = 0
        self.errors: list[str] = []
        self.skipped_fresh = False

    def __repr__(self):
        return (
            f"PipelineResult(source={self.source_id!r}, "
            f"fetched={self.fetched}, matched={self.matched}, "
            f"inserted={self.inserted}, updated={self.updated})"
        )


def run_source(
    source_cfg: SourceConfig,
    crawler_cfg: CrawlerConfig,
    criteria_cfg: CriteriaConfig,
    db: BidDB,
    dry_run: bool = False,
    skip_fresh: bool = False,
) -> PipelineResult:
    result = PipelineResult(source_cfg.id)

    if not source_cfg.enabled:
        logger.info("Source %s is disabled, skipping", source_cfg.id)
        result.errors.append("disabled")
        return result

    # Check if source ran recently (skip_fresh mode)
    last_run = db.get_source_last_run(source_cfg.id)
    if skip_fresh and last_run:
        hours_ago = (datetime.now(timezone.utc) - last_run.replace(tzinfo=timezone.utc)).total_seconds() / 3600
        if hours_ago < crawler_cfg.fresh_threshold_hours:
            logger.info(
                "Source %s ran %.1fh ago (threshold=%dh) — skipping",
                source_cfg.id, hours_ago, crawler_cfg.fresh_threshold_hours,
            )
            result.skipped_fresh = True
            return result

    # Ensure source row exists in DB
    db.ensure_source(source_cfg.id, source_cfg.source_type)

    # Instantiate source plugin
    try:
        source_cls = get_source_class(source_cfg.id)
    except KeyError as exc:
        logger.error("Unknown source: %s", exc)
        result.errors.append(str(exc))
        return result

    matcher = BidMatcher(criteria_cfg)
    source = source_cls(source_cfg, criteria_cfg)

    # Fetch
    logger.info("Fetching from source: %s", source_cfg.id)
    raw_bids: list[dict] = []
    try:
        for raw in source.fetch(since=last_run):
            raw_bids.append(raw)
            result.fetched += 1
            if result.fetched % 50 == 0:
                logger.debug("  %s: fetched %d so far…", source_cfg.id, result.fetched)
    except Exception as exc:
        logger.error("Fetch error from %s: %s", source_cfg.id, exc, exc_info=True)
        result.errors.append(f"fetch error: {exc}")

    # Transform
    transformed: list[dict] = []
    for raw in raw_bids:
        try:
            bid = transform_bid(raw, matcher)
            if bid:
                transformed.append(bid)
                result.matched += 1
        except Exception as exc:
            logger.debug("Transform error: %s", exc)

    logger.info(
        "Source %s: fetched=%d matched=%d",
        source_cfg.id, result.fetched, result.matched,
    )

    if dry_run:
        logger.info("[DRY RUN] Would load %d bids (not saving)", len(transformed))
        _print_dry_run(transformed[:10])
        return result

    # Load
    if transformed:
        ins, upd = load_bids(db, transformed)
        result.inserted = ins
        result.updated = upd

    # Update source last_run timestamp
    db.update_source_run(source_cfg.id, result.matched)

    return result


def _print_dry_run(bids: list[dict]):
    for b in bids:
        print(
            f"  [{b.get('match_score', 0):3d}] {b.get('title', '')[:60]:<60} "
            f"| {b.get('agency', '')[:30]:<30} "
            f"| due={b.get('due_date', 'N/A')} "
            f"| kw={b.get('matched_keywords', '')}"
        )
