"""BidNet Direct scraper source (Playwright-based for JS-heavy site)."""

from __future__ import annotations
import json
import logging
import re
import time
from datetime import datetime
from typing import Any, Iterator, Optional
from urllib.parse import urljoin

from bid_crawler.sources import register
from bid_crawler.sources.base import BaseSource
from bid_crawler.config import CriteriaConfig, SourceConfig

logger = logging.getLogger(__name__)

try:
    from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False
    logger.warning("playwright not installed — BidNet scraper disabled")


@register("bidnet")
class BidNetSource(BaseSource):
    source_id = "bidnet"

    SEARCH_URL = "https://www.bidnetdirect.com/texas"

    def __init__(self, source_cfg: SourceConfig, criteria: CriteriaConfig):
        super().__init__(source_cfg, criteria)
        self._delay = source_cfg.delay or 2.0

    def fetch(self, since: Optional[datetime] = None) -> Iterator[dict[str, Any]]:
        if not PLAYWRIGHT_AVAILABLE:
            logger.error("playwright required for BidNet. pip install playwright && playwright install chromium")
            return

        keywords_to_try = ["construction", "renovation", "general contractor", "HVAC", "roofing"]

        seen_ids: set[str] = set()

        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            )
            page = context.new_page()

            for keyword in keywords_to_try:
                try:
                    yield from self._search_keyword(page, keyword, since, seen_ids)
                except Exception as exc:
                    logger.warning("BidNet keyword %r failed: %s", keyword, exc)
                time.sleep(self._delay)

            browser.close()

    def _search_keyword(
        self, page, keyword: str, since: Optional[datetime], seen_ids: set[str]
    ) -> Iterator[dict[str, Any]]:
        search_url = f"{self.SEARCH_URL}?keywords={keyword}&status=open"
        page.goto(search_url, wait_until="networkidle", timeout=30000)
        time.sleep(self._delay)

        current_page = 1
        while current_page <= self.cfg.max_pages:
            # Wait for bid listings to load
            try:
                page.wait_for_selector("div.bid-listing, table.bids-table, .bid-row", timeout=10000)
            except PWTimeout:
                logger.debug("BidNet: no bid listings found on page %d", current_page)
                break

            items = self._parse_page(page)
            if not items:
                break

            for item in items:
                ext_id = item.get("external_id", "")
                if ext_id in seen_ids:
                    continue
                seen_ids.add(ext_id)

                # Incremental filter
                if since and item.get("posted_date"):
                    try:
                        pd = datetime.fromisoformat(item["posted_date"])
                        if pd < since:
                            continue
                    except (ValueError, TypeError):
                        pass

                yield item

            # Try to go to next page
            next_btn = page.query_selector("a.next-page, button[aria-label='Next'], a:text('Next')")
            if not next_btn:
                break
            next_btn.click()
            time.sleep(self._delay)
            current_page += 1

    def _parse_page(self, page) -> list[dict[str, Any]]:
        """Extract bid listings from the current page."""
        results = []

        # Evaluate JS to extract structured data from the page
        raw_items = page.evaluate("""
            () => {
                const items = [];
                // Try multiple selector strategies
                const rows = document.querySelectorAll(
                    '.bid-row, .bid-item, tr.opportunity, div[class*="bid"]'
                );
                rows.forEach(row => {
                    const link = row.querySelector('a[href*="/bid/"], a[href*="/opportunity/"]');
                    if (!link) return;
                    const cells = row.querySelectorAll('td, .cell, .col');
                    items.push({
                        href: link.href,
                        title: link.textContent.trim(),
                        cells: Array.from(cells).map(c => c.textContent.trim()),
                    });
                });
                return items;
            }
        """)

        for item in raw_items:
            href = item.get("href", "")
            title = item.get("title", "")
            cells = item.get("cells", [])

            # Extract external ID from URL
            id_match = re.search(r"/bid/(\d+)|/opportunity/(\w+)", href)
            ext_id = (id_match.group(1) or id_match.group(2)) if id_match else href

            # Parse cells for agency, dates
            agency = cells[1] if len(cells) > 1 else ""
            due_raw = cells[3] if len(cells) > 3 else ""
            posted_raw = cells[2] if len(cells) > 2 else ""

            # Try to detect county from title/agency text
            location_county = self._detect_county(f"{title} {agency}")

            results.append({
                "source_id": self.source_id,
                "external_id": ext_id,
                "bid_number": ext_id,
                "title": title,
                "description": "",
                "agency": agency,
                "agency_type": self._detect_agency_type(agency),
                "posted_date": self.normalize_date(posted_raw),
                "due_date": self.normalize_date(due_raw),
                "location_county": location_county,
                "location_state": "TX",
                "bid_url": href,
                "status": "open",
                "raw_payload": {"cells": cells, "href": href},
            })

        return results

    def _detect_county(self, text: str) -> str:
        """Heuristic county detection from text."""
        counties = [
            "Dallas", "Tarrant", "Collin", "Denton", "Rockwall",
            "Kaufman", "Ellis", "Johnson", "Parker", "Wise", "Hunt", "Grayson",
        ]
        for county in counties:
            if re.search(r"\b" + county + r"\b", text, re.IGNORECASE):
                return county
        return ""

    def _detect_agency_type(self, agency: str) -> str:
        agency_lower = agency.lower()
        if "isd" in agency_lower or "school" in agency_lower or "district" in agency_lower:
            return "school"
        if "city of" in agency_lower or "municipality" in agency_lower:
            return "city"
        if "county" in agency_lower:
            return "county"
        if "authority" in agency_lower or "mta" in agency_lower or "dart" in agency_lower:
            return "authority"
        return "city"
