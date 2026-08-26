"""Ion Wave (Euna Procurement) source — one class, all tenants.

Each tenant gets its own YAML config with these extras:
    ionwave_tenant:   subdomain (e.g. "aisd" for aisd.ionwave.net)
    agency_name:      display name
    agency_type:      "school_district" | "municipal" | "county" | "cooperative"
    location_county:  county name
    location_city:    city name
    requires_auth:    true | false (default false)

Public tenants scrape without login.
Gated tenants authenticate using IONWAVE_USERNAME + IONWAVE_PASSWORD env vars.
"""

from __future__ import annotations
import logging
import os
import re
import time
from datetime import datetime
from typing import Any, Iterator, Optional

from bid_crawler.sources.base import BaseSource
from bid_crawler.config import CriteriaConfig, SourceConfig

logger = logging.getLogger(__name__)

try:
    from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False
    logger.warning("playwright not installed — IonWave scraper disabled")

# Ion Wave date format includes time: "08/26/2026 2:00 PM"
_DATE_RE = re.compile(r"(\d{1,2}/\d{1,2}/\d{4})")


class IonWaveSource(BaseSource):
    """Generic Ion Wave tenant — configured from YAML extras."""

    def __init__(self, source_cfg: SourceConfig, criteria: CriteriaConfig):
        super().__init__(source_cfg, criteria)
        self.source_id = source_cfg.id

        tenant = source_cfg.extras.get("ionwave_tenant", "")
        self._base_url = f"https://{tenant}.ionwave.net"
        self._bids_url = f"{self._base_url}/SourcingEvents.aspx?SourceType=1"
        self._login_url = f"{self._base_url}/Login.aspx"
        self._agency = source_cfg.extras.get("agency_name", tenant)
        self._agency_type = source_cfg.extras.get("agency_type", "municipal")
        self._county = source_cfg.extras.get("location_county", "")
        self._city = source_cfg.extras.get("location_city", "")
        self._requires_auth = source_cfg.extras.get("requires_auth", False)

        # Per-tenant credential override (e.g. IONWAVE_TIPS_USERNAME) takes
        # precedence over the shared IONWAVE_USERNAME/PASSWORD, since not all
        # gated tenants accept the same Novium login (TIPS-USA is separate).
        tenant_prefix = f"IONWAVE_{tenant.upper()}_"
        self._username = os.environ.get(
            f"{tenant_prefix}USERNAME", os.environ.get("IONWAVE_USERNAME", "")
        )
        self._password = os.environ.get(
            f"{tenant_prefix}PASSWORD", os.environ.get("IONWAVE_PASSWORD", "")
        )

    def fetch(self, since: Optional[datetime] = None) -> Iterator[dict[str, Any]]:
        if not PLAYWRIGHT_AVAILABLE:
            logger.error(
                "playwright required for IonWave. "
                "pip install playwright && playwright install chromium"
            )
            return

        if self._requires_auth and not (self._username and self._password):
            logger.error(
                "%s: requires auth but IONWAVE_USERNAME/IONWAVE_PASSWORD not set", self._agency
            )
            return

        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            )
            page = context.new_page()
            try:
                if self._requires_auth:
                    self._login(page)
                yield from self._scrape_bids(page)
            except Exception as exc:
                logger.error("%s IonWave fetch error: %s", self._agency, exc, exc_info=True)
            finally:
                browser.close()

    def _login(self, page) -> None:
        page.goto(self._login_url, wait_until="domcontentloaded", timeout=60000)
        # ASP.NET login forms — try common selector patterns
        page.fill(
            "input[id*='UserName'], input[id*='Username'], input[name*='UserName']",
            self._username,
        )
        page.fill("input[type='password']", self._password)
        page.click("input[type='submit'], button[type='submit']")
        page.wait_for_load_state("networkidle", timeout=20000)
        logger.info("%s: logged in as %s", self._agency, self._username)

    def _scrape_bids(self, page) -> Iterator[dict[str, Any]]:
        # Use domcontentloaded — ASP.NET pages keep background XHRs alive,
        # preventing networkidle from ever firing on slow tenants.
        try:
            page.goto(self._bids_url, wait_until="domcontentloaded", timeout=60000)
        except Exception:
            page.goto(self._bids_url, wait_until="load", timeout=60000)
        time.sleep(2.0)

        seen_ids: set[str] = set()
        current_page = 1

        while current_page <= self.cfg.max_pages:
            try:
                page.wait_for_selector(
                    "table.rgMasterTable, table[id*='rgBidList'], .RadGrid table",
                    timeout=10000,
                )
            except PWTimeout:
                logger.debug("%s: no bid table on page %d", self._agency, current_page)
                break

            rows = self._parse_table(page)
            logger.info("%s page %d: %d bids", self._agency, current_page, len(rows))

            if not rows:
                break

            new_this_page = 0
            for row in rows:
                ext_id = row.get("external_id", "")
                if ext_id and ext_id in seen_ids:
                    continue
                if ext_id:
                    seen_ids.add(ext_id)
                new_this_page += 1
                yield row

            # If no new IDs this page we've looped back — stop
            if new_this_page == 0:
                break

            # Check if Telerik reports only 1 page total
            total_pages = self._get_total_pages(page)
            if total_pages is not None and current_page >= total_pages:
                break

            # Telerik RadGrid next-page button (disabled state uses class rgPageNextD)
            next_btn = page.query_selector(
                "a.rgPageNext:not(.rgPageNextD), input[title='Next Page']:not([disabled])"
            )
            if not next_btn:
                break

            next_btn.click()
            page.wait_for_load_state("networkidle", timeout=15000)
            time.sleep(1.0)
            current_page += 1

    def _get_total_pages(self, page) -> int | None:
        """Parse 'X items in Y pages' from Telerik pager row."""
        try:
            text = page.evaluate("""
                () => {
                    const pager = document.querySelector('.rgInfoPart, td.rgPagerCell');
                    return pager ? pager.innerText : '';
                }
            """)
            import re as _re
            m = _re.search(r"in\s+(\d+)\s+page", text or "", _re.IGNORECASE)
            return int(m.group(1)) if m else None
        except Exception:
            return None

    def _parse_table(self, page) -> list[dict[str, Any]]:
        raw_rows = page.evaluate("""
            () => {
                const results = [];
                const table = document.querySelector(
                    'table.rgMasterTable, table[id*="rgBidList"], .RadGrid table'
                );
                if (!table) return results;
                const trs = table.querySelectorAll('tbody tr');
                trs.forEach(tr => {
                    const cells = Array.from(tr.querySelectorAll('td'));
                    // Telerik RadGrid has a blank checkbox column at cells[0];
                    // data starts at cells[1]: bid_number, title, bid_type, org, issue_date, close_date
                    if (cells.length < 5) return;
                    const link = tr.querySelector('a[href]');
                    results.push({
                        bid_number:   cells[1]?.innerText.trim() || '',
                        title:        cells[2]?.innerText.trim() || '',
                        bid_type:     cells[3]?.innerText.trim() || '',
                        organization: cells[4]?.innerText.trim() || '',
                        issue_date:   cells[5]?.innerText.trim() || '',
                        close_date:   cells[6]?.innerText.trim() || '',
                        href:         link?.href || '',
                    });
                });
                return results;
            }
        """)

        results = []
        for row in raw_rows:
            title = row.get("title", "").strip()
            bid_number = row.get("bid_number", "").strip()
            if not title and not bid_number:
                continue

            href = row.get("href", "")
            id_match = re.search(r"[Bb]id[Ii][Dd]=(\d+)|/bid/(\d+)", href)
            ext_id = (
                (id_match.group(1) or id_match.group(2))
                if id_match
                else (bid_number or href)
            )

            # Strip time portion from dates ("08/26/2026 2:00 PM" → "08/26/2026")
            raw_close = row.get("close_date", "")
            m = _DATE_RE.match(raw_close)
            close_date_str = m.group(1) if m else raw_close

            raw_issue = row.get("issue_date", "")
            m2 = _DATE_RE.match(raw_issue)
            issue_date_str = m2.group(1) if m2 else raw_issue

            results.append({
                "source_id": self.source_id,
                "external_id": ext_id,
                "bid_number": bid_number,
                "title": title,
                "description": row.get("bid_type", ""),
                "agency": row.get("organization", "") or self._agency,
                "agency_type": self._agency_type,
                "posted_date": self.normalize_date(issue_date_str),
                "due_date": self.normalize_date(close_date_str),
                "location_state": "TX",
                "location_county": self._county,
                "location_city": self._city,
                "naics_code": "",
                "naics_description": "",
                "status": "open",
                "bid_url": href or self._bids_url,
                "raw_payload": row,
            })

        return results
