"""Texas ESBD (Electronic State Business Daily) scraper source."""

from __future__ import annotations
import logging
import re
from datetime import datetime
from typing import Any, Iterator, Optional
from urllib.parse import urljoin

from bid_crawler.sources import register
from bid_crawler.sources.base import BaseSource
from bid_crawler.config import CriteriaConfig, SourceConfig

logger = logging.getLogger(__name__)

try:
    from bs4 import BeautifulSoup
    BS4_AVAILABLE = True
except ImportError:
    BS4_AVAILABLE = False
    logger.warning("beautifulsoup4 not installed — ESBD scraper disabled")


@register("texas_esbd")
class TexasESBDSource(BaseSource):
    source_id = "texas_esbd"

    BASE_URL = "https://www.txsmartbuy.gov/esbd"

    def __init__(self, source_cfg: SourceConfig, criteria: CriteriaConfig):
        super().__init__(source_cfg, criteria)
        self.session.headers.update({
            "Accept": "text/html,application/xhtml+xml",
            "Accept-Language": "en-US,en;q=0.9",
        })

    def fetch(self, since: Optional[datetime] = None) -> Iterator[dict[str, Any]]:
        if not BS4_AVAILABLE:
            logger.error("beautifulsoup4 required for ESBD scraper. pip install beautifulsoup4")
            return

        # Try the search page with keyword filters
        keywords_to_try = [
            "construction",
            "renovation",
            "general contractor",
            "HVAC",
            "roofing",
        ]

        seen_ids: set[str] = set()

        for keyword in keywords_to_try:
            try:
                yield from self._search_keyword(keyword, since, seen_ids)
            except Exception as exc:
                logger.warning("ESBD keyword %r failed: %s", keyword, exc)
            self._sleep()

    def _search_keyword(
        self, keyword: str, since: Optional[datetime], seen_ids: set[str]
    ) -> Iterator[dict[str, Any]]:
        """Search ESBD for a keyword and paginate through results."""
        page = 1
        while page <= self.cfg.max_pages:
            params = {
                "keyword": keyword,
                "status": "Open",
                "page": page,
            }
            try:
                resp = self._get(self.BASE_URL, params=params)
            except Exception as exc:
                logger.warning("ESBD GET error (keyword=%r, page=%d): %s", keyword, page, exc)
                break

            soup = BeautifulSoup(resp.text, "lxml")
            results = self._parse_results(soup)

            if not results:
                break

            for item in results:
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

                # Fetch detail page for full description
                if item.get("bid_url"):
                    try:
                        detail = self._fetch_detail(item["bid_url"])
                        item.update(detail)
                    except Exception as exc:
                        logger.debug("Detail fetch failed for %s: %s", item["bid_url"], exc)

                yield item

            page += 1
            self._sleep()

    def _parse_results(self, soup) -> list[dict[str, Any]]:
        """Parse the ESBD search results page."""
        results = []

        # ESBD result rows — the structure may vary; adapt selectors as needed
        rows = soup.select("table.result-table tr") or soup.select("div.bid-item")
        if not rows:
            # Try a more generic approach
            rows = soup.select("tr[class*='row']")

        for row in rows:
            cells = row.select("td")
            if len(cells) < 3:
                continue

            try:
                link_tag = row.select_one("a[href*='/esbd/']") or row.select_one("a")
                if not link_tag:
                    continue

                bid_url = urljoin(self.BASE_URL, link_tag.get("href", ""))
                # Extract ESBDId from URL like /esbd/12345
                ext_id_match = re.search(r"/esbd/(\d+)", bid_url)
                ext_id = ext_id_match.group(1) if ext_id_match else bid_url

                title = link_tag.get_text(strip=True)
                agency = cells[1].get_text(strip=True) if len(cells) > 1 else ""
                posted_raw = cells[2].get_text(strip=True) if len(cells) > 2 else ""
                due_raw = cells[3].get_text(strip=True) if len(cells) > 3 else ""

                results.append({
                    "source_id": self.source_id,
                    "external_id": ext_id,
                    "bid_number": ext_id,
                    "title": title,
                    "description": "",
                    "agency": agency,
                    "agency_type": "state",
                    "posted_date": self.normalize_date(posted_raw),
                    "due_date": self.normalize_date(due_raw),
                    "location_state": "TX",
                    "bid_url": bid_url,
                    "status": "open",
                    "raw_payload": {},
                })
            except Exception as exc:
                logger.debug("Row parse error: %s", exc)

        return results

    def _fetch_detail(self, url: str) -> dict[str, Any]:
        """Fetch a bid detail page and extract additional fields."""
        resp = self._get(url)
        soup = BeautifulSoup(resp.text, "lxml")

        detail: dict[str, Any] = {}

        # Try to extract description
        desc_el = (
            soup.select_one("div.description")
            or soup.select_one("td:-soup-contains('Description') + td")
            or soup.select_one("p.description")
        )
        if desc_el:
            detail["description"] = desc_el.get_text(separator=" ", strip=True)

        # Try to extract county/city
        for label_text in ["County", "City", "Location"]:
            label_el = soup.find(string=re.compile(label_text, re.IGNORECASE))
            if label_el and label_el.parent:
                sibling = label_el.parent.find_next_sibling()
                if sibling:
                    val = sibling.get_text(strip=True)
                    if "County" in label_text:
                        detail["location_county"] = val.replace(" County", "")
                    elif "City" in label_text:
                        detail["location_city"] = val

        # Contact info
        email_el = soup.select_one("a[href^='mailto:']")
        if email_el:
            detail["contact_email"] = email_el.get_text(strip=True)

        # NAICS
        naics_el = soup.find(string=re.compile(r"NAICS", re.IGNORECASE))
        if naics_el and naics_el.parent:
            sibling = naics_el.parent.find_next_sibling()
            if sibling:
                code = re.search(r"\d{6}", sibling.get_text())
                if code:
                    detail["naics_code"] = code.group(0)

        # Estimated value
        value_el = soup.find(string=re.compile(r"Estimated Value|Amount", re.IGNORECASE))
        if value_el and value_el.parent:
            sibling = value_el.parent.find_next_sibling()
            if sibling:
                val_text = sibling.get_text(strip=True)
                val_match = re.search(r"[\d,]+\.?\d*", val_text.replace("$", ""))
                if val_match:
                    try:
                        detail["estimated_value"] = float(val_match.group(0).replace(",", ""))
                    except ValueError:
                        pass

        self._sleep(0.5)
        return detail
