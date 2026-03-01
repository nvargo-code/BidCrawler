"""Texas ESBD (Electronic State Business Daily) source.

Uses the SuiteCommerce internal API discovered via network inspection.
POST https://www.txsmartbuy.gov/app/extensions/CPA/CPAMain/1.0.0/services/ESBD.Service.ss
"""

from __future__ import annotations
import logging
from datetime import datetime
from typing import Any, Iterator, Optional

from bid_crawler.sources import register
from bid_crawler.sources.base import BaseSource
from bid_crawler.config import CriteriaConfig, SourceConfig

logger = logging.getLogger(__name__)

_STATUS_MAP = {
    "1": "open",
    "2": "closed",
    "3": "awarded",
    "4": "cancelled",
}


@register("texas_esbd")
class TexasESBDSource(BaseSource):
    source_id = "texas_esbd"

    API_URL = (
        "https://www.txsmartbuy.gov/app/extensions/CPA/CPAMain/1.0.0"
        "/services/ESBD.Service.ss"
    )
    API_PARAMS = {"c": "852252", "n": "2"}
    BASE_URL = "https://www.txsmartbuy.gov"

    def __init__(self, source_cfg: SourceConfig, criteria: CriteriaConfig):
        super().__init__(source_cfg, criteria)
        self.session.headers.update({
            "x-sc-touchpoint": "shopping",
            "x-requested-with": "XMLHttpRequest",
            "content-type": "application/json; charset=UTF-8",
            "referer": "https://www.txsmartbuy.gov/esbd",
        })

    def fetch(self, since: Optional[datetime] = None) -> Iterator[dict[str, Any]]:
        page = 1
        seen_ids: set[str] = set()

        while page <= self.cfg.max_pages:
            try:
                resp = self.session.post(
                    self.API_URL,
                    params=self.API_PARAMS,
                    json={"lines": [], "page": page, "urlRoot": "esbd"},
                    timeout=20,
                )
                resp.raise_for_status()
                data = resp.json()
            except Exception as exc:
                logger.error("ESBD API error on page %d: %s", page, exc)
                break

            lines = data.get("lines", [])
            if not lines:
                break

            records_per_page = data.get("recordsPerPage", 25) or 25
            total_records = data.get("totalRecordsFound", 0)

            for item in lines:
                ext_id = str(item.get("internalid", ""))
                if ext_id in seen_ids:
                    continue
                seen_ids.add(ext_id)

                # Incremental: skip bids posted before last run
                if since:
                    posted_raw = item.get("postingDate", "")
                    if posted_raw:
                        try:
                            posted = datetime.strptime(posted_raw, "%m/%d/%Y")
                            if posted < since.replace(tzinfo=None):
                                continue
                        except ValueError:
                            pass

                yield self._normalize(item)

            fetched_so_far = page * records_per_page
            if fetched_so_far >= total_records or len(lines) < records_per_page:
                break

            page += 1
            self._sleep()

    def _normalize(self, item: dict) -> dict[str, Any]:
        ext_id = str(item.get("internalid", ""))
        status = _STATUS_MAP.get(str(item.get("status", "")), "open")

        url_path = item.get("url", "")
        bid_url = (
            f"{self.BASE_URL}{url_path}" if url_path.startswith("/") else url_path
        )

        # NIGP code string used for keyword matching (e.g. "Construction Materials")
        nigp = item.get("nigpCodes", "")

        return {
            "source_id": self.source_id,
            "external_id": ext_id,
            "bid_number": item.get("solicitationId", ""),
            "title": item.get("title", ""),
            "description": nigp,
            "agency": item.get("agencyName", ""),
            "agency_type": "state",
            "posted_date": self.normalize_date(item.get("postingDate")),
            "due_date": self.normalize_date(item.get("responseDue")),
            "location_state": "TX",
            "location_county": "",
            "location_city": "",
            "naics_code": "",
            "naics_description": nigp,
            "status": status,
            "bid_url": bid_url,
            "raw_payload": item,
        }
