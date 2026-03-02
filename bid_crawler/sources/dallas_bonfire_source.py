"""City of Dallas Bonfire procurement portal source.

Uses the same public portal API as Fort Worth Bonfire (no auth required):
GET https://dallascityhall.bonfirehub.com/PublicPortal/getOpenPublicOpportunitiesSectionData
"""

from __future__ import annotations
import logging
import time
from typing import Any, Iterator, Optional
from datetime import datetime

from bid_crawler.sources import register
from bid_crawler.sources.base import BaseSource
from bid_crawler.config import CriteriaConfig, SourceConfig

logger = logging.getLogger(__name__)


@register("dallas_bonfire")
class DallasBonfireSource(BaseSource):
    source_id = "dallas_bonfire"

    API_URL = (
        "https://dallascityhall.bonfirehub.com"
        "/PublicPortal/getOpenPublicOpportunitiesSectionData"
    )
    PORTAL_URL = "https://dallascityhall.bonfirehub.com/portal/"

    def __init__(self, source_cfg: SourceConfig, criteria: CriteriaConfig):
        super().__init__(source_cfg, criteria)
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json, */*",
            "Referer": self.PORTAL_URL,
        })

    def fetch(self, since: Optional[datetime] = None) -> Iterator[dict[str, Any]]:
        try:
            resp = self.session.get(
                self.API_URL,
                params={"_": int(time.time() * 1000)},
                timeout=20,
            )
            resp.raise_for_status()
            data = resp.json()
        except Exception as exc:
            logger.error("Dallas Bonfire API error: %s", exc)
            return

        if not data.get("success"):
            logger.error("Dallas Bonfire API returned failure: %s", data.get("message"))
            return

        projects = data.get("payload", {}).get("projects", {})
        logger.info("Dallas Bonfire: %d open opportunities", len(projects))

        for item in projects.values():
            yield self._normalize(item)

    def _normalize(self, item: dict) -> dict[str, Any]:
        project_id = str(item.get("ProjectID", ""))
        private_id = item.get("PrivateProjectID", "")

        bid_url = f"{self.PORTAL_URL}?tab=openOpportunities&notice={private_id}"

        date_close_raw = item.get("DateClose", "")
        date_close = self.normalize_date(date_close_raw.split(" ")[0] if date_close_raw else None)

        title = item.get("ProjectName", "")

        return {
            "source_id": self.source_id,
            "external_id": project_id,
            "bid_number": item.get("ReferenceID", ""),
            "title": title,
            "description": title,
            "agency": "City of Dallas",
            "agency_type": "municipal",
            "posted_date": None,
            "due_date": date_close,
            "location_state": "TX",
            "location_county": "Dallas",
            "location_city": "Dallas",
            "naics_code": "",
            "naics_description": "",
            "status": "open",
            "bid_url": bid_url,
            "raw_payload": item,
        }
