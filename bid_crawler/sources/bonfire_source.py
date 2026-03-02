"""Generic Bonfire procurement portal source.

Configurable via YAML extras:
    bonfire_tenant:    subdomain (e.g. "dallasisd" for dallasisd.bonfirehub.com)
    agency_name:       display name (e.g. "Dallas ISD")
    agency_type:       "municipal" | "school_district" | "county"
    location_city:     city name
    location_county:   county name

Example config (config/sources/dallas_isd_bonfire.yaml):
    id: dallas_isd_bonfire
    source_type: api
    enabled: true
    max_pages: 1
    delay: 0.0
    bonfire_tenant: dallasisd
    agency_name: Dallas ISD
    agency_type: school_district
    location_county: Dallas
    location_city: Dallas
"""

from __future__ import annotations
import logging
import time
from typing import Any, Iterator, Optional
from datetime import datetime

from bid_crawler.sources.base import BaseSource
from bid_crawler.config import CriteriaConfig, SourceConfig

logger = logging.getLogger(__name__)


class BonfireSource(BaseSource):
    """Generic Bonfire tenant source — configured entirely from YAML extras."""

    def __init__(self, source_cfg: SourceConfig, criteria: CriteriaConfig):
        super().__init__(source_cfg, criteria)
        self.source_id = source_cfg.id

        tenant = source_cfg.extras.get("bonfire_tenant", "")
        self._api_url = (
            f"https://{tenant}.bonfirehub.com"
            "/PublicPortal/getOpenPublicOpportunitiesSectionData"
        )
        self._portal_url = f"https://{tenant}.bonfirehub.com/portal/"
        self._agency = source_cfg.extras.get("agency_name", tenant)
        self._agency_type = source_cfg.extras.get("agency_type", "municipal")
        self._county = source_cfg.extras.get("location_county", "")
        self._city = source_cfg.extras.get("location_city", "")

        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json, */*",
            "Referer": self._portal_url,
        })

    def fetch(self, since: Optional[datetime] = None) -> Iterator[dict[str, Any]]:
        try:
            resp = self.session.get(
                self._api_url,
                params={"_": int(time.time() * 1000)},
                timeout=20,
            )
            resp.raise_for_status()
            data = resp.json()
        except Exception as exc:
            logger.error("%s Bonfire API error: %s", self._agency, exc)
            return

        if not data.get("success"):
            logger.error("%s Bonfire API failure: %s", self._agency, data.get("message"))
            return

        projects = data.get("payload", {}).get("projects", {})
        # Some Bonfire tenants return projects as a list, others as a dict
        if isinstance(projects, dict):
            items = list(projects.values())
        else:
            items = list(projects)
        logger.info("%s: %d open opportunities", self._agency, len(items))

        for item in items:
            yield self._normalize(item)

    def _normalize(self, item: dict) -> dict[str, Any]:
        project_id = str(item.get("ProjectID", ""))
        private_id = item.get("PrivateProjectID", "")
        bid_url = f"{self._portal_url}?tab=openOpportunities&notice={private_id}"

        date_close_raw = item.get("DateClose", "")
        date_close = self.normalize_date(date_close_raw.split(" ")[0] if date_close_raw else None)

        title = item.get("ProjectName", "")

        return {
            "source_id": self.source_id,
            "external_id": project_id,
            "bid_number": item.get("ReferenceID", ""),
            "title": title,
            "description": title,
            "agency": self._agency,
            "agency_type": self._agency_type,
            "posted_date": None,
            "due_date": date_close,
            "location_state": "TX",
            "location_county": self._county,
            "location_city": self._city,
            "naics_code": "",
            "naics_description": "",
            "status": "open",
            "bid_url": bid_url,
            "raw_payload": item,
        }
