"""SAM.gov Opportunities API source (federal construction bids in TX).

Searches by construction keyword using the `q` param, which searches titles
and descriptions. Iterates over multiple keywords and deduplicates by noticeId.
"""

from __future__ import annotations
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Iterator, Optional

from bid_crawler.sources import register
from bid_crawler.sources.base import BaseSource
from bid_crawler.config import CriteriaConfig, SourceConfig

logger = logging.getLogger(__name__)

# High-signal construction keywords to search for on SAM.gov
_SEARCH_KEYWORDS = [
    "construction",
    "renovation",
    "roofing",
    "HVAC",
    "paving",
    "demolition",
    "electrical",
    "plumbing",
    "sitework",
    "concrete",
]


@register("sam_gov")
class SamGovSource(BaseSource):
    source_id = "sam_gov"

    def __init__(self, source_cfg: SourceConfig, criteria: CriteriaConfig):
        super().__init__(source_cfg, criteria)
        self._api_key = source_cfg.api_key()
        if not self._api_key:
            logger.warning(
                "SAM_GOV_API_KEY not set — requests will use public (10 req/day) tier"
            )

    def fetch(self, since: Optional[datetime] = None) -> Iterator[dict[str, Any]]:
        base_url = self.cfg.base_url
        headers = {}
        if self._api_key:
            headers["X-Api-Key"] = self._api_key

        today = datetime.now(timezone.utc)
        if since:
            posted_from = since.strftime("%m/%d/%Y")
        else:
            posted_from = (today - timedelta(days=90)).strftime("%m/%d/%Y")
        posted_to = today.strftime("%m/%d/%Y")

        seen_ids: set[str] = set()

        for keyword in _SEARCH_KEYWORDS:
            logger.debug("SAM.gov: searching keyword %r", keyword)
            yield from self._search_keyword(
                base_url, headers, keyword, posted_from, posted_to, seen_ids
            )
            self._sleep()

    def _search_keyword(
        self,
        base_url: str,
        headers: dict,
        keyword: str,
        posted_from: str,
        posted_to: str,
        seen_ids: set[str],
    ) -> Iterator[dict[str, Any]]:
        page = 0
        page_size = self.cfg.page_size

        while page < self.cfg.max_pages:
            params: dict[str, Any] = {
                "q": keyword,
                "state": "TX",
                "postedFrom": posted_from,
                "postedTo": posted_to,
                "limit": page_size,
                "offset": page * page_size,
            }

            try:
                resp = self._get(base_url, headers=headers, params=params)
                data = resp.json()
            except Exception as exc:
                logger.error("SAM.gov API error (keyword=%r, page=%d): %s", keyword, page, exc)
                break

            opportunities = data.get("opportunitiesData", [])
            if not opportunities:
                break

            for opp in opportunities:
                notice_id = opp.get("noticeId") or opp.get("solicitationNumber", "")
                if notice_id in seen_ids:
                    continue
                seen_ids.add(notice_id)
                yield self._normalize(opp)

            total_records = data.get("totalRecords", 0)
            fetched_so_far = (page + 1) * page_size
            if fetched_so_far >= total_records:
                break

            page += 1
            self._sleep()

    def _normalize(self, opp: dict) -> dict[str, Any]:
        notice_id = opp.get("noticeId") or opp.get("solicitationNumber", "")
        place = opp.get("placeOfPerformance", {}) or {}
        location = place.get("city", {}) or {}

        poc_list = opp.get("pointOfContact", []) or []
        contact = poc_list[0] if poc_list else {}

        award = opp.get("award", {}) or {}

        status_raw = opp.get("active", "")
        status = "open" if status_raw == "Yes" else "closed"
        if award.get("awardee"):
            status = "awarded"

        return {
            "source_id": self.source_id,
            "external_id": notice_id,
            "bid_number": opp.get("solicitationNumber", ""),
            "title": opp.get("title", ""),
            "description": opp.get("description", ""),
            "agency": opp.get("organizationName", ""),
            "agency_type": "federal",
            "posted_date": self.normalize_date(opp.get("postedDate")),
            "due_date": self.normalize_date(opp.get("responseDeadLine")),
            "estimated_value": _parse_float(
                opp.get("award", {}).get("amount")
                if isinstance(opp.get("award"), dict)
                else None
            ),
            "location_city": location.get("name", ""),
            "location_county": "",
            "location_state": (
                place.get("state", {}).get("code", "TX")
                if isinstance(place.get("state"), dict)
                else "TX"
            ),
            "location_zip": place.get("zip", ""),
            "naics_code": opp.get("naicsCode", ""),
            "naics_description": opp.get("classificationCode", ""),
            "set_aside": opp.get("typeOfSetAside", ""),
            "contact_name": contact.get("fullName", ""),
            "contact_email": contact.get("email", ""),
            "contact_phone": contact.get("phone", ""),
            "bid_url": f"https://sam.gov/opp/{notice_id}/view",
            "documents_url": (
                opp.get("resourceLinks", [""])[0] if opp.get("resourceLinks") else ""
            ),
            "status": status,
            "raw_payload": opp,
        }


def _parse_float(value) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(str(value).replace(",", "").replace("$", ""))
    except (ValueError, TypeError):
        return None
