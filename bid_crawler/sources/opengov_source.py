"""OpenGov Procurement API source."""

from __future__ import annotations
import base64
import logging
from datetime import datetime
from typing import Any, Iterator, Optional

from bid_crawler.sources import register
from bid_crawler.sources.base import BaseSource
from bid_crawler.config import CriteriaConfig, SourceConfig

logger = logging.getLogger(__name__)


@register("opengov")
class OpenGovSource(BaseSource):
    source_id = "opengov"

    def __init__(self, source_cfg: SourceConfig, criteria: CriteriaConfig):
        super().__init__(source_cfg, criteria)
        email = source_cfg.api_email()
        api_key = source_cfg.api_key()

        if email and api_key:
            # Basic auth: base64(email:api_key)
            token = base64.b64encode(f"{email}:{api_key}".encode()).decode()
            self.session.headers["Authorization"] = f"Basic {token}"
            self._authenticated = True
        else:
            logger.warning("OPENGOV_EMAIL or OPENGOV_API_KEY not set — OpenGov source disabled")
            self._authenticated = False

    def fetch(self, since: Optional[datetime] = None) -> Iterator[dict[str, Any]]:
        if not self._authenticated:
            logger.error("OpenGov requires OPENGOV_EMAIL and OPENGOV_API_KEY env vars")
            return

        base_url = self.cfg.base_url
        endpoint = f"{base_url}/contract-orders/v1/"

        page = 1
        page_size = self.cfg.page_size

        while page <= self.cfg.max_pages:
            params: dict[str, Any] = {
                "state": "TX",
                "status": "open",
                "page": page,
                "per_page": page_size,
            }
            if since:
                params["posted_after"] = since.strftime("%Y-%m-%d")

            try:
                resp = self._get(endpoint, params=params)
                data = resp.json()
            except Exception as exc:
                logger.error("OpenGov API error on page %d: %s", page, exc)
                break

            items = data.get("data", data.get("results", []))
            if not items:
                break

            for item in items:
                yield self._normalize(item)

            # Pagination
            meta = data.get("meta", data.get("pagination", {}))
            total_pages = meta.get("total_pages", meta.get("pages", 1))
            if page >= total_pages:
                break

            page += 1
            self._sleep()

    def _normalize(self, item: dict) -> dict[str, Any]:
        ext_id = str(item.get("id", item.get("number", "")))
        agency = item.get("organization", {}) or {}
        agency_name = agency.get("name", "") if isinstance(agency, dict) else str(agency)
        location = item.get("location", {}) or {}

        contact = item.get("contact", {}) or {}
        if not isinstance(contact, dict):
            contact = {}

        return {
            "source_id": self.source_id,
            "external_id": ext_id,
            "bid_number": item.get("number", item.get("bid_number", ext_id)),
            "title": item.get("title", item.get("name", "")),
            "description": item.get("description", item.get("summary", "")),
            "agency": agency_name,
            "agency_type": self._classify_agency(agency_name),
            "posted_date": self.normalize_date(item.get("published_at", item.get("posted_date"))),
            "due_date": self.normalize_date(item.get("due_at", item.get("due_date", item.get("close_date")))),
            "estimated_value": _parse_float(item.get("estimated_value", item.get("amount"))),
            "location_city": location.get("city", "") if isinstance(location, dict) else "",
            "location_county": location.get("county", "") if isinstance(location, dict) else "",
            "location_state": location.get("state", "TX") if isinstance(location, dict) else "TX",
            "location_zip": location.get("zip", "") if isinstance(location, dict) else "",
            "naics_code": item.get("naics_code", ""),
            "naics_description": item.get("naics_description", ""),
            "set_aside": item.get("set_aside", ""),
            "contact_name": contact.get("name", ""),
            "contact_email": contact.get("email", ""),
            "contact_phone": contact.get("phone", ""),
            "bid_url": item.get("url", item.get("permalink", "")),
            "documents_url": item.get("documents_url", ""),
            "status": _map_status(item.get("status", "open")),
            "raw_payload": item,
        }

    def _classify_agency(self, name: str) -> str:
        name_lower = name.lower()
        if "isd" in name_lower or "school" in name_lower:
            return "school"
        if "city" in name_lower:
            return "city"
        if "county" in name_lower:
            return "county"
        if "authority" in name_lower:
            return "authority"
        return "city"


def _parse_float(value) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(str(value).replace(",", "").replace("$", ""))
    except (ValueError, TypeError):
        return None


def _map_status(status: str) -> str:
    mapping = {
        "open": "open",
        "active": "open",
        "closed": "closed",
        "awarded": "awarded",
        "cancelled": "cancelled",
        "canceled": "cancelled",
    }
    return mapping.get(status.lower(), "open")
