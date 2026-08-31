"""BidSync (Periscope S2G) source — direct REST API client.

Uses the suppliergateway.phi-production.cloud API discovered via network inspection.
No browser required — OAuth 2.0 password grant + JSON search endpoint.

Credentials via env vars:
    BIDSYNC_EMAIL     login email
    BIDSYNC_PASSWORD  login password

Note: free-tier accounts return blurred bid numbers/titles for out-of-network bids.
Upgrade the BidSync subscription to unlock full bid details.
"""

from __future__ import annotations
import logging
import os
from datetime import datetime
from typing import Any, Iterator, Optional

from bid_crawler.sources import register
from bid_crawler.sources.base import BaseSource
from bid_crawler.config import CriteriaConfig, SourceConfig

logger = logging.getLogger(__name__)

_TOKEN_URL = "https://suppliergateway.phi-production.cloud/oauth/token"
_SEARCH_URL = "https://suppliergateway.phi-production.cloud/api/homepage/search/v1/searchSolicitations"
_BANNER_URL = "https://suppliergateway.phi-production.cloud/api/homepage/search/v1/bidCountForBanner"


@register("bidsync")
class BidSyncSource(BaseSource):
    source_id = "bidsync"

    def __init__(self, source_cfg: SourceConfig, criteria: CriteriaConfig):
        super().__init__(source_cfg, criteria)
        self._email = os.environ.get("BIDSYNC_EMAIL", "")
        self._password = os.environ.get("BIDSYNC_PASSWORD", "")
        self._token: Optional[str] = None
        self._user_id: Optional[str] = None

        if not self._email or not self._password:
            logger.warning("BIDSYNC_EMAIL or BIDSYNC_PASSWORD not set — BidSync source disabled")

    # ------------------------------------------------------------------
    # Auth
    # ------------------------------------------------------------------

    def _get_token(self) -> tuple[Optional[str], Optional[str]]:
        """Obtain OAuth access token — JSON body with camelCase grantType.
        Returns (access_token, userId).
        """
        try:
            resp = self.session.post(
                _TOKEN_URL,
                json={
                    "username": self._email,
                    "password": self._password,
                    "grantType": "password",
                },
                timeout=20,
            )
            resp.raise_for_status()
            data = resp.json()
            token = data.get("access_token")
            user_id = data.get("userId")
            if token:
                logger.info("BidSync: authenticated as %s (userId=%s)", self._email, user_id)
            return token, user_id
        except Exception as exc:
            logger.error("BidSync auth failed: %s", exc)
            return None, None

    # ------------------------------------------------------------------
    # Fetch
    # ------------------------------------------------------------------

    def fetch(self, since: Optional[datetime] = None) -> Iterator[dict[str, Any]]:
        if not self._email or not self._password:
            raise RuntimeError("BidSync requires BIDSYNC_EMAIL and BIDSYNC_PASSWORD env vars")

        self._token, self._user_id = self._get_token()
        if not self._token:
            raise RuntimeError("BidSync authentication failed")

        auth_headers = {"Authorization": f"Bearer {self._token}"}

        # Check plan access
        try:
            banner = self.session.get(_BANNER_URL, headers=auth_headers, timeout=10).json()
            all_access = banner.get("allAccess", False)
            in_sub = banner.get("totalInSubscription", 0)
            if not all_access:
                logger.warning(
                    "BidSync: free-tier plan — %d in-network bids. "
                    "Upgrade plan to unlock full access. Fetching all visible bids.",
                    in_sub,
                )
        except Exception:
            pass

        # Search with TX filter + construction keywords, using exact browser payload format
        limit = 20
        offset = 0
        seen_ids: set[str] = set()
        page_num = 0

        while page_num < self.cfg.max_pages:
            payload = {
                "userId": self._user_id or "",
                "positive": [
                    "general construction", "renovation", "hvac",
                    "roofing", "sitework", "demolition", "mechanical",
                    "electrical", "plumbing", "concrete", "masonry",
                    "general contractor", "construction manager",
                ],
                "negative": [],
                "region": ["TX"],
                "limit": limit,
                "advancedProfileSearch": False,
                "filterOutExpiredBids": True,
                "sortField": "_score",
                "sortOrder": "DESC",
            }
            if offset:
                payload["from"] = offset

            try:
                resp = self.session.post(
                    _SEARCH_URL,
                    json=payload,
                    headers=auth_headers,
                    timeout=30,
                )
                resp.raise_for_status()
                data = resp.json()
            except Exception as exc:
                logger.error("BidSync search error (page %d): %s", page_num, exc)
                raise RuntimeError(f"BidSync search failed on page {page_num}") from exc

            bids = data.get("bids", [])
            if not bids:
                break

            new_this_page = 0
            for bid in bids:
                bid_id = bid.get("bidId", "")
                if bid_id in seen_ids:
                    continue
                seen_ids.add(bid_id)
                new_this_page += 1
                yield self._normalize(bid)

            total = data.get("total", 0)
            offset += limit
            if offset >= total or new_this_page == 0:
                break

            page_num += 1
            self._sleep()

    # ------------------------------------------------------------------
    # Normalize
    # ------------------------------------------------------------------

    def _normalize(self, bid: dict) -> dict[str, Any]:
        bid_id = bid.get("bidId", "")
        bid_number = bid.get("bidNumber", "")

        # Blurred values on free plan look like "blurbidid-1.png"
        if bid_number and bid_number.endswith(".png"):
            bid_number = ""

        title = bid.get("bidTitle") or bid.get("title") or ""
        if title and title.endswith(".png"):
            title = ""

        # Fall back to keyword summary when title is blurred
        if not title:
            kws = bid.get("keywords", [])
            title = ", ".join(kws) if kws else f"BidSync bid {bid_id[:8]}"

        agency = bid.get("agencyName") or bid.get("agency") or ""
        if agency and agency.endswith(".png"):
            agency = bid.get("agencyType", "")

        state = bid.get("state") or bid.get("buyerState") or "TX"
        county = bid.get("county") or bid.get("location") or ""

        return {
            "source_id": self.source_id,
            "external_id": bid_id,
            "bid_number": bid_number,
            "title": title,
            "description": ", ".join(bid.get("keywords", [])),
            "agency": agency,
            "agency_type": _map_agency_type(bid.get("agencyType", "")),
            "posted_date": self.normalize_date(
                bid.get("postedDate") or bid.get("publishedDate") or bid.get("startDate")
            ),
            "due_date": self.normalize_date(
                bid.get("endDate") or bid.get("dueDate") or bid.get("closeDate")
            ),
            "estimated_value": _parse_float(bid.get("estimatedValue") or bid.get("amount")),
            "location_city": "",
            "location_county": county,
            "location_state": state,
            "naics_code": str(bid.get("naicsCode") or ""),
            "naics_description": str(bid.get("naicsDescription") or ""),
            "status": "open",
            "bid_url": bid.get("url") or bid.get("bidUrl") or f"https://app.bidsync.com/dashboard/bid-detail/{bid_id}",
            "raw_payload": bid,
        }


def _map_agency_type(agency_type: str) -> str:
    mapping = {
        "County": "county",
        "City": "city",
        "School": "school",
        "School District": "school",
        "State": "state",
        "Federal": "federal",
        "Authority": "authority",
        "University": "university",
    }
    return mapping.get(agency_type, "city")


def _parse_float(value) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(str(value).replace(",", "").replace("$", ""))
    except (ValueError, TypeError):
        return None
