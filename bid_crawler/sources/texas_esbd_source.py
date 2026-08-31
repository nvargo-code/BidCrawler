"""Texas ESBD (Electronic State Business Daily) source.

Uses the SuiteCommerce internal API discovered via network inspection.
POST https://www.txsmartbuy.gov/app/extensions/CPA/CPAMain/1.0.0/services/ESBD.Service.ss

Key fix: status="1" filter reduces from ~59,000 total records to ~700 open bids.
"""

from __future__ import annotations
import logging
import re
from datetime import datetime
from typing import Any, Iterator, Optional

from bid_crawler.sources import register
from bid_crawler.sources.base import BaseSource
from bid_crawler.config import CriteriaConfig, SourceConfig

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# County detection
# ---------------------------------------------------------------------------

# Direct county name in agency: "Tarrant County - C0740" → "Tarrant"
_COUNTY_IN_AGENCY_RE = re.compile(
    r"\b(Dallas|Tarrant|Collin|Denton|Rockwall|Kaufman|Ellis|Johnson|Parker|Wise|Hunt|Grayson)\s+County\b",
    re.IGNORECASE,
)
# TxDOT-style abbreviation in titles: "DALLAS CO," or "TARRANT CO."
_COUNTY_ABBR_RE = re.compile(
    r"\b(Dallas|Tarrant|Collin|Denton|Rockwall|Kaufman|Ellis|Johnson|Parker|Wise|Hunt|Grayson)\s+CO[\s,.]",
    re.IGNORECASE,
)

# North Texas city → county lookup (covers DFW + surrounding metros)
_CITY_TO_COUNTY: dict[str, str] = {
    # Dallas County
    "dallas": "Dallas", "garland": "Dallas", "irving": "Dallas",
    "mesquite": "Dallas", "richardson": "Dallas", "carrollton": "Dallas",
    "grand prairie": "Dallas", "cedar hill": "Dallas", "desoto": "Dallas",
    "de soto": "Dallas", "duncanville": "Dallas", "lancaster": "Dallas",
    "rowlett": "Dallas", "coppell": "Dallas", "farmers branch": "Dallas",
    "addison": "Dallas", "university park": "Dallas", "highland park": "Dallas",
    "balch springs": "Dallas", "hutchins": "Dallas", "wilmer": "Dallas",
    "seagoville": "Dallas", "sachse": "Dallas", "sunnyvale": "Dallas",
    # Tarrant County
    "fort worth": "Tarrant", "arlington": "Tarrant", "mansfield": "Tarrant",
    "euless": "Tarrant", "hurst": "Tarrant", "bedford": "Tarrant",
    "north richland hills": "Tarrant", "grapevine": "Tarrant", "keller": "Tarrant",
    "southlake": "Tarrant", "colleyville": "Tarrant", "watauga": "Tarrant",
    "saginaw": "Tarrant", "white settlement": "Tarrant", "benbrook": "Tarrant",
    "crowley": "Tarrant", "burleson": "Tarrant", "kennedale": "Tarrant",
    "azle": "Tarrant", "haslet": "Tarrant", "lake worth": "Tarrant",
    # Collin County
    "plano": "Collin", "frisco": "Collin", "mckinney": "Collin",
    "allen": "Collin", "wylie": "Collin", "murphy": "Collin",
    "prosper": "Collin", "celina": "Collin", "anna": "Collin",
    "melissa": "Collin", "princeton": "Collin", "farmersville": "Collin",
    "lovejoy": "Collin", "new hope": "Collin", "lucas": "Collin",
    "parker": "Collin", "st. paul": "Collin",
    # Denton County
    "denton": "Denton", "lewisville": "Denton", "flower mound": "Denton",
    "the colony": "Denton", "little elm": "Denton", "argyle": "Denton",
    "highland village": "Denton", "corinth": "Denton", "lake dallas": "Denton",
    "hickory creek": "Denton", "krugerville": "Denton", "sanger": "Denton",
    "aubrey": "Denton", "pilot point": "Denton",
    # Ellis County
    "waxahachie": "Ellis", "midlothian": "Ellis", "ennis": "Ellis",
    "red oak": "Ellis", "ferris": "Ellis", "italy": "Ellis",
    "palmer": "Ellis", "milford": "Ellis",
    # Kaufman County
    "kaufman": "Kaufman", "terrell": "Kaufman", "forney": "Kaufman",
    "crandall": "Kaufman", "combine": "Kaufman", "kemp": "Kaufman",
    # Rockwall County
    "rockwall": "Rockwall", "heath": "Rockwall", "royse city": "Rockwall",
    "fate": "Rockwall", "mclendon-chisholm": "Rockwall",
    # Johnson County
    "cleburne": "Johnson", "alvarado": "Johnson", "joshua": "Johnson",
    "keene": "Johnson", "godley": "Johnson",
    # Parker County
    "weatherford": "Parker", "mineral wells": "Parker", "springtown": "Parker",
    "willow park": "Parker",
    # Wise County
    "decatur": "Wise", "bridgeport": "Wise", "newark": "Wise",
    "paradise": "Wise",
    # Hunt County
    "greenville": "Hunt", "caddo mills": "Hunt", "commerce": "Hunt",
    "quinlan": "Hunt",
    # Grayson County
    "sherman": "Grayson", "denison": "Grayson", "van alstyne": "Grayson",
    "gunter": "Grayson", "pottsboro": "Grayson", "howe": "Grayson",
    "whitesboro": "Grayson", "whitewright": "Grayson",
}

# North Texas ISD → county (school districts span counties; use primary county)
_ISD_TO_COUNTY: dict[str, str] = {
    "dallas isd": "Dallas", "dallas independent": "Dallas",
    "garland isd": "Dallas", "garland independent": "Dallas",
    "mesquite isd": "Dallas", "mesquite independent": "Dallas",
    "richardson isd": "Dallas", "richardson independent": "Dallas",
    "carrollton-farmers branch": "Dallas", "carrollton farmers branch": "Dallas",
    "duncanville isd": "Dallas", "cedar hill isd": "Dallas",
    "desoto isd": "Dallas", "de soto isd": "Dallas",
    "lancaster isd": "Dallas", "irving isd": "Dallas",
    "grand prairie isd": "Dallas", "coppell isd": "Dallas",
    "highland park isd": "Dallas", "sunnyvale isd": "Dallas",
    "fort worth isd": "Tarrant", "fort worth independent": "Tarrant",
    "arlington isd": "Tarrant", "arlington independent": "Tarrant",
    "mansfield isd": "Tarrant", "keller isd": "Tarrant",
    "birdville isd": "Tarrant", "hurst-euless-bedford": "Tarrant",
    "northwest isd": "Tarrant", "northwest independent": "Tarrant",
    "crowley isd": "Tarrant", "eagle mountain-saginaw": "Tarrant",
    "kennedale isd": "Tarrant", "azle isd": "Tarrant",
    "plano isd": "Collin", "plano independent": "Collin",
    "frisco isd": "Collin", "frisco independent": "Collin",
    "mckinney isd": "Collin", "allen isd": "Collin",
    "wylie isd": "Collin", "prosper isd": "Collin",
    "celina isd": "Collin", "anna isd": "Collin",
    "melissa isd": "Collin", "princeton isd": "Collin",
    "lewisville isd": "Denton", "denton isd": "Denton",
    "argyle isd": "Denton", "flower mound": "Denton",
    "little elm isd": "Denton", "sanger isd": "Denton",
    "aubrey isd": "Denton", "pilot point isd": "Denton",
    "red oak isd": "Ellis", "midlothian isd": "Ellis",
    "waxahachie isd": "Ellis", "ennis isd": "Ellis",
    "ferris isd": "Ellis", "italy isd": "Ellis",
    "terrell isd": "Kaufman", "forney isd": "Kaufman",
    "kaufman isd": "Kaufman", "crandall isd": "Kaufman",
    "rockwall isd": "Rockwall", "royse city isd": "Rockwall",
    "burleson isd": "Johnson", "cleburne isd": "Johnson",
    "alvarado isd": "Johnson", "joshua isd": "Johnson",
    "weatherford isd": "Parker", "mineral wells isd": "Parker",
    "decatur isd": "Wise", "bridgeport isd": "Wise",
    "greenville isd": "Hunt", "commerce isd": "Hunt",
    "sherman isd": "Grayson", "denison isd": "Grayson",
    "van alstyne isd": "Grayson", "gunter isd": "Grayson",
    "whitesboro isd": "Grayson",
}

_DFW_COUNTIES = {
    "dallas", "tarrant", "collin", "denton", "rockwall",
    "kaufman", "ellis", "johnson", "parker", "wise", "hunt", "grayson",
}

_STATUS_MAP = {
    "1": "open",   # Posted
    "2": "closed", # Awarded
    "3": "awarded",
    "4": "cancelled",
    "5": "cancelled",
    "6": "open",   # Addendum Posted — still open
}


def detect_county(agency: str, title: str = "") -> str:
    """Extract DFW county from agency name or title using a priority cascade."""
    text = agency

    # 1. Direct county name: "Tarrant County - C0740"
    m = _COUNTY_IN_AGENCY_RE.search(text)
    if m:
        return m.group(1).title()

    # 2. ISD lookup (check longest match first)
    agency_lower = agency.lower()
    for isd_key, county in sorted(_ISD_TO_COUNTY.items(), key=lambda x: -len(x[0])):
        if isd_key in agency_lower:
            return county

    # 3. City of X pattern: "City of Irving"
    city_match = re.search(r"city of (\w[\w\s]+?)(?:\s*[-,]|\s*$)", agency, re.IGNORECASE)
    if city_match:
        city = city_match.group(1).strip().lower()
        if city in _CITY_TO_COUNTY:
            return _CITY_TO_COUNTY[city]

    # 4. City name anywhere in agency
    for city, county in sorted(_CITY_TO_COUNTY.items(), key=lambda x: -len(x[0])):
        if re.search(r"\b" + re.escape(city) + r"\b", agency_lower):
            return county

    # 5. Try title — spelled-out county or TxDOT abbreviation (e.g. "DALLAS CO,")
    county_in_title = _COUNTY_IN_AGENCY_RE.search(title) or _COUNTY_ABBR_RE.search(title)
    if county_in_title:
        return county_in_title.group(1).title()

    return ""


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
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
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
                    # status="1" filters to open bids only (~700 vs 59,000 total)
                    json={"lines": [], "page": page, "urlRoot": "esbd", "status": "1"},
                    timeout=20,
                )
                resp.raise_for_status()
                data = resp.json()
            except Exception as exc:
                logger.error("ESBD API error on page %d: %s", page, exc)
                raise RuntimeError(f"ESBD API request failed on page {page}") from exc

            lines = data.get("lines", [])
            if not lines:
                break

            records_per_page = data.get("recordsPerPage", 24) or 24
            total_records = data.get("totalRecordsFound", 0)

            for item in lines:
                ext_id = str(item.get("internalid", ""))
                if ext_id in seen_ids:
                    continue
                seen_ids.add(ext_id)
                yield self._normalize(item)

            fetched_so_far = page * records_per_page
            if fetched_so_far >= total_records or len(lines) < records_per_page:
                logger.info("ESBD: fetched all %d open records across %d pages", total_records, page)
                break

            page += 1
            self._sleep()

    def _normalize(self, item: dict) -> dict[str, Any]:
        ext_id = str(item.get("internalid", ""))
        status_code = str(item.get("status", "1"))
        status = _STATUS_MAP.get(status_code, "open")

        url_path = item.get("url", "")
        solicitation_id = item.get("solicitationId", "")
        if url_path.startswith("/"):
            bid_url = f"{self.BASE_URL}{url_path}"
        elif url_path:
            bid_url = url_path
        elif solicitation_id:
            bid_url = f"{self.BASE_URL}/esbd/{solicitation_id}"
        else:
            bid_url = ""

        nigp = item.get("nigpCodes", "")
        agency = item.get("agencyName", "")
        title = item.get("title", "")

        # Use improved county detection
        county = detect_county(agency, title)

        return {
            "source_id": self.source_id,
            "external_id": ext_id,
            "bid_number": solicitation_id,
            "title": title,
            "description": nigp,
            "agency": agency,
            "agency_type": _classify_agency(agency),
            "posted_date": self.normalize_date(item.get("postingDate")),
            "due_date": self.normalize_date(item.get("responseDue")),
            "location_state": "TX",
            "location_county": county,
            "location_city": "",
            "naics_code": "",
            "naics_description": nigp,
            "status": status,
            "bid_url": bid_url,
            "raw_payload": item,
        }


def _classify_agency(agency: str) -> str:
    a = agency.lower()
    if "isd" in a or "school" in a or "independent" in a:
        return "school"
    if "city of" in a or "municipality" in a:
        return "city"
    if "county" in a:
        return "county"
    if "university" in a or "college" in a or "a&m" in a:
        return "university"
    if "txdot" in a or "department of transportation" in a:
        return "state"
    if "department" in a or "commission" in a or "agency" in a:
        return "state"
    return "state"
