"""Transform raw source dicts into normalized bid records with match scoring."""

from __future__ import annotations
import hashlib
import json
from typing import Any

from bid_crawler.matcher import BidMatcher


def make_bid_id(source_id: str, external_id: str) -> str:
    """Stable deterministic PK from source + external id."""
    combined = f"{source_id}:{external_id}"
    return hashlib.sha256(combined.encode()).hexdigest()[:16]


def transform_bid(raw: dict[str, Any], matcher: BidMatcher) -> dict[str, Any] | None:
    """
    Apply field coercion, generate PK, and run matcher scoring.
    Returns None if the bid doesn't pass the match filter.
    """
    source_id = raw.get("source_id", "")
    external_id = str(raw.get("external_id", ""))

    if not external_id:
        return None

    title = str(raw.get("title", "") or "")
    description = str(raw.get("description", "") or "")
    naics_code = raw.get("naics_code") or None
    location_county = raw.get("location_county") or None
    estimated_value = raw.get("estimated_value")

    if estimated_value is not None:
        try:
            estimated_value = float(estimated_value)
        except (ValueError, TypeError):
            estimated_value = None

    is_match, matched_keywords, score = matcher.score_bid(
        title=title,
        description=description,
        naics_code=naics_code,
        location_county=location_county,
        estimated_value=estimated_value,
    )

    if not is_match:
        return None

    # Serialize raw payload
    raw_payload = raw.get("raw_payload", {})
    if not isinstance(raw_payload, str):
        raw_payload = json.dumps(raw_payload, default=str)

    return {
        "id": make_bid_id(source_id, external_id),
        "source_id": source_id,
        "external_id": external_id,
        "bid_number": _str(raw.get("bid_number")),
        "title": title,
        "description": description,
        "agency": _str(raw.get("agency")),
        "agency_type": _str(raw.get("agency_type")),
        "posted_date": _date(raw.get("posted_date")),
        "due_date": _date(raw.get("due_date")),
        "estimated_value": estimated_value,
        "location_city": _str(raw.get("location_city")),
        "location_county": _str(location_county),
        "location_state": _str(raw.get("location_state")) or "TX",
        "location_zip": _str(raw.get("location_zip")),
        "naics_code": _str(naics_code),
        "naics_description": _str(raw.get("naics_description")),
        "set_aside": _str(raw.get("set_aside")),
        "contact_name": _str(raw.get("contact_name")),
        "contact_email": _str(raw.get("contact_email")),
        "contact_phone": _str(raw.get("contact_phone")),
        "bid_url": _str(raw.get("bid_url")),
        "documents_url": _str(raw.get("documents_url")),
        "status": _str(raw.get("status")) or "open",
        "matched_keywords": ",".join(matched_keywords),
        "match_score": score,
        "raw_payload": raw_payload,
    }


def _str(value) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _date(value) -> str | None:
    if not value:
        return None
    s = str(value).strip()
    return s or None
