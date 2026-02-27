"""Keyword + NAICS + county scoring for bids."""

from __future__ import annotations
import re
from typing import Optional
from bid_crawler.config import CriteriaConfig


class BidMatcher:
    def __init__(self, criteria: CriteriaConfig):
        self.criteria = criteria
        # Compile keyword patterns (case-insensitive, word boundary)
        self._kw_patterns = [
            (kw, re.compile(r"\b" + re.escape(kw) + r"\b", re.IGNORECASE))
            for kw in criteria.keywords
        ]
        # Lowercase county names for matching
        self._counties_lower = {c.lower() for c in criteria.counties}

    def score_bid(
        self,
        title: str = "",
        description: str = "",
        naics_code: Optional[str] = None,
        location_county: Optional[str] = None,
        estimated_value: Optional[float] = None,
    ) -> tuple[bool, list[str], int]:
        """
        Returns (is_match, matched_keywords, score).

        Scoring:
            +10 per matched keyword (in title or description)
            +20 for NAICS prefix match
            +15 for county match
        """
        text = f"{title} {description}"
        score = 0
        matched_keywords: list[str] = []

        # Keyword matching
        for kw, pattern in self._kw_patterns:
            if pattern.search(text):
                matched_keywords.append(kw)
                score += 10

        # NAICS matching
        naics_match = False
        if naics_code:
            for prefix in self.criteria.naics_prefixes:
                if naics_code.startswith(prefix):
                    naics_match = True
                    score += 20
                    break

        # County matching
        if location_county and location_county.lower() in self._counties_lower:
            score += 15

        # Value floor filter (hard gate — doesn't affect score)
        if estimated_value is not None and estimated_value < self.criteria.min_value:
            return False, matched_keywords, score

        # is_match: NAICS hit OR at least one keyword
        is_match = naics_match or len(matched_keywords) >= 1

        # Cap score at 100
        score = min(score, 100)

        return is_match, matched_keywords, score

    def matches(self, **kwargs) -> bool:
        is_match, _, _ = self.score_bid(**kwargs)
        return is_match
