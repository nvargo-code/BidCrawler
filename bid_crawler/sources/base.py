"""Base source class with retry session and rate limiting."""

from __future__ import annotations
import logging
import time
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any, Iterator, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from bid_crawler.config import CriteriaConfig, SourceConfig

logger = logging.getLogger(__name__)


def _make_session(retries: int = 3, backoff: float = 1.0) -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=retries,
        backoff_factor=backoff,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({"User-Agent": "bid-crawler/0.1 (+https://github.com/nvargo-code)"})
    return session


class BaseSource(ABC):
    source_id: str = ""

    def __init__(self, source_cfg: SourceConfig, criteria: CriteriaConfig):
        self.cfg = source_cfg
        self.criteria = criteria
        self.session = _make_session()
        self._delay = source_cfg.delay

    def _sleep(self, delay: Optional[float] = None):
        time.sleep(delay if delay is not None else self._delay)

    def _get(self, url: str, **kwargs) -> requests.Response:
        logger.debug("GET %s", url)
        resp = self.session.get(url, timeout=30, **kwargs)
        resp.raise_for_status()
        return resp

    def _post(self, url: str, **kwargs) -> requests.Response:
        logger.debug("POST %s", url)
        resp = self.session.post(url, timeout=30, **kwargs)
        resp.raise_for_status()
        return resp

    @abstractmethod
    def fetch(self, since: Optional[datetime] = None) -> Iterator[dict[str, Any]]:
        """Yield raw bid dicts. Override in subclasses."""
        ...

    def normalize_date(self, value: Any) -> Optional[str]:
        """Try to parse a date string and return ISO format YYYY-MM-DD, or None."""
        if not value:
            return None
        if isinstance(value, datetime):
            return value.date().isoformat()
        s = str(value).strip()
        for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m-%d-%Y", "%Y%m%d", "%B %d, %Y"):
            try:
                return datetime.strptime(s, fmt).date().isoformat()
            except ValueError:
                continue
        return s  # return as-is if unparseable

    def now_utc(self) -> str:
        return datetime.now(timezone.utc).isoformat()
