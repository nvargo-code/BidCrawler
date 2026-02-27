"""Tests for bid_crawler.sources.sam_gov_source."""

from __future__ import annotations
import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, call, patch

import pytest

from bid_crawler.config import CriteriaConfig, SourceConfig
from bid_crawler.sources.sam_gov_source import SamGovSource, _parse_float


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_source_cfg(**overrides) -> SourceConfig:
    defaults = dict(
        id="sam_gov",
        source_type="api",
        enabled=True,
        base_url="https://api.sam.gov/opportunities/v2/search",
        page_size=10,
        max_pages=5,
        delay=0,
    )
    defaults.update(overrides)
    cfg = SourceConfig(**{k: v for k, v in defaults.items() if k in SourceConfig.__dataclass_fields__})
    return cfg


def _make_criteria_cfg(**overrides) -> CriteriaConfig:
    defaults = dict(
        keywords=["construction", "renovation"],
        naics_prefixes=["236", "237", "238"],
        counties=["Dallas", "Tarrant"],
        min_value=50_000.0,
    )
    defaults.update(overrides)
    return CriteriaConfig(**defaults)


@pytest.fixture()
def source_cfg():
    return _make_source_cfg()


@pytest.fixture()
def criteria_cfg():
    return _make_criteria_cfg()


@pytest.fixture()
def source(source_cfg, criteria_cfg):
    return SamGovSource(source_cfg, criteria_cfg)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _mock_response(data: dict) -> MagicMock:
    """Return a mock that behaves like requests.Response with .json()."""
    resp = MagicMock()
    resp.json.return_value = data
    return resp


def _opp(**overrides) -> dict:
    """Minimal valid opportunity payload."""
    base = {
        "noticeId": "NOTICE-001",
        "solicitationNumber": "SOL-001",
        "title": "Roof Replacement at Facility A",
        "description": "Commercial roofing and waterproofing project",
        "organizationName": "Dept of Defense",
        "active": "Yes",
        "postedDate": "2026-01-15",
        "responseDeadLine": "2026-02-28",
        "naicsCode": "238160",
        "classificationCode": "Z",
        "typeOfSetAside": "SBA",
        "placeOfPerformance": {
            "city": {"name": "Dallas"},
            "state": {"code": "TX"},
            "zip": "75201",
        },
        "pointOfContact": [
            {"fullName": "Jane Smith", "email": "jane@example.gov", "phone": "214-555-0100"}
        ],
        "award": {},
        "resourceLinks": ["https://sam.gov/docs/NOTICE-001"],
    }
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# _parse_float
# ---------------------------------------------------------------------------

class TestParseFloat:
    def test_none_returns_none(self):
        assert _parse_float(None) is None

    def test_plain_int(self):
        assert _parse_float(500000) == 500_000.0

    def test_plain_float_string(self):
        assert _parse_float("1234567.89") == 1_234_567.89

    def test_dollar_and_commas(self):
        assert _parse_float("$1,234,567") == 1_234_567.0

    def test_non_numeric_returns_none(self):
        assert _parse_float("TBD") is None

    def test_empty_string_returns_none(self):
        assert _parse_float("") is None


# ---------------------------------------------------------------------------
# _normalize
# ---------------------------------------------------------------------------

class TestNormalize:
    def test_basic_fields(self, source):
        opp = _opp()
        result = source._normalize(opp)

        assert result["source_id"] == "sam_gov"
        assert result["external_id"] == "NOTICE-001"
        assert result["bid_number"] == "SOL-001"
        assert result["title"] == "Roof Replacement at Facility A"
        assert result["agency"] == "Dept of Defense"
        assert result["agency_type"] == "federal"

    def test_external_id_falls_back_to_solicitation_number(self, source):
        opp = _opp(noticeId=None)
        result = source._normalize(opp)
        assert result["external_id"] == "SOL-001"

    def test_dates_normalized_to_iso(self, source):
        result = source._normalize(_opp())
        assert result["posted_date"] == "2026-01-15"
        assert result["due_date"] == "2026-02-28"

    def test_location_fields(self, source):
        result = source._normalize(_opp())
        assert result["location_city"] == "Dallas"
        assert result["location_state"] == "TX"
        assert result["location_zip"] == "75201"

    def test_contact_extracted_from_first_poc(self, source):
        result = source._normalize(_opp())
        assert result["contact_name"] == "Jane Smith"
        assert result["contact_email"] == "jane@example.gov"
        assert result["contact_phone"] == "214-555-0100"

    def test_contact_empty_when_no_poc(self, source):
        result = source._normalize(_opp(pointOfContact=[]))
        assert result["contact_name"] == ""
        assert result["contact_email"] == ""

    def test_status_open_when_active_yes(self, source):
        result = source._normalize(_opp(active="Yes"))
        assert result["status"] == "open"

    def test_status_closed_when_active_no(self, source):
        result = source._normalize(_opp(active="No"))
        assert result["status"] == "closed"

    def test_status_awarded_when_awardee_present(self, source):
        opp = _opp(active="No", award={"awardee": {"name": "Acme Corp"}, "amount": "500000"})
        result = source._normalize(opp)
        assert result["status"] == "awarded"

    def test_estimated_value_parsed_from_award(self, source):
        opp = _opp(award={"amount": "$250,000", "awardee": {"name": "Acme"}})
        result = source._normalize(opp)
        assert result["estimated_value"] == 250_000.0

    def test_estimated_value_none_when_no_award_amount(self, source):
        result = source._normalize(_opp(award={}))
        assert result["estimated_value"] is None

    def test_bid_url_constructed_from_notice_id(self, source):
        result = source._normalize(_opp())
        assert result["bid_url"] == "https://sam.gov/opp/NOTICE-001/view"

    def test_documents_url_from_resource_links(self, source):
        result = source._normalize(_opp())
        assert result["documents_url"] == "https://sam.gov/docs/NOTICE-001"

    def test_documents_url_empty_when_no_resource_links(self, source):
        result = source._normalize(_opp(resourceLinks=[]))
        assert result["documents_url"] == ""

    def test_raw_payload_preserved(self, source):
        opp = _opp()
        result = source._normalize(opp)
        assert result["raw_payload"] is opp

    def test_missing_place_of_performance_safe(self, source):
        opp = _opp()
        del opp["placeOfPerformance"]
        result = source._normalize(opp)
        assert result["location_city"] == ""
        assert result["location_state"] == "TX"

    def test_naics_code_and_set_aside(self, source):
        result = source._normalize(_opp())
        assert result["naics_code"] == "238160"
        assert result["set_aside"] == "SBA"


# ---------------------------------------------------------------------------
# fetch — request params
# ---------------------------------------------------------------------------

class TestFetchParams:
    def test_no_api_key_omits_header(self, source_cfg, criteria_cfg):
        """Without an API key env var, X-Api-Key must not appear in headers."""
        src = SamGovSource(source_cfg, criteria_cfg)
        page_data = {"opportunitiesData": [], "totalRecords": 0}
        with patch.object(src, "_get", return_value=_mock_response(page_data)) as mock_get:
            list(src.fetch())
        _, kwargs = mock_get.call_args
        assert "X-Api-Key" not in kwargs.get("headers", {})

    def test_api_key_added_to_header(self, source_cfg, criteria_cfg, monkeypatch):
        monkeypatch.setenv("SAM_GOV_API_KEY", "test-key-123")
        source_cfg.env_key = "SAM_GOV_API_KEY"
        src = SamGovSource(source_cfg, criteria_cfg)
        page_data = {"opportunitiesData": [], "totalRecords": 0}
        with patch.object(src, "_get", return_value=_mock_response(page_data)) as mock_get:
            list(src.fetch())
        _, kwargs = mock_get.call_args
        assert kwargs["headers"]["X-Api-Key"] == "test-key-123"

    def test_first_run_sends_required_date_params(self, source):
        """With no `since`, postedFrom/postedTo must both be sent."""
        page_data = {"opportunitiesData": [], "totalRecords": 0}
        with patch.object(source, "_get", return_value=_mock_response(page_data)) as mock_get:
            list(source.fetch(since=None))
        params = mock_get.call_args[1]["params"]
        assert "postedFrom" in params
        assert "postedTo" in params

    def test_first_run_posted_from_is_90_days_ago(self, source):
        page_data = {"opportunitiesData": [], "totalRecords": 0}
        fixed_now = datetime(2026, 2, 27, 12, 0, 0, tzinfo=timezone.utc)
        expected_from = (fixed_now - timedelta(days=90)).strftime("%m/%d/%Y")
        expected_to = fixed_now.strftime("%m/%d/%Y")

        with patch("bid_crawler.sources.sam_gov_source.datetime") as mock_dt:
            mock_dt.now.return_value = fixed_now
            mock_dt.strptime = datetime.strptime  # keep real strptime
            with patch.object(source, "_get", return_value=_mock_response(page_data)) as mock_get:
                list(source.fetch(since=None))

        params = mock_get.call_args[1]["params"]
        assert params["postedFrom"] == expected_from
        assert params["postedTo"] == expected_to

    def test_since_overrides_posted_from(self, source):
        since = datetime(2026, 1, 1, tzinfo=timezone.utc)
        page_data = {"opportunitiesData": [], "totalRecords": 0}
        with patch.object(source, "_get", return_value=_mock_response(page_data)) as mock_get:
            list(source.fetch(since=since))
        params = mock_get.call_args[1]["params"]
        assert params["postedFrom"] == "01/01/2026"

    def test_fixed_params_always_present(self, source):
        page_data = {"opportunitiesData": [], "totalRecords": 0}
        with patch.object(source, "_get", return_value=_mock_response(page_data)) as mock_get:
            list(source.fetch())
        params = mock_get.call_args[1]["params"]
        assert params["ptype"] == "o"
        assert params["state"] == "TX"
        assert params["limit"] == source.cfg.page_size
        assert params["offset"] == 0

    def test_no_naics_code_in_params(self, source):
        """NAICS filtering is done locally; API should not receive naicsCode/ncode."""
        page_data = {"opportunitiesData": [], "totalRecords": 0}
        with patch.object(source, "_get", return_value=_mock_response(page_data)) as mock_get:
            list(source.fetch())
        params = mock_get.call_args[1]["params"]
        assert "naicsCode" not in params
        assert "ncode" not in params

    def test_no_active_param(self, source):
        """active=Yes is not a valid SAM v2 param and must not be sent."""
        page_data = {"opportunitiesData": [], "totalRecords": 0}
        with patch.object(source, "_get", return_value=_mock_response(page_data)) as mock_get:
            list(source.fetch())
        params = mock_get.call_args[1]["params"]
        assert "active" not in params


# ---------------------------------------------------------------------------
# fetch — pagination & response handling
# ---------------------------------------------------------------------------

class TestFetchPagination:
    def test_single_page_yields_all_records(self, source):
        opps = [_opp(noticeId=f"N-{i}") for i in range(3)]
        page_data = {"opportunitiesData": opps, "totalRecords": 3}
        with patch.object(source, "_get", return_value=_mock_response(page_data)):
            results = list(source.fetch())
        assert len(results) == 3

    def test_stops_when_empty_opportunities(self, source):
        page_data = {"opportunitiesData": [], "totalRecords": 0}
        with patch.object(source, "_get", return_value=_mock_response(page_data)) as mock_get:
            results = list(source.fetch())
        assert results == []
        assert mock_get.call_count == 1  # stops after first empty page

    def test_paginates_across_pages(self, source):
        page_size = source.cfg.page_size  # 10
        page0_opps = [_opp(noticeId=f"N-{i}") for i in range(page_size)]
        page1_opps = [_opp(noticeId=f"N-{i+page_size}") for i in range(5)]
        total = page_size + 5

        responses = [
            _mock_response({"opportunitiesData": page0_opps, "totalRecords": total}),
            _mock_response({"opportunitiesData": page1_opps, "totalRecords": total}),
        ]
        with patch.object(source, "_get", side_effect=responses):
            results = list(source.fetch())
        assert len(results) == total

    def test_offset_increments_per_page(self, source):
        page_size = source.cfg.page_size
        total = page_size * 2
        page_opps = [_opp(noticeId=f"N-{i}") for i in range(page_size)]
        responses = [
            _mock_response({"opportunitiesData": page_opps, "totalRecords": total}),
            _mock_response({"opportunitiesData": page_opps, "totalRecords": total}),
            _mock_response({"opportunitiesData": [], "totalRecords": total}),
        ]
        with patch.object(source, "_get", side_effect=responses) as mock_get:
            list(source.fetch())
        offsets = [c[1]["params"]["offset"] for c in mock_get.call_args_list]
        assert offsets[0] == 0
        assert offsets[1] == page_size

    def test_respects_max_pages(self, source_cfg, criteria_cfg):
        source_cfg = _make_source_cfg(max_pages=2, page_size=2)
        src = SamGovSource(source_cfg, criteria_cfg)
        always_two = {"opportunitiesData": [_opp(noticeId="A"), _opp(noticeId="B")], "totalRecords": 999}
        with patch.object(src, "_get", return_value=_mock_response(always_two)) as mock_get:
            results = list(src.fetch())
        assert mock_get.call_count == 2
        assert len(results) == 4

    def test_api_error_stops_iteration_without_raising(self, source):
        """HTTP errors should be caught and logged, not propagate to the caller."""
        with patch.object(source, "_get", side_effect=Exception("500 Server Error")):
            results = list(source.fetch())  # must not raise
        assert results == []

    def test_results_are_normalized_dicts(self, source):
        page_data = {"opportunitiesData": [_opp()], "totalRecords": 1}
        with patch.object(source, "_get", return_value=_mock_response(page_data)):
            results = list(source.fetch())
        assert results[0]["source_id"] == "sam_gov"
        assert "external_id" in results[0]
        assert "raw_payload" in results[0]
