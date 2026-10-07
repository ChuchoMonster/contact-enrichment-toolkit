"""BlitzAPIClient (blitz_core.py): request building, retries and response handling.

Every HTTP call is intercepted by `responses`; an unmatched request raises.
"""
from __future__ import annotations

import json

import pytest
import requests
import responses

import blitz_core
from blitz_core import API_BASE, API_BASE_V2, BlitzAPIClient

WATERFALL_URL = f"{API_BASE}/search/waterfall-icp"
EMAIL_URL = f"{API_BASE}/enrichment/email"
COMPANIES_URL = f"{API_BASE_V2}/v2/search/companies"


@pytest.fixture
def client(no_sleep):
    return BlitzAPIClient("test-key")


@responses.activate
def test_post_sends_api_key_and_json_body(client):
    responses.post(WATERFALL_URL, json={"results": []})

    client._post("/search/waterfall-icp", {"company_linkedin_url": "u"})

    sent = responses.calls[0].request
    assert sent.headers["x-api-key"] == "test-key"
    assert sent.headers["Content-Type"] == "application/json"
    assert json.loads(sent.body) == {"company_linkedin_url": "u"}


@responses.activate
def test_post_honours_retry_after_on_429_then_succeeds(client, no_sleep):
    responses.post(EMAIL_URL, status=429, headers={"Retry-After": "7"})
    responses.post(EMAIL_URL, json={"email": "pat@example.com"})

    assert client._post("/enrichment/email", {}) == {"email": "pat@example.com"}
    assert len(responses.calls) == 2
    assert 7 in no_sleep


@responses.activate
def test_post_backs_off_exponentially_on_5xx_and_gives_up(client, no_sleep):
    responses.post(EMAIL_URL, status=503)

    assert client._post("/enrichment/email", {}) is None
    assert len(responses.calls) == 3
    backoffs = [s for s in no_sleep if s != blitz_core.REQUEST_DELAY]
    assert backoffs == [2, 4, 8]


@responses.activate
def test_post_does_not_retry_client_errors(client):
    responses.post(EMAIL_URL, status=404, body="not found")

    assert client._post("/enrichment/email", {}) is None
    assert len(responses.calls) == 1


@responses.activate
def test_post_retries_after_network_error(client):
    responses.post(EMAIL_URL, body=requests.ConnectionError("reset"))
    responses.post(EMAIL_URL, json={"email": "lee@example.com"})

    assert client._post("/enrichment/email", {}) == {"email": "lee@example.com"}
    assert len(responses.calls) == 2


@responses.activate
def test_search_companies_adds_cursor_without_mutating_payload(client):
    responses.post(COMPANIES_URL, json={"results": [{"name": "Acme"}], "cursor": "c2"})
    payload = {"company": {"industry": {"include": ["Software"]}}, "max_results": 50}

    results, cursor = client.search_companies(payload, cursor="c1")

    assert results == [{"name": "Acme"}]
    assert cursor == "c2"
    assert json.loads(responses.calls[0].request.body)["cursor"] == "c1"
    assert "cursor" not in payload


@pytest.mark.parametrize("body, expected", [
    ([{"name": "A"}], ([{"name": "A"}], None)),
    ({"data": [{"name": "B"}], "next_cursor": "n"}, ([{"name": "B"}], "n")),
    ({"companies": [{"name": "C"}], "pagination": {"next": "p"}}, ([{"name": "C"}], "p")),
    ({}, ([], None)),
])
@responses.activate
def test_search_companies_accepts_each_response_shape(client, body, expected):
    responses.post(COMPANIES_URL, json=body)
    assert client.search_companies({}) == expected


@responses.activate
def test_waterfall_icp_builds_payload_and_reads_results(client):
    cascade = [{"include_title": ["CEO"], "exclude_title": [], "location": ["WORLD"]}]
    responses.post(WATERFALL_URL, json={"results": [{"full_name": "Dana Example"}]})

    people = client.waterfall_icp("https://www.linkedin.com/company/acme", cascade, max_results=3)

    assert people == [{"full_name": "Dana Example"}]
    assert json.loads(responses.calls[0].request.body) == {
        "company_linkedin_url": "https://www.linkedin.com/company/acme",
        "max_results": 3,
        "cascade": cascade,
    }


@responses.activate
def test_waterfall_icp_returns_empty_list_when_api_fails(client):
    responses.post(WATERFALL_URL, status=400)
    assert client.waterfall_icp("u", []) == []


@pytest.mark.parametrize("body, expected", [
    ({"email": "first@example.com", "all_emails": [{"email_address": "x@example.com"}]},
     "first@example.com"),
    ({"email": "", "all_emails": [{"email_address": "alt@example.com"},
                                  {"email_address": "other@example.com"}]}, "alt@example.com"),
    ({"email": None, "all_emails": []}, None),
])
@responses.activate
def test_enrich_email_prefers_primary_then_all_emails(client, body, expected):
    responses.post(EMAIL_URL, json=body)

    assert client.enrich_email("https://www.linkedin.com/in/someone") == expected
    assert json.loads(responses.calls[0].request.body) == {
        "linkedin_profile_url": "https://www.linkedin.com/in/someone"}
