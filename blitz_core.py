"""
Shared Blitz API client and pipeline runner.

Extracted from pro_services_pipeline.py and bigcommerce_pipeline.py to provide
a reusable foundation for the web UI and future pipelines.
"""
from __future__ import annotations

import logging
import threading
import time
from typing import Callable

import requests

log = logging.getLogger("blitz_core")

API_BASE = "https://api.blitz-api.ai/api"
API_BASE_V2 = "https://api.blitz-api.ai"
REQUEST_DELAY = 0.25  # ~4 RPS, within 5 RPS limit
MAX_PAGES = 60  # safety cap per search (up to ~3000 companies at 50/page)


class BlitzAPIClient:
    """Thin wrapper around Blitz API with retry logic and rate limiting."""

    def __init__(self, api_key: str):
        self.session = requests.Session()
        self.session.headers.update({
            "x-api-key": api_key,
            "Content-Type": "application/json",
        })
        self._rate_lock = threading.Lock()

    def _post(self, endpoint: str, payload: dict) -> dict | None:
        url = f"{API_BASE}{endpoint}"
        for attempt in range(3):
            try:
                with self._rate_lock:
                    time.sleep(REQUEST_DELAY)
                resp = self.session.post(url, json=payload, timeout=30)
                if resp.status_code == 200:
                    return resp.json()
                if resp.status_code == 429:
                    wait = int(resp.headers.get("Retry-After", 30))
                    log.warning(f"Rate limited — sleeping {wait}s")
                    time.sleep(wait)
                    continue
                if resp.status_code >= 500:
                    wait = 2 ** (attempt + 1)
                    log.warning(f"Server error {resp.status_code} — retry in {wait}s")
                    time.sleep(wait)
                    continue
                log.warning(f"API {endpoint} returned {resp.status_code}: {resp.text[:200]}")
                return None
            except requests.RequestException as e:
                wait = 2 ** (attempt + 1)
                log.warning(f"Network error: {e} — retry in {wait}s")
                time.sleep(wait)
        log.error(f"Failed after 3 retries: {endpoint}")
        return None

    def _post_v2(self, endpoint: str, payload: dict) -> dict | None:
        url = f"{API_BASE_V2}{endpoint}"
        for attempt in range(3):
            try:
                with self._rate_lock:
                    time.sleep(REQUEST_DELAY)
                resp = self.session.post(url, json=payload, timeout=30)
                if resp.status_code == 200:
                    return resp.json()
                if resp.status_code == 429:
                    wait = int(resp.headers.get("Retry-After", 30))
                    log.warning(f"Rate limited — sleeping {wait}s")
                    time.sleep(wait)
                    continue
                if resp.status_code >= 500:
                    wait = 2 ** (attempt + 1)
                    log.warning(f"Server error {resp.status_code} — retry in {wait}s")
                    time.sleep(wait)
                    continue
                log.warning(f"API {endpoint} returned {resp.status_code}: {resp.text[:200]}")
                return None
            except requests.RequestException as e:
                wait = 2 ** (attempt + 1)
                log.warning(f"Network error: {e} — retry in {wait}s")
                time.sleep(wait)
        log.error(f"Failed after 3 retries: {endpoint}")
        return None

    def search_companies(self, payload: dict, cursor: str | None = None) -> tuple[list[dict], str | None]:
        req = dict(payload)
        if cursor:
            req["cursor"] = cursor
        data = self._post_v2("/v2/search/companies", req)
        if not data:
            return ([], None)
        results = []
        next_cursor = None
        if isinstance(data, list):
            results = data
        elif isinstance(data, dict):
            results = data.get("results", data.get("data", data.get("companies", [])))
            next_cursor = (data.get("cursor") or data.get("next_cursor")
                           or (data.get("pagination", {}) or {}).get("next"))
        return (results, next_cursor)

    def waterfall_icp(self, company_linkedin_url: str, cascade: list,
                      max_results: int = 1) -> list[dict]:
        payload = {
            "company_linkedin_url": company_linkedin_url,
            "max_results": max_results,
            "cascade": cascade,
        }
        data = self._post("/search/waterfall-icp", payload)
        if not data:
            return []
        if isinstance(data, list):
            return data
        if isinstance(data, dict) and data.get("results"):
            return data["results"]
        return []

    def enrich_email(self, linkedin_profile_url: str) -> str | None:
        data = self._post("/enrichment/email", {
            "linkedin_profile_url": linkedin_profile_url,
        })
        if data:
            if data.get("email"):
                return data["email"]
            if data.get("all_emails") and len(data["all_emails"]) > 0:
                return data["all_emails"][0].get("email_address", "")
        return None


# ---------------------------------------------------------------------------
# Response field normalization
# ---------------------------------------------------------------------------

def extract_company_linkedin(company: dict) -> str:
    for key in ("linkedin_url", "company_linkedin_url", "linkedin_company_url",
                "linkedin", "url"):
        val = company.get(key)
        if val and "linkedin.com" in str(val):
            return val
    return ""


def extract_company_fields(company: dict) -> dict:
    name = (company.get("name") or company.get("company_name")
            or company.get("organization_name") or "")
    website = (company.get("website") or company.get("domain")
               or company.get("website_url") or "")
    industry = (company.get("industry") or company.get("industries")
                or company.get("primary_industry") or "")
    if isinstance(industry, list):
        industry = ", ".join(industry)
    employees = (company.get("employee_count") or company.get("employees")
                 or company.get("employees_on_linkedin") or company.get("size")
                 or company.get("staff_count") or company.get("employee_range") or "")
    hq_city = (company.get("hq_city") or company.get("city")
               or company.get("headquarters_city") or "")
    hq_country = (company.get("hq_country") or company.get("country")
                  or company.get("headquarters_country") or "")
    hq = company.get("headquarters") or company.get("hq") or {}
    if isinstance(hq, dict):
        hq_city = hq_city or hq.get("city", "")
        hq_country = hq_country or hq.get("country_name", "") or hq.get("country", "")
    location = company.get("location") or company.get("hq_location") or ""
    if location and not hq_city:
        hq_city = location

    return {
        "company_name": name,
        "website": website,
        "industry": industry,
        "employee_count": str(employees),
        "hq_city": hq_city,
        "hq_country": hq_country,
        "company_linkedin_url": extract_company_linkedin(company),
    }


def extract_person_fields(person: dict) -> dict:
    full_name = (person.get("full_name") or "").strip()
    parts = full_name.split()
    first_name = person.get("first_name") or (parts[0] if parts else "")
    last_name = person.get("last_name") or (" ".join(parts[1:]) if len(parts) > 1 else "")
    title = (person.get("job_title") or person.get("title")
             or person.get("linkedin_headline") or "")
    linkedin_url = (person.get("person_linkedin_url")
                    or person.get("linkedin_profile_url")
                    or person.get("linkedin_url") or "")
    return {
        "full_name": full_name,
        "first_name": first_name,
        "last_name": last_name,
        "title": title,
        "person_linkedin_url": linkedin_url,
    }


# ---------------------------------------------------------------------------
# Pipeline runner
# ---------------------------------------------------------------------------

class PipelineRunner:
    """Orchestrates the 3-step pipeline with progress callbacks."""

    def __init__(self, client: BlitzAPIClient,
                 on_progress: Callable[[dict], None] | None = None,
                 max_leads: int = 1000):
        self.client = client
        self.on_progress = on_progress or (lambda e: None)
        self.max_leads = max_leads
        self._cancelled = False

    def cancel(self):
        self._cancelled = True

    def _emit(self, event: dict):
        self.on_progress(event)

    def run(self, company_search_payload: dict,
            include_titles: list[str], exclude_titles: list[str]) -> list[dict]:
        """Run the full 3-step pipeline. Returns list of result dicts."""
        self._cancelled = False

        # Step 1: Company discovery
        self._emit({"type": "status", "step": 1, "message": "Searching for companies..."})
        companies = self._search_companies(company_search_payload)
        if not companies:
            self._emit({"type": "error", "message": "No companies found matching your criteria."})
            return []
        self._emit({"type": "status", "step": 1,
                     "message": f"Found {len(companies)} companies. Starting people search..."})

        # Step 2: People search
        self._emit({"type": "status", "step": 2, "message": "Searching for decision-makers..."})
        cascade = [{
            "include_title": include_titles,
            "exclude_title": exclude_titles,
            "location": ["WORLD"],
            "include_headline_search": False,
        }]
        people_by_company = self._find_people(companies, cascade)

        total_people = sum(len(p) for p in people_by_company.values())
        if total_people == 0:
            self._emit({"type": "error",
                         "message": "No people found matching your title criteria."})
            return []
        self._emit({"type": "status", "step": 2,
                     "message": f"Found {total_people} people. Starting email enrichment..."})

        # Step 3: Email enrichment
        self._emit({"type": "status", "step": 3, "message": "Enriching emails..."})
        results = self._enrich_emails(people_by_company)

        self._emit({"type": "complete", "total_companies": len(companies),
                     "total_people": total_people,
                     "total_emails": sum(1 for r in results if r.get("email"))})
        return results

    def _search_companies(self, payload: dict) -> list[dict]:
        # Oversample by 2x to account for enrichment hit rate (~50-60%)
        target_companies = self.max_leads * 2
        all_companies = []
        cursor = None
        page = 0
        while page < MAX_PAGES and not self._cancelled:
            page += 1
            results, next_cursor = self.client.search_companies(payload, cursor)
            if not results:
                break
            for co in results:
                fields = extract_company_fields(co)
                fields["_raw"] = co
                all_companies.append(fields)
                self._emit({"type": "company", "data": {
                    k: v for k, v in fields.items() if k != "_raw"
                }})
            self._emit({"type": "status", "step": 1,
                         "message": f"Found {len(all_companies)} companies (page {page})...",
                         "progress": {"current": len(all_companies)}})
            if len(all_companies) >= target_companies:
                break
            if not next_cursor:
                break
            cursor = next_cursor
        return all_companies

    def _find_people(self, companies: list[dict],
                     cascade: list[dict]) -> dict[str, list[dict]]:
        # Stop once we have enough people to likely yield max_leads verified
        # emails (~2x oversample accounts for enrichment hit rate)
        target_people = self.max_leads * 2
        people_by_company: dict[str, list[dict]] = {}
        people_count = 0
        for i, co in enumerate(companies):
            if self._cancelled or people_count >= target_people:
                break
            linkedin_url = co.get("company_linkedin_url", "")
            if not linkedin_url:
                continue
            self._emit({"type": "status", "step": 2,
                         "message": f"Searching people at {co['company_name']}...",
                         "progress": {"current": i + 1, "total": len(companies)}})
            raw_people = self.client.waterfall_icp(linkedin_url, cascade, max_results=1)
            if raw_people:
                people = []
                for p in raw_people:
                    person = extract_person_fields(p)
                    person["company_name"] = co["company_name"]
                    person["website"] = co["website"]
                    person["industry"] = co["industry"]
                    person["employee_count"] = co["employee_count"]
                    person["company_linkedin_url"] = linkedin_url
                    people.append(person)
                    self._emit({"type": "person", "data": person})
                people_by_company[linkedin_url] = people
                people_count += len(people)
        return people_by_company

    def _enrich_emails(self, people_by_company: dict[str, list[dict]]) -> list[dict]:
        results = []
        all_people = [p for people in people_by_company.values() for p in people]
        for i, person in enumerate(all_people):
            verified_count = sum(1 for r in results if r.get("email"))
            if self._cancelled or verified_count >= self.max_leads:
                break
            self._emit({"type": "status", "step": 3,
                         "message": f"Enriching email for {person['full_name']}...",
                         "progress": {"current": i + 1, "total": len(all_people)}})
            email = ""
            if person.get("person_linkedin_url"):
                email = self.client.enrich_email(person["person_linkedin_url"]) or ""
            result = {
                "company_name": person["company_name"],
                "website": person["website"],
                "industry": person["industry"],
                "employee_count": person["employee_count"],
                "company_linkedin_url": person["company_linkedin_url"],
                "full_name": person["full_name"],
                "first_name": person["first_name"],
                "last_name": person["last_name"],
                "title": person["title"],
                "person_linkedin_url": person["person_linkedin_url"],
                "email": email,
            }
            results.append(result)
            self._emit({"type": "result", "data": result})
        return results
