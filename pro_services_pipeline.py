#!/usr/bin/env python3
"""
Professional Services Vertical Pipeline — Search for companies across 5
professional-services verticals via BlitzAPI /v2/search/companies, find
decision-makers with Waterfall ICP, enrich emails, and export per-vertical CSVs.

Verticals: Legal, Accounting, Financial Advisory, Recruiting/Staffing, HR Consulting

Usage:
    python pro_services_pipeline.py              # all 5 verticals
    python pro_services_pipeline.py --vertical legal
    python pro_services_pipeline.py --dry-run    # log calls, no API hits
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import re
import signal
import sys
import time
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
API_BASE = "https://api.blitz-api.ai/api"
API_BASE_V2 = "https://api.blitz-api.ai"
REQUEST_DELAY = 0.25  # ~4 RPS, within 5 RPS limit
MAX_PAGES_PER_VERTICAL = 20  # safety cap: 20 pages × 25 = 500 companies max

OUTPUT_DIR = Path(__file__).resolve().parent
CHECKPOINT_FILE = OUTPUT_DIR / "ps_checkpoint.json"
LOG_FILE = OUTPUT_DIR / "ps_pipeline.log"

OUTPUT_COLUMNS = [
    "company_name", "website", "industry", "employee_count",
    "hq_city", "hq_country", "contact_first_name", "contact_last_name",
    "contact_title", "contact_linkedin_url", "contact_email",
    "Company_Linkedin_url",
]

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
log = logging.getLogger("ps_pipeline")
log.setLevel(logging.DEBUG)
_fmt = logging.Formatter("%(asctime)s  %(levelname)-8s  %(message)s",
                         datefmt="%H:%M:%S")
_sh = logging.StreamHandler()
_sh.setLevel(logging.INFO)
_sh.setFormatter(_fmt)
log.addHandler(_sh)
_fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
_fh.setLevel(logging.DEBUG)
_fh.setFormatter(_fmt)
log.addHandler(_fh)

# ---------------------------------------------------------------------------
# Graceful shutdown
# ---------------------------------------------------------------------------
_shutdown_requested = False


def _signal_handler(signum, frame):
    global _shutdown_requested
    _shutdown_requested = True
    log.warning("Shutdown requested — finishing current item then saving…")


signal.signal(signal.SIGINT, _signal_handler)
signal.signal(signal.SIGTERM, _signal_handler)

# ---------------------------------------------------------------------------
# City filter
# ---------------------------------------------------------------------------
PRIMARY_CITIES = {
    "austin", "denver", "miami", "chicago", "atlanta",
    "washington", "boston", "new york", "los angeles", "san francisco",
}
SECONDARY_CITIES = {
    "phoenix", "charlotte", "nashville", "minneapolis",
    "dallas", "houston", "seattle",
}
ALL_TARGET_CITIES = PRIMARY_CITIES | SECONDARY_CITIES

CITY_ALIASES = {
    "nyc": "new york",
    "ny": "new york",
    "dc": "washington",
    "d.c.": "washington",
    "sf": "san francisco",
    "la": "los angeles",
    "atl": "atlanta",
    "dfw": "dallas",
    "phx": "phoenix",
    "msp": "minneapolis",
    "hou": "houston",
    "sea": "seattle",
}


def matches_target_city(hq_string: str) -> bool:
    if not hq_string:
        return False
    norm = hq_string.lower().strip()
    norm = re.sub(r",?\s*(united states|usa|us|u\.s\.a?\.)$", "", norm).strip()
    norm = re.sub(r",\s*[A-Za-z]{2}$", "", norm).strip()  # strip ", TX" style state abbrev

    for alias, city in CITY_ALIASES.items():
        if norm == alias or norm.startswith(alias + " ") or norm.startswith(alias + ","):
            return True
        if alias in norm.split():
            norm = norm.replace(alias, city)

    for city in ALL_TARGET_CITIES:
        if city in norm:
            return True
    return False


def extract_city(hq_string: str) -> str:
    """Return a cleaned city name from the raw HQ string."""
    if not hq_string:
        return ""
    return hq_string.strip()

# ---------------------------------------------------------------------------
# Waterfall ICP cascade — translated to include_title format
# Priority: Managing Partner → Partner → COO → Director Ops → CEO
# ---------------------------------------------------------------------------
CASCADE = [
    {
        "include_title": [
            "Managing Partner", "Senior Partner", "Managing Director",
            "Managing Member",
        ],
        "exclude_title": ["assistant", "associate", "intern", "junior", "analyst"],
        "location": ["WORLD"],
        "include_headline_search": False,
    },
    {
        "include_title": [
            "Partner", "Principal", "Member",
        ],
        "exclude_title": ["assistant", "associate", "intern", "junior", "analyst",
                          "tax", "audit", "litigation"],
        "location": ["WORLD"],
        "include_headline_search": False,
    },
    {
        "include_title": [
            "COO", "Chief Operating Officer",
            "Chief Financial Officer", "CFO",
        ],
        "exclude_title": ["assistant", "associate", "intern", "junior"],
        "location": ["WORLD"],
        "include_headline_search": False,
    },
    {
        "include_title": [
            "Director of Operations", "Director of Business Development",
            "VP Operations", "VP Business Development",
            "Head of Operations",
        ],
        "exclude_title": ["assistant", "associate", "intern", "junior"],
        "location": ["WORLD"],
        "include_headline_search": False,
    },
    {
        "include_title": [
            "CEO", "Founder", "Owner", "President",
        ],
        "exclude_title": ["VP", "assistant", "associate", "intern", "junior"],
        "location": ["WORLD"],
        "include_headline_search": False,
    },
]

# ---------------------------------------------------------------------------
# Vertical definitions
# ---------------------------------------------------------------------------
VERTICALS = [
    {
        "name": "Legal",
        "slug": "legal",
        "output_file": "legal_firms.csv",
        "search_payload": {
            "company": {
                "industry": {"include": ["Law Practice", "Legal Services"]},
                "employee_range": ["11-50", "51-200"],
                "hq": {"country_code": ["US"]},
                "type": {"include": ["Privately Held", "Partnership"]},
                "keywords": {
                    "exclude": ["legal aid", "nonprofit", "public defender", "government"],
                },
            },
            "max_results": 25,
        },
    },
    {
        "name": "Accounting",
        "slug": "accounting",
        "output_file": "accounting_firms.csv",
        "search_payload": {
            "company": {
                "industry": {"include": ["Accounting"]},
                "employee_range": ["11-50", "51-200"],
                "hq": {"country_code": ["US"]},
                "type": {"include": ["Privately Held", "Partnership"]},
                "keywords": {
                    "exclude": ["bookkeeping only", "tax prep chain", "H&R Block"],
                },
            },
            "max_results": 25,
        },
    },
    {
        "name": "Financial Advisory",
        "slug": "financial",
        "output_file": "financial_advisory.csv",
        "search_payload": {
            "company": {
                "industry": {"include": ["Financial Services", "Investment Management"]},
                "employee_range": ["11-50", "51-200"],
                "hq": {"country_code": ["US"]},
                "type": {"include": ["Privately Held", "Partnership"]},
                "keywords": {
                    "include": ["wealth management", "financial planning",
                                "RIA", "advisory", "financial advisor"],
                },
            },
            "max_results": 25,
        },
    },
    {
        "name": "Recruiting / Staffing",
        "slug": "recruiting",
        "output_file": "recruiting_staffing.csv",
        "search_payload": {
            "company": {
                "industry": {"include": ["Staffing and Recruiting", "Human Resources"]},
                "employee_range": ["11-50", "51-200"],
                "hq": {"country_code": ["US"]},
                "type": {"include": ["Privately Held"]},
                "keywords": {
                    "include": ["executive search", "recruiting", "staffing",
                                "talent acquisition", "headhunting"],
                },
            },
            "max_results": 25,
        },
    },
    {
        "name": "HR Consulting",
        "slug": "hr",
        "output_file": "hr_consulting.csv",
        "search_payload": {
            "company": {
                "industry": {"include": ["Human Resources", "Management Consulting"]},
                "employee_range": ["11-50", "51-200"],
                "hq": {"country_code": ["US"]},
                "type": {"include": ["Privately Held"]},
                "keywords": {
                    "include": ["HR consulting", "people operations", "HR strategy",
                                "human resources consulting", "workforce"],
                },
            },
            "max_results": 25,
        },
    },
]

VERTICAL_SLUGS = {v["slug"] for v in VERTICALS}

# ---------------------------------------------------------------------------
# Checkpoint manager
# ---------------------------------------------------------------------------

class CheckpointManager:
    def __init__(self, path: Path):
        self.path = path
        self.data: dict = self._load()

    def _load(self) -> dict:
        if self.path.exists():
            try:
                return json.loads(self.path.read_text())
            except (json.JSONDecodeError, OSError):
                log.warning("Corrupt checkpoint — starting fresh")
        return {"verticals": {}, "stats": {
            "api_calls": 0, "companies_searched": 0,
            "contacts_found": 0, "emails_found": 0,
        }}

    def save(self):
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self.data, indent=2))
        tmp.replace(self.path)

    def _vert(self, slug: str) -> dict:
        if slug not in self.data["verticals"]:
            self.data["verticals"][slug] = {
                "search_complete": False,
                "processed": [],
            }
        return self.data["verticals"][slug]

    def is_search_done(self, slug: str) -> bool:
        return self._vert(slug).get("search_complete", False)

    def mark_search_done(self, slug: str):
        self._vert(slug)["search_complete"] = True
        self.save()

    def is_processed(self, slug: str, key: str) -> bool:
        return key in self._vert(slug).get("processed", [])

    def mark_processed(self, slug: str, key: str):
        self._vert(slug).setdefault("processed", []).append(key)

    def inc(self, stat: str, n: int = 1):
        self.data["stats"][stat] = self.data["stats"].get(stat, 0) + n


# ---------------------------------------------------------------------------
# BlitzAPI client
# ---------------------------------------------------------------------------

class BlitzAPIClient:
    def __init__(self, api_key: str, dry_run: bool = False):
        self.session = requests.Session()
        self.session.headers.update({
            "x-api-key": api_key,
            "Content-Type": "application/json",
        })
        self.dry_run = dry_run

    def _post(self, endpoint: str, payload: dict) -> dict | None:
        url = f"{API_BASE}{endpoint}"
        if self.dry_run:
            log.info(f"[DRY-RUN] POST {url}  payload_keys={list(payload.keys())}")
            return None
        for attempt in range(3):
            try:
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
        if self.dry_run:
            log.info(f"[DRY-RUN] POST {url}  payload_keys={list(payload.keys())}")
            return None
        for attempt in range(3):
            try:
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

    # -- Company search with pagination --
    def search_companies(self, payload: dict, cursor: str | None = None) -> tuple[list[dict], str | None]:
        req = dict(payload)
        if cursor:
            req["cursor"] = cursor
        data = self._post_v2("/v2/search/companies", req)
        if not data:
            return ([], None)

        # Log raw structure on first call for debugging
        if not cursor:
            log.debug(f"Company search raw response keys: {list(data.keys()) if isinstance(data, dict) else 'list'}")
            if isinstance(data, dict):
                log.debug(f"Response sample (truncated): {json.dumps(data, default=str)[:500]}")

        # Parse results — handle multiple response shapes
        results = []
        next_cursor = None
        if isinstance(data, list):
            results = data
        elif isinstance(data, dict):
            results = data.get("results", data.get("data", data.get("companies", [])))
            next_cursor = (data.get("cursor") or data.get("next_cursor")
                           or (data.get("pagination", {}) or {}).get("next"))
        return (results, next_cursor)

    # -- Waterfall ICP --
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

    # -- Email enrichment --
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
# Helpers
# ---------------------------------------------------------------------------

def split_name(full_name: str) -> tuple[str, str]:
    parts = full_name.strip().split()
    if len(parts) == 0:
        return ("", "")
    if len(parts) == 1:
        return (parts[0], "")
    return (parts[0], " ".join(parts[1:]))


def extract_company_linkedin(company: dict) -> str:
    """Try multiple field names for the company LinkedIn URL."""
    for key in ("linkedin_url", "company_linkedin_url", "linkedin_company_url",
                "linkedin", "url"):
        val = company.get(key)
        if val and "linkedin.com" in str(val):
            return val
    return ""


def extract_company_fields(company: dict) -> dict:
    """Pull standard fields from a company search result."""
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

    # Sometimes HQ is nested under a "headquarters" or "hq" dict
    hq = company.get("headquarters") or company.get("hq") or {}
    if isinstance(hq, dict):
        hq_city = hq_city or hq.get("city", "")
        hq_country = hq_country or hq.get("country_name", "") or hq.get("country", "")

    # Sometimes location is a single string like "Austin, TX, United States"
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
        "Company_Linkedin_url": extract_company_linkedin(company),
    }


# ---------------------------------------------------------------------------
# Core pipeline functions
# ---------------------------------------------------------------------------

def search_vertical(client: BlitzAPIClient, vertical: dict,
                    checkpoint: CheckpointManager) -> list[dict]:
    """Paginate company search for one vertical, post-filter by city."""
    slug = vertical["slug"]
    cache_file = OUTPUT_DIR / f"ps_{slug}_companies.json"

    # Resume from cache if search already done
    if checkpoint.is_search_done(slug) and cache_file.exists():
        cached = json.loads(cache_file.read_text())
        log.info(f"[{vertical['name']}] Loaded {len(cached)} cached companies from {cache_file.name}")
        return cached

    all_companies = []
    cursor = None
    page = 0

    while page < MAX_PAGES_PER_VERTICAL:
        page += 1
        results, next_cursor = client.search_companies(vertical["search_payload"], cursor)
        checkpoint.inc("api_calls")

        if not results:
            log.info(f"[{vertical['name']}] Page {page}: no results — done")
            break

        all_companies.extend(results)
        log.info(f"[{vertical['name']}] Page {page}: {len(results)} companies "
                 f"(total raw: {len(all_companies)})")

        if not next_cursor:
            break
        cursor = next_cursor

    # Post-filter by target cities
    filtered = []
    for co in all_companies:
        fields = extract_company_fields(co)
        hq = fields["hq_city"]
        if matches_target_city(hq):
            # Merge raw company dict with our extracted fields for later use
            co["_extracted"] = fields
            filtered.append(co)
        else:
            log.debug(f"[{vertical['name']}] Filtered out: {fields['company_name']} "
                      f"(HQ: {hq})")

    log.info(f"[{vertical['name']}] {len(filtered)} companies in target cities "
             f"out of {len(all_companies)} total")

    # Cache filtered results
    cache_file.write_text(json.dumps(filtered, indent=2, default=str))
    checkpoint.mark_search_done(slug)
    checkpoint.inc("companies_searched", len(filtered))

    return filtered


def process_company(client: BlitzAPIClient, company: dict,
                    checkpoint: CheckpointManager) -> dict | None:
    """Run Waterfall ICP + email enrichment for a single company."""
    fields = company.get("_extracted") or extract_company_fields(company)
    linkedin_url = fields.get("Company_Linkedin_url", "")

    if not linkedin_url:
        log.debug(f"No LinkedIn URL for {fields['company_name']} — skipping ICP")
        return {**fields, "contact_first_name": "", "contact_last_name": "",
                "contact_title": "", "contact_linkedin_url": "", "contact_email": ""}

    # Waterfall ICP — find top decision-maker
    people = client.waterfall_icp(linkedin_url, CASCADE, max_results=1)
    checkpoint.inc("api_calls")

    if not people:
        log.debug(f"No ICP results for {fields['company_name']}")
        return {**fields, "contact_first_name": "", "contact_last_name": "",
                "contact_title": "", "contact_linkedin_url": "", "contact_email": ""}

    person = people[0]
    full_name = (person.get("full_name") or "").strip()
    first, last = split_name(full_name)
    first = person.get("first_name") or first
    last = person.get("last_name") or last
    title = person.get("job_title") or person.get("title") or person.get("linkedin_headline") or ""
    person_linkedin = (person.get("person_linkedin_url")
                       or person.get("linkedin_profile_url")
                       or person.get("linkedin_url") or "")

    checkpoint.inc("contacts_found")

    # Email enrichment
    email = ""
    if person_linkedin:
        email = client.enrich_email(person_linkedin) or ""
        checkpoint.inc("api_calls")
        if email:
            checkpoint.inc("emails_found")
            log.info(f"  ✓ {first} {last} — {title} — {email}")
        else:
            log.info(f"  ✓ {first} {last} — {title} — no email")
    else:
        log.info(f"  ✓ {first} {last} — {title} — no LinkedIn URL for email lookup")

    return {
        **fields,
        "contact_first_name": first,
        "contact_last_name": last,
        "contact_title": title,
        "contact_linkedin_url": person_linkedin,
        "contact_email": email,
    }


def process_vertical(client: BlitzAPIClient, vertical: dict,
                     checkpoint: CheckpointManager):
    """Full pipeline for one vertical: search → filter → ICP → email → CSV."""
    global _shutdown_requested
    slug = vertical["slug"]
    output_path = OUTPUT_DIR / vertical["output_file"]

    log.info(f"\n{'='*60}")
    log.info(f"  VERTICAL: {vertical['name']}")
    log.info(f"  Output: {vertical['output_file']}")
    log.info(f"{'='*60}")

    # Step 1: Company search + city filter
    companies = search_vertical(client, vertical, checkpoint)
    if not companies:
        log.info(f"[{vertical['name']}] No companies found — skipping")
        return

    # Step 2: Process each company (ICP + email)
    file_exists = output_path.exists() and output_path.stat().st_size > 0
    csv_file = open(output_path, "a", newline="", encoding="utf-8")
    writer = csv.DictWriter(csv_file, fieldnames=OUTPUT_COLUMNS, extrasaction="ignore")
    if not file_exists:
        writer.writeheader()

    processed_count = 0
    skipped_count = 0

    for i, company in enumerate(companies):
        if _shutdown_requested:
            log.warning("Shutdown — saving checkpoint")
            break

        fields = company.get("_extracted") or extract_company_fields(company)
        key = fields.get("Company_Linkedin_url") or fields.get("website") or fields.get("company_name")
        if not key:
            continue

        if checkpoint.is_processed(slug, key):
            skipped_count += 1
            continue

        log.info(f"[{vertical['name']}] ({i+1}/{len(companies)}) {fields['company_name']}")
        row = process_company(client, company, checkpoint)

        if row:
            writer.writerow(row)
            csv_file.flush()

        checkpoint.mark_processed(slug, key)
        processed_count += 1

        if processed_count % 10 == 0:
            checkpoint.save()

    csv_file.close()
    checkpoint.save()

    log.info(f"\n[{vertical['name']}] Done — {processed_count} processed, "
             f"{skipped_count} skipped (already done)")
    log.info(f"[{vertical['name']}] Output: {output_path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Professional Services Vertical Pipeline")
    parser.add_argument("--vertical", choices=list(VERTICAL_SLUGS),
                        help="Run a single vertical instead of all 5")
    parser.add_argument("--dry-run", action="store_true",
                        help="Log API calls without executing them")
    args = parser.parse_args()

    api_key = os.getenv("BLITZ_API_KEY")
    if not api_key:
        log.error("BLITZ_API_KEY not found in .env — aborting")
        sys.exit(1)

    client = BlitzAPIClient(api_key, dry_run=args.dry_run)
    checkpoint = CheckpointManager(CHECKPOINT_FILE)

    if args.vertical:
        targets = [v for v in VERTICALS if v["slug"] == args.vertical]
    else:
        targets = VERTICALS

    log.info(f"Starting pipeline — {len(targets)} vertical(s)")
    if args.dry_run:
        log.info("[DRY-RUN MODE] No API calls will be made")

    for vertical in targets:
        if _shutdown_requested:
            break
        process_vertical(client, vertical, checkpoint)

    # Summary
    stats = checkpoint.data.get("stats", {})
    log.info(f"\n{'='*60}")
    log.info(f"  PIPELINE COMPLETE")
    log.info(f"  API calls:       {stats.get('api_calls', 0)}")
    log.info(f"  Companies:       {stats.get('companies_searched', 0)}")
    log.info(f"  Contacts found:  {stats.get('contacts_found', 0)}")
    log.info(f"  Emails found:    {stats.get('emails_found', 0)}")
    log.info(f"{'='*60}")


if __name__ == "__main__":
    main()
