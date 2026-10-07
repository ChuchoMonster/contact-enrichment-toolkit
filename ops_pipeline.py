#!/usr/bin/env python3
"""
Operations Leaders Pipeline — Search for senior operations people (Head/VP/Director
of Operations) at U.S.-based companies (50-500 employees) across multiple sectors.

Sectors: B2B Retail, B2B E-commerce, Consulting, Automotive, E-learning, Events,
Services, Facility Services, Human Resources, Information Services, Logistics/Supply Chain

Usage:
    python ops_pipeline.py                      # all sectors
    python ops_pipeline.py --sector consulting  # single sector
    python ops_pipeline.py --dry-run            # log calls, no API hits
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
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
MAX_PAGES_PER_SECTOR = 20  # safety cap: 20 pages × 25 = 500 companies max

OUTPUT_DIR = Path(__file__).resolve().parent
CHECKPOINT_FILE = OUTPUT_DIR / "ops_checkpoint.json"
LOG_FILE = OUTPUT_DIR / "ops_pipeline.log"

FINAL_OUTPUT = OUTPUT_DIR / "ops_verified_contacts.csv"

OUTPUT_COLUMNS = [
    "sector", "company_name", "website", "industry", "employee_count",
    "hq_city", "hq_country", "contact_first_name", "contact_last_name",
    "contact_title", "contact_linkedin_url", "contact_email",
    "Company_Linkedin_url",
]

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
log = logging.getLogger("ops_pipeline")
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

# ---------------------------------------------------------------------------
# Waterfall ICP cascade — Operations leaders
# Priority: COO → VP Ops → Head of Ops → Director Ops → Sr. Manager Ops
# ---------------------------------------------------------------------------
CASCADE = [
    {
        "include_title": [
            "COO", "Chief Operating Officer", "Chief Operations Officer",
        ],
        "exclude_title": ["assistant", "associate", "intern", "junior", "analyst"],
        "location": ["WORLD"],
        "include_headline_search": False,
    },
    {
        "include_title": [
            "VP Operations", "VP of Operations",
            "Vice President Operations", "Vice President of Operations",
            "SVP Operations", "SVP of Operations",
        ],
        "exclude_title": ["assistant", "associate", "intern", "junior", "analyst"],
        "location": ["WORLD"],
        "include_headline_search": False,
    },
    {
        "include_title": [
            "Head of Operations", "Head of Ops",
        ],
        "exclude_title": ["assistant", "associate", "intern", "junior", "analyst"],
        "location": ["WORLD"],
        "include_headline_search": False,
    },
    {
        "include_title": [
            "Director of Operations", "Director Operations",
            "Operations Director",
        ],
        "exclude_title": ["assistant", "associate", "intern", "junior", "analyst"],
        "location": ["WORLD"],
        "include_headline_search": False,
    },
    {
        "include_title": [
            "Senior Manager Operations", "Sr. Manager Operations",
            "Senior Operations Manager",
        ],
        "exclude_title": ["assistant", "associate", "intern", "junior", "analyst"],
        "location": ["WORLD"],
        "include_headline_search": False,
    },
]

# ---------------------------------------------------------------------------
# Sectors
# ---------------------------------------------------------------------------
SECTORS = [
    {
        "name": "B2B Retail",
        "slug": "b2b_retail",
        "output_file": "ops_b2b_retail.csv",
        "search_payload": {
            "company": {
                "industry": {"include": ["Retail", "Wholesale"]},
                "employee_range": ["51-200", "201-500"],
                "hq": {"country_code": ["US"]},
                "keywords": {
                    "include": ["B2B", "wholesale", "distribution", "trade"],
                    "exclude": ["consumer", "DTC", "direct to consumer"],
                },
            },
            "max_results": 25,
        },
    },
    {
        "name": "B2B E-commerce",
        "slug": "b2b_ecommerce",
        "output_file": "ops_b2b_ecommerce.csv",
        "search_payload": {
            "company": {
                "industry": {"include": ["E-commerce", "Internet", "Technology, Information and Internet",
                                          "Wholesale", "Retail"]},
                "employee_range": ["51-200", "201-500"],
                "hq": {"country_code": ["US"]},
                "keywords": {
                    "include": ["B2B", "ecommerce", "e-commerce", "wholesale", "marketplace"],
                },
            },
            "max_results": 25,
        },
    },
    {
        "name": "Consulting",
        "slug": "consulting",
        "output_file": "ops_consulting.csv",
        "search_payload": {
            "company": {
                "industry": {"include": ["Management Consulting", "Business Consulting and Services"]},
                "employee_range": ["51-200", "201-500"],
                "hq": {"country_code": ["US"]},
            },
            "max_results": 25,
        },
    },
    {
        "name": "Automotive",
        "slug": "automotive",
        "output_file": "ops_automotive.csv",
        "search_payload": {
            "company": {
                "industry": {"include": ["Automotive", "Motor Vehicle Manufacturing"]},
                "employee_range": ["51-200", "201-500"],
                "hq": {"country_code": ["US"]},
            },
            "max_results": 25,
        },
    },
    {
        "name": "E-Learning",
        "slug": "elearning",
        "output_file": "ops_elearning.csv",
        "search_payload": {
            "company": {
                "industry": {"include": ["E-Learning Providers", "Education",
                                          "Education Management", "Professional Training and Coaching"]},
                "employee_range": ["51-200", "201-500"],
                "hq": {"country_code": ["US"]},
            },
            "max_results": 25,
        },
    },
    {
        "name": "Events",
        "slug": "events",
        "output_file": "ops_events.csv",
        "search_payload": {
            "company": {
                "industry": {"include": ["Events Services", "Entertainment Providers"]},
                "employee_range": ["51-200", "201-500"],
                "hq": {"country_code": ["US"]},
                "keywords": {
                    "include": ["events", "conferences", "trade shows", "event management",
                                "event planning", "exhibitions"],
                },
            },
            "max_results": 25,
        },
    },
    {
        "name": "Services",
        "slug": "services",
        "output_file": "ops_services.csv",
        "search_payload": {
            "company": {
                "industry": {"include": ["Business Consulting and Services",
                                          "Professional Services",
                                          "Outsourcing and Offshoring Consulting"]},
                "employee_range": ["51-200", "201-500"],
                "hq": {"country_code": ["US"]},
            },
            "max_results": 25,
        },
    },
    {
        "name": "Facility Services",
        "slug": "facility",
        "output_file": "ops_facility.csv",
        "search_payload": {
            "company": {
                "industry": {"include": ["Facilities Services", "Janitorial Services",
                                          "Building Construction", "Commercial and Industrial Machinery Maintenance"]},
                "employee_range": ["51-200", "201-500"],
                "hq": {"country_code": ["US"]},
            },
            "max_results": 25,
        },
    },
    {
        "name": "Human Resources",
        "slug": "hr",
        "output_file": "ops_hr.csv",
        "search_payload": {
            "company": {
                "industry": {"include": ["Human Resources", "Staffing and Recruiting"]},
                "employee_range": ["51-200", "201-500"],
                "hq": {"country_code": ["US"]},
            },
            "max_results": 25,
        },
    },
    {
        "name": "Information Services",
        "slug": "info_services",
        "output_file": "ops_info_services.csv",
        "search_payload": {
            "company": {
                "industry": {"include": ["Information Services", "IT Services and IT Consulting"]},
                "employee_range": ["51-200", "201-500"],
                "hq": {"country_code": ["US"]},
                "keywords": {
                    "include": ["data", "information", "analytics", "research",
                                "information services"],
                    "exclude": ["software development", "SaaS"],
                },
            },
            "max_results": 25,
        },
    },
    {
        "name": "Logistics & Supply Chain",
        "slug": "logistics",
        "output_file": "ops_logistics.csv",
        "search_payload": {
            "company": {
                "industry": {"include": ["Transportation; Logistics; Supply Chain and Storage",
                                          "Freight and Package Transportation",
                                          "Truck Transportation",
                                          "Logistics and Supply Chain",
                                          "Warehousing and Storage"]},
                "employee_range": ["51-200", "201-500"],
                "hq": {"country_code": ["US"]},
            },
            "max_results": 25,
        },
    },
]

SECTOR_SLUGS = {s["slug"] for s in SECTORS}

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
        return {"sectors": {}, "stats": {
            "api_calls": 0, "companies_searched": 0,
            "contacts_found": 0, "emails_found": 0,
        }}

    def save(self):
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self.data, indent=2))
        tmp.replace(self.path)

    def _sect(self, slug: str) -> dict:
        if slug not in self.data["sectors"]:
            self.data["sectors"][slug] = {
                "search_complete": False,
                "processed": [],
            }
        return self.data["sectors"][slug]

    def is_search_done(self, slug: str) -> bool:
        return self._sect(slug).get("search_complete", False)

    def mark_search_done(self, slug: str):
        self._sect(slug)["search_complete"] = True
        self.save()

    def is_processed(self, slug: str, key: str) -> bool:
        return key in self._sect(slug).get("processed", [])

    def mark_processed(self, slug: str, key: str):
        self._sect(slug).setdefault("processed", []).append(key)

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

    def search_companies(self, payload: dict, cursor: str | None = None) -> tuple[list[dict], str | None]:
        req = dict(payload)
        if cursor:
            req["cursor"] = cursor
        data = self._post_v2("/v2/search/companies", req)
        if not data:
            return ([], None)

        if not cursor:
            log.debug(f"Company search raw response keys: {list(data.keys()) if isinstance(data, dict) else 'list'}")
            if isinstance(data, dict):
                log.debug(f"Response sample (truncated): {json.dumps(data, default=str)[:500]}")

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
        "Company_Linkedin_url": extract_company_linkedin(company),
    }


# ---------------------------------------------------------------------------
# Core pipeline functions
# ---------------------------------------------------------------------------

def search_sector(client: BlitzAPIClient, sector: dict,
                  checkpoint: CheckpointManager) -> list[dict]:
    slug = sector["slug"]
    cache_file = OUTPUT_DIR / f"ops_{slug}_companies.json"

    if checkpoint.is_search_done(slug) and cache_file.exists():
        cached = json.loads(cache_file.read_text())
        log.info(f"[{sector['name']}] Loaded {len(cached)} cached companies from {cache_file.name}")
        return cached

    all_companies = []
    cursor = None
    page = 0

    while page < MAX_PAGES_PER_SECTOR:
        page += 1
        results, next_cursor = client.search_companies(sector["search_payload"], cursor)
        checkpoint.inc("api_calls")

        if not results:
            log.info(f"[{sector['name']}] Page {page}: no results — done")
            break

        for co in results:
            fields = extract_company_fields(co)
            co["_extracted"] = fields
            all_companies.append(co)

        log.info(f"[{sector['name']}] Page {page}: {len(results)} companies "
                 f"(total: {len(all_companies)})")

        if not next_cursor:
            break
        cursor = next_cursor

    log.info(f"[{sector['name']}] {len(all_companies)} companies found")

    cache_file.write_text(json.dumps(all_companies, indent=2, default=str))
    checkpoint.mark_search_done(slug)
    checkpoint.inc("companies_searched", len(all_companies))

    return all_companies


def process_company(client: BlitzAPIClient, company: dict,
                    checkpoint: CheckpointManager) -> dict | None:
    fields = company.get("_extracted") or extract_company_fields(company)
    linkedin_url = fields.get("Company_Linkedin_url", "")

    if not linkedin_url:
        log.debug(f"No LinkedIn URL for {fields['company_name']} — skipping ICP")
        return {**fields, "contact_first_name": "", "contact_last_name": "",
                "contact_title": "", "contact_linkedin_url": "", "contact_email": ""}

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


def process_sector(client: BlitzAPIClient, sector: dict,
                   checkpoint: CheckpointManager, writer, csv_file):
    global _shutdown_requested
    slug = sector["slug"]

    log.info(f"\n{'='*60}")
    log.info(f"  SECTOR: {sector['name']}")
    log.info(f"{'='*60}")

    companies = search_sector(client, sector, checkpoint)
    if not companies:
        log.info(f"[{sector['name']}] No companies found — skipping")
        return

    processed_count = 0
    skipped_count = 0
    email_count = 0

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

        log.info(f"[{sector['name']}] ({i+1}/{len(companies)}) {fields['company_name']}")
        row = process_company(client, company, checkpoint)

        if row and row.get("contact_email"):
            row["sector"] = sector["name"]
            writer.writerow(row)
            csv_file.flush()
            email_count += 1

        checkpoint.mark_processed(slug, key)
        processed_count += 1

        if processed_count % 10 == 0:
            checkpoint.save()

    checkpoint.save()

    log.info(f"\n[{sector['name']}] Done — {processed_count} processed, "
             f"{skipped_count} skipped, {email_count} verified emails")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Operations Leaders Pipeline")
    parser.add_argument("--sector", choices=list(SECTOR_SLUGS),
                        help="Run a single sector instead of all")
    parser.add_argument("--dry-run", action="store_true",
                        help="Log API calls without executing them")
    args = parser.parse_args()

    api_key = os.getenv("BLITZ_API_KEY")
    if not api_key:
        log.error("BLITZ_API_KEY not found in .env — aborting")
        sys.exit(1)

    client = BlitzAPIClient(api_key, dry_run=args.dry_run)
    checkpoint = CheckpointManager(CHECKPOINT_FILE)

    if args.sector:
        targets = [s for s in SECTORS if s["slug"] == args.sector]
    else:
        targets = SECTORS

    log.info(f"Starting pipeline — {len(targets)} sector(s)")
    if args.dry_run:
        log.info("[DRY-RUN MODE] No API calls will be made")

    file_exists = FINAL_OUTPUT.exists() and FINAL_OUTPUT.stat().st_size > 0
    csv_file = open(FINAL_OUTPUT, "a", newline="", encoding="utf-8")
    writer = csv.DictWriter(csv_file, fieldnames=OUTPUT_COLUMNS, extrasaction="ignore")
    if not file_exists:
        writer.writeheader()

    for sector in targets:
        if _shutdown_requested:
            break
        process_sector(client, sector, checkpoint, writer, csv_file)

    csv_file.close()

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
