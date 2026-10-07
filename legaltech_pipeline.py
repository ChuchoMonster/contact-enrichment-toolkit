#!/usr/bin/env python3
"""
Legal Tech Pipeline — Discover legal tech companies in the US & Canada,
find Founders/CEOs and CMOs via Waterfall ICP, enrich verified emails.

Usage:
    python legaltech_pipeline.py --dry-run          # log calls, no API hits
    python legaltech_pipeline.py --step 1           # company discovery only
    python legaltech_pipeline.py --step 2           # waterfall ICP only
    python legaltech_pipeline.py --step 3           # email enrichment only
    python legaltech_pipeline.py --all              # all 3 steps sequentially
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
MAX_PAGES_PER_SEARCH = 40  # 40 pages x 25 = 1000 companies max per search

OUTPUT_DIR = Path(__file__).resolve().parent
COMPANIES_FILE = OUTPUT_DIR / "lt_companies.csv"
COMPANIES_CACHE = OUTPUT_DIR / "lt_companies_raw.json"
CONTACTS_FILE = OUTPUT_DIR / "lt_contacts_results.csv"
CHECKPOINT_FILE = OUTPUT_DIR / "lt_checkpoint.json"
LOG_FILE = OUTPUT_DIR / "lt_pipeline.log"

COMPANIES_COLUMNS = [
    "company_name", "website", "industry", "employee_count",
    "hq_city", "hq_country", "company_linkedin_url",
]

CONTACTS_COLUMNS = [
    "company_name", "website", "industry", "employee_count",
    "hq_city", "hq_country", "company_linkedin_url",
    "full_name", "first_name", "last_name", "job_title",
    "person_linkedin_url", "verified_email", "cascade_type",
]

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
log = logging.getLogger("lt_pipeline")
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
    log.warning("Shutdown requested -- finishing current item then saving...")


signal.signal(signal.SIGINT, _signal_handler)
signal.signal(signal.SIGTERM, _signal_handler)

# ---------------------------------------------------------------------------
# Search definitions — cast a wide net across legal tech
# ---------------------------------------------------------------------------
# We run multiple searches with different industry/keyword combos to maximize
# coverage. Results are deduplicated by company LinkedIn URL or domain.

SEARCHES = [
    {
        "name": "Legal Tech - Core",
        "payload": {
            "company": {
                "industry": {"include": ["Legal Services", "Law Practice"]},
                "hq": {"country_code": ["US", "CA"]},
                "keywords": {
                    "include": ["legal tech", "legaltech", "legal software",
                                "legal AI", "legal automation"],
                },
            },
            "max_results": 25,
        },
    },
    {
        "name": "Legal Tech - Software",
        "payload": {
            "company": {
                "industry": {"include": [
                    "Software Development",
                    "Technology; Information and Internet",
                    "IT System Custom Software Development",
                    "Information Technology and Services",
                ]},
                "hq": {"country_code": ["US", "CA"]},
                "keywords": {
                    "include": ["legal", "law firm", "attorney", "lawyer",
                                "litigation", "contract management"],
                },
            },
            "max_results": 25,
        },
    },
    {
        "name": "Legal Tech - eDiscovery & Compliance",
        "payload": {
            "company": {
                "industry": {"include": [
                    "Software Development",
                    "Technology; Information and Internet",
                    "Information Technology and Services",
                    "Legal Services",
                    "Data Security Software Products",
                ]},
                "hq": {"country_code": ["US", "CA"]},
                "keywords": {
                    "include": ["ediscovery", "e-discovery", "compliance software",
                                "regulatory technology", "regtech",
                                "legal analytics", "legal research"],
                },
            },
            "max_results": 25,
        },
    },
    {
        "name": "Legal Tech - Practice Management",
        "payload": {
            "company": {
                "industry": {"include": [
                    "Software Development",
                    "Technology; Information and Internet",
                    "Information Technology and Services",
                    "Legal Services",
                ]},
                "hq": {"country_code": ["US", "CA"]},
                "keywords": {
                    "include": ["practice management", "case management",
                                "legal billing", "legal document",
                                "court filing", "docket"],
                },
            },
            "max_results": 25,
        },
    },
    {
        "name": "Legal Tech - Contract & IP",
        "payload": {
            "company": {
                "industry": {"include": [
                    "Software Development",
                    "Technology; Information and Internet",
                    "Information Technology and Services",
                    "Legal Services",
                ]},
                "hq": {"country_code": ["US", "CA"]},
                "keywords": {
                    "include": ["contract lifecycle", "CLM",
                                "intellectual property software",
                                "patent management", "trademark software",
                                "legal workflow"],
                },
            },
            "max_results": 25,
        },
    },
]

# ---------------------------------------------------------------------------
# Waterfall ICP cascades — Founder/CEO + CMO
# ---------------------------------------------------------------------------
EXCLUDE_TITLES = [
    "analyst", "associate", "coordinator", "assistant",
    "intern", "junior", "student", "part-time", "part time",
]

CASCADE_FOUNDER = [
    {
        "include_title": [
            "CEO", "Chief Executive Officer",
            "Founder", "Co-Founder", "Co-founder", "Cofounder",
            "Owner", "Managing Director", "President",
            "General Manager", "Principal",
        ],
        "exclude_title": [
            "Vice President", "VP", "SVP", "EVP",
            "Assistant", "Associate",
        ] + EXCLUDE_TITLES,
        "location": ["WORLD"],
        "include_headline_search": False,
    },
]

CASCADE_CMO = [
    # Level 1: C-suite marketing
    {
        "include_title": [
            "CMO", "Chief Marketing Officer",
            "Chief Growth Officer", "Chief Brand Officer",
            "Chief Digital Officer", "Chief Communications Officer",
            "VP Marketing", "VP Growth", "VP Digital",
            "SVP Marketing", "EVP Marketing",
            "Head of Marketing", "Head of Growth", "Head of Brand",
            "Head of Digital", "Head of Content",
            "Head of Communications", "Head of Demand Gen",
        ],
        "exclude_title": EXCLUDE_TITLES,
        "location": ["WORLD"],
        "include_headline_search": False,
    },
    # Level 2: Director level
    {
        "include_title": [
            "Director of Marketing", "Director of Growth",
            "Director of Digital Marketing", "Director of Content",
            "Director of Communications", "Director of Brand",
            "Director of Demand Gen", "Director of Demand Generation",
            "Marketing Director", "Growth Director",
        ],
        "exclude_title": EXCLUDE_TITLES,
        "location": ["WORLD"],
        "include_headline_search": False,
    },
]


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
                log.warning("Corrupt checkpoint -- starting fresh")
        return {
            "step1_searches_done": [],
            "step1_companies": {},       # linkedin_url_or_domain -> company fields
            "step2_processed": [],       # company keys already through ICP
            "step3_processed": [],       # person linkedin URLs already enriched
            "stats": {
                "step1_api_calls": 0, "step1_companies_found": 0,
                "step2_api_calls": 0, "step2_people_found": 0,
                "step3_api_calls": 0, "step3_emails_found": 0,
            },
        }

    def save(self):
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self.data, indent=2))
        tmp.replace(self.path)


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
            log.info(f"[DRY-RUN] POST {url}")
            return None
        for attempt in range(3):
            try:
                time.sleep(REQUEST_DELAY)
                resp = self.session.post(url, json=payload, timeout=30)
                if resp.status_code == 200:
                    return resp.json()
                if resp.status_code == 429:
                    wait = int(resp.headers.get("Retry-After", 30))
                    log.warning(f"Rate limited -- sleeping {wait}s")
                    time.sleep(wait)
                    continue
                if resp.status_code >= 500:
                    wait = 2 ** (attempt + 1)
                    log.warning(f"Server error {resp.status_code} -- retry in {wait}s")
                    time.sleep(wait)
                    continue
                log.warning(f"API {endpoint} returned {resp.status_code}: {resp.text[:200]}")
                return None
            except requests.RequestException as e:
                wait = 2 ** (attempt + 1)
                log.warning(f"Network error: {e} -- retry in {wait}s")
                time.sleep(wait)
        log.error(f"Failed after 3 retries: {endpoint}")
        return None

    def _post_v2(self, endpoint: str, payload: dict) -> dict | None:
        url = f"{API_BASE_V2}{endpoint}"
        if self.dry_run:
            log.info(f"[DRY-RUN] POST {url}")
            return None
        for attempt in range(3):
            try:
                time.sleep(REQUEST_DELAY)
                resp = self.session.post(url, json=payload, timeout=30)
                if resp.status_code == 200:
                    return resp.json()
                if resp.status_code == 429:
                    wait = int(resp.headers.get("Retry-After", 30))
                    log.warning(f"Rate limited -- sleeping {wait}s")
                    time.sleep(wait)
                    continue
                if resp.status_code >= 500:
                    wait = 2 ** (attempt + 1)
                    log.warning(f"Server error {resp.status_code} -- retry in {wait}s")
                    time.sleep(wait)
                    continue
                log.warning(f"API {endpoint} returned {resp.status_code}: {resp.text[:2000]}")
                return None
            except requests.RequestException as e:
                wait = 2 ** (attempt + 1)
                log.warning(f"Network error: {e} -- retry in {wait}s")
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
            log.debug(f"Company search raw keys: {list(data.keys()) if isinstance(data, dict) else 'list'}")
            if isinstance(data, dict):
                log.debug(f"Response sample: {json.dumps(data, default=str)[:500]}")

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
                      max_results: int = 2) -> list[dict]:
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
        "company_linkedin_url": extract_company_linkedin(company),
    }


def company_dedup_key(fields: dict) -> str:
    """Return a dedup key: prefer LinkedIn URL, fall back to domain."""
    li = fields.get("company_linkedin_url", "")
    if li:
        # Normalize: strip trailing slash, lowercase
        return li.rstrip("/").lower()
    website = fields.get("website", "")
    if website:
        return website.lower().replace("www.", "").rstrip("/")
    return fields.get("company_name", "").lower()


# ---------------------------------------------------------------------------
# Step 1: Company Discovery
# ---------------------------------------------------------------------------

def run_step1(client: BlitzAPIClient, checkpoint: CheckpointManager):
    global _shutdown_requested
    log.info("=" * 60)
    log.info("  STEP 1: Legal Tech Company Discovery")
    log.info("=" * 60)

    all_raw = []

    for search in SEARCHES:
        if _shutdown_requested:
            break
        name = search["name"]
        if name in checkpoint.data["step1_searches_done"]:
            log.info(f"[{name}] Already completed -- skipping")
            continue

        log.info(f"[{name}] Starting search...")
        cursor = None
        page = 0
        search_results = []

        while page < MAX_PAGES_PER_SEARCH:
            if _shutdown_requested:
                break
            page += 1
            results, next_cursor = client.search_companies(search["payload"], cursor)
            checkpoint.data["stats"]["step1_api_calls"] += 1

            if not results:
                log.info(f"[{name}] Page {page}: no results -- done")
                break

            search_results.extend(results)
            log.info(f"[{name}] Page {page}: {len(results)} companies "
                     f"(total: {len(search_results)})")

            if not next_cursor:
                break
            cursor = next_cursor

        all_raw.extend(search_results)
        checkpoint.data["step1_searches_done"].append(name)
        checkpoint.save()
        log.info(f"[{name}] Complete: {len(search_results)} companies")

    # Deduplicate across all searches
    seen_keys = set(checkpoint.data["step1_companies"].keys())
    new_count = 0
    for raw in all_raw:
        fields = extract_company_fields(raw)
        key = company_dedup_key(fields)
        if not key or key in seen_keys:
            continue
        seen_keys.add(key)
        checkpoint.data["step1_companies"][key] = fields
        new_count += 1

    checkpoint.data["stats"]["step1_companies_found"] = len(checkpoint.data["step1_companies"])
    checkpoint.save()

    # Write companies CSV
    companies = list(checkpoint.data["step1_companies"].values())
    with open(COMPANIES_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=COMPANIES_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        for co in companies:
            writer.writerow(co)

    # Also save raw JSON for debugging
    COMPANIES_CACHE.write_text(json.dumps(all_raw, indent=2, default=str))

    log.info("=" * 60)
    log.info("STEP 1 COMPLETE")
    log.info(f"  API calls:        {checkpoint.data['stats']['step1_api_calls']}")
    log.info(f"  Unique companies: {len(companies)}")
    log.info(f"  Companies CSV:    {COMPANIES_FILE}")
    log.info("=" * 60)


# ---------------------------------------------------------------------------
# Step 2: Waterfall ICP — Founder/CEO + CMO
# ---------------------------------------------------------------------------

def run_step2(client: BlitzAPIClient, checkpoint: CheckpointManager):
    global _shutdown_requested
    log.info("=" * 60)
    log.info("  STEP 2: Waterfall ICP -- Founders/CEOs + CMOs")
    log.info("=" * 60)

    companies = checkpoint.data.get("step1_companies", {})
    if not companies:
        log.error("No companies found -- run step 1 first")
        return

    # Filter to companies with LinkedIn URLs that haven't been processed
    to_process = []
    for key, fields in companies.items():
        if key in checkpoint.data["step2_processed"]:
            continue
        if not fields.get("company_linkedin_url"):
            continue
        to_process.append((key, fields))

    log.info(f"  {len(to_process)} companies to process "
             f"({len(checkpoint.data['step2_processed'])} already done)")

    file_exists = CONTACTS_FILE.exists() and CONTACTS_FILE.stat().st_size > 0
    csv_file = open(CONTACTS_FILE, "a", newline="", encoding="utf-8")
    writer = csv.DictWriter(csv_file, fieldnames=CONTACTS_COLUMNS, extrasaction="ignore")
    if not file_exists:
        writer.writeheader()

    try:
        for i, (key, fields) in enumerate(to_process):
            if _shutdown_requested:
                break

            company_name = fields["company_name"]
            li_url = fields["company_linkedin_url"]
            log.info(f"[{i+1}/{len(to_process)}] {company_name}")

            # Founder/CEO cascade
            founder_people = client.waterfall_icp(li_url, CASCADE_FOUNDER, max_results=2)
            checkpoint.data["stats"]["step2_api_calls"] += 1

            # CMO cascade
            cmo_people = client.waterfall_icp(li_url, CASCADE_CMO, max_results=2)
            checkpoint.data["stats"]["step2_api_calls"] += 1

            seen_urls = set()
            results = []

            for cascade_type, people in [("founder", founder_people), ("cmo", cmo_people)]:
                for person in people:
                    person_li = (person.get("person_linkedin_url")
                                 or person.get("linkedin_profile_url")
                                 or person.get("linkedin_url") or "")
                    if not person_li or person_li in seen_urls:
                        continue
                    seen_urls.add(person_li)

                    full_name = (person.get("full_name") or "").strip()
                    first, last = split_name(full_name)
                    first = person.get("first_name") or first
                    last = person.get("last_name") or last
                    title = (person.get("job_title") or person.get("title")
                             or person.get("linkedin_headline") or "")

                    results.append({
                        **fields,
                        "full_name": full_name,
                        "first_name": first,
                        "last_name": last,
                        "job_title": title,
                        "person_linkedin_url": person_li,
                        "verified_email": "",
                        "cascade_type": cascade_type,
                    })

            checkpoint.data["stats"]["step2_people_found"] += len(results)
            checkpoint.data["step2_processed"].append(key)

            for r in results:
                writer.writerow(r)
            csv_file.flush()

            if results:
                names = ", ".join(f"{r['full_name']} ({r['job_title'][:30]})"
                                  for r in results[:4])
                extra = f" +{len(results)-4} more" if len(results) > 4 else ""
                log.info(f"  Found {len(results)}: {names}{extra}")
            else:
                log.info(f"  No people found")

            if (i + 1) % 25 == 0:
                checkpoint.save()
                log.info(f"  -- Checkpoint saved at {i+1}/{len(to_process)} --")

    finally:
        csv_file.close()
        checkpoint.save()

    log.info("=" * 60)
    log.info("STEP 2 COMPLETE")
    log.info(f"  Companies searched: {len(checkpoint.data['step2_processed'])}")
    log.info(f"  People found:       {checkpoint.data['stats']['step2_people_found']}")
    log.info(f"  API calls:          {checkpoint.data['stats']['step2_api_calls']}")
    log.info(f"  Contacts file:      {CONTACTS_FILE}")
    log.info("=" * 60)


# ---------------------------------------------------------------------------
# Step 3: Email Enrichment
# ---------------------------------------------------------------------------

def run_step3(client: BlitzAPIClient, checkpoint: CheckpointManager):
    global _shutdown_requested
    log.info("=" * 60)
    log.info("  STEP 3: Email Enrichment")
    log.info("=" * 60)

    if not CONTACTS_FILE.exists():
        log.error("No contacts file found -- run step 2 first")
        return

    rows = []
    with open(CONTACTS_FILE, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            rows.append(row)

    need_email = [r for r in rows
                  if r.get("person_linkedin_url")
                  and not r.get("verified_email")
                  and r["person_linkedin_url"] not in checkpoint.data["step3_processed"]]

    log.info(f"  {len(need_email)} people need email enrichment (of {len(rows)} total)")

    try:
        for i, row in enumerate(need_email):
            if _shutdown_requested:
                break

            person_li = row["person_linkedin_url"]
            email = client.enrich_email(person_li) or ""
            checkpoint.data["stats"]["step3_api_calls"] += 1
            checkpoint.data["step3_processed"].append(person_li)

            if email:
                row["verified_email"] = email
                checkpoint.data["stats"]["step3_emails_found"] += 1
                log.info(f"[{i+1}/{len(need_email)}] {row.get('full_name', '')} -> {email}")
            elif (i + 1) % 25 == 0:
                log.info(f"[{i+1}/{len(need_email)}] Progress -- "
                         f"{checkpoint.data['stats']['step3_emails_found']} emails found")

            if (i + 1) % 50 == 0:
                checkpoint.save()
                _rewrite_contacts(rows)

    finally:
        checkpoint.save()
        _rewrite_contacts(rows)

    # Write a filtered file with only rows that have emails
    verified_file = OUTPUT_DIR / "lt_contacts_verified_emails.csv"
    verified_rows = [r for r in rows if r.get("verified_email")]
    with open(verified_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CONTACTS_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        for r in verified_rows:
            writer.writerow(r)

    log.info("=" * 60)
    log.info("STEP 3 COMPLETE")
    log.info(f"  People processed:    {len(checkpoint.data['step3_processed'])}")
    log.info(f"  Emails found:        {checkpoint.data['stats']['step3_emails_found']}")
    log.info(f"  API calls:           {checkpoint.data['stats']['step3_api_calls']}")
    log.info(f"  Full contacts:       {CONTACTS_FILE}")
    log.info(f"  Verified emails:     {verified_file}")
    log.info("=" * 60)


def _rewrite_contacts(rows: list[dict]):
    with open(CONTACTS_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CONTACTS_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        for r in rows:
            writer.writerow(r)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Legal Tech Pipeline")
    parser.add_argument("--step", type=int, choices=[1, 2, 3],
                        help="Run a specific step (1=discovery, 2=ICP, 3=email)")
    parser.add_argument("--all", action="store_true",
                        help="Run all 3 steps sequentially")
    parser.add_argument("--dry-run", action="store_true",
                        help="Log API calls without executing them")
    args = parser.parse_args()

    api_key = os.getenv("BLITZ_API_KEY")
    if not api_key:
        log.error("BLITZ_API_KEY not found in .env -- aborting")
        sys.exit(1)

    client = BlitzAPIClient(api_key, dry_run=args.dry_run)
    checkpoint = CheckpointManager(CHECKPOINT_FILE)

    if args.dry_run:
        log.info("[DRY-RUN MODE] No API calls will be made")

    if args.all:
        run_step1(client, checkpoint)
        if not _shutdown_requested:
            run_step2(client, checkpoint)
        if not _shutdown_requested:
            run_step3(client, checkpoint)
    elif args.step == 1:
        run_step1(client, checkpoint)
    elif args.step == 2:
        run_step2(client, checkpoint)
    elif args.step == 3:
        run_step3(client, checkpoint)
    else:
        parser.print_help()

    # Final summary
    stats = checkpoint.data.get("stats", {})
    total_calls = (stats.get("step1_api_calls", 0)
                   + stats.get("step2_api_calls", 0)
                   + stats.get("step3_api_calls", 0))
    log.info(f"\nTotal API calls this session: {total_calls}")
    log.info(f"Companies discovered: {stats.get('step1_companies_found', 0)}")
    log.info(f"People found:         {stats.get('step2_people_found', 0)}")
    log.info(f"Emails found:         {stats.get('step3_emails_found', 0)}")


if __name__ == "__main__":
    main()
