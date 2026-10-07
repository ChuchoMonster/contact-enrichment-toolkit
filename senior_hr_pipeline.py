#!/usr/bin/env python3
"""
Senior HR Pipeline — 3-step contact finder for senior People/Talent/HR leaders
at US-based companies with 1,000+ employees across 7 industry buckets.

  Step 1: Company discovery via /v2/search/companies (cap 5,000 companies)
  Step 2: Waterfall ICP with tiered senior HR cascade (C-suite → VP → Director → Sr Mgr)
  Step 3: Email enrichment for each person found

The checkpoint tracks per-industry cursor + count so you can stop any time with
Ctrl+C and resume later from the exact same point.

Usage:
    python senior_hr_pipeline.py --step 1    # discovery
    python senior_hr_pipeline.py --step 2    # waterfall ICP
    python senior_hr_pipeline.py --step 3    # email enrichment
    python senior_hr_pipeline.py --all       # all 3 chained
    python senior_hr_pipeline.py --status    # print checkpoint progress
"""
from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import signal
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path

from dotenv import load_dotenv

from blitz_core import (
    BlitzAPIClient,
    extract_company_fields,
    extract_person_fields,
)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
OUTPUT_DIR = Path(__file__).parent
COMPANIES_FILE = OUTPUT_DIR / "senior_hr_companies.csv"
PEOPLE_FILE = OUTPUT_DIR / "senior_hr_people.csv"
RECENT_MOVERS_FILE = OUTPUT_DIR / "senior_hr_recent_movers.csv"
CONTACTS_FILE = OUTPUT_DIR / "senior_hr_contacts_verified_emails.csv"
CHECKPOINT_FILE = OUTPUT_DIR / "senior_hr_checkpoint.json"
LOG_FILE = OUTPUT_DIR / "senior_hr_pipeline.log"

COMPANY_CAP = 5000
COUNTRY_CODE = "US"
EMPLOYEE_RANGES = ["1001-5000", "5001-10000", "10001+"]

MAX_PEOPLE_PER_COMPANY = 1
WATERFALL_WORKERS = 3
ENRICH_WORKERS = 3

# 90-day "recent mover" filter window
RECENT_MOVE_DAYS = 90

# ---------------------------------------------------------------------------
# Industry plan — order matters, processed sequentially
# ---------------------------------------------------------------------------
INDUSTRY_PLAN = [
    {
        "name": "Manufacturing",
        "blitz": [
            "Manufacturing",
            "Machinery Manufacturing",
            "Industrial Machinery Manufacturing",
            "Chemical Manufacturing",
            "Chemicals",
            "Food and Beverage Manufacturing",
            "Food Production",
            "Pharmaceutical Manufacturing",
            "Electrical and Electronic Manufacturing",
            "Computers and Electronics Manufacturing",
            "Appliances; Electrical; and Electronics Manufacturing",
            "Motor Vehicle Manufacturing",
            "Transportation Equipment Manufacturing",
            "Plastics Manufacturing",
            "Plastics",
            "Packaging and Containers Manufacturing",
            "Packaging and Containers",
            "Industrial Automation",
            "Machinery",
            "Consumer Goods",
        ],
    },
    {
        "name": "Healthcare",
        "blitz": [
            "Hospital and Health Care",
            "Hospitals and Health Care",
            "Hospitals",
            "Medical Practice",
            "Medical Practices",
            "Medical Device",
            "Medical Equipment Manufacturing",
            "Mental Health Care",
            "Pharmaceuticals",
            "Home Health Care Services",
            "Nursing Homes and Residential Care Facilities",
            "Public Health",
        ],
    },
    {
        "name": "Hospitality",
        "blitz": [
            "Hospitality",
            "Restaurants",
            "Leisure; Travel and Tourism",
            "Hotels and Motels",
            "Food and Beverage Services",
            "Accommodation and Food Services",
        ],
    },
    {
        "name": "Education",
        "blitz": [
            "Education",
            "Education Management",
            "Higher Education",
            "Primary/Secondary Education",
            "Primary and Secondary Education",
            "Education Administration Programs",
            "E-learning",
            "E-Learning Providers",
        ],
    },
    {
        "name": "Property Management",
        "blitz": [
            "Real Estate",
            "Commercial Real Estate",
            "Facilities Services",
            "Leasing Non-residential Real Estate",
            "Leasing Residential Real Estate",
            "Real Estate and Equipment Rental Services",
        ],
    },
    {
        "name": "Nonprofit",
        "blitz": [
            "Non-profit Organization Management",
            "Non-profit Organizations",
            "Philanthropy",
            "Philanthropic Fundraising Services",
            "Civic and Social Organization",
            "Civic and Social Organizations",
            "Religious Institutions",
        ],
    },
    {
        "name": "Retail",
        "blitz": [
            "Retail",
            "Supermarkets",
            "Wholesale",
            "Food and Beverage Retail",
            "Online and Mail Order Retail",
        ],
    },
]

# ---------------------------------------------------------------------------
# Senior HR waterfall cascade (tiered, senior-first)
# ---------------------------------------------------------------------------
EXCLUDE_TITLES = [
    "analyst", "associate", "coordinator", "assistant",
    "intern", "junior", "student", "part-time", "part time",
    "specialist", "recruiter", "partner",
]

CASCADE_SENIOR_HR = [
    # L1: C-suite
    {
        "include_title": [
            "CHRO", "Chief Human Resources Officer", "Chief HR Officer",
            "Chief People Officer", "Chief Talent Officer",
            "Chief People and Culture Officer", "Chief Culture Officer",
        ],
        "exclude_title": EXCLUDE_TITLES,
        "location": ["WORLD"],
        "include_headline_search": False,
    },
    # L2: VP / SVP / EVP / Head of
    {
        "include_title": [
            "VP People", "VP Talent", "VP HR", "VP Human Resources",
            "VP People Operations", "VP People and Culture",
            "VP Talent Acquisition", "VP Talent Management",
            "SVP People", "SVP Talent", "SVP HR", "SVP Human Resources",
            "EVP People", "EVP Talent", "EVP HR", "EVP Human Resources",
            "Head of People", "Head of Talent", "Head of HR",
            "Head of Human Resources", "Head of People Operations",
            "Head of People and Culture", "Head of Talent Acquisition",
            "Head of Talent Management",
            "Vice President People", "Vice President Talent",
            "Vice President Human Resources", "Vice President HR",
        ],
        "exclude_title": EXCLUDE_TITLES,
        "location": ["WORLD"],
        "include_headline_search": False,
    },
    # L3: Director
    {
        "include_title": [
            "Director of People", "Director of Talent",
            "Director of HR", "Director of Human Resources",
            "Director of People Operations", "Director of People and Culture",
            "Director of Talent Acquisition", "Director of Talent Management",
            "People Director", "Talent Director", "HR Director",
            "Human Resources Director",
        ],
        "exclude_title": EXCLUDE_TITLES,
        "location": ["WORLD"],
        "include_headline_search": False,
    },
    # L4: Senior Manager
    {
        "include_title": [
            "Senior Manager People", "Senior Manager Talent",
            "Senior Manager HR", "Senior Manager Human Resources",
            "Senior People Manager", "Senior Talent Manager",
            "Senior HR Manager", "Sr Manager People", "Sr Manager Talent",
            "Sr Manager HR", "Sr Manager Human Resources",
            "Sr. Manager People", "Sr. Manager Talent",
            "Sr. Manager HR", "Sr. Manager Human Resources",
        ],
        "exclude_title": EXCLUDE_TITLES,
        "location": ["WORLD"],
        "include_headline_search": False,
    },
]

COMPANY_COLUMNS = [
    "industry_bucket", "company_name", "website", "industry",
    "employee_count", "hq_city", "hq_country", "company_linkedin_url",
]

PEOPLE_COLUMNS = [
    "industry_bucket", "company_name", "website", "industry",
    "employee_count", "company_linkedin_url",
    "full_name", "first_name", "last_name", "title",
    "person_linkedin_url", "cascade_level", "job_start_date",
]

CONTACT_COLUMNS = PEOPLE_COLUMNS + ["email"]

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Graceful shutdown
# ---------------------------------------------------------------------------
_shutdown_requested = False


def _handle_signal(signum, frame):
    global _shutdown_requested
    _shutdown_requested = True
    log.info("Shutdown requested — finishing current work then saving checkpoint…")


signal.signal(signal.SIGINT, _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)

# ---------------------------------------------------------------------------
# Checkpoint
# ---------------------------------------------------------------------------


class Checkpoint:
    def __init__(self, path: Path):
        self.path = path
        self.data = {
            "discover": {
                "industries": [
                    {"name": b["name"], "cursor": None, "count": 0, "done": False}
                    for b in INDUSTRY_PLAN
                ],
                "current_index": 0,
                "total_companies": 0,
            },
            "waterfall": {"processed_linkedin_urls": []},
            "enrich": {"processed_person_urls": []},
        }
        if path.exists():
            raw = json.loads(path.read_text())
            self.data.update(raw)

    def save(self):
        self.path.write_text(json.dumps(self.data, indent=2))

    def discover_state(self):
        return self.data["discover"]

    def mark_waterfall(self, linkedin_url: str):
        if linkedin_url not in self.data["waterfall"]["processed_linkedin_urls"]:
            self.data["waterfall"]["processed_linkedin_urls"].append(linkedin_url)

    def is_waterfall_done(self, linkedin_url: str) -> bool:
        return linkedin_url in self.data["waterfall"]["processed_linkedin_urls"]

    def mark_enrich(self, person_url: str):
        if person_url not in self.data["enrich"]["processed_person_urls"]:
            self.data["enrich"]["processed_person_urls"].append(person_url)

    def is_enrich_done(self, person_url: str) -> bool:
        return person_url in self.data["enrich"]["processed_person_urls"]


# ---------------------------------------------------------------------------
# CSV helpers
# ---------------------------------------------------------------------------


def _ensure_csv(path: Path, columns: list[str]):
    if not path.exists():
        with path.open("w", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=columns).writeheader()


def _append_rows(path: Path, columns: list[str], rows: list[dict]):
    if not rows:
        return
    _ensure_csv(path, columns)
    with path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
        for row in rows:
            writer.writerow(row)


def _read_rows(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8") as f:
        return list(csv.DictReader(f))


# ---------------------------------------------------------------------------
# Step 1: Discover
# ---------------------------------------------------------------------------


def step1_discover(client: BlitzAPIClient, ckpt: Checkpoint):
    state = ckpt.discover_state()
    _ensure_csv(COMPANIES_FILE, COMPANY_COLUMNS)

    # Build fast lookup to dedupe across industries
    existing_linkedin = {row.get("company_linkedin_url", "")
                         for row in _read_rows(COMPANIES_FILE)}

    while (state["total_companies"] < COMPANY_CAP
           and state["current_index"] < len(INDUSTRY_PLAN)
           and not _shutdown_requested):
        idx = state["current_index"]
        industry_entry = state["industries"][idx]
        plan_entry = INDUSTRY_PLAN[idx]

        if industry_entry["done"]:
            state["current_index"] += 1
            ckpt.save()
            continue

        log.info(f"[discover] Industry {idx + 1}/{len(INDUSTRY_PLAN)}: "
                 f"{industry_entry['name']} — so far {industry_entry['count']} "
                 f"companies, total {state['total_companies']}/{COMPANY_CAP}")

        payload = {
            "company": {
                "industry": {"include": plan_entry["blitz"]},
                "employee_range": EMPLOYEE_RANGES,
                "hq": {"country_code": [COUNTRY_CODE]},
            },
            "max_results": 50,
        }

        page_count = 0
        while (state["total_companies"] < COMPANY_CAP
               and not _shutdown_requested):
            results, next_cursor = client.search_companies(
                payload, cursor=industry_entry["cursor"])
            page_count += 1

            if not results:
                log.info(f"[discover] {industry_entry['name']}: no more results")
                industry_entry["done"] = True
                industry_entry["cursor"] = None
                ckpt.save()
                break

            rows = []
            for raw in results:
                fields = extract_company_fields(raw)
                linkedin = fields.get("company_linkedin_url", "")
                if not linkedin or linkedin in existing_linkedin:
                    continue
                existing_linkedin.add(linkedin)
                fields["industry_bucket"] = industry_entry["name"]
                rows.append(fields)

            _append_rows(COMPANIES_FILE, COMPANY_COLUMNS, rows)
            industry_entry["count"] += len(rows)
            state["total_companies"] += len(rows)
            industry_entry["cursor"] = next_cursor

            log.info(f"[discover] {industry_entry['name']} page {page_count}: "
                     f"+{len(rows)} unique (industry total {industry_entry['count']}, "
                     f"grand total {state['total_companies']}/{COMPANY_CAP})")

            ckpt.save()

            if not next_cursor:
                industry_entry["done"] = True
                ckpt.save()
                break

        if industry_entry["done"]:
            state["current_index"] += 1
            ckpt.save()

    # Summary
    done_label = "CAP REACHED" if state["total_companies"] >= COMPANY_CAP else (
        "ALL INDUSTRIES EXHAUSTED"
        if state["current_index"] >= len(INDUSTRY_PLAN) else "PAUSED")
    log.info(f"[discover] {done_label} — total {state['total_companies']} companies")
    for i, entry in enumerate(state["industries"]):
        flag = "✓" if entry["done"] else ("→" if i == state["current_index"] else " ")
        log.info(f"  {flag} {entry['name']}: {entry['count']} companies"
                 + ("" if entry["done"] or entry["cursor"] is None
                    else f" (cursor saved for resume)"))


# ---------------------------------------------------------------------------
# Step 2: Waterfall ICP
# ---------------------------------------------------------------------------


def _waterfall_for_company(client: BlitzAPIClient, company: dict) -> list[dict]:
    linkedin_url = company.get("company_linkedin_url", "")
    if not linkedin_url:
        return []
    for level, cascade_tier in enumerate(CASCADE_SENIOR_HR, start=1):
        people = client.waterfall_icp(
            linkedin_url, [cascade_tier], max_results=MAX_PEOPLE_PER_COMPANY)
        if people:
            rows = []
            for p in people:
                fields = extract_person_fields(p)
                if not fields.get("person_linkedin_url"):
                    continue
                rows.append({
                    "industry_bucket": company.get("industry_bucket", ""),
                    "company_name": company.get("company_name", ""),
                    "website": company.get("website", ""),
                    "industry": company.get("industry", ""),
                    "employee_count": company.get("employee_count", ""),
                    "company_linkedin_url": linkedin_url,
                    "cascade_level": f"L{level}",
                    "job_start_date": p.get("job_start_date", ""),
                    **fields,
                })
            if rows:
                return rows
    return []


def step2_waterfall(client: BlitzAPIClient, ckpt: Checkpoint):
    companies = _read_rows(COMPANIES_FILE)
    if not companies:
        log.warning("[waterfall] No companies found — run step 1 first.")
        return

    _ensure_csv(PEOPLE_FILE, PEOPLE_COLUMNS)
    pending = [c for c in companies
               if c.get("company_linkedin_url")
               and not ckpt.is_waterfall_done(c["company_linkedin_url"])]

    log.info(f"[waterfall] {len(pending)} of {len(companies)} companies to process")

    done = 0
    with ThreadPoolExecutor(max_workers=WATERFALL_WORKERS) as pool:
        futures = {pool.submit(_waterfall_for_company, client, c): c
                   for c in pending}
        for fut in as_completed(futures):
            if _shutdown_requested:
                break
            company = futures[fut]
            linkedin_url = company["company_linkedin_url"]
            try:
                rows = fut.result()
            except Exception as e:
                log.warning(f"[waterfall] {linkedin_url}: {e}")
                rows = []

            _append_rows(PEOPLE_FILE, PEOPLE_COLUMNS, rows)
            ckpt.mark_waterfall(linkedin_url)
            done += 1
            if done % 25 == 0:
                ckpt.save()
                log.info(f"[waterfall] {done}/{len(pending)} processed")
    ckpt.save()
    log.info(f"[waterfall] Complete — {done}/{len(pending)} processed, "
             f"{len(_read_rows(PEOPLE_FILE))} total people rows")


# ---------------------------------------------------------------------------
# Step 2.5: Filter to recent movers (started current role within RECENT_MOVE_DAYS)
# ---------------------------------------------------------------------------


def _parse_start_date(raw: str):
    if not raw:
        return None
    raw = raw.strip()
    # Common formats: "2024-08-01T00:00:00.000Z", "2024-08-01", "2024-08"
    try:
        if "T" in raw:
            return datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if len(raw) == 10:
            return datetime.fromisoformat(raw).replace(tzinfo=timezone.utc)
        if len(raw) == 7:
            return datetime.fromisoformat(raw + "-01").replace(tzinfo=timezone.utc)
    except ValueError:
        return None
    return None


def step2_5_filter_recent(ckpt: Checkpoint):
    del ckpt  # unused; step 2.5 is a local filter pass
    people = _read_rows(PEOPLE_FILE)
    if not people:
        log.warning("[filter] No people found — run step 2 first.")
        return

    # Reset the filtered output each run so it reflects current PEOPLE_FILE state
    if RECENT_MOVERS_FILE.exists():
        RECENT_MOVERS_FILE.unlink()
    _ensure_csv(RECENT_MOVERS_FILE, PEOPLE_COLUMNS)

    cutoff = datetime.now(timezone.utc) - timedelta(days=RECENT_MOVE_DAYS)
    kept = []
    no_date = 0
    too_old = 0
    for row in people:
        start = _parse_start_date(row.get("job_start_date", ""))
        if not start:
            no_date += 1
            continue
        if start < cutoff:
            too_old += 1
            continue
        kept.append(row)

    _append_rows(RECENT_MOVERS_FILE, PEOPLE_COLUMNS, kept)
    log.info(f"[filter] {len(kept)} kept / {len(people)} total "
             f"(dropped: {no_date} missing start date, {too_old} started before "
             f"{cutoff.date().isoformat()})")


# ---------------------------------------------------------------------------
# Step 3: Email enrichment
# ---------------------------------------------------------------------------


def _enrich_person(client: BlitzAPIClient, person: dict) -> dict:
    person_url = person.get("person_linkedin_url", "")
    email = ""
    if person_url:
        email = client.enrich_email(person_url) or ""
    result = {**person, "email": email}
    return result


def step3_enrich(client: BlitzAPIClient, ckpt: Checkpoint):
    # Prefer the 90-day-filtered list if it exists; otherwise enrich all people.
    source_file = RECENT_MOVERS_FILE if RECENT_MOVERS_FILE.exists() else PEOPLE_FILE
    people = _read_rows(source_file)
    if not people:
        log.warning(f"[enrich] No people in {source_file.name} — "
                    f"run step 2 (and optionally step 2.5) first.")
        return
    log.info(f"[enrich] Reading from {source_file.name}")

    _ensure_csv(CONTACTS_FILE, CONTACT_COLUMNS)
    pending = [p for p in people
               if p.get("person_linkedin_url")
               and not ckpt.is_enrich_done(p["person_linkedin_url"])]

    log.info(f"[enrich] {len(pending)} of {len(people)} people to enrich")

    done = 0
    emails_found = 0
    with ThreadPoolExecutor(max_workers=ENRICH_WORKERS) as pool:
        futures = {pool.submit(_enrich_person, client, p): p for p in pending}
        for fut in as_completed(futures):
            if _shutdown_requested:
                break
            person = futures[fut]
            person_url = person["person_linkedin_url"]
            try:
                result = fut.result()
            except Exception as e:
                log.warning(f"[enrich] {person_url}: {e}")
                continue

            _append_rows(CONTACTS_FILE, CONTACT_COLUMNS, [result])
            ckpt.mark_enrich(person_url)
            done += 1
            if result.get("email"):
                emails_found += 1
            if done % 25 == 0:
                ckpt.save()
                log.info(f"[enrich] {done}/{len(pending)} processed, "
                         f"{emails_found} emails found")
    ckpt.save()
    log.info(f"[enrich] Complete — {done}/{len(pending)} processed, "
             f"{emails_found} verified emails")


# ---------------------------------------------------------------------------
# Status
# ---------------------------------------------------------------------------


def print_status(ckpt: Checkpoint):
    state = ckpt.discover_state()
    print(f"\n=== Senior HR Pipeline Status ===")
    print(f"Cap: {COMPANY_CAP} companies")
    print(f"Total discovered: {state['total_companies']}")
    print(f"Current industry index: {state['current_index']}")
    print(f"\nPer-industry progress:")
    for i, entry in enumerate(state["industries"]):
        flag = "✓ done" if entry["done"] else (
            "→ in progress" if i == state["current_index"] else "  pending")
        cursor_note = ""
        if entry["cursor"]:
            cursor_note = "  [cursor saved]"
        print(f"  {flag:16} {entry['name']:22} {entry['count']:>5} companies{cursor_note}")

    print(f"\nWaterfall processed: {len(ckpt.data['waterfall']['processed_linkedin_urls'])}")
    print(f"Email enrich processed: {len(ckpt.data['enrich']['processed_person_urls'])}")

    if COMPANIES_FILE.exists():
        print(f"\nCompanies CSV rows: {len(_read_rows(COMPANIES_FILE))}")
    if PEOPLE_FILE.exists():
        print(f"People CSV rows: {len(_read_rows(PEOPLE_FILE))}")
    if CONTACTS_FILE.exists():
        rows = _read_rows(CONTACTS_FILE)
        with_email = sum(1 for r in rows if r.get("email"))
        print(f"Contacts CSV rows: {len(rows)} ({with_email} with verified email)")
    print()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--step", choices=["1", "2", "2.5", "3"])
    parser.add_argument("--all", action="store_true")
    parser.add_argument("--status", action="store_true")
    args = parser.parse_args()

    load_dotenv()
    api_key = os.getenv("BLITZ_API_KEY")
    if not api_key and not args.status:
        log.error("BLITZ_API_KEY not set in .env")
        sys.exit(1)

    ckpt = Checkpoint(CHECKPOINT_FILE)

    if args.status:
        print_status(ckpt)
        return

    client = BlitzAPIClient(api_key)

    if args.all:
        step1_discover(client, ckpt)
        if _shutdown_requested:
            return
        step2_waterfall(client, ckpt)
        if _shutdown_requested:
            return
        step2_5_filter_recent(ckpt)
        if _shutdown_requested:
            return
        step3_enrich(client, ckpt)
    elif args.step == "1":
        step1_discover(client, ckpt)
    elif args.step == "2":
        step2_waterfall(client, ckpt)
    elif args.step == "2.5":
        step2_5_filter_recent(ckpt)
    elif args.step == "3":
        step3_enrich(client, ckpt)
    else:
        parser.print_help()
        print_status(ckpt)


if __name__ == "__main__":
    main()
