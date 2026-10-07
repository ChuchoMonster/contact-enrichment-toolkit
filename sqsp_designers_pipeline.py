#!/usr/bin/env python3
"""
Squarespace Designers Pipeline — find founders/owners + personal LinkedIn + verified email.

Usage:
    python sqsp_designers_pipeline.py --dry-run    # test CSV parsing
    python sqsp_designers_pipeline.py              # run all 3 steps
"""
from __future__ import annotations

import csv
import json
import logging
import os
import re
import signal
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
import threading

import requests
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
API_BASE = "https://api.blitz-api.ai/api"
REQUEST_DELAY = 0.05

DATA_DIR = Path(os.environ.get("BLITZ_DATA_DIR", Path(__file__).resolve().parent / "data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)
INPUT_FILE = DATA_DIR / "squarespace-designers-combined.csv"
OUTPUT_DIR = DATA_DIR
CONTACTS_FILE = OUTPUT_DIR / "sqsp_designers_contacts.csv"
CHECKPOINT_FILE = OUTPUT_DIR / "sqsp_designers_checkpoint.json"
LOG_FILE = OUTPUT_DIR / "sqsp_designers_pipeline.log"

MAX_PEOPLE = 2  # only want 1-2 people per company

CONTACTS_COLUMNS = [
    "Company Name", "Website URL", "Domain", "Company LinkedIn URL",
    "Full Name", "First Name", "Last Name", "Job Title",
    "Personal LinkedIn URL", "Verified Email", "Source",
]

# ---------------------------------------------------------------------------
# Founder/Owner cascade (design-agency focused)
# ---------------------------------------------------------------------------
CASCADE_FOUNDER = [
    {
        "include_title": [
            "Founder", "Co-Founder", "Co-founder", "Owner",
            "CEO", "Chief Executive Officer",
            "Managing Director", "Principal",
            "Creative Director", "Design Director",
            "Head Designer", "Lead Designer", "Studio Director",
            "Head of Design", "Head of Creative",
        ],
        "exclude_title": [
            "analyst", "associate", "coordinator", "assistant",
            "intern", "junior", "student", "part-time", "part time",
        ],
        "location": ["WORLD"],
        "include_headline_search": False,
    },
    {
        "include_title": ["President"],
        "exclude_title": [
            "Vice President", "VP", "Senior Vice", "EVP", "SVP",
            "Assistant", "Associate",
            "analyst", "coordinator", "intern", "junior", "student",
        ],
        "location": ["WORLD"],
        "include_headline_search": False,
    },
]

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
    log.info("Shutdown requested — saving checkpoint…")


signal.signal(signal.SIGINT, _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)

# ---------------------------------------------------------------------------
# Checkpoint manager
# ---------------------------------------------------------------------------

class CheckpointManager:
    def __init__(self, path: Path):
        self.path = path
        self.data = {
            "step1_processed": [],
            "step1_linkedin_found": {},
            "step2_processed": [],
            "step3_processed": [],
            "stats": {
                "step1_api_calls": 0, "step1_found": 0, "step1_not_found": 0,
                "step2_api_calls": 0, "step2_people_found": 0,
                "step3_api_calls": 0, "step3_emails_found": 0,
            },
        }
        self._load()

    def _load(self):
        if self.path.exists():
            raw = json.loads(self.path.read_text())
            self.data.update(raw)
            log.info(f"Checkpoint loaded — Step1: {len(self.data['step1_processed'])}, "
                     f"Step2: {len(self.data['step2_processed'])}, "
                     f"Step3: {len(self.data['step3_processed'])}")

    def save(self):
        self.path.write_text(json.dumps(self.data, indent=2))

    def is_step1_processed(self, domain: str) -> bool:
        return domain in self.data["step1_processed"]

    def mark_step1(self, domain: str, linkedin_url: str | None):
        if domain not in self.data["step1_processed"]:
            self.data["step1_processed"].append(domain)
        if linkedin_url:
            self.data["step1_linkedin_found"][domain] = linkedin_url
            self.data["stats"]["step1_found"] += 1
        else:
            self.data["stats"]["step1_not_found"] += 1

    def is_step2_processed(self, domain: str) -> bool:
        return domain in self.data["step2_processed"]

    def mark_step2(self, domain: str):
        if domain not in self.data["step2_processed"]:
            self.data["step2_processed"].append(domain)

    def is_step3_processed(self, person_linkedin: str) -> bool:
        return person_linkedin in self.data["step3_processed"]

    def mark_step3(self, person_linkedin: str):
        if person_linkedin not in self.data["step3_processed"]:
            self.data["step3_processed"].append(person_linkedin)

    def get_linkedin_url(self, domain: str) -> str | None:
        return self.data["step1_linkedin_found"].get(domain)


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
        if self.dry_run:
            return None
        url = f"{API_BASE}{endpoint}"
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

    def domain_to_linkedin(self, domain: str) -> str | None:
        data = self._post("/search/domain-to-linkedin-company", {"domain": domain})
        if data:
            return data.get("company_linkedin_url") or data.get("linkedin_url") or None
        return None

    def waterfall_icp(self, company_linkedin_url: str, cascade: list,
                      max_results: int = MAX_PEOPLE) -> list[dict]:
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

def extract_domain(url: str) -> str:
    """Extract domain from a website URL."""
    url = url.strip().lower()
    url = re.sub(r'^https?://', '', url)
    url = re.sub(r'^www\.', '', url)
    url = url.split('/')[0]
    return url


def load_csv() -> list[dict]:
    """Load the input CSV and return normalized rows."""
    rows = []
    seen_domains = set()
    with open(INPUT_FILE, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            website = (row.get("Website URL") or "").strip()
            if not website:
                continue
            domain = extract_domain(website)
            if domain in seen_domains:
                continue
            seen_domains.add(domain)
            rows.append({
                "company_name": (row.get("Company Name") or "").strip(),
                "website_url": website,
                "domain": domain,
                "first_name": (row.get("First Name") or "").strip(),
                "last_name": (row.get("Last Name") or "").strip(),
                "full_name": (row.get("Full Name") or "").strip(),
                "has_person": bool((row.get("First Name") or "").strip()),
            })
    return rows


# ---------------------------------------------------------------------------
# Step 1: Domain → Company LinkedIn
# ---------------------------------------------------------------------------

def run_step1(client: BlitzAPIClient, companies: list[dict], checkpoint: CheckpointManager):
    remaining = [c for c in companies if not checkpoint.is_step1_processed(c["domain"])]
    log.info(f"STEP 1: {len(remaining)} domains to look up (of {len(companies)} total)")

    lock = threading.Lock()
    completed = [0]

    def lookup_one(company):
        if _shutdown_requested:
            return
        domain = company["domain"]
        linkedin_url = client.domain_to_linkedin(domain)

        with lock:
            checkpoint.data["stats"]["step1_api_calls"] += 1
            checkpoint.mark_step1(domain, linkedin_url)
            completed[0] += 1
            n = completed[0]

        if linkedin_url:
            log.info(f"[{n}/{len(remaining)}] {company['company_name'] or domain} → {linkedin_url}")
        elif n % 20 == 0:
            with lock:
                log.info(f"[{n}/{len(remaining)}] Progress — "
                         f"{checkpoint.data['stats']['step1_found']} found, "
                         f"{checkpoint.data['stats']['step1_not_found']} not found")

        if n % 50 == 0:
            with lock:
                checkpoint.save()

    with ThreadPoolExecutor(max_workers=3) as pool:
        futures = {pool.submit(lookup_one, c): c for c in remaining}
        for future in as_completed(futures):
            if _shutdown_requested:
                pool.shutdown(wait=False, cancel_futures=True)
                break
            future.result()

    checkpoint.save()

    total_with_li = sum(1 for c in companies if checkpoint.get_linkedin_url(c["domain"]))
    log.info("=" * 60)
    log.info("STEP 1 COMPLETE")
    log.info(f"  API calls:           {checkpoint.data['stats']['step1_api_calls']}")
    log.info(f"  LinkedIn URLs found: {checkpoint.data['stats']['step1_found']}")
    log.info(f"  Not found:           {checkpoint.data['stats']['step1_not_found']}")
    log.info(f"  Total with LinkedIn: {total_with_li} / {len(companies)}")
    log.info("=" * 60)


# ---------------------------------------------------------------------------
# Step 2: Founder waterfall ICP
# ---------------------------------------------------------------------------

def run_step2(client: BlitzAPIClient, companies: list[dict], checkpoint: CheckpointManager):
    companies_with_li = []
    for c in companies:
        li = checkpoint.get_linkedin_url(c["domain"])
        if li:
            companies_with_li.append({**c, "company_linkedin_url": li})

    remaining = [c for c in companies_with_li if not checkpoint.is_step2_processed(c["domain"])]
    log.info(f"STEP 2: {len(remaining)} companies to search (of {len(companies_with_li)} with LinkedIn)")

    file_exists = CONTACTS_FILE.exists() and CONTACTS_FILE.stat().st_size > 0
    csv_file = open(CONTACTS_FILE, "a", newline="", encoding="utf-8")
    writer = csv.DictWriter(csv_file, fieldnames=CONTACTS_COLUMNS)
    if not file_exists:
        writer.writeheader()

    lock = threading.Lock()
    completed = [0]

    def search_one(company):
        if _shutdown_requested:
            return
        domain = company["domain"]
        linkedin_url = company["company_linkedin_url"]

        people = client.waterfall_icp(linkedin_url, cascade=CASCADE_FOUNDER, max_results=MAX_PEOPLE)

        with lock:
            checkpoint.data["stats"]["step2_api_calls"] += 1
            completed[0] += 1
            n = completed[0]

            if people:
                checkpoint.data["stats"]["step2_people_found"] += len(people)
                for p in people:
                    full_name = p.get("full_name", "")
                    first_name = p.get("first_name", "")
                    last_name = p.get("last_name", "")
                    title = p.get("job_title", "") or p.get("title", "")
                    profile_url = (p.get("person_linkedin_url", "")
                                   or p.get("linkedin_profile_url", "")
                                   or p.get("linkedin_url", ""))

                    writer.writerow({
                        "Company Name": company["company_name"],
                        "Website URL": company["website_url"],
                        "Domain": domain,
                        "Company LinkedIn URL": linkedin_url,
                        "Full Name": full_name,
                        "First Name": first_name,
                        "Last Name": last_name,
                        "Job Title": title,
                        "Personal LinkedIn URL": profile_url,
                        "Verified Email": "",
                        "Source": "api",
                    })
                csv_file.flush()

                names = [p.get("full_name", "?") for p in people[:3]]
                extra = f" +{len(people)-3} more" if len(people) > 3 else ""
                log.info(f"[{n}/{len(remaining)}] {company['company_name'] or domain} — "
                         f"{len(people)} people: {', '.join(names)}{extra}")
            else:
                log.info(f"[{n}/{len(remaining)}] {company['company_name'] or domain} — no results")

            checkpoint.mark_step2(domain)
            if n % 20 == 0:
                checkpoint.save()

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = {pool.submit(search_one, c): c for c in remaining}
        for future in as_completed(futures):
            if _shutdown_requested:
                pool.shutdown(wait=False, cancel_futures=True)
                break
            future.result()

    csv_file.close()
    checkpoint.save()

    log.info("=" * 60)
    log.info("STEP 2 COMPLETE")
    log.info(f"  Companies searched:  {len(checkpoint.data['step2_processed'])}")
    log.info(f"  People found:        {checkpoint.data['stats']['step2_people_found']}")
    log.info(f"  API calls:           {checkpoint.data['stats']['step2_api_calls']}")
    log.info(f"  Output file:         {CONTACTS_FILE}")
    log.info("=" * 60)


# ---------------------------------------------------------------------------
# Step 3: Email enrichment
# ---------------------------------------------------------------------------

def _rewrite_contacts(rows: list[dict]):
    with open(CONTACTS_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CONTACTS_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)


def run_step3(client: BlitzAPIClient, checkpoint: CheckpointManager):
    if not CONTACTS_FILE.exists():
        log.error("No contacts file found — run step 2 first")
        return

    rows = []
    with open(CONTACTS_FILE, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)

    need_email = [r for r in rows if r.get("Personal LinkedIn URL")
                  and not r.get("Verified Email")
                  and not checkpoint.is_step3_processed(r["Personal LinkedIn URL"])]

    log.info(f"STEP 3: {len(need_email)} people need email enrichment (of {len(rows)} total)")

    lock = threading.Lock()
    completed = [0]
    enriched = [0]

    def enrich_one(row):
        if _shutdown_requested:
            return
        person_linkedin = row["Personal LinkedIn URL"]
        email = client.enrich_email(person_linkedin) or ""

        with lock:
            checkpoint.data["stats"]["step3_api_calls"] += 1
            completed[0] += 1
            n = completed[0]

            if email:
                row["Verified Email"] = email
                checkpoint.data["stats"]["step3_emails_found"] += 1
                enriched[0] += 1
                log.info(f"[{n}/{len(need_email)}] {row.get('Full Name', '?')} → {email}")
            elif n % 20 == 0:
                log.info(f"[{n}/{len(need_email)}] Progress — {enriched[0]} emails found")

            checkpoint.mark_step3(person_linkedin)
            if n % 20 == 0:
                checkpoint.save()
                _rewrite_contacts(rows)

    with ThreadPoolExecutor(max_workers=3) as pool:
        futures = {pool.submit(enrich_one, r): r for r in need_email}
        for future in as_completed(futures):
            if _shutdown_requested:
                pool.shutdown(wait=False, cancel_futures=True)
                break
            future.result()

    checkpoint.save()
    _rewrite_contacts(rows)

    log.info("=" * 60)
    log.info("STEP 3 COMPLETE")
    log.info(f"  People processed:    {len(checkpoint.data['step3_processed'])}")
    log.info(f"  Emails found:        {checkpoint.data['stats']['step3_emails_found']}")
    log.info(f"  API calls:           {checkpoint.data['stats']['step3_api_calls']}")
    log.info(f"  Output file:         {CONTACTS_FILE}")
    log.info("=" * 60)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Squarespace Designers Pipeline")
    parser.add_argument("--dry-run", action="store_true", help="Parse CSV only, no API calls")
    args = parser.parse_args()

    load_dotenv()
    api_key = os.getenv("BLITZ_API_KEY", "")
    if not api_key and not args.dry_run:
        log.error("BLITZ_API_KEY not set")
        sys.exit(1)

    companies = load_csv()
    log.info(f"Loaded {len(companies)} unique companies from CSV")
    log.info(f"  With person name: {sum(1 for c in companies if c['has_person'])}")
    log.info(f"  Without person name: {sum(1 for c in companies if not c['has_person'])}")

    if args.dry_run:
        for c in companies[:5]:
            log.info(f"  {c['domain']:30s} {c['company_name']:30s} {c['full_name']}")
        return

    client = BlitzAPIClient(api_key)
    checkpoint = CheckpointManager(CHECKPOINT_FILE)

    log.info("=" * 60)
    log.info("SQUARESPACE DESIGNERS PIPELINE")
    log.info("=" * 60)

    run_step1(client, companies, checkpoint)
    if _shutdown_requested:
        return

    run_step2(client, companies, checkpoint)
    if _shutdown_requested:
        return

    run_step3(client, checkpoint)

    log.info("=" * 60)
    log.info("ALL STEPS COMPLETE")
    log.info(f"  Final output: {CONTACTS_FILE}")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
