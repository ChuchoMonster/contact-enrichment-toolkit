#!/usr/bin/env python3
"""
PE/VC Pipeline — Find operations contacts at private equity and venture capital firms.

- Large/medium firms: Operating Partner, Head of Operations, COO, VP Ops
- Small firms: Founder, Managing Partner, Managing Director

Usage:
    python pe_vc_pipeline.py --step 1    # Domain → LinkedIn lookup
    python pe_vc_pipeline.py --step 2    # Waterfall ICP search
    python pe_vc_pipeline.py --step 3    # Email enrichment
    python pe_vc_pipeline.py --all       # Run all 3 steps sequentially
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
from concurrent.futures import ThreadPoolExecutor, as_completed
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
INPUT_FILE = DATA_DIR / "pe_vc_firms.csv"
OUTPUT_DIR = DATA_DIR

LINKEDIN_LOOKUP_FILE = OUTPUT_DIR / "pevc_linkedin_lookup_results.csv"
CONTACTS_FILE = OUTPUT_DIR / "pevc_contacts_results.csv"
CHECKPOINT_FILE = OUTPUT_DIR / "pevc_checkpoint.json"
LOG_FILE = OUTPUT_DIR / "pevc_pipeline.log"

MAX_PEOPLE_PER_COMPANY = 2  # 1-2 people per firm

CONTACTS_COLUMNS = [
    "Firm Name", "Domain", "Size", "Full Name", "First Name", "Last Name",
    "Job Title", "LinkedIn Profile URL", "Verified Email",
    "Cascade Level", "Cascade Type",
]

# ---------------------------------------------------------------------------
# Waterfall ICP cascades — Operations focus
# ---------------------------------------------------------------------------
EXCLUDE_TITLES = [
    "analyst", "associate", "coordinator", "assistant",
    "intern", "junior", "student", "part-time", "part time",
]

# For medium/large firms: find the operations person
CASCADE_OPERATIONS = [
    # Level 1: C-suite ops + Operating Partner
    {
        "include_title": [
            "COO", "Chief Operating Officer", "Chief Operations Officer",
            "Operating Partner", "Operating Managing Director",
            "Head of Operations", "Head of Portfolio Operations",
            "Head of Value Creation", "Head of Portfolio Value Creation",
            "Chief of Staff",
            "VP Operations", "VP Portfolio Operations",
            "SVP Operations", "EVP Operations",
            "Head of Platform", "VP Platform",
        ],
        "exclude_title": EXCLUDE_TITLES,
        "location": ["WORLD"],
        "include_headline_search": False,
    },
    # Level 2: Director-level ops
    {
        "include_title": [
            "Director of Operations", "Director of Portfolio Operations",
            "Director of Value Creation", "Director Portfolio Operations",
            "Director Operations", "Operations Director",
            "Senior Director Operations", "Sr. Director Operations",
            "Director of Platform", "Platform Director",
            "Managing Director Operations",
        ],
        "exclude_title": EXCLUDE_TITLES,
        "location": ["WORLD"],
        "include_headline_search": False,
    },
]

# For small firms: find the founder/leader (they manage ops directly)
CASCADE_LEADERSHIP = [
    {
        "include_title": [
            "Managing Partner", "Managing Director",
            "Founder", "Co-Founder", "Co-founder",
            "CEO", "Chief Executive Officer",
            "General Partner", "Senior Partner",
            "Partner",
        ],
        "exclude_title": [
            "Vice President", "VP", "SVP", "EVP",
            "Associate", "Assistant", "Intern", "Junior",
            "Operating Partner",  # exclude — different cascade
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
    log.info("Shutdown requested — finishing current work then saving checkpoint...")


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
    def __init__(self, api_key: str):
        self.session = requests.Session()
        self.session.headers.update({
            "x-api-key": api_key,
            "Content-Type": "application/json",
        })

    def _post(self, endpoint: str, payload: dict) -> dict | None:
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
                      max_results: int = MAX_PEOPLE_PER_COMPANY) -> list[dict]:
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

VALID_OPS_KEYWORDS = [
    "operating partner", "operations", "coo", "chief operating",
    "portfolio operations", "value creation", "platform",
    "chief of staff",
]

VALID_LEADERSHIP_KEYWORDS = [
    "managing partner", "managing director", "founder", "co-founder",
    "cofounder", "ceo", "chief executive", "general partner",
    "senior partner", "partner",
]

DISQUALIFYING_KEYWORDS = [
    "assistant", "associate", "intern", "junior",
    "secretary", "coordinator", "receptionist", "clerk",
    "student", "part-time", "part time",
]

import re

def _word_match(title_lower: str, keywords: list[str]) -> bool:
    for kw in keywords:
        if re.search(r'\b' + re.escape(kw) + r'\b', title_lower):
            return True
    return False


def is_disqualified(title: str) -> bool:
    return _word_match(title.lower(), DISQUALIFYING_KEYWORDS)


def is_valid_ops_title(title: str) -> bool:
    if is_disqualified(title):
        return False
    return _word_match(title.lower(), VALID_OPS_KEYWORDS)


def is_valid_leadership_title(title: str) -> bool:
    if is_disqualified(title):
        return False
    return _word_match(title.lower(), VALID_LEADERSHIP_KEYWORDS)


def is_valid_title(title: str, size: str) -> bool:
    if size == "small":
        return is_valid_leadership_title(title) or is_valid_ops_title(title)
    else:
        # For medium/large, prefer ops but accept leadership as fallback
        return is_valid_ops_title(title) or is_valid_leadership_title(title)


def split_name(full_name: str) -> tuple[str, str]:
    parts = full_name.strip().split()
    if len(parts) == 0:
        return ("", "")
    if len(parts) == 1:
        return (parts[0], "")
    return (parts[0], " ".join(parts[1:]))


def load_firms() -> list[dict]:
    """Load firms from the PE/VC CSV."""
    firms = []
    with open(INPUT_FILE, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            domain = (row.get("domain") or "").strip()
            name = (row.get("firm_name") or "").strip()
            size = (row.get("size") or "medium").strip().lower()
            if domain:
                firms.append({"domain": domain, "firm_name": name, "size": size})
    log.info(f"Loaded {len(firms)} firms from {INPUT_FILE.name}")
    return firms


# ---------------------------------------------------------------------------
# Step 1: LinkedIn URL lookup
# ---------------------------------------------------------------------------

def run_step1(client: BlitzAPIClient, firms: list[dict], checkpoint: CheckpointManager):
    remaining = [f for f in firms if not checkpoint.is_step1_processed(f["domain"])]
    log.info(f"STEP 1: {len(remaining)} firms need LinkedIn lookup (of {len(firms)} total)")

    lock = threading.Lock()
    completed = [0]

    def lookup_one(firm):
        if _shutdown_requested:
            return
        linkedin_url = client.domain_to_linkedin(firm["domain"])

        with lock:
            checkpoint.data["stats"]["step1_api_calls"] += 1
            checkpoint.mark_step1(firm["domain"], linkedin_url)
            completed[0] += 1
            n = completed[0]

        if linkedin_url:
            log.info(f"[{n}/{len(remaining)}] {firm['firm_name']} ({firm['domain']}) -> {linkedin_url}")
        else:
            log.info(f"[{n}/{len(remaining)}] {firm['firm_name']} ({firm['domain']}) -> NOT FOUND")

        if n % 10 == 0:
            with lock:
                checkpoint.save()

    with ThreadPoolExecutor(max_workers=3) as pool:
        futures = {pool.submit(lookup_one, f): f for f in remaining}
        for future in as_completed(futures):
            if _shutdown_requested:
                pool.shutdown(wait=False, cancel_futures=True)
                break
            future.result()

    checkpoint.save()

    # Write intermediate results
    with open(LINKEDIN_LOOKUP_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["Firm Name", "Domain", "Size", "LinkedIn URL"])
        writer.writeheader()
        for firm in firms:
            li = checkpoint.get_linkedin_url(firm["domain"]) or ""
            writer.writerow({
                "Firm Name": firm["firm_name"],
                "Domain": firm["domain"],
                "Size": firm["size"],
                "LinkedIn URL": li,
            })

    log.info("=" * 60)
    log.info("STEP 1 COMPLETE")
    log.info(f"  API calls:           {checkpoint.data['stats']['step1_api_calls']}")
    log.info(f"  LinkedIn URLs found: {checkpoint.data['stats']['step1_found']}")
    log.info(f"  Not found:           {checkpoint.data['stats']['step1_not_found']}")
    log.info(f"  Results:             {LINKEDIN_LOOKUP_FILE}")
    log.info("=" * 60)


# ---------------------------------------------------------------------------
# Step 2: Waterfall ICP search
# ---------------------------------------------------------------------------

def run_step2(client: BlitzAPIClient, firms: list[dict], checkpoint: CheckpointManager):
    firms_with_li = [(f, checkpoint.get_linkedin_url(f["domain"]))
                     for f in firms if checkpoint.get_linkedin_url(f["domain"])]
    remaining = [(f, li) for f, li in firms_with_li if not checkpoint.is_step2_processed(f["domain"])]
    log.info(f"STEP 2: {len(remaining)} firms to search (of {len(firms_with_li)} with LinkedIn URLs)")

    file_exists = CONTACTS_FILE.exists() and CONTACTS_FILE.stat().st_size > 0
    csv_file = open(CONTACTS_FILE, "a", newline="", encoding="utf-8")
    writer = csv.DictWriter(csv_file, fieldnames=CONTACTS_COLUMNS)
    if not file_exists:
        writer.writeheader()

    lock = threading.Lock()
    completed = [0]

    def process_one(item):
        if _shutdown_requested:
            return
        firm, linkedin_url = item
        size = firm["size"]

        # For medium/large: try operations cascade first, then leadership fallback
        # For small: try leadership first, then operations
        if size == "small":
            primary_cascade = CASCADE_LEADERSHIP
            primary_type = "leadership"
            fallback_cascade = CASCADE_OPERATIONS
            fallback_type = "operations"
        else:
            primary_cascade = CASCADE_OPERATIONS
            primary_type = "operations"
            fallback_cascade = CASCADE_LEADERSHIP
            fallback_type = "leadership"

        primary_people = client.waterfall_icp(linkedin_url, cascade=primary_cascade,
                                               max_results=MAX_PEOPLE_PER_COMPANY)
        api_calls = 1

        # If primary didn't return enough, try fallback
        fallback_people = []
        if len(primary_people) < 1:
            fallback_people = client.waterfall_icp(linkedin_url, cascade=fallback_cascade,
                                                    max_results=MAX_PEOPLE_PER_COMPANY)
            api_calls += 1

        seen_urls = set()
        seen_names = set()
        results = []

        for cascade_type, people in [(primary_type, primary_people), (fallback_type, fallback_people)]:
            for person in people:
                if len(results) >= MAX_PEOPLE_PER_COMPANY:
                    break

                person_linkedin = (person.get("person_linkedin_url", "")
                                   or person.get("linkedin_profile_url", "")
                                   or person.get("linkedin_url", ""))
                full_name = (person.get("full_name", "") or "").strip().lower()

                if not person_linkedin or person_linkedin in seen_urls:
                    continue
                if full_name and full_name in seen_names:
                    continue

                seen_urls.add(person_linkedin)
                if full_name:
                    seen_names.add(full_name)

                job_title = person.get("job_title", "") or person.get("linkedin_headline", "")
                if not is_valid_title(job_title, size):
                    continue

                actual_name = person.get("full_name", "")
                first_name, last_name = split_name(actual_name)
                cascade_level = person.get("icp", "")

                results.append({
                    "Firm Name": firm["firm_name"],
                    "Domain": firm["domain"],
                    "Size": size,
                    "Full Name": actual_name,
                    "First Name": first_name,
                    "Last Name": last_name,
                    "Job Title": job_title,
                    "LinkedIn Profile URL": person_linkedin,
                    "Verified Email": "",
                    "Cascade Level": cascade_level,
                    "Cascade Type": cascade_type,
                })

        with lock:
            checkpoint.data["stats"]["step2_api_calls"] += api_calls
            checkpoint.data["stats"]["step2_people_found"] += len(results)
            checkpoint.mark_step2(firm["domain"])
            completed[0] += 1
            n = completed[0]

            if results:
                for r in results:
                    writer.writerow(r)
                csv_file.flush()

            names = ", ".join(f"{r['Full Name']} ({r['Job Title']})" for r in results[:3])
            if results:
                log.info(f"[{n}/{len(remaining)}] {firm['firm_name']} — {len(results)} people: {names}")
            else:
                log.info(f"[{n}/{len(remaining)}] {firm['firm_name']} — no matches")

            if n % 10 == 0:
                checkpoint.save()

    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = {pool.submit(process_one, item): item for item in remaining}
            for future in as_completed(futures):
                if _shutdown_requested:
                    pool.shutdown(wait=False, cancel_futures=True)
                    break
                future.result()
    finally:
        csv_file.close()
        checkpoint.save()

    log.info("=" * 60)
    log.info("STEP 2 COMPLETE")
    log.info(f"  Firms searched:      {len(checkpoint.data['step2_processed'])}")
    log.info(f"  People found:        {checkpoint.data['stats']['step2_people_found']}")
    log.info(f"  API calls:           {checkpoint.data['stats']['step2_api_calls']}")
    log.info(f"  Output file:         {CONTACTS_FILE}")
    log.info("=" * 60)


# ---------------------------------------------------------------------------
# Step 3: Email enrichment
# ---------------------------------------------------------------------------

def run_step3(client: BlitzAPIClient, checkpoint: CheckpointManager):
    if not CONTACTS_FILE.exists():
        log.error("No contacts file found — run step 2 first")
        return

    rows = []
    with open(CONTACTS_FILE, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)

    need_email = [r for r in rows if r.get("LinkedIn Profile URL")
                  and not r.get("Verified Email")
                  and not checkpoint.is_step3_processed(r["LinkedIn Profile URL"])]

    log.info(f"STEP 3: {len(need_email)} people need email enrichment (of {len(rows)} total)")

    lock = threading.Lock()
    completed = [0]

    def enrich_one(row):
        if _shutdown_requested:
            return

        person_linkedin = row["LinkedIn Profile URL"]
        email = client.enrich_email(person_linkedin) or ""

        with lock:
            checkpoint.data["stats"]["step3_api_calls"] += 1
            checkpoint.mark_step3(person_linkedin)
            completed[0] += 1
            n = completed[0]

            if email:
                row["Verified Email"] = email
                checkpoint.data["stats"]["step3_emails_found"] += 1
                log.info(f"[{n}/{len(need_email)}] {row['Full Name']} ({row['Firm Name']}) -> {email}")
            else:
                log.info(f"[{n}/{len(need_email)}] {row['Full Name']} ({row['Firm Name']}) -> no email")

            if n % 10 == 0:
                checkpoint.save()
                _rewrite_contacts(rows)

    try:
        with ThreadPoolExecutor(max_workers=3) as pool:
            futures = {pool.submit(enrich_one, r): r for r in need_email}
            for future in as_completed(futures):
                if _shutdown_requested:
                    pool.shutdown(wait=False, cancel_futures=True)
                    break
                future.result()
    finally:
        checkpoint.save()
        _rewrite_contacts(rows)

    # Write verified-only file
    verified_file = OUTPUT_DIR / "pevc_contacts_verified_emails.csv"
    with open(verified_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CONTACTS_COLUMNS)
        writer.writeheader()
        for r in rows:
            if r.get("Verified Email"):
                writer.writerow(r)

    verified_count = sum(1 for r in rows if r.get("Verified Email"))
    log.info("=" * 60)
    log.info("STEP 3 COMPLETE")
    log.info(f"  People processed:    {len(checkpoint.data['step3_processed'])}")
    log.info(f"  Emails found:        {checkpoint.data['stats']['step3_emails_found']}")
    log.info(f"  API calls:           {checkpoint.data['stats']['step3_api_calls']}")
    log.info(f"  Full output:         {CONTACTS_FILE}")
    log.info(f"  Verified only:       {verified_file} ({verified_count} rows)")
    log.info("=" * 60)


def _rewrite_contacts(rows: list[dict]):
    with open(CONTACTS_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CONTACTS_COLUMNS)
        writer.writeheader()
        for r in rows:
            writer.writerow(r)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="PE/VC operations contact finder pipeline")
    parser.add_argument("--step", type=int, choices=[1, 2, 3],
                        help="Run a specific step (1=LinkedIn lookup, 2=waterfall, 3=email)")
    parser.add_argument("--all", action="store_true", help="Run all 3 steps sequentially")
    args = parser.parse_args()

    load_dotenv(Path(__file__).resolve().parent / ".env")
    api_key = os.getenv("BLITZ_API_KEY")
    if not api_key:
        log.error("BLITZ_API_KEY not found in .env")
        sys.exit(1)

    client = BlitzAPIClient(api_key)
    checkpoint = CheckpointManager(CHECKPOINT_FILE)
    firms = load_firms()

    if args.all:
        run_step1(client, firms, checkpoint)
        if not _shutdown_requested:
            run_step2(client, firms, checkpoint)
        if not _shutdown_requested:
            run_step3(client, checkpoint)
    elif args.step == 1:
        run_step1(client, firms, checkpoint)
    elif args.step == 2:
        run_step2(client, firms, checkpoint)
    elif args.step == 3:
        run_step3(client, checkpoint)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
