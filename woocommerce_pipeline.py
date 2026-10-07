#!/usr/bin/env python3
"""
WooCommerce Pipeline — 3-step contact finder for BuiltWith WooCommerce domains:
  Step 1: Domain → LinkedIn URL lookup
  Step 2: Waterfall ICP search (marketing + founder cascades)
  Step 3: Email enrichment for all people found

Usage:
    python woocommerce_pipeline.py --step 1    # LinkedIn lookup
    python woocommerce_pipeline.py --step 2    # waterfall ICP
    python woocommerce_pipeline.py --step 3    # email enrichment
    python woocommerce_pipeline.py --all       # run all 3 steps sequentially
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

from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

import requests
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
API_BASE = "https://api.blitz-api.ai/api"
REQUEST_DELAY = 0.05  # minimal delay; concurrency controls throughput

DATA_DIR = Path(os.environ.get("BLITZ_DATA_DIR", Path(__file__).resolve().parent / "data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)
INPUT_FILE = DATA_DIR / "builtwith_woocommerce_domains.csv"

OUTPUT_DIR = DATA_DIR
LINKEDIN_LOOKUP_FILE = OUTPUT_DIR / "wc_linkedin_lookup_results.csv"
CONTACTS_FILE = OUTPUT_DIR / "wc_contacts_results.csv"
CHECKPOINT_FILE = OUTPUT_DIR / "wc_checkpoint.json"
LOG_FILE = OUTPUT_DIR / "wc_pipeline.log"

MAX_PEOPLE_PER_COMPANY = 50

CONTACTS_COLUMNS = [
    "Domain", "Full Name", "First Name", "Last Name", "Job Title",
    "LinkedIn Profile URL", "Verified Email", "Cascade Level",
    "Cascade Type",
]

# ---------------------------------------------------------------------------
# Waterfall ICP cascades (same as TierA pipeline)
# ---------------------------------------------------------------------------
EXCLUDE_TITLES = [
    "analyst", "associate", "coordinator", "assistant",
    "intern", "junior", "student", "part-time", "part time",
]

CASCADE_MARKETING = [
    # Level 1: C-Suite + VP level
    {
        "include_title": [
            "CMO", "Chief Marketing Officer", "Chief Digital Officer",
            "Chief Content Officer", "Chief Brand Officer", "Chief Growth Officer",
            "Chief Product Officer", "Chief Experience Officer",
            "Chief Communications Officer",
            "VP Marketing", "VP Digital", "VP Content", "VP Brand", "VP Growth",
            "VP Ecommerce", "VP E-commerce", "VP Online", "VP Social Media",
            "VP SEO", "VP Product", "VP Affiliate", "VP Partnerships",
            "VP Communications", "VP Customer Experience", "VP Digital Experience",
            "VP Website",
            "SVP Marketing", "SVP Digital", "SVP Product", "SVP Growth",
            "EVP Marketing", "EVP Digital", "EVP Product",
            "Head of SEO", "Head of Digital", "Head of Marketing",
            "Head of Content", "Head of Brand", "Head of Growth",
            "Head of Ecommerce", "Head of Social Media", "Head of Web",
            "Head of Product", "Head of Affiliate", "Head of Partnerships",
            "Head of Communications", "Head of Customer Experience",
            "Head of Digital Experience", "Head of Website",
        ],
        "exclude_title": EXCLUDE_TITLES,
        "location": ["WORLD"],
        "include_headline_search": False,
    },
    # Level 2: Director level
    {
        "include_title": [
            "Director of SEO", "Director of Content", "Director of Content Strategy",
            "Director of Digital Marketing", "Director of Brand",
            "Director of Growth", "Director of Marketing",
            "Director of Social Media", "Director of Demand Gen",
            "Director of E-commerce", "Director of Ecommerce",
            "Director of Online Marketing", "Director of Web",
            "Director of Communications", "Director of PR",
            "Director of Affiliate", "Director of Partnerships",
            "Director of Digital", "Director of Digital Experience",
            "Director of Product", "Director of Customer Experience",
            "Director of Website", "Director of Brand Communications",
            "Director Website", "Director Digital Experience",
            "Sr. Director Marketing", "Senior Director Marketing",
            "Senior Director Content", "Senior Director Digital",
            "Senior Director Product", "Senior Director Growth",
            "Senior Director SEO", "Senior Director Brand",
            "Senior Director Communications", "Senior Director Partnerships",
            "Marketing Director", "Digital Director", "SEO Director",
            "Content Director", "Brand Director", "Product Director",
            "Creative Director",
        ],
        "exclude_title": EXCLUDE_TITLES,
        "location": ["WORLD"],
        "include_headline_search": False,
    },
    # Level 3: Senior Manager level
    {
        "include_title": [
            "Senior Manager Marketing", "Senior Manager SEO",
            "Senior Manager Content", "Senior Manager Digital",
            "Senior Manager Brand", "Senior Manager Growth",
            "Senior Manager Product", "Senior Manager Affiliate",
            "Senior Manager Partnerships", "Senior Manager Communications",
            "Senior Manager Customer Experience", "Senior Manager Digital Experience",
            "Senior Manager Website", "Senior Manager E-commerce",
            "Senior Manager Ecommerce", "Senior Manager Social Media",
            "Senior Marketing Manager", "Senior SEO Manager",
            "Senior Content Manager", "Senior Digital Manager",
            "Senior Digital Marketing Manager", "Senior Brand Manager",
            "Senior Growth Manager", "Senior Product Manager",
            "Senior Affiliate Manager", "Senior Partnerships Manager",
            "Senior Communications Manager", "Senior Website Manager",
            "Senior Ecommerce Manager", "Senior Social Media Manager",
            "Sr. Manager Marketing", "Sr. Manager SEO", "Sr. Manager Content",
            "Sr. Manager Digital", "Sr. Manager Product",
            "Sr. Marketing Manager", "Sr. SEO Manager", "Sr. Content Manager",
            "Sr. Digital Marketing Manager", "Sr. Product Manager",
            "Strategic Consultant Marketing", "Strategic Consultant SEO",
            "Strategic Consultant Digital", "Strategic Consultant Content",
            "Strategic Consultant Brand", "Strategic Consultant Product",
            "Strategy Consultant Marketing", "Strategy Consultant Digital",
            "Marketing Strategy Consultant", "Digital Strategy Consultant",
            "SEO Strategy Consultant", "Content Strategy Consultant",
        ],
        "exclude_title": EXCLUDE_TITLES,
        "location": ["WORLD"],
        "include_headline_search": False,
    },
    # Level 4: Headline search fallback
    {
        "include_title": [
            "Marketing", "SEO", "Content", "Digital",
            "Brand", "Growth", "Ecommerce", "Social Media", "Web",
            "Product", "Affiliate", "Partnerships", "Communications",
            "Customer Experience", "Digital Experience", "Website",
        ],
        "exclude_title": EXCLUDE_TITLES + [
            "specialist", "designer", "technical", "account", "sales",
            "supervisor", "engineer", "developer", "operations",
            "accountant", "HR", "finance", "recruiter",
        ],
        "location": ["WORLD"],
        "include_headline_search": True,
    },
]

CASCADE_FOUNDER = [
    {
        "include_title": [
            "CEO", "Chief Executive Officer", "Founder", "Co-Founder",
            "Co-founder", "Owner", "Managing Director",
            "General Manager", "Principal",
        ],
        "exclude_title": EXCLUDE_TITLES,
        "location": ["WORLD"],
        "include_headline_search": False,
    },
    {
        "include_title": ["President"],
        "exclude_title": [
            "Vice President", "VP", "Senior Vice", "EVP",
            "SVP", "Assistant", "Associate",
        ] + EXCLUDE_TITLES,
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
    log.info("Shutdown requested — finishing current work then saving checkpoint…")


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

VALID_MARKETING_KEYWORDS = [
    "marketing", "seo", "search engine", "content", "brand", "digital",
    "communications", "comms", "pr", "public relations", "affiliate",
    "growth", "web", "website", "online", "ecommerce", "e-commerce",
    "social media", "demand gen", "demand generation", "go-to-market",
    "gtm", "creative director", "editorial", "product", "partnerships",
    "customer experience", "digital experience", "strategic consultant",
    "strategy consultant",
]

VALID_FOUNDER_KEYWORDS = [
    "ceo", "chief executive", "founder", "co-founder", "cofounder",
    "owner", "president", "managing director", "general manager",
    "principal",
]

DISQUALIFYING_KEYWORDS = [
    "assistant", "associate", "intern", "junior", "liaison",
    "secretary", "coordinator", "receptionist", "clerk",
    "assistant to", "liaison to", "executive assistant",
    "executive liaison", "office of the",
    "student", "part-time", "part time",
]


def _word_match(title_lower: str, keywords: list[str]) -> bool:
    for kw in keywords:
        if re.search(r'\b' + re.escape(kw) + r'\b', title_lower):
            return True
    return False


def is_disqualified(title: str) -> bool:
    title_lower = title.lower()
    if " to the " in title_lower:
        return True
    return _word_match(title_lower, DISQUALIFYING_KEYWORDS)


def is_valid_marketing_title(title: str) -> bool:
    if is_disqualified(title):
        return False
    return _word_match(title.lower(), VALID_MARKETING_KEYWORDS)


def is_valid_founder_title(title: str) -> bool:
    if is_disqualified(title):
        return False
    return _word_match(title.lower(), VALID_FOUNDER_KEYWORDS)


def is_valid_title(title: str) -> bool:
    return is_valid_marketing_title(title) or is_valid_founder_title(title)


def split_name(full_name: str) -> tuple[str, str]:
    parts = full_name.strip().split()
    if len(parts) == 0:
        return ("", "")
    if len(parts) == 1:
        return (parts[0], "")
    return (parts[0], " ".join(parts[1:]))


def load_domains() -> list[str]:
    """Load domains from the WooCommerce CSV."""
    domains = []
    with open(INPUT_FILE, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            domain = (row.get("domain") or "").strip()
            if domain:
                domains.append(domain)
    log.info(f"Loaded {len(domains)} domains from {INPUT_FILE.name}")
    return domains


# ---------------------------------------------------------------------------
# Step 1: LinkedIn URL lookup
# ---------------------------------------------------------------------------

def run_step1(client: BlitzAPIClient, domains: list[str], checkpoint: CheckpointManager):
    remaining = [d for d in domains if not checkpoint.is_step1_processed(d)]
    log.info(f"STEP 1: {len(remaining)} domains need LinkedIn lookup (of {len(domains)} total)")

    lock = threading.Lock()
    completed = [0]

    def lookup_one(domain):
        if _shutdown_requested:
            return
        linkedin_url = client.domain_to_linkedin(domain)

        with lock:
            checkpoint.data["stats"]["step1_api_calls"] += 1
            checkpoint.mark_step1(domain, linkedin_url)
            completed[0] += 1
            n = completed[0]

        if linkedin_url:
            log.info(f"[{n}/{len(remaining)}] {domain} → {linkedin_url}")
        elif n % 50 == 0:
            with lock:
                log.info(f"[{n}/{len(remaining)}] Progress — "
                         f"{checkpoint.data['stats']['step1_found']} found, "
                         f"{checkpoint.data['stats']['step1_not_found']} not found")

        if n % 100 == 0:
            with lock:
                checkpoint.save()

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = {pool.submit(lookup_one, d): d for d in remaining}
        for future in as_completed(futures):
            if _shutdown_requested:
                pool.shutdown(wait=False, cancel_futures=True)
                break
            future.result()

    checkpoint.save()

    # Write intermediate results
    with open(LINKEDIN_LOOKUP_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["Domain", "LinkedIn URL", "Source"])
        writer.writeheader()
        for d in domains:
            li = checkpoint.get_linkedin_url(d) or ""
            source = "api" if li else "not_found"
            writer.writerow({"Domain": d, "LinkedIn URL": li, "Source": source})

    log.info("=" * 60)
    log.info("STEP 1 COMPLETE")
    log.info(f"  API calls:           {checkpoint.data['stats']['step1_api_calls']}")
    log.info(f"  LinkedIn URLs found: {checkpoint.data['stats']['step1_found']}")
    log.info(f"  Not found:           {checkpoint.data['stats']['step1_not_found']}")
    log.info(f"  Lookup results:      {LINKEDIN_LOOKUP_FILE}")
    log.info("=" * 60)


# ---------------------------------------------------------------------------
# Step 2: Waterfall ICP search
# ---------------------------------------------------------------------------

def run_step2(client: BlitzAPIClient, domains: list[str], checkpoint: CheckpointManager):
    domains_with_li = [(d, checkpoint.get_linkedin_url(d)) for d in domains if checkpoint.get_linkedin_url(d)]
    remaining = [(d, li) for d, li in domains_with_li if not checkpoint.is_step2_processed(d)]
    log.info(f"STEP 2: {len(remaining)} companies to search (of {len(domains_with_li)} with LinkedIn URLs)")

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
        domain, linkedin_url = item

        marketing_people = client.waterfall_icp(linkedin_url, cascade=CASCADE_MARKETING,
                                                 max_results=MAX_PEOPLE_PER_COMPANY)
        founder_people = client.waterfall_icp(linkedin_url, cascade=CASCADE_FOUNDER,
                                               max_results=MAX_PEOPLE_PER_COMPANY)

        seen_urls = set()
        seen_names = set()
        results = []

        for cascade_type, people in [("marketing", marketing_people), ("founder", founder_people)]:
            for person in people:
                person_linkedin = person.get("person_linkedin_url", "") or person.get("linkedin_url", "")
                full_name = (person.get("full_name", "") or "").strip().lower()
                if not person_linkedin or person_linkedin in seen_urls:
                    continue
                if full_name and full_name in seen_names:
                    continue
                seen_urls.add(person_linkedin)
                if full_name:
                    seen_names.add(full_name)

                job_title = person.get("job_title", "") or person.get("linkedin_headline", "")
                if not is_valid_title(job_title):
                    continue

                actual_name = person.get("full_name", "")
                first_name, last_name = split_name(actual_name)
                cascade_level = person.get("icp", "")

                results.append({
                    "Domain": domain,
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
            checkpoint.data["stats"]["step2_api_calls"] += 2
            checkpoint.data["stats"]["step2_people_found"] += len(results)
            checkpoint.mark_step2(domain)
            completed[0] += 1
            n = completed[0]

            if results:
                for r in results:
                    writer.writerow(r)
                csv_file.flush()

            if results:
                names = ", ".join(f"{r['Full Name']} ({r['Job Title']})" for r in results[:3])
                extra = f" +{len(results)-3} more" if len(results) > 3 else ""
                log.info(f"[{n}/{len(remaining)}] {domain} — {len(results)} people: {names}{extra}")
            elif n % 50 == 0:
                log.info(f"[{n}/{len(remaining)}] Progress — "
                         f"{checkpoint.data['stats']['step2_people_found']} people found")

            if n % 100 == 0:
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
    log.info(f"  Companies searched:  {len(checkpoint.data['step2_processed'])}")
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
                log.info(f"[{n}/{len(need_email)}] {row['Full Name']} → {email}")
            elif n % 50 == 0:
                log.info(f"[{n}/{len(need_email)}] Progress — "
                         f"{checkpoint.data['stats']['step3_emails_found']} emails found")

            if n % 200 == 0:
                checkpoint.save()
                _rewrite_contacts(rows)

    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = {pool.submit(enrich_one, r): r for r in need_email}
            for future in as_completed(futures):
                if _shutdown_requested:
                    pool.shutdown(wait=False, cancel_futures=True)
                    break
                future.result()
    finally:
        checkpoint.save()
        _rewrite_contacts(rows)

    log.info("=" * 60)
    log.info("STEP 3 COMPLETE")
    log.info(f"  People processed:    {len(checkpoint.data['step3_processed'])}")
    log.info(f"  Emails found:        {checkpoint.data['stats']['step3_emails_found']}")
    log.info(f"  API calls:           {checkpoint.data['stats']['step3_api_calls']}")
    log.info(f"  Output file:         {CONTACTS_FILE}")
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
    parser = argparse.ArgumentParser(description="WooCommerce contact finder pipeline")
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
    domains = load_domains()

    if args.all:
        run_step1(client, domains, checkpoint)
        if not _shutdown_requested:
            run_step2(client, domains, checkpoint)
        if not _shutdown_requested:
            run_step3(client, checkpoint)
    elif args.step == 1:
        run_step1(client, domains, checkpoint)
    elif args.step == 2:
        run_step2(client, domains, checkpoint)
    elif args.step == 3:
        run_step3(client, checkpoint)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
