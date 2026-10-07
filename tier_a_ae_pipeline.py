#!/usr/bin/env python3
"""
TierA Art & Entertainment Pipeline — 3-step contact finder:
  Step 0: Endpoint comparison (domain-to-linkedin vs company enrich) on first N companies
  Step 1: LinkedIn URL lookup for companies missing LinkedIn URLs
  Step 2: Waterfall ICP search (marketing + founder cascades)
  Step 3: Email enrichment for all people found

Usage:
    python tier_a_ae_pipeline.py --dry-run              # test CSV parsing
    python tier_a_ae_pipeline.py --test-endpoints 200   # compare endpoints on 200 companies
    python tier_a_ae_pipeline.py --step 1               # run step 1 (LinkedIn lookup)
    python tier_a_ae_pipeline.py --step 2               # run step 2 (waterfall ICP)
    python tier_a_ae_pipeline.py --step 3               # run step 3 (email enrichment)
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
CSV_DIR = DATA_DIR
INPUT_FILE = "TierA_Art_Entertainment.csv"

OUTPUT_DIR = DATA_DIR
ENDPOINT_TEST_FILE = OUTPUT_DIR / "ae_endpoint_test_results.csv"
LINKEDIN_LOOKUP_FILE = OUTPUT_DIR / "ae_linkedin_lookup_results.csv"
CONTACTS_FILE = OUTPUT_DIR / "ae_contacts_results.csv"
CHECKPOINT_FILE = OUTPUT_DIR / "ae_checkpoint.json"
LOG_FILE = OUTPUT_DIR / "ae_pipeline.log"

MAX_PEOPLE_PER_COMPANY = 50

CONTACTS_COLUMNS = [
    "Company", "Root Domain", "Employees", "Vertical",
    "Full Name", "First Name", "Last Name", "Job Title",
    "LinkedIn Profile URL", "Verified Email", "Cascade Level",
    "Cascade Type",
]

# ---------------------------------------------------------------------------
# Waterfall ICP cascades
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
    log.info("Shutdown requested — finishing current company then saving checkpoint…")


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
            "step1_linkedin_found": {},  # domain -> linkedin_url
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
            # Convert lists to sets for fast lookup, keep as lists in data
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

    def _get(self, endpoint: str) -> dict | None:
        if self.dry_run:
            return None
        url = f"{API_BASE}{endpoint}"
        try:
            resp = self.session.get(url, timeout=30)
            if resp.status_code == 200:
                return resp.json()
            log.warning(f"GET {endpoint} returned {resp.status_code}")
        except requests.RequestException as e:
            log.warning(f"GET {endpoint} failed: {e}")
        return None

    def get_account_info(self) -> dict | None:
        return self._get("/v2/account/key-info")

    def domain_to_linkedin(self, domain: str) -> str | None:
        data = self._post("/search/domain-to-linkedin-company", {"domain": domain})
        if data:
            return data.get("company_linkedin_url") or data.get("linkedin_url") or None
        return None

    def enrich_company(self, domain: str) -> str | None:
        """Use /enrichment/company to get LinkedIn URL from domain."""
        data = self._post("/enrichment/company", {"domain": domain})
        if data:
            li = data.get("company_linkedin_url") or data.get("linkedin_url") or ""
            if "linkedin.com/company/" in li:
                return li
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

def normalize_linkedin_url(raw: str) -> str | None:
    if not raw or "linkedin.com/company/" not in raw:
        return None
    raw = raw.strip()
    if not raw.startswith("http"):
        raw = "https://www." + raw
    return raw


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


def load_companies() -> list[dict]:
    """Load all companies from the input CSV."""
    filepath = CSV_DIR / INPUT_FILE
    companies = []
    with open(filepath, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            domain = (row.get("Root Domain") or row.get("Primary Domain") or "").strip()
            if not domain:
                continue
            companies.append({
                "domain": domain,
                "company": (row.get("Company") or "").strip(),
                "linkedin_url": normalize_linkedin_url(row.get("LinkedIn", "")),
                "employees": (row.get("Employees") or "").strip(),
                "vertical": (row.get("Vertical") or "").strip(),
            })
    log.info(f"Loaded {len(companies)} companies from {INPUT_FILE}")
    has_linkedin = sum(1 for c in companies if c["linkedin_url"])
    log.info(f"  {has_linkedin} have LinkedIn URLs, {len(companies) - has_linkedin} need lookup")
    return companies


# ---------------------------------------------------------------------------
# Step 0: Endpoint comparison test
# ---------------------------------------------------------------------------

def run_endpoint_test(client: BlitzAPIClient, companies: list[dict], n: int):
    """Test both domain-to-linkedin and company enrich on first N companies without LinkedIn URLs."""
    no_linkedin = [c for c in companies if not c["linkedin_url"]][:n]
    log.info(f"ENDPOINT TEST: Testing {len(no_linkedin)} companies with both endpoints")

    results = []
    for i, company in enumerate(no_linkedin):
        if _shutdown_requested:
            break

        domain = company["domain"]
        log.info(f"[{i+1}/{len(no_linkedin)}] Testing {company['company']} ({domain})")

        # Test domain-to-linkedin
        d2l_result = client.domain_to_linkedin(domain)
        client.session  # just to keep reference
        log.info(f"  domain-to-linkedin: {d2l_result or 'NO RESULT'}")

        # Test company enrich
        ce_result = client.enrich_company(domain)
        log.info(f"  company-enrich:     {ce_result or 'NO RESULT'}")

        results.append({
            "Company": company["company"],
            "Domain": domain,
            "domain_to_linkedin": d2l_result or "",
            "company_enrich": ce_result or "",
            "both_match": "YES" if (d2l_result and ce_result and d2l_result == ce_result) else "",
            "d2l_only": "YES" if (d2l_result and not ce_result) else "",
            "ce_only": "YES" if (ce_result and not d2l_result) else "",
            "neither": "YES" if (not d2l_result and not ce_result) else "",
        })

    # Write results
    with open(ENDPOINT_TEST_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "Company", "Domain", "domain_to_linkedin", "company_enrich",
            "both_match", "d2l_only", "ce_only", "neither",
        ])
        writer.writeheader()
        for r in results:
            writer.writerow(r)

    # Summary
    d2l_hits = sum(1 for r in results if r["domain_to_linkedin"])
    ce_hits = sum(1 for r in results if r["company_enrich"])
    both = sum(1 for r in results if r["both_match"] == "YES")
    d2l_only = sum(1 for r in results if r["d2l_only"] == "YES")
    ce_only = sum(1 for r in results if r["ce_only"] == "YES")
    neither = sum(1 for r in results if r["neither"] == "YES")

    log.info("=" * 60)
    log.info("ENDPOINT TEST RESULTS")
    log.info(f"  Companies tested:    {len(results)}")
    log.info(f"  domain-to-linkedin:  {d2l_hits} hits ({d2l_hits/len(results)*100:.1f}%)")
    log.info(f"  company-enrich:      {ce_hits} hits ({ce_hits/len(results)*100:.1f}%)")
    log.info(f"  Both found same:     {both}")
    log.info(f"  d2l only:            {d2l_only}")
    log.info(f"  enrich only:         {ce_only}")
    log.info(f"  Neither found:       {neither}")
    log.info(f"  API calls:           {len(results) * 2}")
    log.info(f"  Results saved to:    {ENDPOINT_TEST_FILE}")
    log.info("=" * 60)


# ---------------------------------------------------------------------------
# Step 1: LinkedIn URL lookup
# ---------------------------------------------------------------------------

def run_step1(client: BlitzAPIClient, companies: list[dict], checkpoint: CheckpointManager,
              endpoint: str = "domain-to-linkedin"):
    """Look up LinkedIn URLs for companies that don't have them. Uses 3 concurrent threads."""
    need_lookup = [c for c in companies if not c["linkedin_url"]]
    remaining = [c for c in need_lookup if not checkpoint.is_step1_processed(c["domain"])]
    log.info(f"STEP 1: {len(remaining)} companies need LinkedIn lookup (of {len(need_lookup)} total without URLs)")

    lock = threading.Lock()
    completed = [0]  # mutable counter for threads

    def lookup_one(company):
        if _shutdown_requested:
            return
        domain = company["domain"]
        if endpoint == "domain-to-linkedin":
            linkedin_url = client.domain_to_linkedin(domain)
        else:
            linkedin_url = client.enrich_company(domain)

        with lock:
            checkpoint.data["stats"]["step1_api_calls"] += 1
            checkpoint.mark_step1(domain, linkedin_url)
            completed[0] += 1
            n = completed[0]

        if linkedin_url:
            log.info(f"[{n}/{len(remaining)}] {company['company']} → {linkedin_url}")
        elif n % 50 == 0:
            with lock:
                log.info(f"[{n}/{len(remaining)}] Progress — "
                         f"{checkpoint.data['stats']['step1_found']} found, "
                         f"{checkpoint.data['stats']['step1_not_found']} not found")

        if n % 100 == 0:
            with lock:
                checkpoint.save()

    with ThreadPoolExecutor(max_workers=3) as pool:
        futures = {pool.submit(lookup_one, c): c for c in remaining}
        for future in as_completed(futures):
            if _shutdown_requested:
                pool.shutdown(wait=False, cancel_futures=True)
                break
            future.result()  # raise any exceptions

    checkpoint.save()

    # Write intermediate results
    with open(LINKEDIN_LOOKUP_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["Company", "Domain", "LinkedIn URL", "Source"])
        writer.writeheader()
        for c in companies:
            li = c["linkedin_url"] or checkpoint.get_linkedin_url(c["domain"]) or ""
            source = "csv" if c["linkedin_url"] else ("api" if li else "not_found")
            writer.writerow({
                "Company": c["company"],
                "Domain": c["domain"],
                "LinkedIn URL": li,
                "Source": source,
            })

    total_with_li = sum(1 for c in companies if c["linkedin_url"] or checkpoint.get_linkedin_url(c["domain"]))
    log.info("=" * 60)
    log.info("STEP 1 COMPLETE")
    log.info(f"  API calls:           {checkpoint.data['stats']['step1_api_calls']}")
    log.info(f"  LinkedIn URLs found: {checkpoint.data['stats']['step1_found']}")
    log.info(f"  Not found:           {checkpoint.data['stats']['step1_not_found']}")
    log.info(f"  Total with LinkedIn: {total_with_li} / {len(companies)}")
    log.info(f"  Lookup results:      {LINKEDIN_LOOKUP_FILE}")
    log.info("=" * 60)


# ---------------------------------------------------------------------------
# Step 2: Waterfall ICP search
# ---------------------------------------------------------------------------

def run_step2(client: BlitzAPIClient, companies: list[dict], checkpoint: CheckpointManager):
    """Run waterfall ICP on all companies with LinkedIn URLs. Uses 2 concurrent threads
    (each thread does 2 API calls per company, so 2 threads ≈ 3-4 RPS)."""
    # Build list of companies with LinkedIn URLs (from CSV or step 1)
    companies_with_li = []
    for c in companies:
        li = c["linkedin_url"] or checkpoint.get_linkedin_url(c["domain"])
        if li:
            companies_with_li.append({**c, "linkedin_url_resolved": li})

    remaining = [c for c in companies_with_li if not checkpoint.is_step2_processed(c["domain"])]
    log.info(f"STEP 2: {len(remaining)} companies to search (of {len(companies_with_li)} with LinkedIn URLs)")

    # Open output CSV (append mode)
    file_exists = CONTACTS_FILE.exists() and CONTACTS_FILE.stat().st_size > 0
    csv_file = open(CONTACTS_FILE, "a", newline="", encoding="utf-8")
    writer = csv.DictWriter(csv_file, fieldnames=CONTACTS_COLUMNS)
    if not file_exists:
        writer.writeheader()

    lock = threading.Lock()
    completed = [0]

    def process_one(company):
        if _shutdown_requested:
            return

        domain = company["domain"]
        linkedin_url = company["linkedin_url_resolved"]

        # Marketing cascade
        marketing_people = client.waterfall_icp(linkedin_url, cascade=CASCADE_MARKETING,
                                                 max_results=MAX_PEOPLE_PER_COMPANY)
        # Founder cascade
        founder_people = client.waterfall_icp(linkedin_url, cascade=CASCADE_FOUNDER,
                                               max_results=MAX_PEOPLE_PER_COMPANY)

        # Deduplicate and validate
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
                    "Company": company["company"],
                    "Root Domain": domain,
                    "Employees": company["employees"],
                    "Vertical": company["vertical"],
                    "Full Name": actual_name,
                    "First Name": first_name,
                    "Last Name": last_name,
                    "Job Title": job_title,
                    "LinkedIn Profile URL": person_linkedin,
                    "Verified Email": "",  # filled in step 3
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
                log.info(f"[{n}/{len(remaining)}] {company['company']} — {len(results)} people: {names}{extra}")
            elif n % 50 == 0:
                log.info(f"[{n}/{len(remaining)}] Progress — "
                         f"{checkpoint.data['stats']['step2_people_found']} people found, "
                         f"{checkpoint.data['stats']['step2_api_calls']} API calls")

            if n % 100 == 0:
                checkpoint.save()

    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = {pool.submit(process_one, c): c for c in remaining}
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
    """Enrich emails for all people found in step 2. Uses 3 concurrent threads."""
    if not CONTACTS_FILE.exists():
        log.error("No contacts file found — run step 2 first")
        return

    # Read all rows
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

    log.info("=" * 60)
    log.info("STEP 3 COMPLETE")
    log.info(f"  People processed:    {len(checkpoint.data['step3_processed'])}")
    log.info(f"  Emails found:        {checkpoint.data['stats']['step3_emails_found']}")
    log.info(f"  API calls:           {checkpoint.data['stats']['step3_api_calls']}")
    log.info(f"  Output file:         {CONTACTS_FILE}")
    log.info("=" * 60)


def _rewrite_contacts(rows: list[dict]):
    """Rewrite the contacts CSV with updated data."""
    with open(CONTACTS_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CONTACTS_COLUMNS)
        writer.writeheader()
        for r in rows:
            writer.writerow(r)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="TierA Art & Entertainment contact finder pipeline")
    parser.add_argument("--dry-run", action="store_true", help="Test CSV parsing without API calls")
    parser.add_argument("--test-endpoints", type=int, default=0,
                        help="Compare both endpoints on N companies")
    parser.add_argument("--step", type=int, choices=[1, 2, 3],
                        help="Run a specific step (1=LinkedIn lookup, 2=waterfall, 3=email)")
    parser.add_argument("--endpoint", default="domain-to-linkedin",
                        choices=["domain-to-linkedin", "company-enrich"],
                        help="Which endpoint to use for step 1 (default: domain-to-linkedin)")
    args = parser.parse_args()

    # Load API key
    load_dotenv(Path(__file__).resolve().parent / ".env")
    api_key = os.getenv("BLITZ_API_KEY")
    if not api_key and not args.dry_run:
        log.error("BLITZ_API_KEY not found in .env")
        sys.exit(1)

    client = BlitzAPIClient(api_key or "", dry_run=args.dry_run)
    checkpoint = CheckpointManager(CHECKPOINT_FILE)

    # Pre-flight
    if not args.dry_run:
        info = client.get_account_info()
        if info:
            log.info(f"Account info: {json.dumps(info, indent=2)}")

    # Load companies
    companies = load_companies()

    if args.dry_run:
        log.info("DRY RUN — showing first 10 companies:")
        for c in companies[:10]:
            log.info(f"  {c['company']} | {c['domain']} | LinkedIn: {c['linkedin_url'] or 'needs lookup'}")
        no_li = sum(1 for c in companies if not c["linkedin_url"])
        log.info(f"\nTotal: {len(companies)} companies, {len(companies)-no_li} have LinkedIn, {no_li} need lookup")
        return

    if args.test_endpoints > 0:
        run_endpoint_test(client, companies, args.test_endpoints)
        return

    if args.step == 1:
        run_step1(client, companies, checkpoint, endpoint=args.endpoint)
    elif args.step == 2:
        run_step2(client, companies, checkpoint)
    elif args.step == 3:
        run_step3(client, checkpoint)
    else:
        log.error("Please specify --step 1, 2, or 3 (or --test-endpoints N, or --dry-run)")


if __name__ == "__main__":
    main()
