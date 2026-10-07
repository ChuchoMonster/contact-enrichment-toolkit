#!/usr/bin/env python3
"""
SEO Contact Finder v2 — Bulk search for senior SEO/content/brand/digital marketing
decision-makers using BlitzAPI Waterfall ICP Search + Email Enrichment.

v2 changes: up to 5 people per company, dual cascades (marketing + founder),
broader title matching, headline search re-enabled on Level 3.

Usage:
    python seo_contact_finder.py              # full run
    python seo_contact_finder.py --dry-run    # test CSV parsing without API calls
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

try:
    from googlesearch import search as google_search
except ImportError:
    google_search = None

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
API_BASE = "https://api.blitz-api.ai/api"
REQUEST_DELAY = 3  # seconds between each API call (conservative; limit is 5 RPS)
MAX_PEOPLE_PER_COMPANY = 50  # effectively uncapped

DATA_DIR = Path(os.environ.get("BLITZ_DATA_DIR", Path(__file__).resolve().parent / "data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)
CSV_DIR = DATA_DIR
INPUT_FILES = [f"TierA_Health_Food_Lifestyle_SalesNav_Part9.csv"]

OUTPUT_DIR = DATA_DIR
OUTPUT_FILE = OUTPUT_DIR / "seo_contacts_results.csv"
CHECKPOINT_FILE = OUTPUT_DIR / "seo_contacts_part9_checkpoint.json"
LOG_FILE = OUTPUT_DIR / "seo_contacts_part9.log"

OUTPUT_COLUMNS = [
    "Company", "Root Domain", "Employees", "Vertical",
    "Full Name", "First Name", "Last Name", "Job Title",
    "LinkedIn Profile URL", "Verified Email", "Cascade Level", "Source File",
]

# ---------------------------------------------------------------------------
# Waterfall ICP cascade — senior SEO / content / brand / digital marketing
# ---------------------------------------------------------------------------
CASCADE_MARKETING = [
    # Level 1: C-Suite + VP level across all target functions
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
        "exclude_title": [
            "analyst", "associate", "coordinator", "assistant",
            "intern", "junior",
        ],
        "location": ["WORLD"],
        "include_headline_search": False,
    },
    # Level 2: Director level across all target functions
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
        "exclude_title": [
            "analyst", "associate", "coordinator", "assistant",
            "intern", "junior",
        ],
        "location": ["WORLD"],
        "include_headline_search": False,
    },
    # Level 3: Senior Manager level across all target functions
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
        "exclude_title": [
            "analyst", "associate", "coordinator", "assistant",
            "intern", "junior",
        ],
        "location": ["WORLD"],
        "include_headline_search": False,
    },
    # Level 4: Headline search fallback — broad keyword match
    {
        "include_title": [
            "Marketing", "SEO", "Content", "Digital",
            "Brand", "Growth", "Ecommerce", "Social Media", "Web",
            "Product", "Affiliate", "Partnerships", "Communications",
            "Customer Experience", "Digital Experience", "Website",
        ],
        "exclude_title": [
            "analyst", "associate", "coordinator", "assistant",
            "intern", "junior", "specialist", "designer",
            "technical", "account", "sales",
            "supervisor", "engineer", "developer", "operations",
            "accountant", "HR", "finance", "recruiter",
        ],
        "location": ["WORLD"],
        "include_headline_search": True,
    },
]

# CEO / Founder / Owner cascade
CASCADE_FOUNDER = [
    {
        "include_title": [
            "CEO", "Chief Executive Officer", "Founder", "Co-Founder",
            "Co-founder", "Owner", "Managing Director",
            "General Manager", "Principal",
        ],
        "exclude_title": [
            "Associate", "Assistant", "Analyst",
        ],
        "location": ["WORLD"],
        "include_headline_search": False,
    },
    {
        "include_title": [
            "President",
        ],
        "exclude_title": [
            "Vice President", "VP", "Senior Vice", "EVP",
            "SVP", "Assistant", "Associate",
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
    log.info("Shutdown requested — finishing current company then saving checkpoint…")


signal.signal(signal.SIGINT, _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)

# ---------------------------------------------------------------------------
# Checkpoint manager
# ---------------------------------------------------------------------------

class CheckpointManager:
    def __init__(self, path: Path):
        self.path = path
        self.processed: set[str] = set()
        self.stats = {"total": 0, "found": 0, "emails": 0, "api_calls": 0}
        self._load()

    def _load(self):
        if self.path.exists():
            data = json.loads(self.path.read_text())
            self.processed = set(data.get("processed", []))
            self.stats = data.get("stats", self.stats)
            log.info(f"Checkpoint loaded — {len(self.processed)} companies already processed")

    def save(self):
        data = {
            "processed": sorted(self.processed),
            "stats": self.stats,
        }
        self.path.write_text(json.dumps(data, indent=2))

    def is_processed(self, domain: str) -> bool:
        return domain in self.processed

    def mark_processed(self, domain: str):
        self.processed.add(domain)
        self.stats["total"] += 1


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

    def _post_v2(self, endpoint: str, payload: dict) -> dict | None:
        """POST to v2 endpoints which use https://api.blitz-api.ai (no /api prefix)."""
        if self.dry_run:
            return None
        url = f"https://api.blitz-api.ai{endpoint}"
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
        if data and data.get("linkedin_url"):
            return data["linkedin_url"]
        return None

    def waterfall_icp(self, company_linkedin_url: str, cascade: list | None = None,
                      max_results: int = MAX_PEOPLE_PER_COMPANY) -> list[dict]:
        """Returns a list of person dicts (may be empty)."""
        payload = {
            "company_linkedin_url": company_linkedin_url,
            "max_results": max_results,
            "cascade": cascade or CASCADE_MARKETING,
        }
        data = self._post("/search/waterfall-icp", payload)
        if not data:
            return []
        # Handle different response shapes
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

    def linkedin_to_domain(self, linkedin_url: str) -> str | None:
        """Convert a LinkedIn company URL to its website domain (0.5 credits)."""
        data = self._post("/search/linkedin-url-to-domain", {
            "linkedin_url": linkedin_url,
        })
        if data and data.get("domain"):
            return data["domain"]
        return None

    def google_find_linkedin(self, company_name: str, vertical: str = "") -> str | None:
        """Use Google to find a company's LinkedIn company page. Free, no credits."""
        if google_search is None:
            log.warning("googlesearch-python not installed — skipping Google fallback")
            return None
        query = f'site:linkedin.com/company "{company_name}"'
        if vertical:
            query += f" {vertical}"
        try:
            for url in google_search(query, num_results=5, sleep_interval=2):
                if "linkedin.com/company/" in url and "/posts" not in url and "/jobs" not in url:
                    # Clean up the URL to just the company page
                    match = re.search(r'(https?://(?:www\.)?linkedin\.com/company/[^/?#]+)', url)
                    if match:
                        return match.group(1)
        except Exception as e:
            log.warning(f"Google search failed for '{company_name}': {e}")
        return None

    def search_companies(self, company_name: str, max_results: int = 3) -> list[dict]:
        """Search for companies by name via /v2/search/companies. Returns list of company dicts.
        Note: this endpoint uses the /v2 base path (no /api prefix)."""
        data = self._post_v2("/v2/search/companies", {
            "company": {
                "name": {"include": [company_name]},
            },
            "max_results": max_results,
        })
        if not data:
            return []
        if isinstance(data, list):
            return data
        if isinstance(data, dict) and data.get("results"):
            return data["results"]
        if isinstance(data, dict) and data.get("data"):
            return data["data"]
        return []


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def normalize_domain(domain: str) -> str:
    """Normalize a domain for comparison: strip www., protocol, trailing slashes."""
    d = domain.lower().strip().rstrip("/")
    for prefix in ("https://", "http://", "www."):
        if d.startswith(prefix):
            d = d[len(prefix):]
    return d.rstrip("/")


def domains_match(domain_a: str, domain_b: str) -> bool:
    """Check if two domains refer to the same site."""
    if not domain_a or not domain_b:
        return False
    return normalize_domain(domain_a) == normalize_domain(domain_b)


def normalize_linkedin_url(raw: str) -> str | None:
    """Ensure LinkedIn URL has https://www. prefix."""
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

# If ANY of these appear in the title, reject the person immediately
DISQUALIFYING_KEYWORDS = [
    "assistant", "associate", "intern", "junior", "liaison",
    "secretary", "coordinator", "receptionist", "clerk",
    "assistant to", "liaison to", "executive assistant",
    "executive liaison", "office of the",
]


def _word_match(title_lower: str, keywords: list[str]) -> bool:
    """Check if any keyword appears as a whole word/phrase in the title."""
    for kw in keywords:
        if re.search(r'\b' + re.escape(kw) + r'\b', title_lower):
            return True
    return False


def is_disqualified(title: str) -> bool:
    """Reject titles with support-staff/liaison keywords."""
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
    """Split a full name into (first, last). Best effort."""
    parts = full_name.strip().split()
    if len(parts) == 0:
        return ("", "")
    if len(parts) == 1:
        return (parts[0], "")
    return (parts[0], " ".join(parts[1:]))


def load_companies() -> list[dict]:
    """Load all companies from the input CSVs."""
    companies = []
    for filename in INPUT_FILES:
        filepath = CSV_DIR / filename
        if not filepath.exists():
            log.warning(f"File not found: {filepath}")
            continue
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
                    "source_file": filename,
                })
    log.info(f"Loaded {len(companies)} companies from {len(INPUT_FILES)} files")
    has_linkedin = sum(1 for c in companies if c["linkedin_url"])
    log.info(f"  {has_linkedin} have LinkedIn URLs, {len(companies) - has_linkedin} need domain lookup")
    return companies


# ---------------------------------------------------------------------------
# Main processing
# ---------------------------------------------------------------------------

def process_company(client: BlitzAPIClient, company: dict, checkpoint: CheckpointManager) -> list[dict]:
    """Process a single company. Returns list of result dicts (may be empty)."""
    domain = company["domain"]

    # Step 1: Get LinkedIn URL
    linkedin_url = company["linkedin_url"]
    if not linkedin_url:
        linkedin_url = client.domain_to_linkedin(domain)
        checkpoint.stats["api_calls"] += 1

    # Step 1b: Fallback — Google search for LinkedIn company page (free)
    if not linkedin_url and company["company"]:
        google_url = client.google_find_linkedin(company["company"], company.get("vertical", ""))
        if google_url:
            # Verify the Google result matches our company (0.5 credits)
            verified_domain = client.linkedin_to_domain(google_url)
            checkpoint.stats["api_calls"] += 1
            if verified_domain and domains_match(domain, verified_domain):
                linkedin_url = google_url
                log.info(f"  Google+verified: {company['company']} → {google_url}")
            elif not verified_domain:
                # LinkedIn-to-domain returned nothing — use Google result anyway for small companies
                linkedin_url = google_url
                log.info(f"  Google (unverified): {company['company']} → {google_url}")
            else:
                log.info(f"  Google mismatch: {company['company']} — expected {domain}, got {verified_domain}")

    if not linkedin_url:
        log.debug(f"  No LinkedIn URL for {domain} (all lookup methods failed)")
        return []

    # Step 2a: Marketing cascade (uncapped)
    marketing_people = client.waterfall_icp(linkedin_url, cascade=CASCADE_MARKETING, max_results=MAX_PEOPLE_PER_COMPANY)
    checkpoint.stats["api_calls"] += 1

    # Step 2b: Founder cascade (uncapped)
    founder_people = client.waterfall_icp(linkedin_url, cascade=CASCADE_FOUNDER, max_results=MAX_PEOPLE_PER_COMPANY)
    checkpoint.stats["api_calls"] += 1

    # Combine and deduplicate by LinkedIn URL AND name, marketing people first
    seen_urls = set()
    seen_names = set()
    all_people = []

    for person in marketing_people + founder_people:
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

        # Validate title
        if not is_valid_title(job_title):
            log.debug(f"  Skipping irrelevant title: {job_title} @ {company['company']}")
            continue

        all_people.append(person)
        if len(all_people) >= MAX_PEOPLE_PER_COMPANY:
            break

    # Step 3: Enrich emails for all valid people
    results = []
    for person in all_people:
        full_name = person.get("full_name", "")
        first_name, last_name = split_name(full_name)
        job_title = person.get("job_title", "") or person.get("linkedin_headline", "")
        person_linkedin = person.get("person_linkedin_url", "") or person.get("linkedin_url", "")
        cascade_level = person.get("icp", "")

        email = ""
        if person_linkedin:
            email = client.enrich_email(person_linkedin) or ""
            checkpoint.stats["api_calls"] += 1
            if email:
                checkpoint.stats["emails"] += 1

        checkpoint.stats["found"] += 1
        results.append({
            "Company": company["company"],
            "Root Domain": domain,
            "Employees": company["employees"],
            "Vertical": company["vertical"],
            "Full Name": full_name,
            "First Name": first_name,
            "Last Name": last_name,
            "Job Title": job_title,
            "LinkedIn Profile URL": person_linkedin,
            "Verified Email": email,
            "Cascade Level": cascade_level,
            "Source File": company["source_file"],
        })

    return results


def main():
    parser = argparse.ArgumentParser(description="Find senior SEO/content/brand contacts at companies")
    parser.add_argument("--dry-run", action="store_true", help="Test CSV parsing without API calls")
    parser.add_argument("--test-fallback", type=int, default=0,
                        help="Test company name fallback on N companies without LinkedIn URLs, then stop")
    parser.add_argument("--test-google", type=int, default=0,
                        help="Test Google search fallback on N companies, then stop")
    parser.add_argument("--google-only", action="store_true",
                        help="Only process companies that need Google fallback (no LinkedIn URL from domain lookup)")
    args = parser.parse_args()

    # Load API key
    load_dotenv(Path(__file__).resolve().parent / ".env")
    api_key = os.getenv("BLITZ_API_KEY")
    if not api_key and not args.dry_run:
        log.error("BLITZ_API_KEY not found in .env")
        sys.exit(1)

    client = BlitzAPIClient(api_key or "", dry_run=args.dry_run)
    checkpoint = CheckpointManager(CHECKPOINT_FILE)

    # Pre-flight check
    if not args.dry_run:
        info = client.get_account_info()
        if info:
            log.info(f"Account info: {json.dumps(info, indent=2)}")

    # Load companies
    companies = load_companies()
    remaining = [c for c in companies if not checkpoint.is_processed(c["domain"])]
    log.info(f"{len(remaining)} companies remaining to process (of {len(companies)} total)")

    # Test fallback mode: only run companies without LinkedIn URLs
    if args.test_fallback > 0:
        no_linkedin = [c for c in remaining if not c["linkedin_url"]]
        remaining = no_linkedin[:args.test_fallback]
        log.info(f"TEST FALLBACK MODE: running {len(remaining)} companies without LinkedIn URLs")

    # Test Google mode: test Google search on N companies
    if args.test_google > 0:
        remaining = remaining[:args.test_google]
        log.info(f"TEST GOOGLE MODE: running {len(remaining)} companies")

    if args.dry_run:
        log.info("DRY RUN — showing first 5 companies:")
        for c in remaining[:5]:
            log.info(f"  {c['company']} | {c['domain']} | LinkedIn: {c['linkedin_url'] or 'needs lookup'}")
        log.info("Dry run complete.")
        return

    # Open output CSV (append mode)
    file_exists = OUTPUT_FILE.exists() and OUTPUT_FILE.stat().st_size > 0
    csv_file = open(OUTPUT_FILE, "a", newline="", encoding="utf-8")
    writer = csv.DictWriter(csv_file, fieldnames=OUTPUT_COLUMNS)
    if not file_exists:
        writer.writeheader()

    try:
        for i, company in enumerate(remaining):
            if _shutdown_requested:
                log.info("Shutdown — saving checkpoint and exiting")
                break

            results = process_company(client, company, checkpoint)
            checkpoint.mark_processed(company["domain"])

            if results:
                for result in results:
                    writer.writerow(result)
                csv_file.flush()
                names = ", ".join(f"{r['Full Name']} ({r['Job Title']})" for r in results)
                log.info(
                    f"[{checkpoint.stats['total']}/{len(remaining)}] "
                    f"{company['company']} — {len(results)} people: {names}"
                )
            else:
                if (checkpoint.stats["total"] % 50) == 0 and checkpoint.stats["total"] > 0:
                    log.info(
                        f"[{checkpoint.stats['total']}/{len(remaining)}] "
                        f"Progress — {checkpoint.stats['found']} people found, "
                        f"{checkpoint.stats['emails']} emails, "
                        f"{checkpoint.stats['api_calls']} API calls"
                    )

            # Save checkpoint every 10 companies
            if checkpoint.stats["total"] % 10 == 0:
                checkpoint.save()

    finally:
        csv_file.close()
        checkpoint.save()
        log.info("=" * 60)
        log.info("FINAL SUMMARY")
        log.info(f"  Companies processed: {checkpoint.stats['total']}")
        log.info(f"  People found:        {checkpoint.stats['found']}")
        log.info(f"  Emails obtained:     {checkpoint.stats['emails']}")
        log.info(f"  Total API calls:     {checkpoint.stats['api_calls']}")
        log.info(f"  Output file:         {OUTPUT_FILE}")
        log.info("=" * 60)


if __name__ == "__main__":
    main()
