#!/usr/bin/env python3
"""
Marketplace Pipeline — Firecrawl scraper + 3-step Blitz enrichment for
two-sided marketplace companies.

  Phase 0: Scrape marketplace databases via Firecrawl (extract/map/scrape)
  Step 1:  Domain → LinkedIn URL lookup
  Step 2:  Waterfall ICP search (marketing + founder cascades)
  Step 3:  Email enrichment for all people found

Usage:
    python marketplace_pipeline.py --scrape           # Firecrawl only
    python marketplace_pipeline.py --step 1           # LinkedIn lookup
    python marketplace_pipeline.py --step 2           # Waterfall ICP
    python marketplace_pipeline.py --step 3           # Email enrichment
    python marketplace_pipeline.py --enrichment-only  # Steps 1-3
    python marketplace_pipeline.py --all              # Scrape + all 3 steps
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
BLITZ_API_BASE = "https://api.blitz-api.ai/api"
FIRECRAWL_API_BASE = "https://api.firecrawl.dev/v1"
REQUEST_DELAY = 0.05

DATA_DIR = Path(os.environ.get("BLITZ_DATA_DIR", Path(__file__).resolve().parent / "data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR = DATA_DIR

# Output files — all use mp_ prefix
SCRAPED_COMPANIES_FILE = OUTPUT_DIR / "mp_scraped_companies.csv"
DOMAINS_FILE = OUTPUT_DIR / "mp_domains_for_blitz.csv"
LINKEDIN_LOOKUP_FILE = OUTPUT_DIR / "mp_linkedin_lookup_results.csv"
CONTACTS_FILE = OUTPUT_DIR / "mp_contacts_results.csv"
CHECKPOINT_FILE = OUTPUT_DIR / "mp_checkpoint.json"
LOG_FILE = OUTPUT_DIR / "mp_pipeline.log"

MAX_PEOPLE_PER_COMPANY = 50

CONTACTS_COLUMNS = [
    "Domain", "Company Name", "Full Name", "First Name", "Last Name",
    "Job Title", "LinkedIn Profile URL", "Verified Email",
    "Cascade Level", "Cascade Type",
]

# ---------------------------------------------------------------------------
# Marketplace sources to scrape
# ---------------------------------------------------------------------------
MARKETPLACE_SOURCES = [
    {
        "name": "failory",
        "url": "https://www.failory.com/startups/marketplace",
        "description": "Failory top marketplace startups",
    },
    {
        "name": "yc_companies",
        "url": "https://www.ycombinator.com/companies?tags=Marketplace",
        "description": "Y Combinator marketplace companies",
    },
    {
        "name": "nfx",
        "url": "https://www.nfx.com/company-type/marketplace",
        "description": "NFX marketplace portfolio companies",
    },
    {
        "name": "growthlist",
        "url": "https://growthlist.co/marketplace-startups/",
        "description": "GrowthList marketplace startups database",
    },
]

# Shared extraction schema for Firecrawl /v1/extract
MARKETPLACE_SCHEMA = {
    "type": "object",
    "properties": {
        "companies": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "name": {"type": "string", "description": "Company name"},
                    "domain": {"type": "string", "description": "Company website domain (e.g. betterup.com)"},
                    "description": {"type": "string", "description": "Brief description of what the company does"},
                    "category": {"type": "string", "description": "Marketplace category (e.g. talent, services, goods, B2B)"},
                },
                "required": ["name", "domain"],
            },
        },
    },
    "required": ["companies"],
}

EXTRACT_PROMPT = (
    "Extract all two-sided marketplace companies listed on this page. "
    "For each company, extract the company name, website domain (without http:// or www.), "
    "a brief description, and the marketplace category. "
    "Only include actual marketplace/platform companies, not blog posts or navigation links."
)

# Domain regex for markdown fallback (from Firecrawl project's extract_domains.py)
TLDS = r'(?:com|org|net|edu|gov|io|co|us|me|tv|biz|info|app|dev|ai|tech|store|shop|health|club|pro|live|online|site|xyz|world|design|digital|agency|solutions|consulting|group|services|marketing|ventures|capital|partners|enterprises|management|investments|holdings|properties|realty|ca|uk|au|de|fr|it|es|nl|be|at|ch|pl|ie|nz|se|no|dk|fi|pt|br|mx|jp|kr|in|sg|hk|ae|il|za|ru|cz|hu|ro|bg|hr|sk|si|lt|lv|ee|is|lu|mt|cy|gr)'
DOMAIN_PATTERN = re.compile(r'([\w][\w.-]*\.' + TLDS + r')\b', re.IGNORECASE)

EXCLUDE_DOMAINS = {
    'failory.com', 'ycombinator.com', 'nfx.com', 'growthlist.co',
    'twitter.com', 'x.com', 'facebook.com', 'linkedin.com', 'instagram.com',
    'youtube.com', 'github.com', 'medium.com', 'crunchbase.com',
    'google.com', 'apple.com', 'amazonaws.com', 'cloudfront.net',
    'w3.org', 'schema.org', 'googleapis.com',
}

# ---------------------------------------------------------------------------
# Waterfall ICP cascades (same as bigcommerce_pipeline.py)
# ---------------------------------------------------------------------------
EXCLUDE_TITLES = [
    "analyst", "associate", "coordinator", "assistant",
    "intern", "junior", "student", "part-time", "part time",
]

CASCADE_MARKETING = [
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
            "Marketing Director", "Digital Director", "SEO Director",
            "Content Director", "Brand Director", "Product Director",
            "Creative Director",
            "Sr. Director Marketing", "Senior Director Marketing",
            "Senior Director Content", "Senior Director Digital",
        ],
        "exclude_title": EXCLUDE_TITLES,
        "location": ["WORLD"],
        "include_headline_search": False,
    },
    {
        "include_title": [
            "Senior Manager Marketing", "Senior Manager SEO",
            "Senior Manager Content", "Senior Manager Digital",
            "Senior Manager Brand", "Senior Manager Growth",
            "Senior Marketing Manager", "Senior SEO Manager",
            "Senior Content Manager", "Senior Digital Manager",
            "Senior Digital Marketing Manager", "Senior Brand Manager",
            "Senior Growth Manager", "Senior Product Manager",
            "Sr. Manager Marketing", "Sr. Manager SEO", "Sr. Manager Content",
            "Sr. Marketing Manager", "Sr. SEO Manager", "Sr. Content Manager",
            "Sr. Digital Marketing Manager", "Sr. Product Manager",
        ],
        "exclude_title": EXCLUDE_TITLES,
        "location": ["WORLD"],
        "include_headline_search": False,
    },
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
    log.info("Shutdown requested — finishing current work then saving checkpoint...")


signal.signal(signal.SIGINT, _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)

# ---------------------------------------------------------------------------
# Firecrawl client
# ---------------------------------------------------------------------------


class FirecrawlClient:
    def __init__(self, api_key: str):
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        })

    def extract(self, urls: list[str], prompt: str, schema: dict) -> dict | None:
        """Call /v1/extract for structured data extraction."""
        payload = {
            "urls": urls,
            "prompt": prompt,
            "schema": schema,
        }
        log.info(f"Firecrawl extract: {urls}")
        try:
            resp = self.session.post(
                f"{FIRECRAWL_API_BASE}/extract",
                json=payload,
                timeout=120,
            )
            if resp.status_code != 200:
                log.warning(f"Extract failed ({resp.status_code}): {resp.text[:300]}")
                return None
            data = resp.json()
            if not data.get("success"):
                log.warning(f"Extract unsuccessful: {data.get('error', 'unknown')}")
                return None
            return data.get("data")
        except requests.RequestException as e:
            log.error(f"Extract network error: {e}")
            return None

    def map_site(self, url: str, search: str | None = None, limit: int = 200) -> list[str]:
        """Call /v1/map to discover URLs on a site."""
        payload = {"url": url, "limit": limit}
        if search:
            payload["search"] = search
        log.info(f"Firecrawl map: {url} (search={search})")
        try:
            resp = self.session.post(
                f"{FIRECRAWL_API_BASE}/map",
                json=payload,
                timeout=60,
            )
            if resp.status_code != 200:
                log.warning(f"Map failed ({resp.status_code}): {resp.text[:300]}")
                return []
            data = resp.json()
            if not data.get("success"):
                return []
            return data.get("links", [])
        except requests.RequestException as e:
            log.error(f"Map network error: {e}")
            return []

    def scrape(self, url: str, formats: list[str] | None = None,
               wait_for: int = 5000, timeout: int = 60000) -> dict | None:
        """Call /v1/scrape for markdown/content extraction."""
        payload = {
            "url": url,
            "formats": formats or ["markdown"],
            "onlyMainContent": True,
            "waitFor": wait_for,
            "timeout": timeout,
        }
        log.info(f"Firecrawl scrape: {url}")
        try:
            resp = self.session.post(
                f"{FIRECRAWL_API_BASE}/scrape",
                json=payload,
                timeout=180,
            )
            if resp.status_code != 200:
                log.warning(f"Scrape failed ({resp.status_code}): {resp.text[:300]}")
                return None
            data = resp.json()
            if not data.get("success"):
                return None
            return data.get("data")
        except requests.RequestException as e:
            log.error(f"Scrape network error: {e}")
            return None


# ---------------------------------------------------------------------------
# Domain normalization & deduplication
# ---------------------------------------------------------------------------


def normalize_domain(raw: str) -> str | None:
    """Strip protocol, www, trailing slashes/paths. Return lowercase domain or None."""
    d = raw.strip().lower()
    d = re.sub(r'^https?://', '', d)
    d = re.sub(r'^www\.', '', d)
    d = d.split('/')[0].strip('.')
    if not d or '.' not in d:
        return None
    if any(excl in d for excl in EXCLUDE_DOMAINS):
        return None
    return d


def extract_domains_from_markdown(md_text: str) -> set[str]:
    """Regex-extract domains from markdown content (fallback when extract fails)."""
    domains = set()
    for match in DOMAIN_PATTERN.finditer(md_text):
        domain = match.group(1).lower().strip('.')
        if len(domain) > 4:
            normalized = normalize_domain(domain)
            if normalized:
                domains.add(normalized)
    return domains


# ---------------------------------------------------------------------------
# Marketplace scraper
# ---------------------------------------------------------------------------


class MarketplaceScraper:
    def __init__(self, firecrawl: FirecrawlClient):
        self.fc = firecrawl
        self.companies = []  # list of {name, domain, description, category, source}
        self.seen_domains = set()

    def _add_companies(self, companies: list[dict], source: str):
        """Add companies, deduplicating by normalized domain."""
        added = 0
        for c in companies:
            raw_domain = c.get("domain", "")
            domain = normalize_domain(raw_domain)
            if not domain or domain in self.seen_domains:
                continue
            self.seen_domains.add(domain)
            self.companies.append({
                "name": c.get("name", ""),
                "domain": domain,
                "description": c.get("description", ""),
                "category": c.get("category", ""),
                "source": source,
            })
            added += 1
        return added

    def _fallback_scrape(self, url: str, source: str) -> int:
        """Fallback: scrape as markdown, regex-extract domains."""
        data = self.fc.scrape(url)
        if not data:
            return 0
        md = data.get("markdown", "")
        domains = extract_domains_from_markdown(md)
        companies = [{"name": "", "domain": d} for d in domains]
        added = self._add_companies(companies, source)
        log.info(f"  Fallback extracted {len(domains)} domains, {added} new")
        return added

    def scrape_failory(self) -> int:
        """Scrape Failory marketplace startups list."""
        log.info("--- Scraping Failory marketplace list ---")
        result = self.fc.extract(
            ["https://www.failory.com/startups/marketplace"],
            EXTRACT_PROMPT,
            MARKETPLACE_SCHEMA,
        )
        if result and isinstance(result, dict) and result.get("companies"):
            added = self._add_companies(result["companies"], "failory")
            log.info(f"  Failory extract: {len(result['companies'])} companies, {added} new")
            return added
        # Handle list response (some extract calls return a list)
        if result and isinstance(result, list):
            for item in result:
                if isinstance(item, dict) and item.get("companies"):
                    added = self._add_companies(item["companies"], "failory")
                    log.info(f"  Failory extract: {len(item['companies'])} companies, {added} new")
                    return added

        log.warning("  Failory extract returned no companies, trying fallback...")
        return self._fallback_scrape("https://www.failory.com/startups/marketplace", "failory")

    def scrape_yc(self) -> int:
        """Scrape Y Combinator marketplace companies."""
        log.info("--- Scraping YC Companies (marketplace tag) ---")

        # Try extract first on the filtered page
        result = self.fc.extract(
            ["https://www.ycombinator.com/companies?tags=Marketplace"],
            EXTRACT_PROMPT,
            MARKETPLACE_SCHEMA,
        )
        if result:
            companies = []
            if isinstance(result, dict) and result.get("companies"):
                companies = result["companies"]
            elif isinstance(result, list):
                for item in result:
                    if isinstance(item, dict) and item.get("companies"):
                        companies = item["companies"]
                        break
            if companies:
                added = self._add_companies(companies, "yc")
                log.info(f"  YC extract: {len(companies)} companies, {added} new")
                return added

        # Fallback: map site for marketplace company pages, then extract in batches
        log.warning("  YC extract returned no companies, trying map + batch extract...")
        urls = self.fc.map_site("https://www.ycombinator.com/companies", search="marketplace", limit=300)
        company_urls = [u for u in urls if "/companies/" in u and u.count("/") >= 4
                        and "?" not in u and "#" not in u]
        if not company_urls:
            log.warning("  YC map returned no company URLs, trying markdown fallback...")
            return self._fallback_scrape("https://www.ycombinator.com/companies?tags=Marketplace", "yc")

        log.info(f"  YC map found {len(company_urls)} company pages, extracting in batches...")
        total_added = 0
        batch_size = 10
        for i in range(0, len(company_urls), batch_size):
            if _shutdown_requested:
                break
            batch = company_urls[i:i + batch_size]
            batch_result = self.fc.extract(batch, EXTRACT_PROMPT, MARKETPLACE_SCHEMA)
            if batch_result:
                companies = []
                if isinstance(batch_result, dict) and batch_result.get("companies"):
                    companies = batch_result["companies"]
                elif isinstance(batch_result, list):
                    for item in batch_result:
                        if isinstance(item, dict) and item.get("companies"):
                            companies.extend(item["companies"])
                total_added += self._add_companies(companies, "yc")
            time.sleep(1)  # rate limit courtesy

        log.info(f"  YC batch extract total: {total_added} new companies")
        return total_added

    def scrape_nfx(self) -> int:
        """Scrape NFX marketplace portfolio."""
        log.info("--- Scraping NFX marketplace portfolio ---")
        result = self.fc.extract(
            ["https://www.nfx.com/company-type/marketplace"],
            EXTRACT_PROMPT,
            MARKETPLACE_SCHEMA,
        )
        if result:
            companies = []
            if isinstance(result, dict) and result.get("companies"):
                companies = result["companies"]
            elif isinstance(result, list):
                for item in result:
                    if isinstance(item, dict) and item.get("companies"):
                        companies = item["companies"]
                        break
            if companies:
                added = self._add_companies(companies, "nfx")
                log.info(f"  NFX extract: {len(companies)} companies, {added} new")
                return added

        log.warning("  NFX extract returned no companies, trying fallback...")
        return self._fallback_scrape("https://www.nfx.com/company-type/marketplace", "nfx")

    def scrape_growthlist(self) -> int:
        """Scrape GrowthList marketplace startups."""
        log.info("--- Scraping GrowthList marketplace startups ---")
        result = self.fc.extract(
            ["https://growthlist.co/marketplace-startups/"],
            EXTRACT_PROMPT,
            MARKETPLACE_SCHEMA,
        )
        if result:
            companies = []
            if isinstance(result, dict) and result.get("companies"):
                companies = result["companies"]
            elif isinstance(result, list):
                for item in result:
                    if isinstance(item, dict) and item.get("companies"):
                        companies = item["companies"]
                        break
            if companies:
                added = self._add_companies(companies, "growthlist")
                log.info(f"  GrowthList extract: {len(companies)} companies, {added} new")
                return added

        log.warning("  GrowthList extract returned no companies, trying fallback...")
        return self._fallback_scrape("https://growthlist.co/marketplace-startups/", "growthlist")

    def run_all(self, checkpoint: CheckpointManager) -> list[dict]:
        """Run all scrapers, skipping already-completed sources."""
        scrapers = [
            ("failory", self.scrape_failory),
            ("yc_companies", self.scrape_yc),
            ("nfx", self.scrape_nfx),
            ("growthlist", self.scrape_growthlist),
        ]

        # Reload any previously scraped companies from checkpoint
        prev_companies = checkpoint.data.get("scraped_companies", [])
        if prev_companies:
            for c in prev_companies:
                domain = c.get("domain", "")
                if domain and domain not in self.seen_domains:
                    self.seen_domains.add(domain)
                    self.companies.append(c)
            log.info(f"Loaded {len(self.companies)} previously scraped companies from checkpoint")

        for name, scraper_fn in scrapers:
            if _shutdown_requested:
                break
            if checkpoint.is_source_scraped(name):
                log.info(f"Skipping {name} (already scraped)")
                continue
            try:
                added = scraper_fn()
                checkpoint.mark_source_scraped(name, added)
                checkpoint.data["scraped_companies"] = self.companies
                checkpoint.save()
                log.info(f"  {name}: {added} new companies added (total: {len(self.companies)})")
            except Exception as e:
                log.error(f"  {name} failed: {e}")

        return self.companies


# ---------------------------------------------------------------------------
# Checkpoint manager (extended from bigcommerce_pipeline.py)
# ---------------------------------------------------------------------------


class CheckpointManager:
    def __init__(self, path: Path):
        self.path = path
        self.data = {
            "sources_scraped": {},
            "scraped_companies": [],
            "step1_processed": [],
            "step1_linkedin_found": {},
            "step2_processed": [],
            "step3_processed": [],
            "stats": {
                "scrape_total_companies": 0,
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
            log.info(f"Checkpoint loaded — Sources: {list(self.data['sources_scraped'].keys())}, "
                     f"Step1: {len(self.data['step1_processed'])}, "
                     f"Step2: {len(self.data['step2_processed'])}, "
                     f"Step3: {len(self.data['step3_processed'])}")

    def save(self):
        self.path.write_text(json.dumps(self.data, indent=2))

    # Scrape tracking
    def is_source_scraped(self, name: str) -> bool:
        return name in self.data["sources_scraped"]

    def mark_source_scraped(self, name: str, count: int):
        self.data["sources_scraped"][name] = count
        self.data["stats"]["scrape_total_companies"] = sum(self.data["sources_scraped"].values())

    # Step 1
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

    # Step 2
    def is_step2_processed(self, domain: str) -> bool:
        return domain in self.data["step2_processed"]

    def mark_step2(self, domain: str):
        if domain not in self.data["step2_processed"]:
            self.data["step2_processed"].append(domain)

    # Step 3
    def is_step3_processed(self, person_linkedin: str) -> bool:
        return person_linkedin in self.data["step3_processed"]

    def mark_step3(self, person_linkedin: str):
        if person_linkedin not in self.data["step3_processed"]:
            self.data["step3_processed"].append(person_linkedin)

    def get_linkedin_url(self, domain: str) -> str | None:
        return self.data["step1_linkedin_found"].get(domain)


# ---------------------------------------------------------------------------
# BlitzAPI client (same as bigcommerce_pipeline.py)
# ---------------------------------------------------------------------------


class BlitzAPIClient:
    def __init__(self, api_key: str):
        self.session = requests.Session()
        self.session.headers.update({
            "x-api-key": api_key,
            "Content-Type": "application/json",
        })

    def _post(self, endpoint: str, payload: dict) -> dict | None:
        url = f"{BLITZ_API_BASE}{endpoint}"
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


def load_domains() -> list[dict]:
    """Load domains from mp_domains_for_blitz.csv or mp_scraped_companies.csv."""
    if DOMAINS_FILE.exists():
        domains = []
        with open(DOMAINS_FILE, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                domain = (row.get("domain") or "").strip()
                if domain:
                    domains.append(domain)
        log.info(f"Loaded {len(domains)} domains from {DOMAINS_FILE.name}")
        return domains

    if SCRAPED_COMPANIES_FILE.exists():
        domains = []
        seen = set()
        with open(SCRAPED_COMPANIES_FILE, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                domain = (row.get("domain") or "").strip()
                if domain and domain not in seen:
                    seen.add(domain)
                    domains.append(domain)
        log.info(f"Loaded {len(domains)} domains from {SCRAPED_COMPANIES_FILE.name}")
        return domains

    log.error("No domain file found — run --scrape first")
    return []


# ---------------------------------------------------------------------------
# Phase 0: Scrape
# ---------------------------------------------------------------------------


def run_scrape(firecrawl: FirecrawlClient, checkpoint: CheckpointManager):
    log.info("=" * 60)
    log.info("PHASE 0: Scraping marketplace databases via Firecrawl")
    log.info("=" * 60)

    scraper = MarketplaceScraper(firecrawl)
    companies = scraper.run_all(checkpoint)

    if not companies:
        log.warning("No companies found from any source!")
        return

    # Write full scraped companies CSV
    with open(SCRAPED_COMPANIES_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["name", "domain", "description", "category", "source"])
        writer.writeheader()
        for c in companies:
            writer.writerow(c)

    # Write domain-only CSV for Blitz pipeline
    with open(DOMAINS_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["domain"])
        writer.writeheader()
        for c in companies:
            writer.writerow({"domain": c["domain"]})

    log.info("=" * 60)
    log.info("PHASE 0 COMPLETE")
    log.info(f"  Total unique companies: {len(companies)}")
    log.info(f"  Sources: {dict(checkpoint.data['sources_scraped'])}")
    log.info(f"  Full results:  {SCRAPED_COMPANIES_FILE}")
    log.info(f"  Domain list:   {DOMAINS_FILE}")
    log.info("=" * 60)


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
            log.info(f"[{n}/{len(remaining)}] {domain} -> {linkedin_url}")
        elif n % 50 == 0:
            with lock:
                log.info(f"[{n}/{len(remaining)}] Progress — "
                         f"{checkpoint.data['stats']['step1_found']} found, "
                         f"{checkpoint.data['stats']['step1_not_found']} not found")

        if n % 100 == 0:
            with lock:
                checkpoint.save()

    with ThreadPoolExecutor(max_workers=3) as pool:
        futures = {pool.submit(lookup_one, d): d for d in remaining}
        for future in as_completed(futures):
            if _shutdown_requested:
                pool.shutdown(wait=False, cancel_futures=True)
                break
            future.result()

    checkpoint.save()

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

    # Load company name lookup from scraped data
    company_names = {}
    if SCRAPED_COMPANIES_FILE.exists():
        with open(SCRAPED_COMPANIES_FILE, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                company_names[row.get("domain", "")] = row.get("name", "")

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
                    "Company Name": company_names.get(domain, ""),
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
                log.info(f"[{n}/{len(need_email)}] {row['Full Name']} -> {email}")
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

    # Write verified-only file
    verified_file = OUTPUT_DIR / "mp_contacts_verified_emails.csv"
    verified = [r for r in rows if r.get("Verified Email")]
    with open(verified_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CONTACTS_COLUMNS)
        writer.writeheader()
        for r in verified:
            writer.writerow(r)

    log.info("=" * 60)
    log.info("STEP 3 COMPLETE")
    log.info(f"  People processed:    {len(checkpoint.data['step3_processed'])}")
    log.info(f"  Emails found:        {checkpoint.data['stats']['step3_emails_found']}")
    log.info(f"  API calls:           {checkpoint.data['stats']['step3_api_calls']}")
    log.info(f"  All contacts:        {CONTACTS_FILE}")
    log.info(f"  Verified only:       {verified_file}")
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
    parser = argparse.ArgumentParser(description="Marketplace company scraper + Blitz enrichment pipeline")
    parser.add_argument("--scrape", action="store_true", help="Phase 0: Firecrawl scrape only")
    parser.add_argument("--step", type=int, choices=[1, 2, 3],
                        help="Run a specific step (1=LinkedIn lookup, 2=waterfall, 3=email)")
    parser.add_argument("--enrichment-only", action="store_true", help="Run steps 1-3 (skip scrape)")
    parser.add_argument("--all", action="store_true", help="Run scrape + all 3 enrichment steps")
    args = parser.parse_args()

    load_dotenv(Path(__file__).resolve().parent / ".env")

    firecrawl_key = os.getenv("FIRECRAWL_API_KEY")
    blitz_key = os.getenv("BLITZ_API_KEY")

    checkpoint = CheckpointManager(CHECKPOINT_FILE)

    if args.scrape or args.all:
        if not firecrawl_key:
            log.error("FIRECRAWL_API_KEY not found in .env")
            sys.exit(1)
        firecrawl = FirecrawlClient(firecrawl_key)
        run_scrape(firecrawl, checkpoint)

    if args.all or args.enrichment_only or args.step:
        if not blitz_key:
            log.error("BLITZ_API_KEY not found in .env")
            sys.exit(1)
        client = BlitzAPIClient(blitz_key)
        domains = load_domains()

        if not domains:
            log.error("No domains to process")
            sys.exit(1)

        if args.all or args.enrichment_only:
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

    if not any([args.scrape, args.step, args.enrichment_only, args.all]):
        parser.print_help()


if __name__ == "__main__":
    main()
