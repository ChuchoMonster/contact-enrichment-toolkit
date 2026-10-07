#!/usr/bin/env python3
"""
Website Team Scraper — For companies without LinkedIn company pages, scrape their
websites for team/about/leadership pages to find founders, CEOs, and key people.

Then search for their LinkedIn profiles and enrich emails via BlitzAPI.

Usage:
    python website_scraper.py              # full run
    python website_scraper.py --test 20    # test on 20 companies
    python website_scraper.py --dry-run    # just show what would be scraped
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
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

try:
    from duckduckgo_search import DDGS
except ImportError:
    DDGS = None

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
API_BASE = "https://api.blitz-api.ai/api"
REQUEST_DELAY = 3
SCRAPE_DELAY = 2  # seconds between website requests (be polite)
SEARCH_DELAY = 3  # seconds between DuckDuckGo searches

DATA_DIR = Path(os.environ.get("BLITZ_DATA_DIR", Path(__file__).resolve().parent / "data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR = DATA_DIR
OUTPUT_FILE = OUTPUT_DIR / "website_scraper_results.csv"
CHECKPOINT_FILE = OUTPUT_DIR / "website_scraper_checkpoint.json"
LOG_FILE = OUTPUT_DIR / "website_scraper.log"

CSV_DIR = DATA_DIR
INPUT_FILE = "TierA_Health_Food_Lifestyle_SalesNav_Part7.csv"

OUTPUT_COLUMNS = [
    "Company", "Root Domain", "Employees", "Vertical",
    "Full Name", "First Name", "Last Name", "Job Title",
    "LinkedIn Profile URL", "Verified Email", "Source Page", "Source File",
]

# Common team/about page paths to try
TEAM_PATHS = [
    "/about", "/about-us", "/about-us/", "/about/",
    "/team", "/our-team", "/the-team", "/team/",
    "/leadership", "/leadership-team",
    "/staff", "/our-staff",
    "/people", "/our-people",
    "/who-we-are", "/meet-the-team",
    "/founders", "/our-founders",
    "/company", "/company/about",
]

# Title patterns that indicate a founder/leader
LEADER_TITLE_PATTERNS = [
    r'\b(?:CEO|Chief Executive Officer)\b',
    r'\b(?:Founder|Co-Founder|Co-founder|Cofounder)\b',
    r'\b(?:Owner|Co-Owner|Co-owner)\b',
    r'\b(?:President)\b',
    r'\b(?:Managing Director)\b',
    r'\b(?:Principal)\b',
    r'\b(?:General Manager)\b',
    r'\b(?:CMO|Chief Marketing Officer)\b',
    r'\b(?:VP|Vice President)\b.*(?:Marketing|Digital|Content|Brand|Growth|SEO)',
    r'\b(?:Director)\b.*(?:Marketing|Digital|Content|Brand|Growth|SEO|Web)',
    r'\b(?:Head of)\b.*(?:Marketing|Digital|Content|Brand|Growth|SEO|Web)',
]

# Disqualifying patterns
DISQUALIFY_PATTERNS = [
    r'\b(?:assistant|associate|intern|junior|liaison|secretary|coordinator|receptionist)\b',
    r'\bto the\b',
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
        self.processed: set[str] = set()
        self.stats = {
            "total": 0, "websites_reached": 0, "team_pages_found": 0,
            "people_extracted": 0, "linkedin_found": 0, "emails_found": 0,
        }
        self._load()

    def _load(self):
        if self.path.exists():
            data = json.loads(self.path.read_text())
            self.processed = set(data.get("processed", []))
            self.stats = data.get("stats", self.stats)
            log.info(f"Checkpoint loaded — {len(self.processed)} domains already processed")

    def save(self):
        data = {"processed": sorted(self.processed), "stats": self.stats}
        self.path.write_text(json.dumps(data, indent=2))

    def is_processed(self, domain: str) -> bool:
        return domain in self.processed

    def mark_processed(self, domain: str):
        self.processed.add(domain)
        self.stats["total"] += 1


# ---------------------------------------------------------------------------
# Website scraper
# ---------------------------------------------------------------------------

class WebsiteScraper:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/122.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
        })

    def fetch_page(self, url: str, timeout: int = 10) -> str | None:
        """Fetch a page and return its HTML, or None on failure."""
        try:
            resp = self.session.get(url, timeout=timeout, allow_redirects=True)
            if resp.status_code == 200 and "text/html" in resp.headers.get("content-type", ""):
                return resp.text
        except requests.RequestException:
            pass
        return None

    def find_team_page(self, domain: str) -> tuple[str | None, str | None]:
        """Try to find a team/about page on the website. Returns (url, html) or (None, None)."""
        base_url = f"https://{domain}"

        # First, fetch the homepage and look for team/about links
        homepage_html = self.fetch_page(base_url)
        time.sleep(SCRAPE_DELAY)

        if homepage_html:
            soup = BeautifulSoup(homepage_html, "html.parser")
            # Look for links that suggest team/about pages
            for a_tag in soup.find_all("a", href=True):
                href = a_tag["href"].lower()
                text = a_tag.get_text(strip=True).lower()
                if any(kw in href or kw in text for kw in [
                    "about", "team", "leadership", "staff", "people",
                    "founder", "who-we-are", "meet", "our-story"
                ]):
                    full_url = urljoin(base_url, a_tag["href"])
                    # Only follow links on the same domain
                    if urlparse(full_url).netloc.replace("www.", "") == domain.replace("www.", ""):
                        html = self.fetch_page(full_url)
                        time.sleep(SCRAPE_DELAY)
                        if html:
                            return full_url, html

        # Fallback: try common paths directly
        for path in TEAM_PATHS:
            url = f"{base_url}{path}"
            html = self.fetch_page(url)
            time.sleep(SCRAPE_DELAY)
            if html:
                return url, html

        # Last resort: check homepage itself for team info
        if homepage_html:
            return base_url, homepage_html

        return None, None

    def extract_people(self, html: str, company_name: str) -> list[dict]:
        """Extract people with titles from HTML. Returns list of {name, title} dicts."""
        soup = BeautifulSoup(html, "html.parser")
        people = []
        seen_names = set()

        # Strategy 1: Look for structured team sections
        # Common patterns: <div class="team-member">, <div class="bio">, etc.
        team_selectors = [
            {"class_": re.compile(r"team|staff|member|bio|leader|founder|executive|people", re.I)},
        ]

        for selector in team_selectors:
            for container in soup.find_all(["div", "section", "article", "li"], **selector):
                person = self._extract_person_from_container(container)
                if person and person["name"].lower() not in seen_names:
                    seen_names.add(person["name"].lower())
                    people.append(person)

        # Strategy 2: Look for patterns like "Name, Title" or "Name - Title" in text
        if not people:
            text = soup.get_text(separator="\n")
            people = self._extract_people_from_text(text, seen_names)

        # Strategy 3: Look for schema.org / JSON-LD structured data
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                data = json.loads(script.string)
                if isinstance(data, list):
                    for item in data:
                        person = self._extract_person_from_jsonld(item)
                        if person and person["name"].lower() not in seen_names:
                            seen_names.add(person["name"].lower())
                            people.append(person)
                elif isinstance(data, dict):
                    person = self._extract_person_from_jsonld(data)
                    if person and person["name"].lower() not in seen_names:
                        seen_names.add(person["name"].lower())
                        people.append(person)
            except (json.JSONDecodeError, TypeError):
                pass

        # Filter to only leaders/founders
        return [p for p in people if self._is_relevant_title(p.get("title", ""))]

    def _extract_person_from_container(self, container) -> dict | None:
        """Try to extract a person's name and title from an HTML container."""
        # Look for heading tags (h2, h3, h4) for the name
        name = None
        title = None

        for tag in container.find_all(["h2", "h3", "h4", "h5", "strong", "b"]):
            text = tag.get_text(strip=True)
            if self._looks_like_name(text) and not name:
                name = text
                break

        # Look for title in <p>, <span>, or text with title keywords
        for tag in container.find_all(["p", "span", "div", "em", "small"]):
            text = tag.get_text(strip=True)
            if self._looks_like_title(text) and not title:
                title = text
                break

        if name and title:
            return {"name": self._clean_name(name), "title": self._clean_title(title)}
        return None

    def _extract_people_from_text(self, text: str, seen_names: set) -> list[dict]:
        """Extract people from raw text using pattern matching."""
        people = []
        lines = text.split("\n")

        for i, line in enumerate(lines):
            line = line.strip()
            if not line or len(line) > 200:
                continue

            # Pattern: "Name - Title" or "Name | Title" or "Name, Title"
            for sep in [" - ", " – ", " — ", " | ", ", "]:
                if sep in line:
                    parts = line.split(sep, 1)
                    if (len(parts) == 2
                        and self._looks_like_name(parts[0].strip())
                        and self._looks_like_title(parts[1].strip())):
                        name = self._clean_name(parts[0].strip())
                        if name.lower() not in seen_names:
                            seen_names.add(name.lower())
                            people.append({
                                "name": name,
                                "title": self._clean_title(parts[1].strip()),
                            })
                        break

            # Pattern: Name on one line, title on next line
            if (self._looks_like_name(line) and i + 1 < len(lines)
                and self._looks_like_title(lines[i + 1].strip())):
                name = self._clean_name(line)
                if name.lower() not in seen_names:
                    seen_names.add(name.lower())
                    people.append({
                        "name": name,
                        "title": self._clean_title(lines[i + 1].strip()),
                    })

        return people

    def _extract_person_from_jsonld(self, data: dict) -> dict | None:
        """Extract a person from JSON-LD structured data."""
        if data.get("@type") == "Person":
            name = data.get("name", "")
            title = data.get("jobTitle", "") or data.get("description", "")
            if name and title:
                return {"name": self._clean_name(name), "title": self._clean_title(title)}
        # Check for Organization with founder/employee
        if data.get("@type") == "Organization":
            for key in ["founder", "employee", "member"]:
                val = data.get(key)
                if isinstance(val, dict) and val.get("name"):
                    return {
                        "name": self._clean_name(val["name"]),
                        "title": self._clean_title(val.get("jobTitle", key.capitalize())),
                    }
                if isinstance(val, list):
                    for item in val[:5]:
                        if isinstance(item, dict) and item.get("name"):
                            return {
                                "name": self._clean_name(item["name"]),
                                "title": self._clean_title(item.get("jobTitle", key.capitalize())),
                            }
        return None

    def _looks_like_name(self, text: str) -> bool:
        """Heuristic: does this look like a person's name?"""
        text = text.strip()
        if not text or len(text) < 3 or len(text) > 60:
            return False
        words = text.split()
        if len(words) < 2 or len(words) > 5:
            return False
        # Names should be mostly alphabetic (allow periods, hyphens, apostrophes)
        cleaned = re.sub(r"[.\-'']", "", text)
        if not all(c.isalpha() or c.isspace() for c in cleaned):
            return False
        # At least first word should be capitalized
        if not words[0][0].isupper():
            return False
        # Reject common non-name phrases
        lower = text.lower()
        non_names = ["read more", "learn more", "view all", "our team", "about us",
                     "contact us", "get started", "sign up", "log in", "meet the",
                     "follow us", "join us", "see more", "board member", "board of",
                     "advisory board", "leadership team", "our staff", "our founder",
                     "executive team", "management team", "senior leadership"]
        if any(nn in lower for nn in non_names):
            return False
        return True

    def _looks_like_title(self, text: str) -> bool:
        """Heuristic: does this look like a job title?"""
        text = text.strip()
        if not text or len(text) < 3 or len(text) > 100:
            return False
        lower = text.lower()
        title_keywords = [
            "ceo", "cto", "cfo", "cmo", "coo", "chief", "founder", "co-founder",
            "owner", "president", "director", "vp ", "vice president",
            "head of", "manager", "lead", "principal", "partner",
            "marketing", "digital", "content", "brand", "growth", "seo",
        ]
        return any(kw in lower for kw in title_keywords)

    def _is_relevant_title(self, title: str) -> bool:
        """Check if the title is a senior/founder role we care about."""
        if not title:
            return False
        lower = title.lower()
        # Check disqualifying patterns first
        for pat in DISQUALIFY_PATTERNS:
            if re.search(pat, lower):
                return False
        # Check for relevant patterns
        for pat in LEADER_TITLE_PATTERNS:
            if re.search(pat, lower, re.I):
                return True
        return False

    def _clean_name(self, name: str) -> str:
        """Clean up a name string."""
        # Remove common suffixes/titles
        name = re.sub(r',?\s*(Ph\.?D\.?|M\.?D\.?|MBA|CPA|Esq\.?|Jr\.?|Sr\.?|III?|IV)$', '', name, flags=re.I)
        return name.strip()

    def _clean_title(self, title: str) -> str:
        """Clean up a title string."""
        title = title.strip()
        # Remove leading "- " or "| "
        title = re.sub(r'^[\-–—|]\s*', '', title)
        return title.strip()


# ---------------------------------------------------------------------------
# LinkedIn profile finder (via DuckDuckGo)
# ---------------------------------------------------------------------------

def find_linkedin_profile(name: str, company: str) -> str | None:
    """Search DuckDuckGo for a person's LinkedIn profile."""
    if DDGS is None:
        log.warning("duckduckgo_search not installed — skipping LinkedIn search")
        return None

    query = f'site:linkedin.com/in "{name}" "{company}"'
    try:
        with DDGS() as ddgs:
            results = list(ddgs.text(query, max_results=3))
            for r in results:
                url = r.get("href", "")
                if "linkedin.com/in/" in url:
                    match = re.search(r'(https?://(?:www\.)?linkedin\.com/in/[^/?#]+)', url)
                    if match:
                        return match.group(1)
    except Exception as e:
        log.warning(f"DuckDuckGo search failed for '{name}': {e}")
    return None


# ---------------------------------------------------------------------------
# BlitzAPI email enrichment
# ---------------------------------------------------------------------------

class BlitzEmailClient:
    def __init__(self, api_key: str):
        self.session = requests.Session()
        self.session.headers.update({
            "x-api-key": api_key,
            "Content-Type": "application/json",
        })

    def enrich_email(self, linkedin_profile_url: str) -> str | None:
        url = f"{API_BASE}/enrichment/email"
        try:
            time.sleep(REQUEST_DELAY)
            resp = self.session.post(url, json={"linkedin_profile_url": linkedin_profile_url}, timeout=30)
            if resp.status_code == 200:
                data = resp.json()
                if data.get("email"):
                    return data["email"]
                if data.get("all_emails") and len(data["all_emails"]) > 0:
                    return data["all_emails"][0].get("email_address", "")
            elif resp.status_code == 429:
                wait = int(resp.headers.get("Retry-After", 30))
                log.warning(f"Rate limited — sleeping {wait}s")
                time.sleep(wait)
            elif resp.status_code >= 500:
                log.warning(f"API server error {resp.status_code} — skipping email enrichment")
        except requests.RequestException as e:
            log.warning(f"Email enrichment failed: {e}")
        return None

    def is_available(self) -> bool:
        """Quick health check."""
        try:
            resp = self.session.post(
                f"{API_BASE}/search/domain-to-linkedin-company",
                json={"domain": "nike.com"}, timeout=10
            )
            return resp.status_code == 200
        except requests.RequestException:
            return False


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


def load_no_linkedin_companies() -> list[dict]:
    """Load Part 7 companies that don't have LinkedIn company URLs."""
    companies = []
    filepath = CSV_DIR / INPUT_FILE
    with open(filepath, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            domain = (row.get("Root Domain") or "").strip()
            linkedin = (row.get("LinkedIn") or "").strip()
            if not domain:
                continue
            # Only include companies WITHOUT LinkedIn company URLs
            if linkedin and "linkedin.com/company" in linkedin:
                continue
            companies.append({
                "domain": domain,
                "company": (row.get("Company") or "").strip(),
                "employees": (row.get("Employees") or "").strip(),
                "vertical": (row.get("Vertical") or "").strip(),
                "source_file": INPUT_FILE,
            })
    log.info(f"Loaded {len(companies)} companies without LinkedIn URLs")
    return companies


# ---------------------------------------------------------------------------
# Main processing
# ---------------------------------------------------------------------------

def process_company(
    scraper: WebsiteScraper,
    email_client: BlitzEmailClient | None,
    company: dict,
    checkpoint: CheckpointManager,
    api_available: bool,
) -> list[dict]:
    """Scrape a company's website for team members."""
    domain = company["domain"]
    company_name = company["company"]

    # Step 1: Find team/about page
    team_url, team_html = scraper.find_team_page(domain)
    if not team_html:
        checkpoint.stats["websites_reached"] += 0
        return []

    checkpoint.stats["websites_reached"] += 1

    # Step 2: Extract people
    people = scraper.extract_people(team_html, company_name)
    if not people:
        return []

    checkpoint.stats["team_pages_found"] += 1
    log.info(f"  Found {len(people)} people on {team_url}")

    # Step 3: For each person, find LinkedIn + email
    results = []
    for person in people[:5]:  # Cap at 5 per company
        full_name = person["name"]
        first_name, last_name = split_name(full_name)
        job_title = person["title"]

        checkpoint.stats["people_extracted"] += 1

        # Find LinkedIn profile via DuckDuckGo
        linkedin_url = ""
        if DDGS:
            linkedin_url = find_linkedin_profile(full_name, company_name) or ""
            time.sleep(SEARCH_DELAY)
            if linkedin_url:
                checkpoint.stats["linkedin_found"] += 1

        # Get email via BlitzAPI (if available and we have LinkedIn)
        email = ""
        if linkedin_url and api_available and email_client:
            email = email_client.enrich_email(linkedin_url) or ""
            if email:
                checkpoint.stats["emails_found"] += 1

        results.append({
            "Company": company_name,
            "Root Domain": domain,
            "Employees": company["employees"],
            "Vertical": company["vertical"],
            "Full Name": full_name,
            "First Name": first_name,
            "Last Name": last_name,
            "Job Title": job_title,
            "LinkedIn Profile URL": linkedin_url,
            "Verified Email": email,
            "Source Page": team_url or "",
            "Source File": company["source_file"],
        })

    return results


def main():
    parser = argparse.ArgumentParser(description="Scrape company websites for team members")
    parser.add_argument("--test", type=int, default=0, help="Test on N companies then stop")
    parser.add_argument("--dry-run", action="store_true", help="Show companies without scraping")
    parser.add_argument("--skip-email", action="store_true", help="Skip BlitzAPI email enrichment")
    args = parser.parse_args()

    # Load API key
    load_dotenv(Path(__file__).resolve().parent / ".env")
    api_key = os.getenv("BLITZ_API_KEY")

    email_client = None
    api_available = False
    if api_key and not args.skip_email:
        email_client = BlitzEmailClient(api_key)
        api_available = email_client.is_available()
        if api_available:
            log.info("BlitzAPI is available — will enrich emails")
        else:
            log.info("BlitzAPI is down — will skip email enrichment (can be added later)")

    scraper = WebsiteScraper()
    checkpoint = CheckpointManager(CHECKPOINT_FILE)

    companies = load_no_linkedin_companies()
    remaining = [c for c in companies if not checkpoint.is_processed(c["domain"])]
    log.info(f"{len(remaining)} companies remaining to process")

    if args.test > 0:
        remaining = remaining[:args.test]
        log.info(f"TEST MODE: running {len(remaining)} companies")

    if args.dry_run:
        for c in remaining[:10]:
            log.info(f"  {c['company']} | {c['domain']} | {c['vertical']}")
        log.info("Dry run complete.")
        return

    # Open output CSV
    file_exists = OUTPUT_FILE.exists() and OUTPUT_FILE.stat().st_size > 0
    csv_file = open(OUTPUT_FILE, "a", newline="", encoding="utf-8")
    writer = csv.DictWriter(csv_file, fieldnames=OUTPUT_COLUMNS)
    if not file_exists:
        writer.writeheader()

    try:
        for i, company in enumerate(remaining):
            if _shutdown_requested:
                log.info("Shutdown — saving checkpoint")
                break

            log.info(f"[{i+1}/{len(remaining)}] {company['company']} ({company['domain']})")
            results = process_company(scraper, email_client, company, checkpoint, api_available)
            checkpoint.mark_processed(company["domain"])

            if results:
                for result in results:
                    writer.writerow(result)
                csv_file.flush()
                names = ", ".join(f"{r['Full Name']} ({r['Job Title']})" for r in results)
                log.info(f"  → {len(results)} people: {names}")

            # Save checkpoint every 10 companies
            if checkpoint.stats["total"] % 10 == 0:
                checkpoint.save()

            # Re-check API availability every 50 companies
            if not api_available and email_client and checkpoint.stats["total"] % 50 == 0:
                api_available = email_client.is_available()
                if api_available:
                    log.info("BlitzAPI is back online — will enrich emails going forward")

    finally:
        csv_file.close()
        checkpoint.save()
        log.info("=" * 60)
        log.info("FINAL SUMMARY")
        log.info(f"  Companies processed:    {checkpoint.stats['total']}")
        log.info(f"  Websites reached:       {checkpoint.stats['websites_reached']}")
        log.info(f"  Team pages found:       {checkpoint.stats['team_pages_found']}")
        log.info(f"  People extracted:       {checkpoint.stats['people_extracted']}")
        log.info(f"  LinkedIn profiles found: {checkpoint.stats['linkedin_found']}")
        log.info(f"  Emails found:           {checkpoint.stats['emails_found']}")
        log.info(f"  Output file:            {OUTPUT_FILE}")
        log.info("=" * 60)


if __name__ == "__main__":
    main()
