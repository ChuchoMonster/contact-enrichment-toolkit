#!/usr/bin/env python3
"""
EDU Waterfall ICP — Find marketing/comms/digital decision-makers at US
higher-ed institutions using BlitzAPI /v2/search/waterfall-icp-keyword.

Cascade: C-suite → VP → Head of → Director → Manager
Functions: Marketing, Communications, Innovation, Chief of Staff, Content,
           SEO, Website, Web Dev/Design, Multimedia, Digital Production,
           Brand, PR/Public Relations
"""
from __future__ import annotations

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

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
API_BASE = "https://api.blitz-api.ai/v2"
REQUEST_DELAY = 0.25  # ~4 requests per second
MAX_PEOPLE_PER_COMPANY = 50  # effectively uncapped

DATA_DIR = Path(os.environ.get("BLITZ_DATA_DIR", Path(__file__).resolve().parent / "data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)
INPUT_FILE = DATA_DIR / "edu_companies_enriched.csv"
OUTPUT_DIR = DATA_DIR
OUTPUT_FILE = OUTPUT_DIR / "edu_waterfall_results.csv"
CHECKPOINT_FILE = OUTPUT_DIR / "edu_waterfall_checkpoint.json"
LOG_FILE = OUTPUT_DIR / "edu_waterfall.log"

OUTPUT_COLUMNS = [
    "Institution", "Domain", "City", "State", "LinkedIn Company URL",
    "Full Name", "First Name", "Last Name", "Job Title", "LinkedIn Profile URL",
    "Cascade Level", "Ranking",
]

# ---------------------------------------------------------------------------
# Exclusions — applied both in cascade and post-filter
# ---------------------------------------------------------------------------
EXCLUDE_TITLES = [
    "junior", "assistant", "associate", "intern",
    "professor", "teacher", "student", "consultant",
    "part-time", "part time", "adjunct", "lecturer",
    "teaching", "instructor", "fellow", "researcher",
    "analyst", "coordinator",
]

# ---------------------------------------------------------------------------
# Waterfall cascade
# ---------------------------------------------------------------------------
CASCADE = [
    # Level 1: C-Suite / Chief
    {
        "include_title": [
            "CMO", "Chief Marketing Officer", "Chief Communications Officer",
            "Chief Innovation Officer", "Chief Digital Officer",
            "Chief Content Officer", "Chief Brand Officer",
            "Chief of Staff",
        ],
        "exclude_title": EXCLUDE_TITLES,
        "location": ["WORLD"],
        "include_headline_search": False,
    },
    # Level 2: VP
    {
        "include_title": [
            "VP Marketing", "VP Communications", "VP Innovation",
            "VP Content", "VP Digital", "VP Brand",
            "VP Public Relations", "VP PR",
            "VP Web", "VP Multimedia",
            "Vice President Marketing", "Vice President Communications",
            "Vice President Innovation", "Vice President Digital",
            "Vice President Content", "Vice President Brand",
            "Vice President Public Relations",
            "SVP Marketing", "SVP Communications", "SVP Digital",
            "EVP Marketing", "EVP Communications",
            "AVP Marketing", "AVP Communications", "AVP Digital",
            "Associate Vice President Marketing",
            "Associate Vice President Communications",
            "Associate Vice President Digital",
        ],
        "exclude_title": EXCLUDE_TITLES,
        "location": ["WORLD"],
        "include_headline_search": False,
    },
    # Level 3: Head of
    {
        "include_title": [
            "Head of Marketing", "Head of Communications",
            "Head of Innovation", "Head of Content",
            "Head of SEO", "Head of Digital",
            "Head of Web", "Head of Brand",
            "Head of Public Relations", "Head of PR",
            "Head of Multimedia", "Head of Digital Production",
            "Head of Web Development", "Head of Web Design",
        ],
        "exclude_title": EXCLUDE_TITLES,
        "location": ["WORLD"],
        "include_headline_search": False,
    },
    # Level 4: Director
    {
        "include_title": [
            "Director of Marketing", "Director of Communications",
            "Director of Innovation", "Director of Content",
            "Director of SEO", "Director of Digital",
            "Director of Web", "Director of Brand",
            "Director of Public Relations", "Director of PR",
            "Director of Multimedia", "Director of Digital Production",
            "Director of Web Development", "Director of Web Design",
            "Director of Digital Marketing", "Director of Digital Communications",
            "Director of Brand Communications",
            "Director of Strategic Communications",
            "Director of University Communications",
            "Director of College Communications",
            "Marketing Director", "Communications Director",
            "Digital Director", "Creative Director",
            "Web Director", "Brand Director",
            "Senior Director Marketing", "Senior Director Communications",
            "Senior Director Digital", "Senior Director Web",
            "Senior Director Content", "Senior Director Brand",
            "Executive Director Marketing", "Executive Director Communications",
        ],
        "exclude_title": EXCLUDE_TITLES,
        "location": ["WORLD"],
        "include_headline_search": False,
    },
    # Level 5: Manager
    {
        "include_title": [
            "Marketing Manager", "Communications Manager",
            "Innovation Manager", "Content Manager",
            "SEO Manager", "Digital Manager",
            "Web Manager", "Brand Manager",
            "PR Manager", "Public Relations Manager",
            "Multimedia Manager", "Digital Production Manager",
            "Web Development Manager", "Web Design Manager",
            "Digital Marketing Manager", "Social Media Manager",
            "Website Manager", "Web Content Manager",
            "Manager of Marketing", "Manager of Communications",
            "Manager of Digital", "Manager of Web",
            "Manager of Content", "Manager of Brand",
            "Manager of Public Relations",
            "Senior Manager Marketing", "Senior Manager Communications",
            "Senior Manager Digital", "Senior Manager Web",
            "Senior Manager Content",
        ],
        "exclude_title": EXCLUDE_TITLES,
        "location": ["WORLD"],
        "include_headline_search": False,
    },
    # Level 6: Headline search fallback — broad keywords
    {
        "include_title": [
            "Marketing", "Communications", "Innovation",
            "Content", "SEO", "Digital",
            "Web", "Brand", "Public Relations",
            "Multimedia", "Digital Production",
        ],
        "exclude_title": EXCLUDE_TITLES + [
            "specialist", "engineer", "developer", "designer",
            "sales", "accountant", "HR", "finance", "recruiter",
            "librarian", "nurse", "counselor", "advisor",
            "custodian", "maintenance", "security", "food service",
        ],
        "location": ["WORLD"],
        "include_headline_search": True,
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
# Checkpoint
# ---------------------------------------------------------------------------

class CheckpointManager:
    def __init__(self, path: Path):
        self.path = path
        self.processed: set[str] = set()
        self.stats = {"total": 0, "people_found": 0, "api_calls": 0}
        self._load()

    def _load(self):
        if self.path.exists():
            data = json.loads(self.path.read_text())
            self.processed = set(data.get("processed", []))
            self.stats = data.get("stats", self.stats)
            log.info(f"Checkpoint loaded — {len(self.processed)} institutions already processed")

    def save(self):
        data = {"processed": sorted(self.processed), "stats": self.stats}
        self.path.write_text(json.dumps(data, indent=2))

    def is_processed(self, domain: str) -> bool:
        return domain in self.processed

    def mark_processed(self, domain: str):
        self.processed.add(domain)
        self.stats["total"] += 1


# ---------------------------------------------------------------------------
# API
# ---------------------------------------------------------------------------

def waterfall_icp(session: requests.Session, company_linkedin_url: str) -> list[dict]:
    url = f"{API_BASE}/search/waterfall-icp-keyword"
    payload = {
        "company_linkedin_url": company_linkedin_url,
        "max_results": MAX_PEOPLE_PER_COMPANY,
        "cascade": CASCADE,
    }
    for attempt in range(3):
        try:
            time.sleep(REQUEST_DELAY)
            resp = session.post(url, json=payload, timeout=30)
            if resp.status_code == 200:
                data = resp.json()
                return data.get("results", [])
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
            log.warning(f"API returned {resp.status_code}: {resp.text[:200]}")
            return []
        except requests.RequestException as e:
            wait = 2 ** (attempt + 1)
            log.warning(f"Network error: {e} — retry in {wait}s")
            time.sleep(wait)
    log.error(f"Failed after 3 retries: {company_linkedin_url}")
    return []


def split_name(full_name: str) -> tuple[str, str]:
    parts = full_name.strip().split()
    if len(parts) == 0:
        return ("", "")
    if len(parts) == 1:
        return (parts[0], "")
    return (parts[0], " ".join(parts[1:]))


# ---------------------------------------------------------------------------
# Post-filter: reject titles that slipped through
# ---------------------------------------------------------------------------
DISQUALIFY_KEYWORDS = [
    "assistant", "associate", "intern", "junior",
    "professor", "teacher", "student", "consultant",
    "part-time", "part time", "adjunct", "lecturer",
    "teaching", "instructor", "fellow", "researcher",
]


def is_disqualified(title: str) -> bool:
    title_lower = title.lower()
    for kw in DISQUALIFY_KEYWORDS:
        if kw in title_lower:
            return True
    return False


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    load_dotenv(Path(__file__).resolve().parent / ".env")
    api_key = os.getenv("BLITZ_API_KEY")
    if not api_key:
        log.error("BLITZ_API_KEY not found in .env")
        sys.exit(1)

    session = requests.Session()
    session.headers.update({
        "x-api-key": api_key,
        "Content-Type": "application/json",
    })

    checkpoint = CheckpointManager(CHECKPOINT_FILE)

    # Load institutions with LinkedIn URLs
    institutions = []
    with open(INPUT_FILE, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get("LinkedIn URL", "").strip():
                institutions.append(row)

    remaining = [i for i in institutions if not checkpoint.is_processed(i["Domain"])]
    log.info(f"Loaded {len(institutions)} institutions with LinkedIn URLs, {len(remaining)} remaining")

    # Open output CSV
    file_exists = OUTPUT_FILE.exists() and OUTPUT_FILE.stat().st_size > 0
    csv_file = open(OUTPUT_FILE, "a", newline="", encoding="utf-8")
    writer = csv.DictWriter(csv_file, fieldnames=OUTPUT_COLUMNS)
    if not file_exists:
        writer.writeheader()

    try:
        for i, inst in enumerate(remaining):
            if _shutdown_requested:
                log.info("Shutdown — saving checkpoint")
                break

            linkedin_url = inst["LinkedIn URL"].strip()
            results = waterfall_icp(session, linkedin_url)
            checkpoint.stats["api_calls"] += 1

            # Filter and write results
            people = []
            seen_urls = set()
            for r in results:
                person = r.get("person", r)  # v2 nests under .person
                person_url = person.get("linkedin_url", "")
                if not person_url or person_url in seen_urls:
                    continue
                seen_urls.add(person_url)

                # Get job title from current experience at this company
                job_title = ""
                for exp in person.get("experiences", []):
                    if exp.get("job_is_current"):
                        job_title = exp.get("job_title", "")
                        break
                if not job_title:
                    job_title = person.get("headline", "") or ""

                if is_disqualified(job_title):
                    continue

                full_name = person.get("full_name", "")
                first, last = split_name(full_name)

                people.append({
                    "Institution": inst["Name"],
                    "Domain": inst["Domain"],
                    "City": inst.get("City", ""),
                    "State": inst.get("State", ""),
                    "LinkedIn Company URL": linkedin_url,
                    "Full Name": full_name,
                    "First Name": first,
                    "Last Name": last,
                    "Job Title": job_title,
                    "LinkedIn Profile URL": person_url,
                    "Cascade Level": r.get("icp", ""),
                    "Ranking": r.get("ranking", ""),
                })

            for p in people:
                writer.writerow(p)
                checkpoint.stats["people_found"] += 1
            csv_file.flush()

            checkpoint.mark_processed(inst["Domain"])

            if people:
                names = ", ".join(f"{p['Full Name']} ({p['Job Title']})" for p in people[:3])
                extra = f" +{len(people)-3} more" if len(people) > 3 else ""
                log.info(
                    f"[{checkpoint.stats['total']}/{len(remaining)}] "
                    f"{inst['Name']} — {len(people)} people: {names}{extra}"
                )
            elif checkpoint.stats["total"] % 100 == 0 and checkpoint.stats["total"] > 0:
                log.info(
                    f"[{checkpoint.stats['total']}/{len(remaining)}] Progress — "
                    f"{checkpoint.stats['people_found']} people found, "
                    f"{checkpoint.stats['api_calls']} API calls"
                )

            if checkpoint.stats["total"] % 50 == 0:
                checkpoint.save()

    finally:
        csv_file.close()
        checkpoint.save()
        log.info("=" * 60)
        log.info("FINAL SUMMARY")
        log.info(f"  Institutions processed: {checkpoint.stats['total']}")
        log.info(f"  People found:           {checkpoint.stats['people_found']}")
        log.info(f"  API calls:              {checkpoint.stats['api_calls']}")
        log.info(f"  Output file:            {OUTPUT_FILE}")
        log.info("=" * 60)


if __name__ == "__main__":
    main()
