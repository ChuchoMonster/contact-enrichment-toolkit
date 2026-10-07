#!/usr/bin/env python3
"""
Austin Local Business Lead Finder — LinkedIn Outreach List

Step 1: Google Maps scrape via SerpAPI for Austin, TX businesses
Step 2: Domain → Company LinkedIn URL (Blitz API)
Step 3: Waterfall ICP — Owner/Founder only (Blitz API)

Usage:
    python austin_leads_pipeline.py --step 1    # Google Maps scrape
    python austin_leads_pipeline.py --step 2    # LinkedIn lookup
    python austin_leads_pipeline.py --step 3    # Waterfall ICP
    python austin_leads_pipeline.py --all       # Run all 3 steps
"""
from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import re
import sys
import time
from pathlib import Path
from urllib.parse import urlparse

from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

import requests
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
load_dotenv()

API_BASE = "https://api.blitz-api.ai/api"
BLITZ_KEY = os.getenv("BLITZ_API_KEY")
SERPAPI_KEY = os.getenv("SERPAPI_KEY")

DATA_DIR = Path(os.environ.get("BLITZ_DATA_DIR", Path(__file__).resolve().parent / "data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR = DATA_DIR
MAPS_FILE = OUTPUT_DIR / "austin_maps_results.csv"
LEADS_FILE = OUTPUT_DIR / "austin_leads.csv"
LOG_FILE = OUTPUT_DIR / "austin_pipeline.log"

# Austin coordinates
AUSTIN_LL = "@30.2672,-97.7431,13z"

SEARCH_QUERIES = {
    "professional_services": [
        "law firm Austin TX",
        "personal injury lawyer Austin TX",
        "family law attorney Austin TX",
        "criminal defense lawyer Austin TX",
        "estate planning attorney Austin TX",
        "CPA firm Austin TX",
        "accountant Austin TX",
        "bookkeeper Austin TX",
        "financial advisor Austin TX",
        "tax preparation Austin TX",
    ],
    "home_services": [
        "HVAC Austin TX",
        "plumber Austin TX",
        "roofer Austin TX",
        "electrician Austin TX",
        "landscaping company Austin TX",
        "pest control Austin TX",
        "garage door repair Austin TX",
        "fence company Austin TX",
        "painting company Austin TX",
        "foundation repair Austin TX",
    ],
}

# Waterfall cascade — owner/founder only (small businesses)
CASCADE_OWNER = [
    {
        "include_title": [
            "Owner", "Founder", "Co-Founder", "CEO",
            "Chief Executive Officer", "Managing Partner",
            "Partner", "Principal", "President",
            "General Manager", "Managing Director",
            "Managing Member", "Proprietor",
        ],
        "exclude_title": [
            "Vice President", "VP", "Senior Vice",
            "EVP", "SVP", "Assistant", "Associate",
            "intern", "junior", "student",
        ],
        "location": ["WORLD"],
        "include_headline_search": False,
    },
]

MAX_PEOPLE_PER_COMPANY = 2

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
# API helpers
# ---------------------------------------------------------------------------
blitz_session = requests.Session()
blitz_session.headers.update({"x-api-key": BLITZ_KEY, "Content-Type": "application/json"})


def blitz_post(endpoint, payload, retries=3):
    for attempt in range(retries):
        try:
            resp = blitz_session.post(f"{API_BASE}{endpoint}", json=payload, timeout=30)
            if resp.status_code == 429:
                time.sleep(1)
                continue
            if resp.status_code == 200:
                return resp.json()
            if resp.status_code >= 500:
                time.sleep(2 ** (attempt + 1))
                continue
            return None
        except requests.RequestException:
            time.sleep(2 ** (attempt + 1))
    return None


def extract_domain(url):
    """Extract clean domain from a URL."""
    if not url:
        return ""
    try:
        parsed = urlparse(url if "://" in url else f"https://{url}")
        domain = parsed.netloc or parsed.path
        domain = domain.lower().replace("www.", "")
        # Remove tracking params
        domain = domain.split("/")[0]
        return domain
    except Exception:
        return ""


# ---------------------------------------------------------------------------
# Step 1: Google Maps scrape
# ---------------------------------------------------------------------------
def scrape_google_maps():
    log.info("=" * 60)
    log.info("STEP 1: Google Maps Scrape")
    log.info("=" * 60)

    all_businesses = {}  # keyed by place_id to deduplicate

    total_queries = sum(len(v) for v in SEARCH_QUERIES.values())
    query_num = 0

    for category, queries in SEARCH_QUERIES.items():
        for query in queries:
            query_num += 1
            log.info(f"[{query_num}/{total_queries}] Searching: {query}")

            # Paginate through results (up to 3 pages = ~60 results)
            for start in [0, 20, 40]:
                try:
                    resp = requests.get("https://serpapi.com/search", params={
                        "engine": "google_maps",
                        "q": query,
                        "type": "search",
                        "ll": AUSTIN_LL,
                        "start": start,
                        "api_key": SERPAPI_KEY,
                    }, timeout=30)

                    if resp.status_code != 200:
                        log.warning(f"  SerpAPI returned {resp.status_code} at start={start}")
                        break

                    data = resp.json()
                    results = data.get("local_results", [])

                    if not results:
                        break

                    new_count = 0
                    for r in results:
                        pid = r.get("place_id", "")
                        if pid and pid not in all_businesses:
                            website = r.get("website", "")
                            domain = extract_domain(website)
                            all_businesses[pid] = {
                                "company_name": r.get("title", ""),
                                "domain": domain,
                                "website": website,
                                "address": r.get("address", ""),
                                "phone": r.get("phone", ""),
                                "rating": r.get("rating", ""),
                                "reviews": r.get("reviews", ""),
                                "type": r.get("type", ""),
                                "category": category,
                                "search_query": query,
                                "place_id": pid,
                            }
                            new_count += 1

                    log.info(f"  start={start}: {len(results)} results, {new_count} new")

                    if len(results) < 20:
                        break

                    time.sleep(0.5)

                except Exception as e:
                    log.warning(f"  Error at start={start}: {e}")
                    break

    # Write results
    businesses = list(all_businesses.values())
    has_domain = sum(1 for b in businesses if b["domain"])

    fieldnames = ["company_name", "domain", "website", "address", "phone",
                  "rating", "reviews", "type", "category", "search_query", "place_id"]

    with open(MAPS_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(businesses)

    log.info("=" * 60)
    log.info("STEP 1 COMPLETE")
    log.info(f"  Total unique businesses: {len(businesses)}")
    log.info(f"  With domain:             {has_domain}")
    log.info(f"  Without domain:          {len(businesses) - has_domain}")
    log.info(f"  Professional services:   {sum(1 for b in businesses if b['category'] == 'professional_services')}")
    log.info(f"  Home services:           {sum(1 for b in businesses if b['category'] == 'home_services')}")
    log.info(f"  Output:                  {MAPS_FILE}")
    log.info("=" * 60)


# ---------------------------------------------------------------------------
# Step 2: Domain → Company LinkedIn URL
# ---------------------------------------------------------------------------
def linkedin_lookup():
    log.info("=" * 60)
    log.info("STEP 2: Domain → Company LinkedIn URL")
    log.info("=" * 60)

    with open(MAPS_FILE, newline="", encoding="utf-8") as f:
        businesses = list(csv.DictReader(f))

    # Filter to those with domains
    with_domain = [b for b in businesses if b["domain"].strip()]
    log.info(f"  {len(with_domain)} businesses with domains to look up")

    lock = threading.Lock()
    completed = [0]
    found = [0]
    results = {}

    def lookup_one(biz):
        domain = biz["domain"]
        data = blitz_post("/search/domain-to-linkedin-company", {"domain": domain})
        li_url = ""
        if data and data.get("found"):
            li_url = data.get("company_linkedin_url", "")

        with lock:
            completed[0] += 1
            if li_url:
                found[0] += 1
            if completed[0] % 50 == 0:
                log.info(f"  [{completed[0]}/{len(with_domain)}] {found[0]} found so far")

        return {"place_id": biz["place_id"], "company_linkedin_url": li_url}

    with ThreadPoolExecutor(max_workers=5) as pool:
        futures = {pool.submit(lookup_one, b): b["place_id"] for b in with_domain}
        for future in as_completed(futures):
            r = future.result()
            results[r["place_id"]] = r["company_linkedin_url"]

    # Merge back into businesses
    for biz in businesses:
        biz["company_linkedin_url"] = results.get(biz["place_id"], "")

    # Overwrite maps file with LinkedIn URLs added
    fieldnames = list(businesses[0].keys())
    with open(MAPS_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(businesses)

    total_found = sum(1 for b in businesses if b.get("company_linkedin_url"))
    log.info("=" * 60)
    log.info("STEP 2 COMPLETE")
    log.info(f"  Domains looked up:       {len(with_domain)}")
    log.info(f"  LinkedIn URLs found:     {total_found}")
    log.info(f"  Not found:               {len(with_domain) - total_found}")
    log.info("=" * 60)


# ---------------------------------------------------------------------------
# Step 3: Waterfall ICP — Owner/Founder
# ---------------------------------------------------------------------------
def waterfall_icp():
    log.info("=" * 60)
    log.info("STEP 3: Waterfall ICP — Owner/Founder")
    log.info("=" * 60)

    with open(MAPS_FILE, newline="", encoding="utf-8") as f:
        businesses = list(csv.DictReader(f))

    with_linkedin = [b for b in businesses if b.get("company_linkedin_url", "").strip()]
    log.info(f"  {len(with_linkedin)} companies with LinkedIn URLs")

    lock = threading.Lock()
    completed = [0]
    total_people = [0]
    all_contacts = []

    def search_one(biz):
        li_url = biz["company_linkedin_url"]
        data = blitz_post("/search/waterfall-icp", {
            "company_linkedin_url": li_url,
            "max_results": MAX_PEOPLE_PER_COMPANY,
            "cascade": CASCADE_OWNER,
        })

        people = []
        if data:
            if isinstance(data, list):
                people = data
            elif isinstance(data, dict) and data.get("results"):
                people = data["results"]

        contacts = []
        for p in people:
            contacts.append({
                "company_name": biz["company_name"],
                "domain": biz["domain"],
                "category": biz["category"],
                "address": biz["address"],
                "phone": biz["phone"],
                "rating": biz["rating"],
                "reviews": biz["reviews"],
                "business_type": biz["type"],
                "company_linkedin_url": li_url,
                "full_name": p.get("full_name", ""),
                "first_name": p.get("first_name", ""),
                "last_name": p.get("last_name", ""),
                "job_title": p.get("job_title", ""),
                "personal_linkedin_url": p.get("person_linkedin_url", "") or p.get("linkedin_profile_url", ""),
            })

        with lock:
            completed[0] += 1
            total_people[0] += len(contacts)
            all_contacts.extend(contacts)
            if completed[0] % 50 == 0:
                log.info(f"  [{completed[0]}/{len(with_linkedin)}] {total_people[0]} people found")

        name_preview = contacts[0]["full_name"] if contacts else "none"
        log.info(f"  [{completed[0]}/{len(with_linkedin)}] {biz['company_name']} — {len(contacts)} people ({name_preview})")

        return contacts

    with ThreadPoolExecutor(max_workers=5) as pool:
        futures = {pool.submit(search_one, b): b["place_id"] for b in with_linkedin}
        for future in as_completed(futures):
            future.result()  # already collected in all_contacts

    # Write final output
    if all_contacts:
        fieldnames = list(all_contacts[0].keys())
        with open(LEADS_FILE, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(all_contacts)

    with_linkedin_url = sum(1 for c in all_contacts if c["personal_linkedin_url"])

    log.info("=" * 60)
    log.info("STEP 3 COMPLETE")
    log.info(f"  Companies searched:      {len(with_linkedin)}")
    log.info(f"  People found:            {len(all_contacts)}")
    log.info(f"  With LinkedIn URL:       {with_linkedin_url}")
    log.info(f"  Professional services:   {sum(1 for c in all_contacts if c['category'] == 'professional_services')}")
    log.info(f"  Home services:           {sum(1 for c in all_contacts if c['category'] == 'home_services')}")
    log.info(f"  Output:                  {LEADS_FILE}")
    log.info("=" * 60)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Austin Local Business Lead Finder")
    parser.add_argument("--step", type=int, choices=[1, 2, 3], help="Run a specific step")
    parser.add_argument("--all", action="store_true", help="Run all 3 steps")
    args = parser.parse_args()

    if args.all:
        scrape_google_maps()
        linkedin_lookup()
        waterfall_icp()
    elif args.step == 1:
        scrape_google_maps()
    elif args.step == 2:
        linkedin_lookup()
    elif args.step == 3:
        waterfall_icp()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
