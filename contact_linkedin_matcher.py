#!/usr/bin/env python3
"""Find personal LinkedIn URLs for a list of known contacts (domain + contact_name) via Blitz API.

Strategy: domain → company LinkedIn → waterfall ICP → fuzzy match contact name.
"""

import csv, json, os, time, re, logging
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from difflib import SequenceMatcher
import requests
from dotenv import load_dotenv

load_dotenv()

API_BASE = "https://api.blitz-api.ai/api"
API_KEY = os.getenv("BLITZ_API_KEY")
DATA_DIR = Path(os.environ.get("BLITZ_DATA_DIR", Path(__file__).resolve().parent / "data"))
INPUT_FILE = DATA_DIR / "contacts_input.csv"            # columns: domain, contact_name, ...
OUTPUT_FILE = DATA_DIR / "contacts_with_linkedin.csv"

log = logging.getLogger("contact_matcher")
logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s  %(message)s")

session = requests.Session()
session.headers.update({"x-api-key": API_KEY, "Content-Type": "application/json"})


def api_post(endpoint, payload, retries=3):
    for attempt in range(retries):
        try:
            resp = session.post(f"{API_BASE}{endpoint}", json=payload, timeout=30)
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


def domain_to_linkedin(domain):
    data = api_post("/search/domain-to-linkedin-company", {"domain": domain})
    if data:
        return data.get("company_linkedin_url")
    return None


def waterfall_find_people(company_linkedin_url):
    """Broad waterfall to find as many people as possible for matching."""
    data = api_post("/search/waterfall-icp", {
        "company_linkedin_url": company_linkedin_url,
        "max_results": 10,
        "cascade": [
            {
                "include_title": ["Marketing", "Director", "Manager", "VP", "Head",
                                  "Founder", "CEO", "Owner", "President", "CMO",
                                  "Designer", "Web", "Digital", "Content", "Brand",
                                  "Communications", "Ecommerce", "E-commerce"],
                "exclude_title": ["intern", "junior"],
                "location": ["WORLD"],
                "include_headline_search": False,
            }
        ],
    })
    if not data:
        return []
    if isinstance(data, list):
        return data
    if isinstance(data, dict) and data.get("results"):
        return data["results"]
    return []


def normalize_name(name):
    """Normalize a name for fuzzy matching."""
    if not name:
        return ""
    # Remove titles, punctuation
    name = re.sub(r'\b(mr|mrs|ms|dr|prof|jr|sr|ii|iii|iv)\b', '', name.lower())
    name = re.sub(r'[^a-z\s]', '', name)
    return ' '.join(name.split())


def name_match_score(contact_name, api_full_name, api_first=None, api_last=None):
    """Score how well a contact name matches an API result."""
    cn = normalize_name(contact_name)
    an = normalize_name(api_full_name)
    if not cn or not an:
        return 0.0

    # Direct match
    if cn == an:
        return 1.0

    # Check if first name or last name appears
    cn_parts = cn.split()
    an_parts = an.split()

    # First name match
    if cn_parts and an_parts and cn_parts[0] == an_parts[0]:
        return 0.8

    # Last name match
    if len(cn_parts) > 1 and len(an_parts) > 1 and cn_parts[-1] == an_parts[-1]:
        return 0.7

    # Fuzzy
    return SequenceMatcher(None, cn, an).ratio()


def process_row(row):
    """Process a single row: find company LinkedIn, waterfall, match name."""
    domain = row["domain"].strip()
    contact_name = row["contact_name"].strip()

    if not domain:
        return {**row, "personal_linkedin": "", "matched_name": "", "match_method": "no_domain"}

    # Step 1: domain → company LinkedIn
    company_li = domain_to_linkedin(domain)
    if not company_li:
        return {**row, "personal_linkedin": "", "matched_name": "", "match_method": "no_company_linkedin"}

    # Step 2: waterfall to find people
    people = waterfall_find_people(company_li)
    if not people:
        return {**row, "personal_linkedin": "", "matched_name": "", "match_method": "no_people_found"}

    # Step 3: match contact name
    best_match = None
    best_score = 0.0

    for p in people:
        full_name = p.get("full_name", "")
        score = name_match_score(contact_name, full_name,
                                 p.get("first_name"), p.get("last_name"))
        if score > best_score:
            best_score = score
            best_match = p

    if best_match and best_score >= 0.5:
        profile_url = (best_match.get("person_linkedin_url", "")
                       or best_match.get("linkedin_profile_url", ""))
        return {**row,
                "personal_linkedin": profile_url,
                "matched_name": best_match.get("full_name", ""),
                "matched_title": best_match.get("job_title", ""),
                "match_score": f"{best_score:.2f}",
                "match_method": "name_match"}

    # No good name match — return the top person anyway as a suggestion
    top = people[0]
    profile_url = top.get("person_linkedin_url", "") or top.get("linkedin_profile_url", "")
    return {**row,
            "personal_linkedin": profile_url,
            "matched_name": top.get("full_name", ""),
            "matched_title": top.get("job_title", ""),
            "match_score": "0.00",
            "match_method": "top_result_fallback"}


def main():
    rows = []
    with open(INPUT_FILE, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            rows.append(row)

    log.info(f"Processing {len(rows)} contacts")
    results = []
    import threading
    lock = threading.Lock()
    completed = [0]

    def worker(row):
        result = process_row(row)
        with lock:
            completed[0] += 1
            n = completed[0]
        li = result.get("personal_linkedin", "")
        method = result.get("match_method", "")
        if li:
            log.info(f"[{n}/{len(rows)}] {row['contact_name']} ({row['domain']}) → {result.get('matched_name','')} — {li}")
        else:
            log.info(f"[{n}/{len(rows)}] {row['contact_name']} ({row['domain']}) — {method}")
        return result

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = {pool.submit(worker, r): i for i, r in enumerate(rows)}
        indexed_results = {}
        for future in as_completed(futures):
            idx = futures[future]
            indexed_results[idx] = future.result()

    # Preserve original order
    results = [indexed_results[i] for i in range(len(rows))]

    # Write output
    fieldnames = list(rows[0].keys()) + ["personal_linkedin", "matched_name", "matched_title", "match_score", "match_method"]
    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(results)

    found = sum(1 for r in results if r.get("personal_linkedin"))
    exact = sum(1 for r in results if r.get("match_method") == "name_match")
    fallback = sum(1 for r in results if r.get("match_method") == "top_result_fallback")
    log.info("=" * 60)
    log.info(f"COMPLETE: {found}/{len(rows)} with LinkedIn URLs")
    log.info(f"  Name matches: {exact}")
    log.info(f"  Fallback (top result): {fallback}")
    log.info(f"  No result: {len(rows) - found}")
    log.info(f"  Output: {OUTPUT_FILE}")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
