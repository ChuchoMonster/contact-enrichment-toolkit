#!/usr/bin/env python3
"""Series A/B Fundraising Pipeline — Find CEOs + Heads of Marketing + Verified Emails."""
from __future__ import annotations
import csv, json, logging, os, sys, time, re
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse
import threading
import requests
from dotenv import load_dotenv

load_dotenv()

API_BASE = "https://api.blitz-api.ai/api"
API_KEY = os.getenv("BLITZ_API_KEY")
DATA_DIR = Path(os.environ.get("BLITZ_DATA_DIR", Path(__file__).resolve().parent / "data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)
INPUT_FILE = DATA_DIR / "series-ab-fundraising.csv"
OUTPUT_FILE = DATA_DIR / "series_ab_contacts.csv"
LOG_FILE = DATA_DIR / "series_ab_pipeline.log"

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s  %(message)s",
                    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler(sys.stdout)])
log = logging.getLogger(__name__)

session = requests.Session()
session.headers.update({"x-api-key": API_KEY, "Content-Type": "application/json"})

CASCADE_CEO = [
    {
        "include_title": [
            "CEO", "Chief Executive Officer", "Founder", "Co-Founder",
            "Owner", "Managing Director", "President", "General Manager",
        ],
        "exclude_title": ["Vice President", "VP", "SVP", "EVP", "Assistant", "Associate", "intern"],
        "location": ["WORLD"],
        "include_headline_search": False,
    },
]

CASCADE_MARKETING = [
    {
        "include_title": [
            "CMO", "Chief Marketing Officer", "Chief Growth Officer",
            "VP Marketing", "VP Growth", "VP Digital",
            "Head of Marketing", "Head of Growth", "Head of Digital",
            "Head of Content", "Head of Brand",
        ],
        "exclude_title": ["intern", "junior", "assistant", "associate"],
        "location": ["WORLD"],
        "include_headline_search": False,
    },
    {
        "include_title": [
            "Director of Marketing", "Director of Growth",
            "Director of Digital Marketing", "Director of Content",
            "Marketing Director", "Growth Director",
        ],
        "exclude_title": ["intern", "junior", "assistant", "associate"],
        "location": ["WORLD"],
        "include_headline_search": False,
    },
]


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


def extract_domain(raw):
    if not raw:
        return ""
    raw = raw.strip().lower()
    if not raw.startswith("http"):
        raw = "https://" + raw
    try:
        parsed = urlparse(raw)
        d = parsed.netloc or parsed.path
        return d.replace("www.", "").split("/")[0]
    except:
        return raw.replace("www.", "")


def main():
    with open(INPUT_FILE, newline="", encoding="utf-8") as f:
        companies = list(csv.DictReader(f))

    log.info(f"Loaded {len(companies)} companies")

    # Step 1: Domain → Company LinkedIn
    log.info("=" * 60)
    log.info("STEP 1: Domain → Company LinkedIn")
    lock = threading.Lock()
    completed = [0]
    linkedin_map = {}

    def lookup_li(company):
        domain = extract_domain(company["Company Domain"])
        data = api_post("/search/domain-to-linkedin-company", {"domain": domain})
        li = ""
        if data and data.get("company_linkedin_url"):
            li = data["company_linkedin_url"]
        with lock:
            completed[0] += 1
            linkedin_map[domain] = li
            if li:
                log.info(f"[{completed[0]}/{len(companies)}] {company['Company Name']} → {li}")
            elif completed[0] % 20 == 0:
                log.info(f"[{completed[0]}/{len(companies)}] Progress...")

    with ThreadPoolExecutor(max_workers=5) as pool:
        list(pool.map(lookup_li, companies))

    found = sum(1 for v in linkedin_map.values() if v)
    log.info(f"STEP 1 COMPLETE: {found}/{len(companies)} LinkedIn URLs found")

    # Step 2: Waterfall ICP — CEO + Marketing
    log.info("=" * 60)
    log.info("STEP 2: Waterfall ICP — CEO + Head of Marketing")
    all_contacts = []
    completed[0] = 0

    def search_company(company):
        domain = extract_domain(company["Company Domain"])
        li_url = linkedin_map.get(domain, "")
        if not li_url:
            return

        # CEO cascade
        ceo_data = api_post("/search/waterfall-icp", {
            "company_linkedin_url": li_url, "max_results": 2, "cascade": CASCADE_CEO,
        })
        ceo_people = []
        if ceo_data:
            if isinstance(ceo_data, list):
                ceo_people = ceo_data
            elif isinstance(ceo_data, dict) and ceo_data.get("results"):
                ceo_people = ceo_data["results"]

        # Marketing cascade
        mktg_data = api_post("/search/waterfall-icp", {
            "company_linkedin_url": li_url, "max_results": 2, "cascade": CASCADE_MARKETING,
        })
        mktg_people = []
        if mktg_data:
            if isinstance(mktg_data, list):
                mktg_people = mktg_data
            elif isinstance(mktg_data, dict) and mktg_data.get("results"):
                mktg_people = mktg_data["results"]

        contacts = []
        seen_urls = set()
        for p_list, role_type in [(ceo_people, "CEO/Founder"), (mktg_people, "Marketing")]:
            for p in p_list:
                purl = p.get("person_linkedin_url", "") or p.get("linkedin_profile_url", "")
                if purl in seen_urls:
                    continue
                seen_urls.add(purl)
                contacts.append({
                    "Company Name": company["Company Name"],
                    "Domain": domain,
                    "Series": company.get("Series", ""),
                    "Amount Raised": company.get("Amount Raised", ""),
                    "Description": company.get("Description", ""),
                    "Company LinkedIn URL": li_url,
                    "Full Name": p.get("full_name", ""),
                    "First Name": p.get("first_name", ""),
                    "Last Name": p.get("last_name", ""),
                    "Job Title": p.get("job_title", ""),
                    "Personal LinkedIn URL": purl,
                    "Role Type": role_type,
                    "Verified Email": "",
                })

        with lock:
            completed[0] += 1
            all_contacts.extend(contacts)
            names = ", ".join(c["Full Name"] for c in contacts[:3])
            log.info(f"[{completed[0]}] {company['Company Name']} — {len(contacts)} people: {names}")

    with ThreadPoolExecutor(max_workers=3) as pool:
        list(pool.map(search_company, companies))

    log.info(f"STEP 2 COMPLETE: {len(all_contacts)} people found")

    # Step 3: Email enrichment
    log.info("=" * 60)
    log.info("STEP 3: Email Enrichment")
    completed[0] = 0
    emails_found = [0]

    def enrich_email(contact):
        purl = contact["Personal LinkedIn URL"]
        if not purl:
            return
        data = api_post("/enrichment/email", {"linkedin_profile_url": purl})
        email = ""
        if data:
            email = data.get("email", "")
            if not email and data.get("all_emails"):
                email = data["all_emails"][0].get("email_address", "")
        contact["Verified Email"] = email
        with lock:
            completed[0] += 1
            if email:
                emails_found[0] += 1
                log.info(f"[{completed[0]}/{len(all_contacts)}] {contact['Full Name']} → {email}")
            elif completed[0] % 20 == 0:
                log.info(f"[{completed[0]}/{len(all_contacts)}] Progress — {emails_found[0]} emails found")

    with ThreadPoolExecutor(max_workers=5) as pool:
        list(pool.map(enrich_email, all_contacts))

    log.info(f"STEP 3 COMPLETE: {emails_found[0]}/{len(all_contacts)} emails found")

    # Write output — verified emails only
    verified = [c for c in all_contacts if (c.get("Verified Email") or "").strip()]
    if verified:
        fieldnames = list(verified[0].keys())
        with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(verified)

    log.info("=" * 60)
    log.info("ALL STEPS COMPLETE")
    log.info(f"  Companies:             {len(companies)}")
    log.info(f"  LinkedIn found:        {found}")
    log.info(f"  People found:          {len(all_contacts)}")
    log.info(f"  Verified emails:       {len(verified)}")
    log.info(f"  Output:                {OUTPUT_FILE}")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
