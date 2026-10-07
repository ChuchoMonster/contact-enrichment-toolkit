#!/usr/bin/env python3
"""NYC Professional Training & Coaching founders/CEOs — emails + LinkedIn."""

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

load_dotenv()

API_BASE = "https://api.blitz-api.ai/api"
API_BASE_V2 = "https://api.blitz-api.ai"
REQUEST_DELAY = 0.25

OUTPUT_DIR = Path(__file__).resolve().parent
OUTPUT_CSV = OUTPUT_DIR / "nyc_training_founders.csv"
COMPANY_CACHE = OUTPUT_DIR / "nyc_training_companies.json"
CHECKPOINT_FILE = OUTPUT_DIR / "nyc_training_checkpoint.json"
LOG_FILE = OUTPUT_DIR / "nyc_training.log"

EXCLUDE_TITLES = [
    "analyst", "associate", "coordinator", "assistant",
    "intern", "junior", "student",
]

CASCADE_FOUNDER = [
    {
        "include_title": [
            "CEO", "Chief Executive Officer", "Founder", "Co-Founder",
            "Co-founder", "Owner", "Managing Director",
        ],
        "exclude_title": EXCLUDE_TITLES,
        "location": ["WORLD"],
        "include_headline_search": False,
    },
    {
        "include_title": ["President"],
        "exclude_title": [
            "Vice President", "VP", "Senior Vice", "EVP", "SVP",
        ] + EXCLUDE_TITLES,
        "location": ["WORLD"],
        "include_headline_search": False,
    },
]

SEARCH_PAYLOAD = {
    "company": {
        "employee_range": ["11-50"],
        "hq": {"country_code": ["US"], "city": {"include": ["New York"]}},
        "industry": {"include": ["Professional Training and Coaching"]},
    },
    "max_results": 50,
}

OUTPUT_COLUMNS = [
    "company_name", "website", "industry", "employee_count",
    "hq_city", "hq_country",
    "contact_first_name", "contact_last_name", "contact_title",
    "contact_linkedin_url", "contact_email", "Company_Linkedin_url",
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()],
)
log = logging.getLogger("nyc_training")

_shutdown = False
def _sig(sig, frame):
    global _shutdown
    _shutdown = True
    log.warning("Shutdown requested — finishing current item")
signal.signal(signal.SIGINT, _sig)


class API:
    def __init__(self, key):
        self.s = requests.Session()
        self.s.headers.update({"x-api-key": key, "Content-Type": "application/json"})

    def _post(self, base, path, payload):
        url = f"{base}{path}"
        for attempt in range(3):
            try:
                time.sleep(REQUEST_DELAY)
                r = self.s.post(url, json=payload, timeout=30)
                if r.status_code == 200:
                    return r.json()
                if r.status_code == 429:
                    wait = int(r.headers.get("Retry-After", 30))
                    log.warning(f"Rate limited — sleeping {wait}s")
                    time.sleep(wait)
                    continue
                if r.status_code >= 500:
                    time.sleep(2 ** (attempt + 1))
                    continue
                log.warning(f"{path} -> {r.status_code}: {r.text[:200]}")
                return None
            except requests.RequestException as e:
                log.warning(f"Net error: {e}")
                time.sleep(2 ** (attempt + 1))
        return None

    def search_companies(self, payload, cursor=None):
        body = dict(payload)
        if cursor:
            body["cursor"] = cursor
        data = self._post(API_BASE_V2, "/v2/search/companies", body)
        if not data:
            return [], None
        return data.get("results", []), data.get("cursor")

    def waterfall(self, company_linkedin_url, cascade, max_results=1):
        return self._post(API_BASE, "/search/waterfall-icp", {
            "company_linkedin_url": company_linkedin_url,
            "max_results": max_results,
            "cascade": cascade,
        }) or {}

    def enrich_email(self, linkedin_profile_url):
        return self._post(API_BASE, "/enrichment/email", {
            "linkedin_profile_url": linkedin_profile_url,
        }) or {}


def load_checkpoint():
    if CHECKPOINT_FILE.exists():
        try:
            return json.loads(CHECKPOINT_FILE.read_text())
        except Exception:
            pass
    return {"processed": []}


def save_checkpoint(cp):
    CHECKPOINT_FILE.write_text(json.dumps(cp, indent=2))


def extract_co(c):
    name = c.get("name", "")
    website = c.get("website") or c.get("domain") or ""
    industry = c.get("industry", "")
    if isinstance(industry, list):
        industry = ", ".join(industry)
    employees = c.get("employee_count") or c.get("size") or ""
    hq = c.get("hq") or c.get("headquarters") or {}
    city = hq.get("city") if isinstance(hq, dict) else ""
    country = (hq.get("country_name") if isinstance(hq, dict) else "") or "US"
    # company LinkedIn URL
    linkedin = ""
    for k in ("linkedin_url", "company_linkedin_url", "linkedin"):
        v = c.get(k)
        if v and "linkedin.com" in str(v):
            linkedin = v
            break
    if not linkedin and isinstance(c.get("linkedin"), dict):
        linkedin = c["linkedin"].get("url", "")
    return {
        "company_name": name,
        "website": website,
        "industry": industry,
        "employee_count": str(employees),
        "hq_city": city or "",
        "hq_country": country,
        "Company_Linkedin_url": linkedin,
    }


def main():
    key = os.getenv("BLITZ_API_KEY")
    if not key:
        log.error("BLITZ_API_KEY not set")
        sys.exit(1)
    api = API(key)
    cp = load_checkpoint()
    processed_keys = set(cp.get("processed", []))

    # Step 1: load or fetch company list
    if COMPANY_CACHE.exists():
        companies = json.loads(COMPANY_CACHE.read_text())
        log.info(f"Loaded {len(companies)} companies from cache")
    else:
        log.info("Fetching NYC training & coaching companies...")
        companies = []
        cursor = None
        page = 0
        while True:
            page += 1
            results, cursor = api.search_companies(SEARCH_PAYLOAD, cursor)
            if not results:
                break
            companies.extend(results)
            log.info(f"  page {page}: {len(results)} (total: {len(companies)})")
            if not cursor or _shutdown:
                break
        COMPANY_CACHE.write_text(json.dumps(companies, indent=2, default=str))
        log.info(f"Saved {len(companies)} companies to {COMPANY_CACHE.name}")

    # Step 2 + 3: waterfall + email
    file_exists = OUTPUT_CSV.exists() and OUTPUT_CSV.stat().st_size > 0
    csv_file = open(OUTPUT_CSV, "a", newline="", encoding="utf-8")
    writer = csv.DictWriter(csv_file, fieldnames=OUTPUT_COLUMNS, extrasaction="ignore")
    if not file_exists:
        writer.writeheader()

    found_emails = 0
    no_linkedin = 0
    no_person = 0

    for i, co in enumerate(companies):
        if _shutdown:
            log.warning("Stopping — saving checkpoint")
            break

        fields = extract_co(co)
        key = fields["Company_Linkedin_url"] or fields["website"] or fields["company_name"]
        if not key or key in processed_keys:
            continue

        if not fields["Company_Linkedin_url"]:
            log.info(f"({i+1}/{len(companies)}) {fields['company_name']}: no LinkedIn URL — skip")
            no_linkedin += 1
            processed_keys.add(key)
            cp["processed"] = list(processed_keys)
            continue

        log.info(f"({i+1}/{len(companies)}) {fields['company_name']}")
        wf = api.waterfall(fields["Company_Linkedin_url"], CASCADE_FOUNDER, max_results=1)
        people = wf.get("results", []) if isinstance(wf, dict) else (wf if isinstance(wf, list) else [])

        if not people:
            log.info(f"  no founder/CEO found")
            no_person += 1
            processed_keys.add(key)
            cp["processed"] = list(processed_keys)
            if len(processed_keys) % 10 == 0:
                save_checkpoint(cp)
            continue

        p = people[0]
        full_name = p.get("full_name") or p.get("name") or ""
        parts = full_name.strip().split()
        first = parts[0] if parts else ""
        last = " ".join(parts[1:]) if len(parts) > 1 else ""
        title = p.get("job_title") or p.get("title") or ""
        person_linkedin = (p.get("person_linkedin_url") or p.get("linkedin_profile_url")
                           or p.get("linkedin_url") or "")

        # Enrich email
        email = ""
        if person_linkedin:
            er = api.enrich_email(person_linkedin)
            if er.get("email"):
                email = er["email"]
            elif er.get("all_emails"):
                email = er["all_emails"][0].get("email_address", "")

        if email:
            row = dict(fields)
            row.update({
                "contact_first_name": first,
                "contact_last_name": last,
                "contact_title": title,
                "contact_linkedin_url": person_linkedin,
                "contact_email": email,
            })
            writer.writerow(row)
            csv_file.flush()
            found_emails += 1
            log.info(f"  ✓ {full_name} ({title}) — {email}")
        else:
            log.info(f"  {full_name} ({title}) — no email")

        processed_keys.add(key)
        cp["processed"] = list(processed_keys)
        if len(processed_keys) % 10 == 0:
            save_checkpoint(cp)

    save_checkpoint(cp)
    csv_file.close()

    log.info("")
    log.info("=" * 50)
    log.info(f"  DONE")
    log.info(f"  Companies processed:  {len(processed_keys)}")
    log.info(f"  No LinkedIn URL:      {no_linkedin}")
    log.info(f"  No founder found:     {no_person}")
    log.info(f"  Verified emails:      {found_emails}")
    log.info(f"  Output: {OUTPUT_CSV}")
    log.info("=" * 50)


if __name__ == "__main__":
    main()
