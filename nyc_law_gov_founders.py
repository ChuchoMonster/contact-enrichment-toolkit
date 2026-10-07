#!/usr/bin/env python3
"""NYC Law & Government (ex. Government Administration) — founders/CEOs."""

from __future__ import annotations

import csv
import json
import os
import signal
import sys
import time
import logging
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv()

API_BASE = "https://api.blitz-api.ai/api"
API_BASE_V2 = "https://api.blitz-api.ai"
REQUEST_DELAY = 0.25

OUTPUT_DIR = Path(__file__).resolve().parent
OUTPUT_CSV = OUTPUT_DIR / "nyc_law_gov_founders.csv"
COMPANY_CACHE = OUTPUT_DIR / "nyc_law_gov_companies.json"
CHECKPOINT_FILE = OUTPUT_DIR / "nyc_law_gov_checkpoint.json"
LOG_FILE = OUTPUT_DIR / "nyc_law_gov.log"

LAW_GOV = [
    "Law Practice", "Legal Services",
    "Government Relations", "Government Relations Services",
    "Political Organization", "Political Organizations",
    "Public Policy", "Public Policy Offices",
    "Public Assistance Programs", "Public Safety", "Public Works",
    "Emergency and Relief Services",
    "Military", "Military and International Affairs",
    "Translation and Localization",
]

SEARCH_PAYLOAD = {
    "company": {
        "employee_range": ["11-50"],
        "hq": {"country_code": ["US"], "city": {"include": ["New York"]}},
        "industry": {"include": LAW_GOV},
    },
    "max_results": 50,
}

EXCLUDE_TITLES = ["analyst", "associate", "coordinator", "assistant",
                  "intern", "junior", "student"]

CASCADE_FOUNDER = [
    {
        "include_title": [
            "CEO", "Chief Executive Officer", "Founder", "Co-Founder",
            "Co-founder", "Owner", "Managing Director", "Managing Partner",
            "President", "Executive Director", "Principal",
        ],
        "exclude_title": EXCLUDE_TITLES + ["vice president", "VP", "senior vice", "EVP", "SVP"],
        "location": ["WORLD"],
        "include_headline_search": True,
    },
]

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
log = logging.getLogger("nyc_law_gov")

_shutdown = False
def _sig(sig, frame):
    global _shutdown
    _shutdown = True
    log.warning("Shutdown requested")
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
                    log.warning(f"Rate limited — sleep {wait}s")
                    time.sleep(wait)
                    continue
                if r.status_code >= 500:
                    time.sleep(2 ** (attempt + 1))
                    continue
                log.warning(f"{path} {r.status_code}: {r.text[:200]}")
                return None
            except requests.RequestException as e:
                log.warning(f"Net err: {e}")
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

    def waterfall(self, company_linkedin, cascade, max_results=1):
        return self._post(API_BASE, "/search/waterfall-icp", {
            "company_linkedin_url": company_linkedin,
            "max_results": max_results,
            "cascade": cascade,
        }) or {}

    def enrich_email(self, linkedin_profile_url):
        return self._post(API_BASE, "/enrichment/email", {
            "linkedin_profile_url": linkedin_profile_url,
        }) or {}


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
    linkedin = ""
    for k in ("linkedin_url", "company_linkedin_url", "linkedin"):
        v = c.get(k)
        if v and "linkedin.com" in str(v):
            linkedin = v
            break
    return {
        "company_name": name,
        "website": website,
        "industry": industry,
        "employee_count": str(employees),
        "hq_city": city or "",
        "hq_country": country,
        "Company_Linkedin_url": linkedin,
    }


def load_checkpoint():
    if CHECKPOINT_FILE.exists():
        try:
            return json.loads(CHECKPOINT_FILE.read_text())
        except Exception:
            pass
    return {"processed": []}


def save_checkpoint(cp):
    CHECKPOINT_FILE.write_text(json.dumps(cp, indent=2))


def main():
    key = os.getenv("BLITZ_API_KEY")
    if not key:
        log.error("BLITZ_API_KEY not set")
        sys.exit(1)
    api = API(key)

    if COMPANY_CACHE.exists():
        companies = json.loads(COMPANY_CACHE.read_text())
        log.info(f"Loaded {len(companies)} from cache")
    else:
        log.info("Fetching NYC law & government companies (excl. Gov Admin)...")
        companies = []
        cursor = None
        page = 0
        while True:
            page += 1
            results, cursor = api.search_companies(SEARCH_PAYLOAD, cursor)
            if not results:
                break
            companies.extend(results)
            log.info(f"  page {page}: +{len(results)} (total {len(companies)})")
            if not cursor or _shutdown:
                break
        COMPANY_CACHE.write_text(json.dumps(companies, indent=2, default=str))
        log.info(f"Saved {len(companies)} companies")

    cp = load_checkpoint()
    processed = set(cp.get("processed", []))

    file_exists = OUTPUT_CSV.exists() and OUTPUT_CSV.stat().st_size > 0
    csv_file = open(OUTPUT_CSV, "a", newline="", encoding="utf-8")
    writer = csv.DictWriter(csv_file, fieldnames=OUTPUT_COLUMNS, extrasaction="ignore")
    if not file_exists:
        writer.writeheader()

    found_emails = no_linkedin = no_person = 0

    for i, co in enumerate(companies):
        if _shutdown:
            log.warning("Stopping")
            break
        fields = extract_co(co)
        key_co = fields["Company_Linkedin_url"] or fields["website"] or fields["company_name"]
        if not key_co or key_co in processed:
            continue
        if not fields["Company_Linkedin_url"]:
            log.info(f"({i+1}/{len(companies)}) {fields['company_name']}: no LinkedIn — skip")
            no_linkedin += 1
            processed.add(key_co)
            continue

        log.info(f"({i+1}/{len(companies)}) {fields['company_name']}")
        wf = api.waterfall(fields["Company_Linkedin_url"], CASCADE_FOUNDER, max_results=1)
        people = wf.get("results", []) if isinstance(wf, dict) else (wf if isinstance(wf, list) else [])

        if not people:
            log.info(f"  no founder/CEO")
            no_person += 1
            processed.add(key_co)
            if len(processed) % 10 == 0:
                cp["processed"] = list(processed)
                save_checkpoint(cp)
            continue

        p = people[0]
        full_name = p.get("full_name") or ""
        parts = full_name.strip().split()
        first = parts[0] if parts else ""
        last = " ".join(parts[1:]) if len(parts) > 1 else ""
        title = p.get("job_title") or ""
        person_linkedin = (p.get("person_linkedin_url") or p.get("linkedin_profile_url")
                           or p.get("linkedin_url") or "")

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

        processed.add(key_co)
        if len(processed) % 20 == 0:
            cp["processed"] = list(processed)
            save_checkpoint(cp)

    cp["processed"] = list(processed)
    save_checkpoint(cp)
    csv_file.close()

    log.info("")
    log.info("=" * 50)
    log.info(f"  DONE")
    log.info(f"  Companies processed:  {len(processed)}")
    log.info(f"  No LinkedIn URL:      {no_linkedin}")
    log.info(f"  No founder found:     {no_person}")
    log.info(f"  Verified emails:      {found_emails}")
    log.info(f"  Output: {OUTPUT_CSV}")
    log.info("=" * 50)


if __name__ == "__main__":
    main()
