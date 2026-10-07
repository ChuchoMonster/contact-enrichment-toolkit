#!/usr/bin/env python3
"""Retry pass: catch founders Blitz missed by enabling headline search and broader titles."""
from __future__ import annotations

import csv
import json
import os
import sys
import time
import logging
from pathlib import Path
import requests
from dotenv import load_dotenv

load_dotenv()
sys.path.insert(0, str(Path(__file__).resolve().parent))
from nyc_training_founders import (
    API, OUTPUT_CSV, COMPANY_CACHE, OUTPUT_COLUMNS,
    extract_co, log, REQUEST_DELAY,
)

# Broader cascade w/ headline search
CASCADE_BROAD = [
    {
        "include_title": [
            "CEO", "Chief Executive Officer", "Founder", "Co-Founder",
            "Co-founder", "Owner", "President", "Managing Director",
            "Principal", "Executive Director",
        ],
        "exclude_title": ["analyst", "associate", "coordinator", "assistant",
                          "intern", "junior", "student", "vice president", "VP"],
        "location": ["WORLD"],
        "include_headline_search": True,
    },
]


def main():
    key = os.getenv("BLITZ_API_KEY")
    api = API(key)

    # Load companies + existing CSV to find what's already covered
    companies = json.loads(COMPANY_CACHE.read_text())
    existing_keys = set()
    if OUTPUT_CSV.exists():
        with open(OUTPUT_CSV) as f:
            for r in csv.DictReader(f):
                key_co = r.get("Company_Linkedin_url") or r.get("website") or r.get("company_name")
                if key_co:
                    existing_keys.add(key_co)

    log.info(f"Existing verified-email rows: {len(existing_keys)}")
    log.info(f"Total companies: {len(companies)}")

    csv_file = open(OUTPUT_CSV, "a", newline="", encoding="utf-8")
    writer = csv.DictWriter(csv_file, fieldnames=OUTPUT_COLUMNS, extrasaction="ignore")

    new_emails = 0
    tried = 0

    for i, co in enumerate(companies):
        fields = extract_co(co)
        key_co = fields["Company_Linkedin_url"] or fields["website"] or fields["company_name"]
        if not fields["Company_Linkedin_url"] or key_co in existing_keys:
            continue
        tried += 1
        log.info(f"({tried}) {fields['company_name']}")
        wf = api.waterfall(fields["Company_Linkedin_url"], CASCADE_BROAD, max_results=1)
        people = wf.get("results", []) if isinstance(wf, dict) else (wf if isinstance(wf, list) else [])
        if not people:
            log.info(f"  still no match")
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
            new_emails += 1
            log.info(f"  ✓ {full_name} ({title}) — {email}")
        else:
            log.info(f"  {full_name} ({title}) — no email")

    csv_file.close()
    log.info("")
    log.info(f"Retry pass complete: {new_emails} new emails added (tried {tried})")


if __name__ == "__main__":
    main()
