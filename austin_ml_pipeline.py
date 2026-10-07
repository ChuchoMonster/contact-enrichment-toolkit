#!/usr/bin/env python3
"""
Austin Medical + Legal Lead Pipeline

4-step pipeline for Austin, TX medical and legal practice decision-makers:
  Step 1: Google Maps scrape (SerpAPI) for new medical queries +
          merge prior law/CPA/accountant rows from austin_maps_results.csv
  Step 2: Domain → Company LinkedIn URL (Blitz)
  Step 3: Waterfall ICP — Owner/Founder cascade (Blitz)
  Step 4: Email enrichment (Blitz)

Usage:
    python austin_ml_pipeline.py --step 1    # Maps scrape + merge
    python austin_ml_pipeline.py --step 2    # LinkedIn lookup
    python austin_ml_pipeline.py --step 3    # Waterfall ICP
    python austin_ml_pipeline.py --step 4    # Email enrichment
    python austin_ml_pipeline.py --all       # Run all 4 sequentially
    python austin_ml_pipeline.py --status    # Print checkpoint progress
"""
from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import re
import shutil
import signal
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.parse import urlparse

import requests
from dotenv import load_dotenv

from blitz_core import BlitzAPIClient

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
load_dotenv()

SERPAPI_KEY = os.getenv("SERPAPI_KEY")

OUTPUT_DIR = Path(__file__).parent
PRIOR_MAPS_FILE = OUTPUT_DIR / "austin_maps_results.csv"
MAPS_FILE = OUTPUT_DIR / "austin_ml_maps_results.csv"
LINKEDIN_FILE = OUTPUT_DIR / "austin_ml_linkedin_lookup.csv"
PEOPLE_FILE = OUTPUT_DIR / "austin_ml_people.csv"
CONTACTS_FILE = OUTPUT_DIR / "austin_ml_contacts_verified_emails.csv"
CHECKPOINT_FILE = OUTPUT_DIR / "austin_ml_checkpoint.json"
LOG_FILE = OUTPUT_DIR / "austin_ml_pipeline.log"

DOWNLOADS_COPY = Path.home() / "Downloads" / "austin_ml_contacts_verified_emails.csv"

AUSTIN_LL = "@30.2672,-97.7431,13z"

# Categories and their SerpAPI queries
NEW_QUERIES = {
    "dentists": [
        "dentist Austin TX", "dental office Austin TX",
    ],
    "physical_therapists": [
        "physical therapist Austin TX", "physical therapy clinic Austin TX",
    ],
    "occupational_therapists": [
        "occupational therapist Austin TX", "occupational therapy clinic Austin TX",
    ],
    "physiatrists": [
        "physiatrist Austin TX", "physical medicine and rehabilitation doctor Austin TX",
    ],
    "rehab_specialists": [
        "rehabilitation center Austin TX", "rehab specialist Austin TX",
    ],
    "physiologists": [
        "exercise physiologist Austin TX", "clinical physiologist Austin TX",
    ],
    "chiropractors": [
        "chiropractor Austin TX", "chiropractic clinic Austin TX",
    ],
    "mental_health": [
        "mental health counselor Austin TX", "mental health clinic Austin TX",
    ],
    "therapists": [
        "therapist Austin TX", "counselor Austin TX",
    ],
    "psychologists_psychiatrists": [
        "psychologist Austin TX", "psychiatrist Austin TX",
    ],
}

# Search queries from the prior run we want to REUSE (not re-scrape)
PRIOR_QUERIES_TO_REUSE = {
    "legal_offices": {
        "law firm Austin TX",
        "personal injury lawyer Austin TX",
        "family law attorney Austin TX",
        "criminal defense lawyer Austin TX",
        "estate planning attorney Austin TX",
    },
    "accountants": {
        "CPA firm Austin TX",
        "accountant Austin TX",
        "tax preparation Austin TX",
    },
}

# Waterfall cascade — owner / founder / managing partner / principal / president
CASCADE_OWNER = [
    {
        "include_title": [
            "Owner", "Founder", "Co-Founder", "Co Founder",
            "CEO", "Chief Executive Officer",
            "Managing Partner", "Managing Director",
            "Managing Member", "Principal", "President",
            "Proprietor", "General Manager",
        ],
        "exclude_title": [
            "Vice President", "VP", "Senior Vice",
            "EVP", "SVP", "Assistant", "Associate",
            "intern", "junior", "student", "part-time",
        ],
        "location": ["WORLD"],
        "include_headline_search": False,
    },
]

MAX_PEOPLE_PER_COMPANY = 2
LOOKUP_WORKERS = 5
WATERFALL_WORKERS = 3
ENRICH_WORKERS = 3

MAPS_COLUMNS = [
    "company_name", "domain", "website", "address", "phone",
    "rating", "reviews", "type", "category", "search_query", "place_id",
]
LINKEDIN_COLUMNS = MAPS_COLUMNS + ["company_linkedin_url"]
PEOPLE_COLUMNS = LINKEDIN_COLUMNS + [
    "full_name", "first_name", "last_name", "title", "person_linkedin_url",
]
CONTACT_COLUMNS = PEOPLE_COLUMNS + ["email"]

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
    log.info("Shutdown requested — finishing current work then saving checkpoint…")


signal.signal(signal.SIGINT, _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)


# ---------------------------------------------------------------------------
# Checkpoint
# ---------------------------------------------------------------------------

class Checkpoint:
    def __init__(self, path: Path):
        self.path = path
        self.data = {
            "maps_queries_done": [],
            "lookup_processed": [],
            "waterfall_processed": [],
            "enrich_processed": [],
        }
        if path.exists():
            self.data.update(json.loads(path.read_text()))

    def save(self):
        self.path.write_text(json.dumps(self.data, indent=2))

    def mark(self, key: str, value: str):
        lst = self.data.setdefault(key, [])
        if value not in lst:
            lst.append(value)

    def done(self, key: str, value: str) -> bool:
        return value in self.data.get(key, [])


# ---------------------------------------------------------------------------
# CSV helpers
# ---------------------------------------------------------------------------

def _ensure_csv(path: Path, columns: list[str]):
    if not path.exists():
        with path.open("w", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=columns).writeheader()


def _append_rows(path: Path, columns: list[str], rows: list[dict]):
    if not rows:
        return
    _ensure_csv(path, columns)
    with path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
        for row in rows:
            writer.writerow(row)


def _read_rows(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def _extract_domain(url: str) -> str:
    if not url:
        return ""
    try:
        parsed = urlparse(url if "://" in url else f"https://{url}")
        domain = (parsed.netloc or parsed.path).lower().replace("www.", "")
        return domain.split("/")[0]
    except Exception:
        return ""


# ---------------------------------------------------------------------------
# Step 1: Maps scrape (new queries) + merge prior law/CPA/accountant rows
# ---------------------------------------------------------------------------

def step1_maps(ckpt: Checkpoint):
    # Seed with prior results (law / CPA / accountant rows) — dedupe by place_id
    seen_place_ids: set[str] = set()
    results: list[dict] = []

    if PRIOR_MAPS_FILE.exists():
        reuse_queries: set[str] = set()
        for qset in PRIOR_QUERIES_TO_REUSE.values():
            reuse_queries.update(qset)
        category_lookup = {q: cat for cat, queries in PRIOR_QUERIES_TO_REUSE.items()
                           for q in queries}

        reused = 0
        with PRIOR_MAPS_FILE.open(newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                if row.get("search_query", "") in reuse_queries:
                    pid = row.get("place_id", "")
                    if pid and pid not in seen_place_ids:
                        seen_place_ids.add(pid)
                        new_row = {k: row.get(k, "") for k in MAPS_COLUMNS}
                        # Rewrite category to our new taxonomy
                        new_row["category"] = category_lookup[row["search_query"]]
                        results.append(new_row)
                        reused += 1
        log.info(f"[maps] Reused {reused} rows from {PRIOR_MAPS_FILE.name}")

    # Scrape new queries
    total_queries = sum(len(v) for v in NEW_QUERIES.values())
    query_num = 0
    for category, queries in NEW_QUERIES.items():
        for query in queries:
            query_num += 1
            if ckpt.done("maps_queries_done", query):
                log.info(f"[maps] {query_num}/{total_queries} skipping (cached): {query}")
                continue
            if _shutdown_requested:
                break
            log.info(f"[maps] {query_num}/{total_queries} scraping: {query}")
            for start in (0, 20, 40):
                try:
                    resp = requests.get(
                        "https://serpapi.com/search",
                        params={
                            "engine": "google_maps",
                            "q": query,
                            "type": "search",
                            "ll": AUSTIN_LL,
                            "start": start,
                            "api_key": SERPAPI_KEY,
                        },
                        timeout=30,
                    )
                    if resp.status_code != 200:
                        log.warning(f"  SerpAPI {resp.status_code} at start={start}")
                        break
                    data = resp.json()
                    page = data.get("local_results", [])
                    if not page:
                        break
                    added = 0
                    for r in page:
                        pid = r.get("place_id", "")
                        if pid and pid not in seen_place_ids:
                            seen_place_ids.add(pid)
                            website = r.get("website", "")
                            results.append({
                                "company_name": r.get("title", ""),
                                "domain": _extract_domain(website),
                                "website": website,
                                "address": r.get("address", ""),
                                "phone": r.get("phone", ""),
                                "rating": r.get("rating", ""),
                                "reviews": r.get("reviews", ""),
                                "type": r.get("type", ""),
                                "category": category,
                                "search_query": query,
                                "place_id": pid,
                            })
                            added += 1
                    log.info(f"  start={start}: {len(page)} results, {added} new")
                    if len(page) < 20:
                        break
                    time.sleep(0.5)
                except Exception as e:
                    log.warning(f"  error start={start}: {e}")
                    break
            ckpt.mark("maps_queries_done", query)
            ckpt.save()

    # Write the merged CSV
    _ensure_csv(MAPS_FILE, MAPS_COLUMNS)
    MAPS_FILE.write_text("")
    _append_rows(MAPS_FILE, MAPS_COLUMNS, results)
    # Rewrite header (MAPS_FILE was truncated)
    with MAPS_FILE.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=MAPS_COLUMNS, extrasaction="ignore")
        w.writeheader()
        w.writerows(results)

    with_domain = sum(1 for r in results if r["domain"])
    log.info(f"[maps] Total businesses: {len(results)} (with domain: {with_domain})")
    # Per-category breakdown
    by_cat: dict[str, int] = {}
    for r in results:
        by_cat[r["category"]] = by_cat.get(r["category"], 0) + 1
    for cat in sorted(by_cat):
        log.info(f"  {cat}: {by_cat[cat]}")


# ---------------------------------------------------------------------------
# Step 2: Domain → LinkedIn company URL
# ---------------------------------------------------------------------------

def step2_linkedin_lookup(client: BlitzAPIClient, ckpt: Checkpoint):
    rows = _read_rows(MAPS_FILE)
    if not rows:
        log.warning("[lookup] No maps results — run step 1 first.")
        return

    with_domain = [r for r in rows if r.get("domain", "").strip()]
    log.info(f"[lookup] {len(with_domain)}/{len(rows)} have domains")

    # Merge with any prior results already on disk (for resumes)
    existing = {r["place_id"]: r for r in _read_rows(LINKEDIN_FILE)}
    already_done = {pid for pid, r in existing.items()
                    if ckpt.done("lookup_processed", pid)}
    pending = [r for r in with_domain if r["place_id"] not in already_done]

    log.info(f"[lookup] {len(pending)} pending lookups")

    def _lookup(biz: dict):
        domain = biz["domain"]
        data = client._post("/search/domain-to-linkedin-company", {"domain": domain})
        li = ""
        if data and data.get("found"):
            li = data.get("company_linkedin_url", "")
        return biz["place_id"], {**biz, "company_linkedin_url": li}

    _ensure_csv(LINKEDIN_FILE, LINKEDIN_COLUMNS)
    lock_rows: list[dict] = []
    done = 0
    found = 0

    with ThreadPoolExecutor(max_workers=LOOKUP_WORKERS) as pool:
        futures = [pool.submit(_lookup, b) for b in pending]
        for fut in as_completed(futures):
            if _shutdown_requested:
                break
            pid, row = fut.result()
            existing[pid] = row
            lock_rows.append(row)
            ckpt.mark("lookup_processed", pid)
            done += 1
            if row["company_linkedin_url"]:
                found += 1
            if done % 25 == 0:
                _append_rows(LINKEDIN_FILE, LINKEDIN_COLUMNS, lock_rows)
                lock_rows.clear()
                ckpt.save()
                log.info(f"[lookup] {done}/{len(pending)} done, {found} linkedin URLs found")

    if lock_rows:
        _append_rows(LINKEDIN_FILE, LINKEDIN_COLUMNS, lock_rows)
    ckpt.save()

    # Merge: re-write LINKEDIN_FILE with full merged set so partial runs accumulate
    merged = list(existing.values())
    with LINKEDIN_FILE.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=LINKEDIN_COLUMNS, extrasaction="ignore")
        w.writeheader()
        w.writerows(merged)
    total_found = sum(1 for r in merged if r.get("company_linkedin_url"))
    log.info(f"[lookup] Complete — {len(merged)} total, {total_found} with LinkedIn URL")


# ---------------------------------------------------------------------------
# Step 3: Waterfall ICP
# ---------------------------------------------------------------------------

def step3_waterfall(client: BlitzAPIClient, ckpt: Checkpoint):
    companies = [r for r in _read_rows(LINKEDIN_FILE)
                 if r.get("company_linkedin_url")]
    if not companies:
        log.warning("[waterfall] No companies with LinkedIn URLs — run step 2 first.")
        return

    pending = [c for c in companies
               if not ckpt.done("waterfall_processed", c["company_linkedin_url"])]
    log.info(f"[waterfall] {len(pending)}/{len(companies)} to process")

    _ensure_csv(PEOPLE_FILE, PEOPLE_COLUMNS)

    def _for_company(company: dict) -> list[dict]:
        linkedin = company["company_linkedin_url"]
        people = client.waterfall_icp(linkedin, CASCADE_OWNER,
                                       max_results=MAX_PEOPLE_PER_COMPANY)
        rows: list[dict] = []
        for p in people or []:
            full_name = (p.get("full_name") or "").strip()
            parts = full_name.split()
            first = p.get("first_name") or (parts[0] if parts else "")
            last = p.get("last_name") or (" ".join(parts[1:]) if len(parts) > 1 else "")
            title = p.get("job_title") or p.get("title") or ""
            person_li = (p.get("person_linkedin_url")
                         or p.get("linkedin_profile_url")
                         or p.get("linkedin_url") or "")
            if not person_li:
                continue
            rows.append({**company, "full_name": full_name, "first_name": first,
                         "last_name": last, "title": title,
                         "person_linkedin_url": person_li})
        return rows

    done = 0
    people_found = 0
    with ThreadPoolExecutor(max_workers=WATERFALL_WORKERS) as pool:
        futures = {pool.submit(_for_company, c): c for c in pending}
        for fut in as_completed(futures):
            if _shutdown_requested:
                break
            company = futures[fut]
            try:
                rows = fut.result()
            except Exception as e:
                log.warning(f"[waterfall] {company.get('company_name','?')}: {e}")
                rows = []
            _append_rows(PEOPLE_FILE, PEOPLE_COLUMNS, rows)
            ckpt.mark("waterfall_processed", company["company_linkedin_url"])
            done += 1
            people_found += len(rows)
            if done % 25 == 0:
                ckpt.save()
                log.info(f"[waterfall] {done}/{len(pending)} processed, "
                         f"{people_found} people found")
    ckpt.save()
    log.info(f"[waterfall] Complete — {done} companies, "
             f"{len(_read_rows(PEOPLE_FILE))} total people rows")


# ---------------------------------------------------------------------------
# Step 4: Email enrichment
# ---------------------------------------------------------------------------

def step4_enrich(client: BlitzAPIClient, ckpt: Checkpoint):
    people = _read_rows(PEOPLE_FILE)
    if not people:
        log.warning("[enrich] No people — run step 3 first.")
        return
    pending = [p for p in people
               if p.get("person_linkedin_url")
               and not ckpt.done("enrich_processed", p["person_linkedin_url"])]
    log.info(f"[enrich] {len(pending)}/{len(people)} to enrich")

    _ensure_csv(CONTACTS_FILE, CONTACT_COLUMNS)

    def _enrich(person: dict):
        url = person["person_linkedin_url"]
        email = client.enrich_email(url) or ""
        return {**person, "email": email}

    done = 0
    emails = 0
    with ThreadPoolExecutor(max_workers=ENRICH_WORKERS) as pool:
        futures = {pool.submit(_enrich, p): p for p in pending}
        for fut in as_completed(futures):
            if _shutdown_requested:
                break
            person = futures[fut]
            try:
                result = fut.result()
            except Exception as e:
                log.warning(f"[enrich] {person.get('full_name','?')}: {e}")
                continue
            _append_rows(CONTACTS_FILE, CONTACT_COLUMNS, [result])
            ckpt.mark("enrich_processed", person["person_linkedin_url"])
            done += 1
            if result.get("email"):
                emails += 1
            if done % 25 == 0:
                ckpt.save()
                log.info(f"[enrich] {done}/{len(pending)} processed, {emails} emails")
    ckpt.save()

    # Copy final to Downloads
    try:
        shutil.copyfile(CONTACTS_FILE, DOWNLOADS_COPY)
        log.info(f"[enrich] Copied final CSV to {DOWNLOADS_COPY}")
    except Exception as e:
        log.warning(f"[enrich] Could not copy to Downloads: {e}")
    log.info(f"[enrich] Complete — {done} processed, {emails} verified emails")


# ---------------------------------------------------------------------------
# Status
# ---------------------------------------------------------------------------

def print_status(ckpt: Checkpoint):
    maps = _read_rows(MAPS_FILE)
    li = _read_rows(LINKEDIN_FILE)
    pp = _read_rows(PEOPLE_FILE)
    cc = _read_rows(CONTACTS_FILE)
    with_email = sum(1 for r in cc if r.get("email"))
    print("=== Austin Medical + Legal Pipeline Status ===")
    print(f"Step 1 (maps):     {len(maps)} businesses "
          f"(queries done: {len(ckpt.data.get('maps_queries_done',[]))})")
    print(f"Step 2 (linkedin): {len(li)} rows; "
          f"processed: {len(ckpt.data.get('lookup_processed',[]))}")
    li_with = sum(1 for r in li if r.get("company_linkedin_url"))
    print(f"                   {li_with} companies with LinkedIn URL")
    print(f"Step 3 (waterfall):{len(pp)} people rows; "
          f"companies processed: {len(ckpt.data.get('waterfall_processed',[]))}")
    print(f"Step 4 (emails):   {len(cc)} contacts ({with_email} with verified email); "
          f"processed: {len(ckpt.data.get('enrich_processed',[]))}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--step", choices=["1", "2", "3", "4"])
    parser.add_argument("--all", action="store_true")
    parser.add_argument("--status", action="store_true")
    args = parser.parse_args()

    if not args.status and not SERPAPI_KEY:
        log.error("SERPAPI_KEY not set (set it in .env)")
        sys.exit(1)

    ckpt = Checkpoint(CHECKPOINT_FILE)

    if args.status:
        print_status(ckpt)
        return

    blitz_key = os.getenv("BLITZ_API_KEY")
    if not blitz_key:
        log.error("BLITZ_API_KEY not set")
        sys.exit(1)
    client = BlitzAPIClient(blitz_key)

    if args.all:
        step1_maps(ckpt)
        if _shutdown_requested:
            return
        step2_linkedin_lookup(client, ckpt)
        if _shutdown_requested:
            return
        step3_waterfall(client, ckpt)
        if _shutdown_requested:
            return
        step4_enrich(client, ckpt)
    elif args.step == "1":
        step1_maps(ckpt)
    elif args.step == "2":
        step2_linkedin_lookup(client, ckpt)
    elif args.step == "3":
        step3_waterfall(client, ckpt)
    elif args.step == "4":
        step4_enrich(client, ckpt)
    else:
        parser.print_help()
        print_status(ckpt)


if __name__ == "__main__":
    main()
