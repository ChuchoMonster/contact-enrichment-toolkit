#!/usr/bin/env python3
"""
RevOps Agencies Pipeline — name-known contact enrichment.

Input is a list of known people (name + company + title) at RevOps agencies.
Strategy:

  Step 1: Resolve each unique agency name → company LinkedIn URL via
          /search/domain-to-linkedin-company (preferred, reliable) with a
          /v2/search/companies name-include fallback.
  Step 2: For each row build a TARGETED cascade based on the known title (e.g.
          "Chief Operating Officer" → COO/Operations terms). Run waterfall-icp
          with include_headline_search=True and max_results=20, then fuzzy-
          match returned people against the known first+last name.
  Step 3: Email-enrich on matched personal LinkedIn URLs.

Input:  revops_agencies_input.csv  (agency, domain [optional], first_name, last_name, title, role)
Output: revops_agencies_results.csv (input cols + company_linkedin_url,
        matched_name, matched_title, person_linkedin_url, email, match_score,
        status)
"""
from __future__ import annotations

import csv
import logging
import os
import re
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from difflib import SequenceMatcher
from pathlib import Path

from dotenv import load_dotenv

from blitz_core import BlitzAPIClient, extract_company_linkedin

INPUT_FILE = Path(__file__).parent / "revops_agencies_input.csv"
OUTPUT_FILE = Path(__file__).parent / "revops_agencies_results.csv"
LOG_FILE = Path(__file__).parent / "revops_agencies_pipeline.log"

WATERFALL_MAX_RESULTS = 20
WORKERS = 2


OUTPUT_COLUMNS = [
    "agency", "first_name", "last_name", "title", "role",
    "company_linkedin_url", "matched_name", "matched_title",
    "person_linkedin_url", "email", "match_score", "status",
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("revops_agencies")


# ---------------------------------------------------------------------------
# Name normalization & matching
# ---------------------------------------------------------------------------

def normalize_name(name: str) -> str:
    if not name:
        return ""
    name = re.sub(r"\b(mr|mrs|ms|dr|prof|jr|sr|ii|iii|iv)\b", "", name.lower())
    name = re.sub(r"[^a-z\s]", " ", name)
    return " ".join(name.split())


def name_match_score(target_first: str, target_last: str, api_full_name: str) -> float:
    target = normalize_name(f"{target_first} {target_last}")
    candidate = normalize_name(api_full_name)
    if not target or not candidate:
        return 0.0
    if target == candidate:
        return 1.0

    t_parts = target.split()
    c_parts = candidate.split()
    t_first = t_parts[0] if t_parts else ""
    t_last = t_parts[-1] if t_parts else ""
    c_first = c_parts[0] if c_parts else ""
    c_last = c_parts[-1] if c_parts else ""

    # Both first and last name match (handles middle names)
    if t_first and t_last and t_first in c_parts and t_last in c_parts:
        return 0.95
    if t_first and t_last and t_first == c_first and t_last == c_last:
        return 0.95
    # Last-name match with first-name initial / similar (handles "Jen" vs "Jennifer")
    if t_last and t_last == c_last:
        if t_first and c_first and (
            t_first.startswith(c_first[:1]) or c_first.startswith(t_first[:1])
        ):
            return 0.85
        return 0.7
    if t_first and t_first == c_first:
        return 0.55
    return SequenceMatcher(None, target, candidate).ratio()


# ---------------------------------------------------------------------------
# Step 1: Resolve agency → company LinkedIn URL
# ---------------------------------------------------------------------------

def resolve_company_linkedin(client: BlitzAPIClient, agency_name: str,
                             domain: str = "") -> str:
    # 1) Domain-to-linkedin (most reliable) when the input row supplies a domain
    domain = (domain or "").strip()
    if domain:
        data = client._post("/search/domain-to-linkedin-company", {"domain": domain})
        if data and data.get("company_linkedin_url"):
            return data["company_linkedin_url"]

    # 2) Fall back to name search
    data = client._post_v2("/v2/search/companies", {
        "company": {"name": {"include": [agency_name]}},
        "max_results": 5,
    })
    if not data:
        return ""
    if isinstance(data, list):
        results = data
    else:
        results = data.get("results") or data.get("data") or data.get("companies") or []
    if not results:
        return ""

    target = normalize_name(agency_name)
    best_url = ""
    best_score = 0.0
    for company in results:
        url = extract_company_linkedin(company)
        if not url:
            continue
        candidate_name = (company.get("name") or company.get("company_name")
                          or company.get("organization_name") or "")
        score = SequenceMatcher(None, target, normalize_name(candidate_name)).ratio()
        if score > best_score:
            best_score = score
            best_url = url
    return best_url


# ---------------------------------------------------------------------------
# Title → targeted cascade
# ---------------------------------------------------------------------------

EXCLUDE_TITLES = ["intern", "junior", "student", "assistant", "coordinator"]

# Generic broad-senior fallback used when no narrow cascade hits.
BROAD_SENIOR_TIER = {
    "include_title": [
        "Founder", "Co-Founder", "CEO", "President", "Owner",
        "Chief", "VP", "Vice President", "SVP", "EVP",
        "Head of", "Director",
    ],
    "exclude_title": EXCLUDE_TITLES,
    "location": ["WORLD"],
    "include_headline_search": True,
}


def build_cascade_for_title(title: str) -> list[dict]:
    """Build a multi-tier cascade. Tier 1 is narrow (title-derived), Tier 2 is
    broad senior fallback to surface the person if their indexed title differs."""
    t = title.lower()
    narrow_terms: list[str] = []

    if "founder" in t or "ceo" in t or "chief executive" in t or "co-ceo" in t:
        narrow_terms = ["Founder", "Co-Founder", "Cofounder", "CEO", "Co-CEO",
                        "Chief Executive", "President", "Owner"]
    elif "coo" in t or "operating" in t or "operations" in t and "vp" not in t:
        narrow_terms = ["COO", "Chief Operating", "Chief Operations",
                        "Head of Operations", "VP Operations"]
    elif "cro" in t or "chief revenue" in t:
        narrow_terms = ["CRO", "Chief Revenue", "Chief Commercial",
                        "VP Revenue", "Head of Revenue"]
    elif "ccо" in t or "chief commercial" in t or "chief customer" in t:
        narrow_terms = ["Chief Commercial", "Chief Customer", "CCO",
                        "Chief Revenue", "VP Commercial", "Head of Commercial"]
    elif "cxo" in t or "experience" in t:
        narrow_terms = ["Chief Experience", "CXO", "Chief Customer Experience",
                        "VP Experience", "Head of Experience",
                        "Chief Customer Officer"]
    elif "cmo" in t or "chief marketing" in t:
        narrow_terms = ["CMO", "Chief Marketing", "VP Marketing",
                        "Head of Marketing"]
    elif "cto" in t or "chief technology" in t:
        narrow_terms = ["CTO", "Chief Technology", "VP Engineering",
                        "Head of Engineering"]
    elif "vp" in t or "vice president" in t:
        # Try to extract the function (RevOps, Business Operations, etc.)
        narrow_terms = [
            "VP", "Vice President", "SVP", "EVP",
            "Head of", "Director",
        ]
        # Add specific function terms from the title
        for fn in ["RevOps", "Revenue Operations", "Business Operations",
                   "Operations", "Sales", "Marketing", "Customer Success"]:
            if fn.lower() in t:
                narrow_terms.append(f"VP {fn}")
                narrow_terms.append(f"Head of {fn}")
                narrow_terms.append(f"Director of {fn}")
    elif "director" in t:
        narrow_terms = ["Director", "Head of", "Senior Director"]
        for fn in ["Delivery", "Operations", "Customer Success", "Services"]:
            if fn.lower() in t:
                narrow_terms.append(f"Director of {fn}")
                narrow_terms.append(f"Head of {fn}")

    cascade = []
    if narrow_terms:
        cascade.append({
            "include_title": narrow_terms,
            "exclude_title": EXCLUDE_TITLES,
            "location": ["WORLD"],
            "include_headline_search": True,
        })
    cascade.append(BROAD_SENIOR_TIER)
    return cascade


# ---------------------------------------------------------------------------
# Per-row pipeline
# ---------------------------------------------------------------------------

def process_row(client: BlitzAPIClient, row: dict, company_li_cache: dict) -> dict:
    agency = row["agency"].strip()
    first = row["first_name"].strip()
    last = row["last_name"].strip()
    title = row["title"].strip()

    out = {
        **row,
        "company_linkedin_url": "",
        "matched_name": "",
        "matched_title": "",
        "person_linkedin_url": "",
        "email": "",
        "match_score": "",
        "status": "",
    }

    company_li = company_li_cache.get(agency, "")
    out["company_linkedin_url"] = company_li
    if not company_li:
        out["status"] = "no_company_linkedin"
        return out

    cascade = build_cascade_for_title(title)
    people = client.waterfall_icp(company_li, cascade,
                                  max_results=WATERFALL_MAX_RESULTS)
    if not people:
        out["status"] = "no_people_returned"
        return out

    best = None
    best_score = 0.0
    for p in people:
        full = (p.get("full_name") or "").strip()
        if not full:
            f = p.get("first_name") or ""
            l = p.get("last_name") or ""
            full = f"{f} {l}".strip()
        score = name_match_score(first, last, full)
        if score > best_score:
            best_score = score
            best = p

    # 0.80 threshold: requires both first and last name to match. Last-name-only
    # matches (e.g. Patrick → Dana Biddiscombe) score 0.70 and are correctly
    # rejected as likely-different-person false positives.
    if not best or best_score < 0.80:
        out["status"] = f"not_in_blitz_index (best={best_score:.2f}, n={len(people)})"
        out["match_score"] = f"{best_score:.2f}"
        return out

    person_url = (best.get("person_linkedin_url")
                  or best.get("linkedin_profile_url")
                  or best.get("linkedin_url") or "")
    out["matched_name"] = best.get("full_name", "")
    out["matched_title"] = (best.get("job_title") or best.get("title") or "")
    out["person_linkedin_url"] = person_url
    out["match_score"] = f"{best_score:.2f}"

    if not person_url:
        out["status"] = "match_no_linkedin"
        return out

    email = client.enrich_email(person_url) or ""
    out["email"] = email
    out["status"] = "ok" if email else "no_email"
    return out


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    load_dotenv()
    api_key = os.getenv("BLITZ_API_KEY")
    if not api_key:
        log.error("BLITZ_API_KEY not set in .env")
        sys.exit(1)

    if not INPUT_FILE.exists():
        log.error(f"Input file not found: {INPUT_FILE}")
        sys.exit(1)

    with INPUT_FILE.open("r", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    log.info(f"Loaded {len(rows)} contacts across "
             f"{len(set(r['agency'] for r in rows))} agencies")

    client = BlitzAPIClient(api_key)

    # Step 1: resolve unique agencies once
    unique_agencies = sorted({r["agency"].strip() for r in rows})
    agency_domains = {r["agency"].strip(): (r.get("domain") or "").strip()
                      for r in rows if (r.get("domain") or "").strip()}
    log.info(f"[step1] Resolving {len(unique_agencies)} unique agency LinkedIn URLs")
    company_li_cache = {}
    for agency in unique_agencies:
        li = resolve_company_linkedin(client, agency, agency_domains.get(agency, ""))
        company_li_cache[agency] = li
        log.info(f"[step1] {agency!r:35} → {li or '(NOT FOUND)'}")

    # Steps 2 + 3: per-row
    log.info(f"[step2+3] Processing {len(rows)} contacts (workers={WORKERS})")
    results: list[dict] = [None] * len(rows)
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = {pool.submit(process_row, client, r, company_li_cache): i
                   for i, r in enumerate(rows)}
        for fut in as_completed(futures):
            idx = futures[fut]
            try:
                res = fut.result()
            except Exception as e:
                log.warning(f"[step2+3] row {idx} crashed: {e}")
                res = {**rows[idx], "status": f"error: {e}"}
            results[idx] = res
            row = rows[idx]
            tag = res.get("status", "")
            email = res.get("email", "")
            score = res.get("match_score", "")
            matched = res.get("matched_name", "")
            log.info(f"  {row['first_name']} {row['last_name']:<22} "
                     f"@ {row['agency']:<22} | {tag:<32} | "
                     f"score={score} | matched={matched or '-'} | email={email or '-'}")

    # Write output
    with OUTPUT_FILE.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        for r in results:
            writer.writerow(r)

    matched = sum(1 for r in results if r.get("person_linkedin_url"))
    ok = sum(1 for r in results if r.get("status") == "ok")
    log.info("=" * 60)
    log.info(f"COMPLETE: {len(rows)} rows processed")
    log.info(f"  Personal LinkedIn matched: {matched}/{len(rows)}")
    log.info(f"  Verified emails:           {ok}/{len(rows)}")
    log.info(f"  Output: {OUTPUT_FILE}")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
