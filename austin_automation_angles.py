#!/usr/bin/env python3
"""
Austin Automation Angles — Firecrawl-augmented automation angles for each
company in the Austin medical/legal contacts list.

For each unique website domain in the input CSV:
  1. Call Firecrawl /v1/extract with a schema asking for exactly 3 short
     AI-automation angles grounded in the site's real content.
  2. If that fails or returns <3, fall back to category-generic angles.
  3. Write the result back to the automation_angle column for every row
     sharing that domain.

Usage:
    python austin_automation_angles.py           # run
    python austin_automation_angles.py --status  # show checkpoint progress
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

import requests
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
load_dotenv()

FIRECRAWL_API_KEY = os.getenv("FIRECRAWL_API_KEY")
FIRECRAWL_API_BASE = "https://api.firecrawl.dev/v1"

PROJECT_DIR = Path(__file__).parent
DATA_DIR = Path(os.environ.get("BLITZ_DATA_DIR", Path(__file__).resolve().parent / "data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)
INPUT_CSV = DATA_DIR / "austin_contacts_with_automation.csv"
OUTPUT_CSV = PROJECT_DIR / "austin_contacts_with_automation_v2.csv"
DOWNLOADS_COPY = Path.home() / "Downloads" / "austin_contacts_with_automation_v2.csv"
CHECKPOINT_FILE = PROJECT_DIR / "austin_automation_checkpoint.json"
LOG_FILE = PROJECT_DIR / "austin_automation.log"

WORKERS = 3
ANGLE_COLUMN = "automation_angle"

# ---------------------------------------------------------------------------
# Category-generic fallback angles (used when Firecrawl fails or returns < 3)
# ---------------------------------------------------------------------------
CATEGORY_FALLBACKS: dict[str, list[str]] = {
    "legal_offices": [
        "Intake triage bot that routes inquiries to the right attorney by practice area.",
        "Automated client status updates and court-date reminders via SMS or email.",
        "Document drafting assistants that turn intake forms into first-draft pleadings.",
    ],
    "accountants": [
        "Automated client document-chase workflows for missing tax forms during season.",
        "AI categorization of transactions and receipt capture for monthly bookkeeping clients.",
        "Engagement-letter and onboarding-packet automation for every new client signup.",
    ],
    "dentists": [
        "Automated new-patient intake, insurance verification, and consent forms before visit one.",
        "SMS appointment-reminder and rebooking nudges to reduce no-shows and gaps.",
        "Post-visit review-request automation to grow local Google presence.",
    ],
    "physical_therapists": [
        "Automated home-exercise-program delivery and daily adherence check-ins by text.",
        "Insurance-auth and visit-limit tracking that alerts front desk before capacity.",
        "Progress-note drafting from therapist voice memos after each session.",
    ],
    "occupational_therapists": [
        "Automated pediatric or adult intake and parent-education packets before first visit.",
        "Sensory/daily-living home program reminders and adherence tracking by SMS.",
        "Insurance-auth tracking with alerts when visits approach the authorized cap.",
    ],
    "physiatrists": [
        "Pre-visit patient history and imaging-review summary generated before each consult.",
        "Automated referral intake and triage from orthopedics and primary care partners.",
        "Post-injection follow-up and outcome-tracking surveys sent on a schedule.",
    ],
    "rehab_specialists": [
        "Cross-discipline scheduling automation for PT, OT, and speech within one case.",
        "Care-plan adherence tracking with nudges to families and caregivers.",
        "Insurance-auth tracking with alerts before authorized-visit caps.",
    ],
    "physiologists": [
        "Automated assessment-to-program generation so new clients get a first workout plan fast.",
        "Session-prep summaries auto-drafted from last visit notes and wearable data.",
        "Renewal and package-expiration nudges to keep clients on a recurring cadence.",
    ],
    "chiropractors": [
        "Automated new-patient intake and insurance-verification forms before the first visit.",
        "SMS appointment-reminder and rebooking nudges to reduce no-shows.",
        "Post-visit review-request automation to grow local Google presence.",
    ],
    "mental_health": [
        "Automated intake, screening-questionnaire scoring, and therapist matching.",
        "Session-note draft generation from therapist voice memos after each appointment.",
        "Between-session check-in messages and homework reminders by SMS.",
    ],
    "therapists": [
        "Automated intake and screening with therapist-match routing before the first call.",
        "Session-note draft generation from a quick voice memo after each appointment.",
        "Between-session check-in messages and homework reminders by SMS.",
    ],
    "psychologists_psychiatrists": [
        "Automated intake, screening scoring, and pre-visit history summary for each new patient.",
        "Med-refill and follow-up reminder flows that reduce missed prescriber windows.",
        "Session- or eval-note draft generation from a post-visit voice memo.",
    ],
}
GENERIC_FALLBACK = [
    "Automated client intake and pre-visit onboarding to cut front-desk admin.",
    "SMS reminders and rebooking nudges to reduce no-shows and keep cadence.",
    "Review-request automation after each visit to grow local Google presence.",
]

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler(sys.stdout)],
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
# Firecrawl client (extract only)
# ---------------------------------------------------------------------------

class FirecrawlClient:
    def __init__(self, api_key: str):
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        })

    def extract(self, urls: list[str], prompt: str, schema: dict,
                poll_interval: int = 5, max_wait: int = 240) -> dict | None:
        """POST /v1/extract then poll GET /v1/extract/{id} until completed."""
        payload = {"urls": urls, "prompt": prompt, "schema": schema}
        try:
            resp = self.session.post(
                f"{FIRECRAWL_API_BASE}/extract", json=payload, timeout=60,
            )
            if resp.status_code != 200:
                log.warning(f"Firecrawl submit {resp.status_code}: {resp.text[:200]}")
                return None
            data = resp.json()
            if not data.get("success") or not data.get("id"):
                log.warning(f"Firecrawl submit bad response: {str(data)[:200]}")
                return None
            job_id = data["id"]
        except requests.RequestException as e:
            log.error(f"Firecrawl submit network error: {e}")
            return None

        waited = 0
        while waited < max_wait:
            if _shutdown_requested:
                return None
            time.sleep(poll_interval)
            waited += poll_interval
            try:
                r = self.session.get(
                    f"{FIRECRAWL_API_BASE}/extract/{job_id}", timeout=30,
                )
                if r.status_code != 200:
                    log.warning(f"Firecrawl poll {r.status_code}: {r.text[:200]}")
                    continue
                pd = r.json()
                status = pd.get("status")
                if status == "completed":
                    return pd.get("data")
                if status in ("failed", "cancelled"):
                    log.warning(f"Firecrawl job {job_id} {status}: "
                                f"{pd.get('error', '?')}")
                    return None
                # else: processing / pending — keep polling
            except requests.RequestException as e:
                log.warning(f"Firecrawl poll network error: {e}")
                continue
        log.warning(f"Firecrawl job {job_id} timed out after {max_wait}s")
        return None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def normalize_url(raw: str) -> str:
    raw = (raw or "").strip()
    if not raw:
        return ""
    if not raw.startswith(("http://", "https://")):
        raw = "https://" + raw
    return raw


def normalize_domain(raw: str) -> str:
    d = (raw or "").strip().lower()
    d = re.sub(r"^https?://", "", d)
    d = re.sub(r"^www\.", "", d)
    d = d.split("/")[0].strip(".")
    return d


def format_angle_cell(angles: list[str]) -> str:
    """Produce the numbered, newline-stacked cell format."""
    cleaned = []
    for a in angles[:3]:
        s = (a or "").strip()
        # Strip any accidental leading bullet / numbering
        s = re.sub(r"^[\-•\*\d\.\)\s]+", "", s)
        s = s.strip()
        if not s:
            continue
        # Capitalize first letter, ensure ending period
        s = s[0].upper() + s[1:]
        if s and s[-1] not in ".!?":
            s += "."
        cleaned.append(s)
    while len(cleaned) < 3:
        cleaned.append(GENERIC_FALLBACK[len(cleaned)])
    return "\n".join(f"{i + 1}. {cleaned[i]}" for i in range(3))


def firecrawl_angles_for(fc: FirecrawlClient, url: str, category: str) -> list[str]:
    schema = {
        "type": "object",
        "properties": {
            "automation_angles": {
                "type": "array",
                "minItems": 3,
                "maxItems": 3,
                "items": {"type": "string"},
                "description": (
                    "Exactly 3 short (max ~18 words each), specific AI-automation "
                    "angles this company could benefit from, based on what they "
                    "actually do. Reference specific services, pain points, or "
                    "positioning from the site when possible. No bullet chars, "
                    "no numbering — plain sentences."
                ),
            }
        },
        "required": ["automation_angles"],
    }
    prompt = (
        f"Read this company's website carefully. Note specific service lines, "
        f"practice areas, tag lines, stated pain points, or recurring manual "
        f"processes. Then produce exactly 3 SHORT AI automation angles tailored "
        f"to THIS company (not generic {category} advice). Each angle: ONE "
        f"sentence, max ~18 words, no bullet characters, no numbering. Reference "
        f"something specific from the site (a practice area, a service name, a "
        f"line from their copy) whenever you can — 'automate X because you "
        f"offer Y'. Only fall back to generic {category} angles if the site "
        f"has almost no content. Avoid bland phrasing like 'chatbots for 24/7 "
        f"support' or 'AI-driven client intake' — be specific."
    )
    data = fc.extract([url], prompt, schema)
    if not data:
        return []
    angles = data.get("automation_angles") or []
    if isinstance(angles, list):
        return [str(a) for a in angles if a]
    return []


# ---------------------------------------------------------------------------
# Checkpoint
# ---------------------------------------------------------------------------

class Checkpoint:
    def __init__(self, path: Path):
        self.path = path
        self.data: dict[str, dict] = {"angles_by_domain": {}}
        if path.exists():
            try:
                self.data.update(json.loads(path.read_text()))
            except Exception as e:
                log.warning(f"Could not load checkpoint: {e}")

    def save(self):
        self.path.write_text(json.dumps(self.data, indent=2))

    def has(self, domain: str) -> bool:
        return domain in self.data["angles_by_domain"]

    def get(self, domain: str) -> list[str]:
        entry = self.data["angles_by_domain"].get(domain, {})
        return entry.get("angles", [])

    def put(self, domain: str, angles: list[str], source: str):
        self.data["angles_by_domain"][domain] = {
            "angles": angles,
            "source": source,
        }


# ---------------------------------------------------------------------------
# Main processing
# ---------------------------------------------------------------------------

def process_domain(fc: FirecrawlClient, domain: str, url: str,
                   category: str) -> tuple[list[str], str]:
    """Return (angles, source). source is 'firecrawl' or 'fallback:<category>'."""
    angles = firecrawl_angles_for(fc, url, category)
    if len(angles) >= 3:
        return angles[:3], "firecrawl"
    fallback = CATEGORY_FALLBACKS.get(category, GENERIC_FALLBACK)
    return fallback[:3], f"fallback:{category}"


def run(ckpt: Checkpoint):
    if not FIRECRAWL_API_KEY:
        log.error("FIRECRAWL_API_KEY not set in .env")
        sys.exit(1)
    fc = FirecrawlClient(FIRECRAWL_API_KEY)

    # Load input rows
    rows: list[dict] = []
    with INPUT_CSV.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = list(reader.fieldnames or [])
        for r in reader:
            rows.append(r)

    if ANGLE_COLUMN not in fieldnames:
        fieldnames.append(ANGLE_COLUMN)

    # Collect unique (domain, url, category) triples. Keep the first
    # non-empty website encountered per domain.
    domain_info: dict[str, dict] = {}
    for r in rows:
        website = (r.get("website") or "").strip()
        raw_domain = (r.get("domain") or "").strip()
        category = (r.get("category") or "").strip()
        domain = normalize_domain(raw_domain) or normalize_domain(website)
        if not domain:
            continue
        if domain not in domain_info:
            domain_info[domain] = {
                "url": normalize_url(website) or f"https://{domain}",
                "category": category,
            }

    log.info(f"Loaded {len(rows)} rows, {len(domain_info)} unique domains")
    pending = [d for d in domain_info if not ckpt.has(d)]
    log.info(f"{len(pending)} domains pending ({len(domain_info) - len(pending)} "
             f"already cached in checkpoint)")

    done = 0
    fc_success = 0
    fc_fallback = 0
    if pending:
        with ThreadPoolExecutor(max_workers=WORKERS) as pool:
            futures = {
                pool.submit(process_domain, fc, d, domain_info[d]["url"],
                            domain_info[d]["category"]): d for d in pending
            }
            for fut in as_completed(futures):
                if _shutdown_requested:
                    break
                d = futures[fut]
                try:
                    angles, source = fut.result()
                except Exception as e:
                    log.warning(f"[{d}] error: {e}")
                    category = domain_info[d]["category"]
                    angles = CATEGORY_FALLBACKS.get(category, GENERIC_FALLBACK)[:3]
                    source = f"error-fallback:{category}"
                ckpt.put(d, angles, source)
                done += 1
                if source == "firecrawl":
                    fc_success += 1
                else:
                    fc_fallback += 1
                if done % 5 == 0:
                    ckpt.save()
                    log.info(f"[progress] {done}/{len(pending)} "
                             f"(firecrawl: {fc_success}, fallback: {fc_fallback})")
    ckpt.save()

    # Write output CSV
    with OUTPUT_CSV.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for r in rows:
            raw_domain = (r.get("domain") or "").strip()
            website = (r.get("website") or "").strip()
            domain = normalize_domain(raw_domain) or normalize_domain(website)
            angles = ckpt.get(domain) if domain else []
            if not angles:
                category = (r.get("category") or "").strip()
                angles = CATEGORY_FALLBACKS.get(category, GENERIC_FALLBACK)[:3]
            r[ANGLE_COLUMN] = format_angle_cell(angles)
            writer.writerow(r)

    try:
        shutil.copyfile(OUTPUT_CSV, DOWNLOADS_COPY)
        log.info(f"Copied output to {DOWNLOADS_COPY}")
    except Exception as e:
        log.warning(f"Could not copy to Downloads: {e}")

    total_cached = len(ckpt.data["angles_by_domain"])
    fc_total = sum(1 for v in ckpt.data["angles_by_domain"].values()
                   if v.get("source") == "firecrawl")
    log.info(f"Complete — {total_cached} domains in checkpoint "
             f"({fc_total} firecrawl, {total_cached - fc_total} fallback)")
    log.info(f"Output: {OUTPUT_CSV}")


def print_status(ckpt: Checkpoint):
    total = 0
    if INPUT_CSV.exists():
        with INPUT_CSV.open(newline="", encoding="utf-8") as f:
            domains = set()
            for r in csv.DictReader(f):
                d = normalize_domain(r.get("domain", ""))
                if d:
                    domains.add(d)
            total = len(domains)
    cached = ckpt.data["angles_by_domain"]
    fc = sum(1 for v in cached.values() if v.get("source") == "firecrawl")
    fallback = len(cached) - fc
    print("=== Austin Automation Angles Status ===")
    print(f"Unique domains in input:   {total}")
    print(f"Domains in checkpoint:     {len(cached)}  "
          f"(firecrawl: {fc}, fallback: {fallback})")
    if OUTPUT_CSV.exists():
        with OUTPUT_CSV.open(newline="", encoding="utf-8") as f:
            n = sum(1 for _ in csv.DictReader(f))
        print(f"Output rows:               {n}")
    else:
        print("Output CSV not yet written")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--status", action="store_true")
    args = parser.parse_args()

    ckpt = Checkpoint(CHECKPOINT_FILE)
    if args.status:
        print_status(ckpt)
        return
    run(ckpt)


if __name__ == "__main__":
    main()
