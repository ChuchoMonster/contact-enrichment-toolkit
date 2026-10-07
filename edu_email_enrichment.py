#!/usr/bin/env python3
"""
EDU Email Enrichment — Get verified work emails for people found via
waterfall ICP at US higher-ed institutions. ~4 RPS.
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
REQUEST_DELAY = 0.25  # ~4 RPS

DATA_DIR = Path(os.environ.get("BLITZ_DATA_DIR", Path(__file__).resolve().parent / "data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)
INPUT_FILE = DATA_DIR / "edu_waterfall_results.csv"
OUTPUT_DIR = DATA_DIR
OUTPUT_FILE = OUTPUT_DIR / "edu_email_results.csv"
CHECKPOINT_FILE = OUTPUT_DIR / "edu_email_checkpoint.json"
LOG_FILE = OUTPUT_DIR / "edu_email_enrichment.log"

OUTPUT_COLUMNS = [
    "Institution", "Domain", "City", "State", "LinkedIn Company URL",
    "Full Name", "First Name", "Last Name", "Job Title", "LinkedIn Profile URL",
    "Verified Email", "Cascade Level",
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
        self.stats = {"total": 0, "emails_found": 0, "api_calls": 0}
        self._load()

    def _load(self):
        if self.path.exists():
            data = json.loads(self.path.read_text())
            self.processed = set(data.get("processed", []))
            self.stats = data.get("stats", self.stats)
            log.info(f"Checkpoint loaded — {len(self.processed)} people already processed")

    def save(self):
        data = {"processed": sorted(self.processed), "stats": self.stats}
        self.path.write_text(json.dumps(data, indent=2))

    def is_processed(self, linkedin_url: str) -> bool:
        return linkedin_url in self.processed

    def mark_processed(self, linkedin_url: str):
        self.processed.add(linkedin_url)
        self.stats["total"] += 1


# ---------------------------------------------------------------------------
# API
# ---------------------------------------------------------------------------

def enrich_email(session: requests.Session, person_linkedin_url: str) -> str | None:
    url = f"{API_BASE}/enrichment/email"
    for attempt in range(3):
        try:
            time.sleep(REQUEST_DELAY)
            resp = session.post(url, json={"person_linkedin_url": person_linkedin_url}, timeout=15)
            if resp.status_code == 200:
                data = resp.json()
                if data.get("email"):
                    return data["email"]
                if data.get("all_emails") and len(data["all_emails"]) > 0:
                    return data["all_emails"][0].get("email_address") or data["all_emails"][0].get("email", "")
                return None
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
            return None
        except requests.RequestException as e:
            wait = 2 ** (attempt + 1)
            log.warning(f"Network error: {e} — retry in {wait}s")
            time.sleep(wait)
    log.error(f"Failed after 3 retries: {person_linkedin_url}")
    return None


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

    # Load people
    people = []
    with open(INPUT_FILE, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            linkedin = (row.get("LinkedIn Profile URL") or "").strip()
            if linkedin:
                people.append(row)

    remaining = [p for p in people if not checkpoint.is_processed(p["LinkedIn Profile URL"].strip())]
    log.info(f"Loaded {len(people)} people, {len(remaining)} remaining")

    # Open output CSV
    file_exists = OUTPUT_FILE.exists() and OUTPUT_FILE.stat().st_size > 0
    csv_file = open(OUTPUT_FILE, "a", newline="", encoding="utf-8")
    writer = csv.DictWriter(csv_file, fieldnames=OUTPUT_COLUMNS)
    if not file_exists:
        writer.writeheader()

    try:
        for i, person in enumerate(remaining):
            if _shutdown_requested:
                log.info("Shutdown — saving checkpoint")
                break

            linkedin = person["LinkedIn Profile URL"].strip()
            email = enrich_email(session, linkedin)
            checkpoint.stats["api_calls"] += 1

            if email:
                checkpoint.stats["emails_found"] += 1

            checkpoint.mark_processed(linkedin)

            writer.writerow({
                "Institution": person.get("Institution", ""),
                "Domain": person.get("Domain", ""),
                "City": person.get("City", ""),
                "State": person.get("State", ""),
                "LinkedIn Company URL": person.get("LinkedIn Company URL", ""),
                "Full Name": person.get("Full Name", ""),
                "First Name": person.get("First Name", ""),
                "Last Name": person.get("Last Name", ""),
                "Job Title": person.get("Job Title", ""),
                "LinkedIn Profile URL": linkedin,
                "Verified Email": email or "",
                "Cascade Level": person.get("Cascade Level", ""),
            })
            csv_file.flush()

            if email:
                log.info(
                    f"[{checkpoint.stats['total']}/{len(remaining)}] "
                    f"{person.get('Full Name','')} ({person.get('Job Title','')}) "
                    f"@ {person.get('Institution','')} → {email}"
                )
            elif checkpoint.stats["total"] % 200 == 0 and checkpoint.stats["total"] > 0:
                log.info(
                    f"[{checkpoint.stats['total']}/{len(remaining)}] Progress — "
                    f"{checkpoint.stats['emails_found']} emails found, "
                    f"{checkpoint.stats['api_calls']} API calls"
                )

            if checkpoint.stats["total"] % 100 == 0:
                checkpoint.save()

    finally:
        csv_file.close()
        checkpoint.save()
        log.info("=" * 60)
        log.info("FINAL SUMMARY")
        log.info(f"  People processed:  {checkpoint.stats['total']}")
        log.info(f"  Emails found:      {checkpoint.stats['emails_found']}")
        log.info(f"  API calls:         {checkpoint.stats['api_calls']}")
        log.info(f"  Output file:       {OUTPUT_FILE}")
        log.info("=" * 60)


if __name__ == "__main__":
    main()
