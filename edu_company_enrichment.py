#!/usr/bin/env python3
"""
EDU Company Enrichment — Convert .edu domains to LinkedIn company URLs
using BlitzAPI /v2/enrichment/domain-to-linkedin at ~4 RPS.
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
REQUEST_DELAY = 0.25  # 4 requests per second

DATA_DIR = Path(os.environ.get("BLITZ_DATA_DIR", Path(__file__).resolve().parent / "data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)
INPUT_FILE = DATA_DIR / "us_institutions_new.csv"
OUTPUT_DIR = DATA_DIR
OUTPUT_FILE = OUTPUT_DIR / "edu_companies_enriched.csv"
CHECKPOINT_FILE = OUTPUT_DIR / "edu_companies_checkpoint.json"
LOG_FILE = OUTPUT_DIR / "edu_companies_enrichment.log"

OUTPUT_COLUMNS = [
    "Name", "Domain", "Website", "City", "State", "LinkedIn URL",
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
        self.stats = {"total": 0, "found": 0, "not_found": 0, "errors": 0}
        self._load()

    def _load(self):
        if self.path.exists():
            data = json.loads(self.path.read_text())
            self.processed = set(data.get("processed", []))
            self.stats = data.get("stats", self.stats)
            log.info(f"Checkpoint loaded — {len(self.processed)} domains already processed")

    def save(self):
        data = {"processed": sorted(self.processed), "stats": self.stats}
        self.path.write_text(json.dumps(data, indent=2))

    def is_processed(self, domain: str) -> bool:
        return domain in self.processed

    def mark_processed(self, domain: str):
        self.processed.add(domain)
        self.stats["total"] += 1


# ---------------------------------------------------------------------------
# API
# ---------------------------------------------------------------------------

def domain_to_linkedin(session: requests.Session, domain: str) -> str | None:
    url = f"{API_BASE}/enrichment/domain-to-linkedin"
    for attempt in range(3):
        try:
            time.sleep(REQUEST_DELAY)
            resp = session.post(url, json={"domain": domain}, timeout=15)
            if resp.status_code == 200:
                data = resp.json()
                return data.get("linkedin_url") or data.get("company_linkedin_url") or None
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
            # 404 or other client error = domain not found
            return None
        except requests.RequestException as e:
            wait = 2 ** (attempt + 1)
            log.warning(f"Network error: {e} — retry in {wait}s")
            time.sleep(wait)
    log.error(f"Failed after 3 retries: {domain}")
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

    # Load institutions
    institutions = []
    with open(INPUT_FILE, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            institutions.append(row)

    remaining = [i for i in institutions if not checkpoint.is_processed(i["Domain"])]
    log.info(f"Loaded {len(institutions)} institutions, {len(remaining)} remaining")

    # Open output CSV
    file_exists = OUTPUT_FILE.exists() and OUTPUT_FILE.stat().st_size > 0
    csv_file = open(OUTPUT_FILE, "a", newline="", encoding="utf-8")
    writer = csv.DictWriter(csv_file, fieldnames=OUTPUT_COLUMNS)
    if not file_exists:
        writer.writeheader()

    try:
        for i, inst in enumerate(remaining):
            if _shutdown_requested:
                log.info("Shutdown — saving checkpoint")
                break

            domain = inst["Domain"]
            linkedin_url = domain_to_linkedin(session, domain)

            if linkedin_url:
                checkpoint.stats["found"] += 1
                log.info(f"[{checkpoint.stats['total']+1}/{len(remaining)}] {inst['Name']} → {linkedin_url}")
            else:
                checkpoint.stats["not_found"] += 1

            checkpoint.mark_processed(domain)

            writer.writerow({
                "Name": inst["Name"],
                "Domain": domain,
                "Website": inst["Website"],
                "City": inst.get("City", ""),
                "State": inst.get("State", ""),
                "LinkedIn URL": linkedin_url or "",
            })
            csv_file.flush()

            if checkpoint.stats["total"] % 100 == 0:
                log.info(
                    f"Progress: {checkpoint.stats['total']}/{len(remaining)} — "
                    f"{checkpoint.stats['found']} found, "
                    f"{checkpoint.stats['not_found']} not found"
                )
                checkpoint.save()

    finally:
        csv_file.close()
        checkpoint.save()
        log.info("=" * 60)
        log.info("FINAL SUMMARY")
        log.info(f"  Domains processed:  {checkpoint.stats['total']}")
        log.info(f"  LinkedIn found:     {checkpoint.stats['found']}")
        log.info(f"  Not found:          {checkpoint.stats['not_found']}")
        log.info(f"  Output file:        {OUTPUT_FILE}")
        log.info("=" * 60)


if __name__ == "__main__":
    main()
