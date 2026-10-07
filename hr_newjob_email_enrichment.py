#!/usr/bin/env python3
"""Email enrichment for HR new-job contacts. Only keeps emails matching current company domain."""
import csv, json, logging, os, sys, time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import requests
from dotenv import load_dotenv

load_dotenv()

API_BASE = "https://api.blitz-api.ai/api"
API_KEY = os.getenv("BLITZ_API_KEY")
DATA_DIR = Path(os.environ.get("BLITZ_DATA_DIR", Path(__file__).resolve().parent / "data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)
INPUT_FILE = DATA_DIR / "hr_new_job_contacts.csv"
OUTPUT_FILE = DATA_DIR / "hr_newjob_verified_emails.csv"
LOG_FILE = DATA_DIR / "hr_newjob_pipeline.log"

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s  %(message)s",
                    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler(sys.stdout)])
log = logging.getLogger(__name__)

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


def email_matches_domain(email, domain):
    """Check if email domain matches the company domain."""
    if not email or not domain:
        return False
    email_domain = email.split("@")[-1].lower().strip()
    company_domain = domain.lower().strip().replace("www.", "")
    # Direct match or subdomain match
    return email_domain == company_domain or email_domain.endswith("." + company_domain)


def main():
    with open(INPUT_FILE, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    log.info(f"Loaded {len(rows)} contacts")

    lock = threading.Lock()
    completed = [0]
    matched = [0]
    found_but_wrong = [0]

    def enrich_one(row):
        li_url = (row.get("Person LI") or "").strip()
        domain = (row.get("URL Path") or "").strip()

        if not li_url:
            return {**row, "New Verified Email": "", "Email Match Status": "no_linkedin"}

        data = api_post("/enrichment/email", {"linkedin_profile_url": li_url})

        # Collect ALL emails from response
        all_emails = []
        if data:
            if data.get("email"):
                all_emails.append(data["email"])
            if data.get("all_emails"):
                for e in data["all_emails"]:
                    addr = e.get("email_address", "")
                    if addr and addr not in all_emails:
                        all_emails.append(addr)

        # Find the email that matches the current company domain
        matching_email = ""
        for email in all_emails:
            if email_matches_domain(email, domain):
                matching_email = email
                break

        with lock:
            completed[0] += 1
            if matching_email:
                matched[0] += 1
                log.info(f"[{completed[0]}/{len(rows)}] {row['Full Name']} → {matching_email}")
            elif all_emails:
                found_but_wrong[0] += 1
                if completed[0] % 20 == 0:
                    log.info(f"[{completed[0]}/{len(rows)}] Progress — {matched[0]} matched, {found_but_wrong[0]} wrong domain")
            elif completed[0] % 20 == 0:
                log.info(f"[{completed[0]}/{len(rows)}] Progress — {matched[0]} matched")

        status = "matched" if matching_email else ("wrong_domain" if all_emails else "no_email_found")
        return {**row, "New Verified Email": matching_email, "Email Match Status": status}

    results = {}
    with ThreadPoolExecutor(max_workers=5) as pool:
        futures = {pool.submit(enrich_one, r): i for i, r in enumerate(rows)}
        for future in as_completed(futures):
            idx = futures[future]
            results[idx] = future.result()

    # Preserve order
    ordered = [results[i] for i in range(len(rows))]

    # Write output — only those with matching emails
    verified = [r for r in ordered if (r.get("New Verified Email") or "").strip()]

    # Slim down columns for output
    out_columns = [
        "Full Name", "First Name", "Last Name", "Position",
        "Company Name", "URL Path", "Company Linkedin", "Person LI",
        "New Verified Email", "HQ City", "HQ State", "Employee Count",
    ]

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=out_columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(verified)

    no_email = sum(1 for r in ordered if r.get("Email Match Status") == "no_email_found")
    wrong = sum(1 for r in ordered if r.get("Email Match Status") == "wrong_domain")

    log.info("=" * 60)
    log.info("COMPLETE")
    log.info(f"  Total contacts:          {len(rows)}")
    log.info(f"  Matched (new company):   {len(verified)}")
    log.info(f"  Wrong domain (old job):  {wrong}")
    log.info(f"  No email found:          {no_email}")
    log.info(f"  Output:                  {OUTPUT_FILE}")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
