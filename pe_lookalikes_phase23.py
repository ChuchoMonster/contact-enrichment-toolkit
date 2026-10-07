"""
PE look-alike pipeline — Phase 2/3: decision-makers + verified emails.

Input : data/pe_lookalike_firms.csv (from phase 1)
Output: data/pe_lookalike_contacts.csv  (verified-email contacts)
        data/pe_lookalike_checkpoint.json     (resumable progress)

Blitz v2 endpoints:
  POST /v2/search/waterfall-icp-keyword  -> results[].person{linkedin_url, full_name, experiences[].job_title}
  POST /v2/enrichment/email              -> {email, all_emails[].email}  (param: person_linkedin_url)
"""
import csv, json, os, time, threading, urllib.request, urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed

BASE = "https://api.blitz-api.ai"
FIRMS = "data/pe_lookalike_firms.csv"
OUT = "data/pe_lookalike_contacts.csv"
CKPT = "data/pe_lookalike_checkpoint.json"
MAX_WORKERS = 5
MAX_PER_FIRM = 2

KEY = None
for line in open(".env"):
    if line.startswith("BLITZ_API_KEY"):
        KEY = line.split("=", 1)[1].strip().strip('"').strip("'")

CASCADE = [
    {"include_title": ["Managing Partner", "Founder", "Co-Founder", "Co Founder",
                        "Managing Director", "Partner", "Principal", "CEO",
                        "Chief Executive", "President", "Owner"],
     "exclude_title": ["analyst", "associate", "coordinator", "assistant", "intern",
                        "junior", "student", "vice president", "vp"],
     "location": ["WORLD"], "include_headline_search": False},
    {"include_title": ["Head of Business Development", "Business Development",
                        "Head of Origination", "Origination", "Head of Sourcing",
                        "Investor Relations", "Operating Partner", "Chief Operating"],
     "exclude_title": ["analyst", "associate", "intern", "junior", "assistant"],
     "location": ["WORLD"], "include_headline_search": True},
]

def post(path, payload):
    req = urllib.request.Request(BASE + path, data=json.dumps(payload).encode(),
        headers={"x-api-key": KEY, "Content-Type": "application/json"}, method="POST")
    for _ in range(4):
        try:
            with urllib.request.urlopen(req, timeout=45) as r:
                return json.loads(r.read().decode())
        except urllib.error.HTTPError as e:
            if e.code in (429, 500, 502, 503): time.sleep(4); continue
            return None
        except Exception:
            time.sleep(3)
    return None

def enrich_email(person_url):
    d = post("/v2/enrichment/email", {"person_linkedin_url": person_url})
    if not isinstance(d, dict): return ""
    if d.get("email"): return d["email"]
    ae = d.get("all_emails") or []
    if ae: return ae[0].get("email") or ae[0].get("email_address") or ""
    return ""

lock = threading.Lock()
processed = set()
if os.path.exists(CKPT):
    processed = set(json.load(open(CKPT)).get("done", []))

FIELDS = ["Firm Name", "Domain", "Employees", "Category", "Full Name", "First Name",
          "Last Name", "Job Title", "LinkedIn Profile URL", "Verified Email", "Cascade Level"]
new_file = not os.path.exists(OUT)
fout = open(OUT, "a", newline="")
writer = csv.DictWriter(fout, fieldnames=FIELDS)
if new_file:
    writer.writeheader(); fout.flush()

counter = {"firms": 0, "contacts": 0, "emails": 0}

def handle_firm(firm):
    url = firm["linkedin_url"]
    if not url:
        return firm["domain"], []
    wf = post("/v2/search/waterfall-icp-keyword",
              {"company_linkedin_url": url, "cascade": CASCADE, "max_results": MAX_PER_FIRM})
    results = wf.get("results") if isinstance(wf, dict) else None
    rows = []
    for r in (results or []):
        per = r.get("person", {}) or {}
        purl = per.get("linkedin_url", "")
        title = ""
        exps = per.get("experiences") or []
        if exps: title = exps[0].get("job_title", "")
        if not title: title = per.get("headline", "") or ""
        email = enrich_email(purl) if purl else ""
        rows.append({
            "Firm Name": firm["name"], "Domain": firm["domain"], "Employees": firm["emp"],
            "Category": firm["cat"], "Full Name": per.get("full_name", ""),
            "First Name": per.get("first_name", ""), "Last Name": per.get("last_name", ""),
            "Job Title": title, "LinkedIn Profile URL": purl,
            "Verified Email": email, "Cascade Level": r.get("ranking", ""),
        })
    return firm["domain"], rows

def main():
    firms = [r for r in csv.DictReader(open(FIRMS))]
    todo = [f for f in firms if f["domain"] not in processed]
    # PE-confident first so the most valuable contacts land early
    todo.sort(key=lambda f: {"PE": 0, "PE?": 1, "unknown": 2}.get(f["cat"], 3))
    print(f"firms total={len(firms)} | already done={len(processed)} | to process={len(todo)}", flush=True)
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = {ex.submit(handle_firm, f): f for f in todo}
        for fut in as_completed(futs):
            dom, rows = fut.result()
            with lock:
                for row in rows:
                    writer.writerow(row)
                    counter["contacts"] += 1
                    if row["Verified Email"]: counter["emails"] += 1
                fout.flush()
                processed.add(dom)
                counter["firms"] += 1
                json.dump({"done": sorted(processed)}, open(CKPT, "w"))
                if counter["firms"] % 25 == 0:
                    print(f"  firms {counter['firms']}/{len(todo)} | contacts {counter['contacts']} | emails {counter['emails']}", flush=True)
    fout.close()
    print(f"\nDONE. firms processed this run={counter['firms']} | contacts={counter['contacts']} | verified emails={counter['emails']}")
    print("output:", OUT)

if __name__ == "__main__":
    main()
