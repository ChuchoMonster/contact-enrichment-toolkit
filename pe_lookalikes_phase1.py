"""
PE look-alike pipeline — Phase 1: firm sweep.

Sweeps Blitz company-search for US investment firms (15-40 employees) that look
like a seed list of lower-middle-market PE shops (data/existing_firms.csv, optional).
Filters by exact LinkedIn headcount, classifies PE-ness from about/specialties,
dedupes against firms already in the seed list, and writes a firm list.

Uses Blitz v2 endpoints only.
"""
import csv, json, os, time, urllib.request, urllib.error, collections

BASE = "https://api.blitz-api.ai"
EXISTING_CSV = "data/existing_firms.csv"  # optional seed/dedupe list with a "Domain" column
PRIOR_JSONS = []  # optional: JSON lists of {"domain": ...} from earlier runs, to dedupe against
OUT_CSV = "data/pe_lookalike_firms.csv"
EMP_MIN, EMP_MAX = 15, 40
INDUSTRIES = ["Investment Management", "Venture Capital and Private Equity"]

KEY = None
for line in open(".env"):
    if line.startswith("BLITZ_API_KEY"):
        KEY = line.split("=", 1)[1].strip().strip('"').strip("'")

def post(path, payload):
    req = urllib.request.Request(BASE + path, data=json.dumps(payload).encode(),
        headers={"x-api-key": KEY, "Content-Type": "application/json"}, method="POST")
    for _ in range(4):
        try:
            with urllib.request.urlopen(req, timeout=45) as r:
                return json.loads(r.read().decode())
        except urllib.error.HTTPError as e:
            if e.code in (429, 500, 502, 503): time.sleep(4); continue
            return {"_err": e.code}
        except Exception:
            time.sleep(3)
    return {}

def norm_domain(u):
    if not u: return ""
    return u.lower().replace("https://", "").replace("http://", "").replace("www.", "").split("/")[0]

# ---- dedupe set: existing CSV + prior run finds ----
os.makedirs("data", exist_ok=True)
existing = set()
if os.path.exists(EXISTING_CSV):
    with open(EXISTING_CSV) as f:
        for row in csv.DictReader(f):
            existing.add(row["Domain"].strip().lower().replace("www.", ""))
for p in PRIOR_JSONS:
    if os.path.exists(p):
        for r in json.load(open(p)):
            if r.get("domain"): existing.add(r["domain"])

# ---- classification ----
PE = ["private equity", "buyout", "lower middle market", "lower-middle", "middle market",
      "portfolio compan", "recapitaliz", "control equity", "control investment", "growth equity",
      "leveraged buyout", "majority stake", "majority interest", "platform investment",
      "add-on acquisition", "operating partner"]
NEG = ["wealth management", "wealth advisor", "financial advisor", "financial planning",
       "hedge fund", "registered investment advisor", " ria ", "retirement plan", "insurance",
       "estate planning", "tax planning", "wealth advisory", "asset allocation for individuals"]

def classify(c):
    t = (str(c.get("name", "")) + " " + str(c.get("about", "")) + " " +
         " ".join(c.get("specialties") or [])).lower()
    pe = any(p in t for p in PE); neg = any(n in t for n in NEG)
    if pe and not neg: return "PE"
    if pe and neg: return "PE?"
    if neg: return "wealth/other"
    return "unknown"

# ---- sweep ----
seen_dom = set()
kept = []
stats = collections.Counter()
fieldnames = ["name", "domain", "emp", "cat", "industry", "linkedin_url", "about"]
fout = open(OUT_CSV, "w", newline="")
writer = csv.DictWriter(fout, fieldnames=fieldnames)
writer.writeheader()

for industry in INDUSTRIES:
    cursor = None; page = 0; pulled = 0
    print(f"\n=== sweeping industry: {industry} ===", flush=True)
    while True:
        page += 1
        pl = {"company": {"industry": {"include": [industry]},
                          "hq": {"country_code": ["US"]},
                          "employee_range": ["11-50"]}, "max_results": 50}
        if cursor: pl["cursor"] = cursor
        d = post("/v2/search/companies", pl)
        res = d.get("results") if isinstance(d, dict) else None
        if not res: break
        pulled += len(res)
        for c in res:
            emp = c.get("employees_on_linkedin")
            if emp is None or not (EMP_MIN <= emp <= EMP_MAX): continue
            dom = norm_domain(c.get("website") or c.get("linkedin_url"))
            key = dom or c.get("name")
            if not key or key in seen_dom or dom in existing: continue
            seen_dom.add(key)
            cat = classify(c)
            stats[cat] += 1
            if cat == "wealth/other":  # drop clear non-PE
                continue
            row = {"name": c.get("name", ""), "domain": dom, "emp": emp, "cat": cat,
                   "industry": industry, "linkedin_url": c.get("linkedin_url", ""),
                   "about": (c.get("about") or "")[:300]}
            kept.append(row); writer.writerow(row)
        if page % 10 == 0:
            fout.flush()
            print(f"  page {page} | pulled {pulled} | kept {len(kept)} | cats={dict(stats)}", flush=True)
        cursor = d.get("cursor")
        if not cursor: break
        time.sleep(0.15)
    print(f"  DONE {industry}: pulled {pulled} over {page} pages", flush=True)

fout.close()
print(f"\nTOTAL kept (PE/PE?/unknown, net-new, {EMP_MIN}-{EMP_MAX} emp): {len(kept)}")
print("classification of all in-band net-new:", dict(stats))
byc = collections.Counter(r["cat"] for r in kept)
print("kept by category:", dict(byc))
print("wrote", OUT_CSV)
