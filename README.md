# Contact Enrichment Toolkit

A set of Python pipelines, plus a small deployed web app, that turn a target-market
definition (a list of domains, a company search, or a Google Maps query) into a list of
decision-makers with personal LinkedIn URLs and verified work emails, using the
[Blitz API](https://blitz-api.ai).

> **Code only.** This is a sanitized public copy. All input lists, results, checkpoints,
> logs and the web app's database are excluded, and no real contact data is included.
> The only sample data, `examples/sample_input.csv`, is fictional (reserved
> `example.*` domains). Keys and addresses come from environment variables.

## Architecture

Every pipeline follows the same three steps, each resumable from a JSON checkpoint:

```
input (domains CSV | Blitz company search | SerpAPI Google Maps | Firecrawl scrape)
  -> Step 1  domain -> company LinkedIn URL      /search/domain-to-linkedin-company
  -> Step 2  waterfall ICP people search         /search/waterfall-icp  (title cascades)
  -> Step 3  email enrichment                    /enrichment/email
  -> CSV of verified contacts
```

- **Title cascades**: ordered fallbacks (C-suite, then VP/Head, then Director, then Sr. Manager),
  with exclusion terms, so each company returns its most senior relevant person.
- **Checkpoint/resume**: Ctrl+C saves progress; rerunning picks up where it stopped.
- **Concurrency**: `ThreadPoolExecutor`, tuned per step to Blitz's per-endpoint rate limits,
  with retry and backoff on 429/5xx.
- **`blitz_core.py`**: shared `BlitzAPIClient` (rate-limited, retrying) and `PipelineRunner`
  (the three steps with progress callbacks), used by the web app and newer pipelines.

### Web app (`web/`)

FastAPI service deployed on Fly.io (`Dockerfile`, `fly.toml`). A visitor submits an ICP
(industry, company size, HQ country, job titles); a background thread runs `PipelineRunner`
(capped at 1,000 leads), stores the job in SQLite, and emails the results CSV through the
`gws` Google Workspace CLI. Per-email-domain submission limits curb abuse; Slack webhook
notifications are optional.

## Scripts

| Script | Input | What it does |
|---|---|---|
| `blitz_core.py` | — | Shared API client and pipeline runner |
| `bigcommerce_pipeline.py`, `woocommerce_pipeline.py`, `contentful_pipeline.py` | BuiltWith domain export | Standard 3-step pipeline, marketing + founder cascades (simplest template) |
| `tier_a_ae_pipeline.py`, `tier_a_so_pipeline.py`, `tier_a_tef_pipeline.py` | Sales Navigator-style CSV | 3-step pipeline that reuses existing LinkedIn URLs; includes an endpoint-comparison mode (most complete template) |
| `seo_contact_finder.py` | CSV | Up to 5 senior SEO/content/brand people per company, dual cascades |
| `website_scraper.py` | CSV | For companies without a LinkedIn page: scrape team/about pages, then enrich |
| `email_enrichment.py` | CSV of LinkedIn profile URLs | Standalone email enrichment step |
| `contact_linkedin_matcher.py` | CSV of `domain, contact_name` | Find a known person's LinkedIn URL by fuzzy name matching |
| `revops_agencies_pipeline.py` | CSV of known people at agencies | Targeted cascade per known title + fuzzy name match + enrichment |
| `hr_newjob_email_enrichment.py` | CSV | Enrichment that keeps only emails on the current company's domain |
| `series_ab_pipeline.py` | CSV of funded companies | CEOs + heads of marketing at recently funded startups |
| `sqsp_designers_pipeline.py` | CSV | Founders/owners of design studios |
| `edu_company_enrichment.py`, `edu_waterfall_icp.py`, `edu_email_enrichment.py` | CSV of .edu domains | Higher-ed pipeline split into three scripts (keyword waterfall endpoint) |
| `pe_vc_pipeline.py` | CSV of firms | Operations contacts at PE/VC firms, cascade chosen by firm size |
| `pe_lookalikes_phase1.py`, `pe_lookalikes_phase23.py` | Blitz company search | Find look-alike PE firms by headcount + text classification, then contacts |
| `legaltech_pipeline.py`, `ops_pipeline.py`, `pro_services_pipeline.py`, `senior_hr_pipeline.py` | Blitz company search | Search-driven discovery by industry/keywords/size, then people + emails |
| `nyc_law_gov_founders.py`, `nyc_logistics_founders.py`, `nyc_training_founders.py`, `nyc_training_retry.py` | Blitz company search | City + industry founder searches; retry pass with headline search |
| `austin_leads_pipeline.py`, `austin_ml_pipeline.py` | SerpAPI Google Maps | Local-business discovery from Maps, then LinkedIn + owner/founder + email |
| `austin_automation_angles.py` | CSV | Firecrawl `/extract` to write three site-specific automation ideas per company, with category fallbacks |
| `marketplace_pipeline.py` | Public startup directories | Firecrawl scrape of marketplace-company lists, then the 3 steps |
| `run_when_api_ready.sh` | — | Poll the API and start a run once it responds |

## Setup

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements-web.txt          # requests, python-dotenv, fastapi, uvicorn
cp .env.example .env                          # add BLITZ_API_KEY (and others as needed)
mkdir -p data                                 # input CSVs go here (override: BLITZ_DATA_DIR)

python bigcommerce_pipeline.py --step 1       # or --all
python legaltech_pipeline.py --dry-run        # log calls, no API hits

uvicorn web.app:app --reload --port 8000      # web app on http://localhost:8000
fly deploy                                    # needs a Fly volume named blitz_data and secrets set
```

Each pipeline's input filename is set near the top of the script (`INPUT_FILE`).
`examples/sample_input.csv` shows the minimal `domain` / `contact_name` shape.

## Environment variables

| Variable | Used by |
|---|---|
| `BLITZ_API_KEY` | All pipelines and the web app |
| `SERPAPI_KEY` | `austin_*` Maps pipelines |
| `FIRECRAWL_API_KEY` | `marketplace_pipeline.py`, `austin_automation_angles.py` |
| `BLITZ_DATA_DIR` | Input/output directory (default `./data`) |
| `DB_PATH` | Web app SQLite path (Fly: `/data/blitz_web.db`) |
| `GWS_CLI_PATH` | Path to the `gws` CLI used to email results |
| `SLACK_WEBHOOK_URL` | Optional job notifications |
| `RESULTS_BCC_EMAIL`, `SUPPORT_EMAIL`, `BOOKING_URL` | Optional web app email settings |

## Notes

- Several pipelines are deliberate copies of a template adapted to one segment; they are
  kept as-is to show how the template was varied (cascades, inputs, worker counts).
- `web/static/*.html` load an optional `/static/js/app.js` animation script that is not
  part of this repository; the form works without it.
