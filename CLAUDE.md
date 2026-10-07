# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Contact enrichment pipeline system built on the [Blitz API](https://api.blitz-api.ai/api). Takes company domains from various sources (BuiltWith, SerpAPI Google Maps, SalesNav exports) and produces verified email contacts for marketing/founder decision-makers.

## Environment

- Python 3.9+ with `requests`, `python-dotenv`, `concurrent.futures`
- `BLITZ_API_KEY` in `.env` (plus `SERPAPI_KEY` / `FIRECRAWL_API_KEY` for the pipelines that use them)
- Input CSVs and outputs live in `data/` (override with `BLITZ_DATA_DIR`); nothing in `data/` is committed

## Running Pipelines

```bash
# Standard 3-step (BigCommerce, WooCommerce, Contentful, TierA variants):
python bigcommerce_pipeline.py --step 1    # Domain → LinkedIn
python bigcommerce_pipeline.py --step 2    # Waterfall ICP (people search)
python bigcommerce_pipeline.py --step 3    # Email enrichment
python bigcommerce_pipeline.py --all       # All 3 chained

# Simpler pipelines run all steps automatically:
python series_ab_pipeline.py
python austin_leads_pipeline.py --all
```

All pipelines support Ctrl+C graceful shutdown with checkpoint save.

## 3-Step Pipeline Architecture

Every pipeline follows the same pattern:

1. **Domain → Company LinkedIn** — `/search/domain-to-linkedin-company` (1 credit/call)
2. **Waterfall ICP** — `/search/waterfall-icp` with title cascades to find decision-makers (1 credit/result)
3. **Email Enrichment** — `/enrichment/email` using personal LinkedIn URL (1 credit/call)

### Checkpoint/Resume

Multi-step pipelines use `CheckpointManager` writing to `{prefix}_checkpoint.json`. Each step tracks processed items so the pipeline can resume after interruption. Checkpoints save every 10-100 items.

### Concurrency

`ThreadPoolExecutor` with 2-5 workers.

**Rate limits are now per-endpoint, not per-account** (changed June 2026). You get the full 5 RPS on *each* endpoint independently — e.g., 5 RPS on Waterfall ICP *and* 5 RPS on Email Enrichment at the same time. This applies to all plans (the per-endpoint cap is 5/20/50 RPS depending on plan).

Implications:
- **Do NOT split workers across concurrently running pipelines.** Two pipelines no longer share one 5 RPS budget, so the old "3+2 split" is obsolete — run each at full worker count.
- Within a single pipeline, each step hits a different endpoint, so each phase already gets that endpoint's full RPS budget. Tune `max_workers` per-step against the *single endpoint* it hammers, not against a global account budget.
- The cap is still per-endpoint: one step flooding a single endpoint with too many workers still tops out at 5 RPS. More than ~5 workers on a single-endpoint step yields no speedup.

## Blitz API Endpoints

| Endpoint | Response Key | Cost |
|----------|-------------|------|
| `/search/domain-to-linkedin-company` | `company_linkedin_url` | 1 credit |
| `/search/waterfall-icp` | `results[]` with `person_linkedin_url`, `job_title`, `full_name` | 1/result |
| `/enrichment/email` | `email` or `all_emails[].email_address` | 1 credit |
| `/search/linkedin-url-to-domain` | domain string | 1 credit |
| `/v2/search/waterfall-icp-keyword` | Different response structure — nests under `.person` and `.experiences` | 1/result |
| `/v2/enrichment/company-distribution-by-country` | `total_employees` + `distribution[]` of `{country, count}` (ISO alpha-2) | 1 credit (free on Unlimited) |
| `/v2/enrichment/company-distribution-by-department` | `total_employees` + `distribution[]` of `{department, count}`, desc by count | 1 credit (free on Unlimited) |

**Common gotcha:** Person LinkedIn URL field varies across endpoints — check `person_linkedin_url`, `linkedin_profile_url`, and `linkedin_url` in that order. Same for `job_title` vs `title`.

### Fundraising filters (beta)

`/v2/search/companies` and `/search/waterfall-icp` (People Search) now accept beta investment filters: `total_funding`, `last_funding_amount`, `last_funding_year`, `last_funding_type`, `lead_investors`. Useful for targeting by recent raise (e.g., Series A in the last 12 months) instead of just industry/size. Beta — validate the response shape before relying on it.

### Company intelligence APIs (optional pre-qualification)

The two `company-distribution-by-*` endpoints take a **`company_linkedin_url`** (not a domain), which Step 1 already produces — so they slot in right after Step 1 with no extra lookup. Optional use: pre-qualify accounts *before* spending credits on waterfall + email enrichment (e.g., prune companies whose Marketing department is too small, or filter by employee geography). Can net-save credits by dropping weak accounts early. Not wired into any pipeline yet.

## Waterfall Cascade Definitions

Cascades are arrays of objects with `include_title`, `exclude_title`, `location`, `include_headline_search`. Standard patterns:

- **Marketing cascade** (4 levels): C-suite → VP/Head → Director → Sr. Manager, with headline fallback
- **Founder cascade** (2 levels): CEO/Founder/Owner → President (excluding VP)
- **Exclusions**: `analyst, associate, coordinator, assistant, intern, junior, student`

Cascades are defined at the top of each pipeline file. When creating new pipelines, copy from `tier_a_tef_pipeline.py` (most complete) or `bigcommerce_pipeline.py` (simplest).

## File Naming Conventions

| Prefix | Source |
|--------|--------|
| `bc_` | BigCommerce |
| `cf_` | Contentful |
| `wc_` | WooCommerce |
| `ae_` | Art & Entertainment |
| `so_` | Services/Other |
| `tef_` | Tech/Edu/Finance |
| `sqsp_` | Squarespace |
| `austin_` | Austin local |
| `edu_` | Education |

Each produces: `{prefix}_checkpoint.json`, `{prefix}_linkedin_lookup_results.csv`, `{prefix}_contacts_results.csv`, `{prefix}_pipeline.log`. Final deliverable is usually `{prefix}_contacts_verified_emails.csv` (filtered to rows with emails).

## Creating a New Pipeline

1. Copy `bigcommerce_pipeline.py` (domain-only input) or `tier_a_tef_pipeline.py` (rich CSV input with existing LinkedIn)
2. Update: `INPUT_FILE` (under `DATA_DIR`), output prefix
3. Adjust cascades if targeting different roles
4. Set `max_workers` per-step against the single endpoint it hits (rate limits are per-endpoint now — no need to throttle for other concurrent pipelines)

## Web UI (Blitz Contact Finder)

A browser-based frontend for the pipeline. No login; users enter contact info and are limited per email domain.

### Setup

```bash
pip install -r requirements-web.txt
```

Needs `BLITZ_API_KEY`; optional `SLACK_WEBHOOK_URL`, `RESULTS_BCC_EMAIL`, `SUPPORT_EMAIL`, `BOOKING_URL`. Results are emailed via the gws CLI (`GWS_CLI_PATH`) from whichever Google account it is authenticated as.

### Running

```bash
uvicorn web.app:app --reload --port 8000
```

Open `http://localhost:8000`, fill in contact info + search criteria, submit.

### Architecture

- `blitz_core.py` — shared `BlitzAPIClient` and `PipelineRunner` (extracted from CLI pipelines)
- `web/app.py` — FastAPI routes, background pipeline execution, CSV email delivery via gws
- `web/database.py` — SQLite job history (`blitz_web.db`)
- `web/static/index.html` — single-page UI (vanilla JS + Pico CSS)

### Pipeline Flow (Web)

1. **Company Discovery** — `/v2/search/companies` with user-provided industry/size/country filters
2. **People Search** — `/api/search/waterfall-icp` with user-provided job titles
3. **Email Enrichment** — `/api/enrichment/email` (automatic)

Capped at 1,000 leads. Results CSV is emailed to the user's business email via gws CLI.

## External Tools

- **gws CLI** (`GWS_CLI_PATH`) — Google Workspace CLI, used by the web app to send results via Gmail.
- **SerpAPI** — Google Maps scraping for local business leads (used in `austin_leads_pipeline.py`)
