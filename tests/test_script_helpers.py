"""Pure helpers inside the pipeline scripts: normalisers, name matching,
checkpointing, cascade building, date parsing and output formatting."""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from helpers import load_script


# ---------------------------------------------------------------------------
# URL / domain normalisation
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("raw, expected", [
    ("https://www.linkedin.com/company/acme", "https://www.linkedin.com/company/acme"),
    ("  linkedin.com/company/acme ", "https://www.linkedin.com/company/acme"),
    ("https://www.linkedin.com/in/a-person", None),
    ("", None),
])
def test_normalize_company_linkedin_url(raw, expected):
    assert load_script("tier_a_tef_pipeline").normalize_linkedin_url(raw) == expected


@pytest.mark.parametrize("a, b, same", [
    ("https://www.Acme.example.com/", "acme.example.com", True),
    ("http://acme.example.com", "www.acme.example.com", True),
    ("acme.example.com", "acme.example.org", False),
    ("", "acme.example.com", False),
])
def test_seo_domains_match(a, b, same):
    assert load_script("seo_contact_finder").domains_match(a, b) is same


@pytest.mark.parametrize("raw, expected", [
    ("HTTPS://www.Skill-Swap.example.com/about?ref=1", "skill-swap.example.com"),
    ("tutorhub.example.io.", "tutorhub.example.io"),
    ("localhost", None),
    ("https://twitter.com/someone", None),
    ("linkedin.com", None),
])
def test_marketplace_normalize_domain(raw, expected):
    assert load_script("marketplace_pipeline").normalize_domain(raw) == expected


def test_marketplace_extracts_unique_domains_from_markdown():
    md = (
        "## Directory\n"
        "- [SkillSwap](https://www.skillswap.example.com/about) — talent\n"
        "- TutorHub: tutorhub.example.io, also at TUTORHUB.EXAMPLE.IO\n"
        "- Follow us on twitter.com/directory and linkedin.com/company/dir\n"
    )
    domains = load_script("marketplace_pipeline").extract_domains_from_markdown(md)
    assert domains == {"skillswap.example.com", "tutorhub.example.io"}


@pytest.mark.parametrize("raw, expected", [
    ("acme.example.com", "https://acme.example.com"),
    ("http://acme.example.com", "http://acme.example.com"),
    ("   ", ""),
    (None, ""),
])
def test_angles_normalize_url(raw, expected):
    assert load_script("austin_automation_angles").normalize_url(raw) == expected


@pytest.mark.parametrize("email, domain, ok", [
    ("pat@acme.example.com", "www.acme.example.com", True),
    ("pat@eu.acme.example.com", "acme.example.com", True),
    ("pat@notacme.example.com", "acme.example.com", False),
    ("pat@gmail.example", "acme.example.com", False),
    ("", "acme.example.com", False),
])
def test_hr_email_must_be_on_current_company_domain(email, domain, ok):
    assert load_script("hr_newjob_email_enrichment").email_matches_domain(email, domain) is ok


# ---------------------------------------------------------------------------
# Fuzzy name matching
# ---------------------------------------------------------------------------

def test_matcher_normalize_name_strips_honorifics_and_punctuation():
    m = load_script("contact_linkedin_matcher")
    assert m.normalize_name("Dr. Ann-Marie O'Example Jr.") == "annmarie oexample"


@pytest.mark.parametrize("contact, candidate, score", [
    ("Ann Able", "ann able", 1.0),
    ("Ann Able", "Ann Baker", 0.8),
    ("Ann Able", "Zoe Able", 0.7),
    ("Ann Able", "", 0.0),
])
def test_matcher_name_match_score_tiers(contact, candidate, score):
    assert load_script("contact_linkedin_matcher").name_match_score(contact, candidate) == score


def test_matcher_process_row_picks_best_scoring_person(monkeypatch):
    m = load_script("contact_linkedin_matcher")
    monkeypatch.setattr(m, "domain_to_linkedin", lambda d: "https://www.linkedin.com/company/acme")
    monkeypatch.setattr(m, "waterfall_find_people", lambda url: [
        {"full_name": "Bo Baker", "person_linkedin_url": "https://www.linkedin.com/in/bo"},
        {"full_name": "Ann Able", "job_title": "CMO",
         "linkedin_profile_url": "https://www.linkedin.com/in/ann"},
    ])

    out = m.process_row({"domain": " acme.example.com ", "contact_name": "Ann Able"})

    assert out["personal_linkedin"] == "https://www.linkedin.com/in/ann"
    assert out["match_method"] == "name_match"
    assert out["match_score"] == "1.00"


def test_matcher_process_row_falls_back_to_top_result(monkeypatch):
    m = load_script("contact_linkedin_matcher")
    monkeypatch.setattr(m, "domain_to_linkedin", lambda d: "https://www.linkedin.com/company/acme")
    monkeypatch.setattr(m, "waterfall_find_people", lambda url: [
        {"full_name": "Xyzzy Qwerty", "person_linkedin_url": "https://www.linkedin.com/in/xq"},
    ])

    out = m.process_row({"domain": "acme.example.com", "contact_name": "Ann Able"})

    assert out["match_method"] == "top_result_fallback"
    assert out["personal_linkedin"] == "https://www.linkedin.com/in/xq"


def test_matcher_process_row_reports_missing_company(monkeypatch):
    m = load_script("contact_linkedin_matcher")
    monkeypatch.setattr(m, "domain_to_linkedin", lambda d: None)

    out = m.process_row({"domain": "acme.example.com", "contact_name": "Ann Able"})
    assert out["match_method"] == "no_company_linkedin"


@pytest.mark.parametrize("first, last, candidate, score", [
    ("Ann", "Able", "Ann B. Able", 0.95),     # middle name tolerated
    ("Jen", "Able", "Jennifer Able", 0.85),   # nickname via shared initial
    ("Ann", "Able", "Zoe Able", 0.7),
    ("Ann", "Able", "Ann Baker", 0.55),
])
def test_revops_name_match_score(first, last, candidate, score):
    assert load_script("revops_agencies_pipeline").name_match_score(first, last, candidate) == score


# ---------------------------------------------------------------------------
# Cascade building
# ---------------------------------------------------------------------------

def test_revops_cascade_for_founder_puts_narrow_tier_before_broad_fallback():
    r = load_script("revops_agencies_pipeline")
    cascade = r.build_cascade_for_title("Co-Founder & CEO")

    assert len(cascade) == 2
    assert "Founder" in cascade[0]["include_title"]
    assert cascade[0]["include_headline_search"] is True
    assert cascade[1] is r.BROAD_SENIOR_TIER


def test_revops_cascade_for_vp_adds_function_specific_terms():
    tier = load_script("revops_agencies_pipeline").build_cascade_for_title(
        "VP of Revenue Operations")[0]
    assert {"VP Revenue Operations", "Head of Revenue Operations"} <= set(tier["include_title"])


def test_revops_cascade_without_recognised_title_is_broad_only():
    r = load_script("revops_agencies_pipeline")
    assert r.build_cascade_for_title("Consultant") == [r.BROAD_SENIOR_TIER]


# ---------------------------------------------------------------------------
# Checkpointing
# ---------------------------------------------------------------------------

def test_checkpoint_round_trip_and_dedup(tmp_path):
    bc = load_script("bigcommerce_pipeline")
    path = tmp_path / "bc_checkpoint.json"

    cp = bc.CheckpointManager(path)
    cp.mark_step1("acme.example.com", "https://www.linkedin.com/company/acme")
    cp.mark_step1("acme.example.com", None)
    cp.mark_step1("globex.example.com", None)
    cp.mark_step2("acme.example.com")
    cp.mark_step3("https://www.linkedin.com/in/ann")
    cp.mark_step3("https://www.linkedin.com/in/ann")
    cp.save()

    resumed = bc.CheckpointManager(path)
    assert resumed.data["step1_processed"] == ["acme.example.com", "globex.example.com"]
    assert resumed.get_linkedin_url("acme.example.com") == "https://www.linkedin.com/company/acme"
    assert resumed.get_linkedin_url("globex.example.com") is None
    assert resumed.is_step2_processed("acme.example.com")
    assert not resumed.is_step2_processed("globex.example.com")
    assert resumed.data["step3_processed"] == ["https://www.linkedin.com/in/ann"]
    assert resumed.data["stats"]["step1_found"] == 1


def test_load_domains_skips_blank_rows(tmp_path, monkeypatch):
    bc = load_script("bigcommerce_pipeline")
    csv_file = tmp_path / "domains.csv"
    csv_file.write_text("domain,rank\n acme.example.com ,1\n,2\nglobex.example.com,3\n")
    monkeypatch.setattr(bc, "INPUT_FILE", csv_file)

    assert bc.load_domains() == ["acme.example.com", "globex.example.com"]


# ---------------------------------------------------------------------------
# Dates and output formatting
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("raw, expected", [
    ("2024-08-15T00:00:00.000Z", datetime(2024, 8, 15, tzinfo=timezone.utc)),
    ("2024-08-15", datetime(2024, 8, 15, tzinfo=timezone.utc)),
    ("2024-08", datetime(2024, 8, 1, tzinfo=timezone.utc)),
    ("August 2024", None),
    ("2024-13-01", None),
    ("", None),
])
def test_parse_role_start_date(raw, expected):
    assert load_script("senior_hr_pipeline")._parse_start_date(raw) == expected


def test_angle_cell_cleans_numbering_and_pads_with_fallbacks():
    angles = load_script("austin_automation_angles")
    cell = angles.format_angle_cell(["- automate intake forms", "2) route leads by service line!", "  "])

    lines = cell.split("\n")
    assert lines[0] == "1. Automate intake forms."
    assert lines[1] == "2. Route leads by service line!"
    assert lines[2] == "3. " + angles.GENERIC_FALLBACK[2]


def test_angle_cell_keeps_only_first_three():
    angles = load_script("austin_automation_angles")
    cell = angles.format_angle_cell(["one.", "two.", "three.", "four."])
    assert cell == "1. One.\n2. Two.\n3. Three."
