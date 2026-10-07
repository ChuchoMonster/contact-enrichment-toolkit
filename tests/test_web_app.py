"""FastAPI routes and job plumbing (web/app.py).

The background pipeline, Slack and the gws email CLI are all replaced with
fakes; the database is a temporary SQLite file.
"""
from __future__ import annotations

import csv
import email
import email.policy
import io
import json
import os
import threading

import pytest
import responses
from fastapi.testclient import TestClient

from web import app as web_app
from web import database

VALID_BODY = {
    "first_name": "Riley",
    "last_name": "Example",
    "company_name": "Example Labs",
    "email": "riley@example.com",
    "industry_include": ["Software Development"],
    "employee_range": ["11-50"],
    "country_code": "US",
    "include_title": ["Head of Marketing"],
}


class PipelineLaunches(list):
    """Records background pipeline launches instead of running the real pipeline."""

    def __init__(self):
        super().__init__()
        self._launched = threading.Event()

    def __call__(self, *args):
        self.append(args)
        self._launched.set()

    def wait(self):
        return self._launched.wait(timeout=5)


@pytest.fixture
def started(monkeypatch):
    launches = PipelineLaunches()
    monkeypatch.setattr(web_app, "_run_pipeline", launches)
    return launches


@pytest.fixture
def client(tmp_path, monkeypatch, started):
    monkeypatch.setattr(database, "DB_PATH", tmp_path / "web.db")
    with TestClient(web_app.app) as c:
        yield c


def test_index_and_thank_you_pages_are_served(client):
    for path in ("/", "/thank-you"):
        resp = client.get(path)
        assert resp.status_code == 200
        assert resp.headers["content-type"].startswith("text/html")
        assert "<html" in resp.text.lower()


def test_missing_fields_return_every_validation_error(client, started):
    resp = client.post("/api/jobs", json={"email": "not-an-email"})

    assert resp.status_code == 422
    errors = resp.json()["errors"]
    assert len(errors) == 8
    assert "A valid business email is required." in errors
    assert "Select a headquarters country." in errors
    assert started == []


def test_valid_submission_saves_job_and_starts_pipeline(client, started):
    resp = client.post("/api/jobs", json=VALID_BODY)

    assert resp.status_code == 200
    job_id = resp.json()["job_id"]
    assert started.wait()
    run_job_id, payload, include, exclude, contact = started[0]
    assert run_job_id == job_id
    assert payload == {
        "company": {
            "industry": {"include": ["Software Development"]},
            "employee_range": ["11-50"],
            "hq": {"country_code": ["US"]},
        },
        "max_results": 50,
    }
    assert (include, exclude) == (["Head of Marketing"], [])
    assert contact == {"first_name": "Riley", "last_name": "Example",
                       "company_name": "Example Labs", "email": "riley@example.com"}
    stored = json.loads(database.get_job(job_id)["config_json"])
    assert stored["_company_payload"] == payload


def test_optional_filters_are_mapped_into_company_payload(client, started):
    body = {**VALID_BODY,
            "industry_exclude": ["Retail"],
            "city_include": ["Springfield"], "city_exclude": ["Shelbyville"],
            "type_include": ["Privately Held"], "type_exclude": ["Nonprofit"],
            "keywords_include": ["saas"], "keywords_exclude": ["agency"],
            "founded_year_min": "2010", "founded_year_max": 2020,
            "min_linkedin_followers": "500",
            "exclude_title": ["Intern"]}

    assert client.post("/api/jobs", json=body).status_code == 200
    started.wait()
    _, payload, _, exclude, _ = started[0]
    co = payload["company"]
    assert co["industry"] == {"include": ["Software Development"], "exclude": ["Retail"]}
    assert co["hq"] == {"country_code": ["US"],
                        "city": {"include": ["Springfield"], "exclude": ["Shelbyville"]}}
    assert co["type"] == {"include": ["Privately Held"], "exclude": ["Nonprofit"]}
    assert co["keywords"] == {"include": ["saas"], "exclude": ["agency"]}
    assert co["founded_year"] == {"min": 2010, "max": 2020}
    assert co["min_linkedin_followers"] == 500
    assert exclude == ["Intern"]


def test_submissions_are_limited_per_email_domain(client, monkeypatch):
    monkeypatch.setattr(web_app, "SUPPORT_EMAIL", "help@example.net")
    for n in range(3):
        body = {**VALID_BODY, "email": f"user{n}@Example.com"}
        assert client.post("/api/jobs", json=body).status_code == 200

    blocked = client.post("/api/jobs", json={**VALID_BODY, "email": "late@example.com"})
    assert blocked.status_code == 429
    assert "example.com" in blocked.json()["errors"][0]
    assert "help@example.net" in blocked.json()["errors"][0]

    other = client.post("/api/jobs", json={**VALID_BODY, "email": "first@example.org"})
    assert other.status_code == 200


@responses.activate
def test_new_job_posts_slack_notification_when_configured(client, monkeypatch):
    monkeypatch.setenv("SLACK_WEBHOOK_URL", "https://hooks.example.com/T000/B000")
    responses.post("https://hooks.example.com/T000/B000", json={})

    job_id = client.post("/api/jobs", json=VALID_BODY).json()["job_id"]

    text = json.loads(responses.calls[0].request.body)["text"]
    assert job_id in text and "riley@example.com" in text


# ---------------------------------------------------------------------------
# Background job
# ---------------------------------------------------------------------------

CONTACT = {"first_name": "Riley", "last_name": "Example",
           "company_name": "Example Labs", "email": "riley@example.com"}


@pytest.fixture
def job(tmp_path, monkeypatch):
    monkeypatch.setattr(database, "DB_PATH", tmp_path / "jobs.db")
    database.init_db()
    return database.create_job({"email": CONTACT["email"]})


def fake_runner(monkeypatch, outcome):
    class Runner:
        def __init__(self, client, max_leads):
            assert max_leads == web_app.MAX_LEADS

        def run(self, payload, include, exclude):
            if isinstance(outcome, Exception):
                raise outcome
            return outcome

    monkeypatch.setattr(web_app, "BlitzAPIClient", lambda key: object())
    monkeypatch.setattr(web_app, "PipelineRunner", Runner)


def test_run_pipeline_success_saves_results_and_emails_them(job, monkeypatch):
    results = [{"full_name": "Ann Able", "email": "ann@example.com"}]
    fake_runner(monkeypatch, results)
    sent = []
    monkeypatch.setattr(web_app, "_email_results", lambda *a: sent.append(a))

    web_app._run_pipeline(job, {}, ["CEO"], [], CONTACT)

    stored = database.get_job(job)
    assert stored["status"] == "complete"
    assert json.loads(stored["results_json"]) == results
    assert sent == [(CONTACT, results, job)]


def test_run_pipeline_with_no_results_sends_no_email(job, monkeypatch):
    fake_runner(monkeypatch, [])
    monkeypatch.setattr(web_app, "_email_results",
                        lambda *a: pytest.fail("should not email an empty result"))

    web_app._run_pipeline(job, {}, ["CEO"], [], CONTACT)

    assert database.get_job(job)["status"] == "complete"


def test_run_pipeline_failure_marks_job_as_error(job, monkeypatch):
    fake_runner(monkeypatch, RuntimeError("upstream exploded"))

    web_app._run_pipeline(job, {}, ["CEO"], [], CONTACT)

    assert database.get_job(job)["status"] == "error"


# ---------------------------------------------------------------------------
# Results CSV + email
# ---------------------------------------------------------------------------

ROWS = [
    {"company_name": "Acme", "full_name": "Ann Able", "email": "ann@acme.example.com",
     "internal_note": "dropped"},
    {"company_name": "Globex", "full_name": "Bo Baker", "email": ""},
]


def test_build_csv_keeps_only_rows_with_email_in_fixed_column_order():
    rows = list(csv.reader(io.StringIO(web_app._build_csv(ROWS))))

    assert rows[0] == ["company_name", "website", "industry", "employee_count",
                       "full_name", "first_name", "last_name", "title",
                       "person_linkedin_url", "company_linkedin_url", "email"]
    assert len(rows) == 2
    assert rows[1][0] == "Acme" and rows[1][-1] == "ann@acme.example.com"


def test_email_results_hands_gws_a_complete_message_and_cleans_up(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(web_app, "BCC_EMAIL", "copy@example.net")
    captured = {}

    def fake_run(cmd, capture_output, text, timeout, cwd):
        upload = cmd[cmd.index("--upload") + 1]
        captured["cmd"] = cmd
        captured["raw"] = (tmp_path / upload).read_bytes()

        class Done:
            returncode, stdout, stderr = 0, "", ""
        return Done()

    monkeypatch.setattr(web_app.subprocess, "run", fake_run)

    web_app._email_results(CONTACT, ROWS, "job123")

    cmd = captured["cmd"]
    assert cmd[1:5] == ["gmail", "users", "messages", "send"]
    assert cmd[cmd.index("--upload-content-type") + 1] == "message/rfc822"
    msg = email.message_from_bytes(captured["raw"], policy=email.policy.default)
    assert msg["To"] == "riley@example.com"
    assert msg["Bcc"] == "copy@example.net"
    assert "1 verified emails" in msg["Subject"]
    attachment = next(p for p in msg.walk() if p.get_filename())
    assert attachment.get_filename() == "coldstart_leads_job123.csv"
    assert "ann@acme.example.com" in attachment.get_payload(decode=True).decode()
    assert not [f for f in os.listdir(tmp_path) if f.endswith(".eml")]
