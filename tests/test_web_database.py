"""Job storage (web/database.py) against a throwaway SQLite file."""
from __future__ import annotations

import json

import pytest

from web import database


@pytest.fixture
def db(tmp_path, monkeypatch):
    monkeypatch.setattr(database, "DB_PATH", tmp_path / "jobs.db")
    database.init_db()
    return database


def test_create_job_stores_config_as_running(db):
    job_id = db.create_job({"email": "ops@example.com", "industry_include": ["Software"]})

    job = db.get_job(job_id)
    assert len(job_id) == 12
    assert job["status"] == "running"
    assert json.loads(job["config_json"])["industry_include"] == ["Software"]
    assert job["completed_at"] is None


def test_update_job_records_results_and_completion_time(db):
    job_id = db.create_job({})
    db.update_job(job_id, "complete", [{"email": "a@example.com"}])

    job = db.get_job(job_id)
    assert job["status"] == "complete"
    assert json.loads(job["results_json"]) == [{"email": "a@example.com"}]
    assert job["completed_at"]


def test_non_terminal_status_has_no_completion_time(db):
    job_id = db.create_job({})
    db.update_job(job_id, "running")
    assert db.get_job(job_id)["completed_at"] is None


def test_get_job_unknown_id_returns_none(db):
    assert db.get_job("does-not-exist") is None


def test_count_jobs_for_domain_is_case_insensitive_and_exact(db):
    db.create_job({"email": "a@Example.com"})
    db.create_job({"email": "B@EXAMPLE.COM"})
    db.create_job({"email": "c@sub.example.com"})
    db.create_job({"email": "c@example.org"})
    db.create_job({"email": "no-at-sign"})

    assert db.count_jobs_for_domain("EXAMPLE.com") == 2
    assert db.count_jobs_for_domain("sub.example.com") == 1


def test_count_jobs_for_domain_ignores_corrupt_rows(db):
    db.create_job({"email": "a@example.com"})
    conn = db.get_db()
    conn.execute("INSERT INTO jobs (id, status, config_json, created_at) "
                 "VALUES ('bad', 'running', 'not json', 'now')")
    conn.commit()
    conn.close()

    assert db.count_jobs_for_domain("example.com") == 1
