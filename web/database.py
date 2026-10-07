"""SQLite database for job history."""
from __future__ import annotations

import json
import os
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path

DB_PATH = Path(os.environ.get("DB_PATH", Path(__file__).resolve().parent.parent / "blitz_web.db"))


def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db():
    conn = get_db()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS jobs (
            id TEXT PRIMARY KEY,
            status TEXT NOT NULL DEFAULT 'pending',
            config_json TEXT NOT NULL,
            results_json TEXT,
            created_at TEXT NOT NULL,
            completed_at TEXT
        );
    """)
    conn.close()


def create_job(config: dict) -> str:
    job_id = uuid.uuid4().hex[:12]
    conn = get_db()
    conn.execute(
        "INSERT INTO jobs (id, status, config_json, created_at) VALUES (?, 'running', ?, ?)",
        (job_id, json.dumps(config), datetime.now(timezone.utc).isoformat()),
    )
    conn.commit()
    conn.close()
    return job_id


def update_job(job_id: str, status: str, results: list[dict] | None = None):
    conn = get_db()
    completed = datetime.now(timezone.utc).isoformat() if status in ("complete", "error") else None
    conn.execute(
        "UPDATE jobs SET status = ?, results_json = ?, completed_at = ? WHERE id = ?",
        (status, json.dumps(results) if results else None, completed, job_id),
    )
    conn.commit()
    conn.close()


def count_jobs_for_domain(domain: str) -> int:
    """Count jobs whose submitted email ends in @domain (case-insensitive)."""
    domain = domain.lower()
    conn = get_db()
    try:
        rows = conn.execute("SELECT config_json FROM jobs").fetchall()
    finally:
        conn.close()
    count = 0
    for row in rows:
        try:
            cfg = json.loads(row["config_json"])
            email = (cfg.get("email") or "").lower()
            if "@" in email and email.split("@", 1)[1] == domain:
                count += 1
        except (json.JSONDecodeError, TypeError):
            continue
    return count


def get_job(job_id: str) -> dict | None:
    conn = get_db()
    row = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
    conn.close()
    if not row:
        return None
    return dict(row)
