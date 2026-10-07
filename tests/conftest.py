"""Test configuration: no secrets, no network, no files written into the repo."""
from __future__ import annotations

import os
import socket
import tempfile

import pytest

# Must run before any product module is imported: several scripts read these
# at import time (and create BLITZ_DATA_DIR), and web/database.py fixes DB_PATH.
_SANDBOX = tempfile.mkdtemp(prefix="cet-tests-")
os.environ["BLITZ_DATA_DIR"] = os.path.join(_SANDBOX, "data")
os.environ["DB_PATH"] = os.path.join(_SANDBOX, "default.db")
for _var in ("BLITZ_API_KEY", "SERPAPI_KEY", "FIRECRAWL_API_KEY",
             "SLACK_WEBHOOK_URL", "RESULTS_BCC_EMAIL", "SUPPORT_EMAIL", "BOOKING_URL"):
    os.environ.pop(_var, None)


@pytest.fixture(autouse=True)
def _no_network(monkeypatch):
    """Fail loudly if anything tries to open a real socket connection."""
    def guard(*args, **kwargs):
        raise RuntimeError("Network access attempted during tests")

    monkeypatch.setattr(socket.socket, "connect", guard)
    monkeypatch.setattr(socket, "create_connection", guard)


@pytest.fixture
def no_sleep(monkeypatch):
    """Replace time.sleep with a recorder so retry/backoff logic runs instantly."""
    import time

    calls: list[float] = []
    monkeypatch.setattr(time, "sleep", lambda s: calls.append(s))
    return calls
