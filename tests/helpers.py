"""Shared test helpers."""
from __future__ import annotations

import importlib
import logging
import signal
import sys
from unittest import mock


def load_script(name: str):
    """Import a pipeline script without its import-time side effects.

    The scripts configure logging with a FileHandler (some next to the source
    file) and install SIGINT/SIGTERM handlers when imported. For tests we swap
    the file handler for a NullHandler and skip the signal handlers, so importing
    a script never writes into the repo or changes how Ctrl+C behaves in pytest.
    """
    if name in sys.modules:
        return sys.modules[name]
    with mock.patch.object(logging, "FileHandler", lambda *a, **k: logging.NullHandler()), \
         mock.patch.object(signal, "signal", lambda *a, **k: None):
        return importlib.import_module(name)

