"""Tests for the runserver-only in-process worker autostart gate."""

import pytest
from django.test import override_settings

from werker.worker.autostart import AutostartManager


@pytest.fixture(autouse=True)
def _reset_autostart():
    AutostartManager.stop()
    yield
    AutostartManager.stop()


def test_does_not_start_when_not_runserver(monkeypatch):
    monkeypatch.setattr("sys.argv", ["manage.py", "migrate"])
    assert AutostartManager.start_if_requested() is False
    assert AutostartManager._thread is None


@override_settings(WERKER_AUTOSTART=False)
def test_does_not_start_when_disabled(monkeypatch):
    monkeypatch.setattr("sys.argv", ["manage.py", "runserver"])
    assert AutostartManager.start_if_requested() is False
    assert AutostartManager._thread is None


def test_starts_worker_thread_when_runserver_and_enabled(monkeypatch):
    monkeypatch.setattr("sys.argv", ["manage.py", "runserver"])

    started = {}

    class FakeWorker:
        def __init__(self, *args, **kwargs):
            pass

        async def run(self):
            started["ran"] = True

    # Worker is imported lazily inside start_if_requested from core.
    monkeypatch.setattr("werker.worker.core.Worker", FakeWorker)

    assert AutostartManager.start_if_requested() is True
    assert AutostartManager._thread is not None
    AutostartManager._thread.join(timeout=5)
    assert started.get("ran") is True


def test_is_idempotent_within_one_process(monkeypatch):
    monkeypatch.setattr("sys.argv", ["manage.py", "runserver"])

    class FakeWorker:
        async def run(self):
            pass

    monkeypatch.setattr("werker.worker.core.Worker", FakeWorker)

    assert AutostartManager.start_if_requested() is True
    # Second call in the same process must not spawn another thread.
    assert AutostartManager.start_if_requested() is False
