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


def test_does_not_start_in_autoreloader_parent(monkeypatch):
    # The parent only watches files; the RUN_MAIN child serves requests.
    monkeypatch.setattr("sys.argv", ["manage.py", "runserver"])
    monkeypatch.delenv("RUN_MAIN", raising=False)
    assert AutostartManager.start_if_requested() is False
    assert AutostartManager._thread is None


def test_starts_under_noreload(monkeypatch):
    monkeypatch.setattr("sys.argv", ["manage.py", "runserver", "--noreload"])
    monkeypatch.delenv("RUN_MAIN", raising=False)

    class FakeWorker:
        async def run(self):
            pass

        def request_shutdown(self):
            pass

    monkeypatch.setattr("werker.worker.core.Worker", FakeWorker)

    assert AutostartManager.start_if_requested() is True


def test_starts_worker_thread_when_runserver_and_enabled(monkeypatch):
    monkeypatch.setattr("sys.argv", ["manage.py", "runserver"])
    monkeypatch.setenv("RUN_MAIN", "true")

    started = {}

    class FakeWorker:
        def __init__(self, *args, **kwargs):
            pass

        async def run(self):
            started["ran"] = True

        def request_shutdown(self):
            pass

    # Worker is imported lazily inside start_if_requested from core.
    monkeypatch.setattr("werker.worker.core.Worker", FakeWorker)

    assert AutostartManager.start_if_requested() is True
    assert AutostartManager._thread is not None
    AutostartManager._thread.join(timeout=5)
    assert started.get("ran") is True


def test_is_idempotent_within_one_process(monkeypatch):
    monkeypatch.setattr("sys.argv", ["manage.py", "runserver"])
    monkeypatch.setenv("RUN_MAIN", "true")

    class FakeWorker:
        async def run(self):
            pass

        def request_shutdown(self):
            pass

    monkeypatch.setattr("werker.worker.core.Worker", FakeWorker)

    assert AutostartManager.start_if_requested() is True
    # Second call in the same process must not spawn another thread.
    assert AutostartManager.start_if_requested() is False


@override_settings(
    TASKS={"default": {"BACKEND": "django.tasks.backends.immediate.ImmediateBackend"}}
)
def test_does_not_start_when_default_backend_is_not_werker(monkeypatch):
    monkeypatch.setattr("sys.argv", ["manage.py", "runserver"])
    monkeypatch.setenv("RUN_MAIN", "true")
    assert AutostartManager.start_if_requested() is False
    assert AutostartManager._thread is None
