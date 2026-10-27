"""Runs the real Worker through the runserver autostart path: a daemon
thread with its own event loop, where installing signal handlers fails."""

import time

import pytest
from django.tasks.base import TaskResultStatus

from example.demoapp.tasks import say_hello
from werker.worker.autostart import AutostartManager


@pytest.mark.django_db(transaction=True)
def test_autostarted_worker_runs_tasks(monkeypatch):
    monkeypatch.setattr("sys.argv", ["manage.py", "runserver"])
    monkeypatch.setenv("RUN_MAIN", "true")
    AutostartManager.stop()

    result = say_hello.enqueue("autostart")
    assert AutostartManager.start_if_requested() is True
    try:
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            result.refresh()
            if result.status == TaskResultStatus.SUCCESSFUL:
                break
            assert AutostartManager._thread is not None
            assert AutostartManager._thread.is_alive(), "autostart worker thread died"
            time.sleep(0.1)
        assert result.status == TaskResultStatus.SUCCESSFUL
        assert result.return_value == "hello, autostart"
    finally:
        AutostartManager.stop()
