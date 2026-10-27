"""WorkerRecord presence, the health file, and `werker_health`."""

import asyncio
import os
import time
from datetime import timedelta

import pytest
from django.core.management import call_command
from django.core.management.base import CommandError
from django.utils import timezone

from werker.models import WorkerRecord
from werker.worker.core import Worker


@pytest.mark.django_db(transaction=True)
async def test_worker_records_presence_and_marks_itself_stopped(tmp_path):
    health = tmp_path / "alive"
    worker = Worker(health_file=str(health))
    worker.heartbeat_interval = 0.05
    worker.poll_interval = 0.05
    run = asyncio.ensure_future(worker.run())

    for _ in range(100):
        record = await WorkerRecord.objects.filter(worker_id=worker.worker_id).afirst()
        if record and record.last_seen_at > record.started_at:
            break
        await asyncio.sleep(0.05)
    assert record is not None
    assert record.stopped_at is None
    assert record.last_seen_at > record.started_at
    assert record.pid == os.getpid()
    assert health.exists()

    worker.request_shutdown()
    await asyncio.wait_for(run, timeout=10)
    record = await WorkerRecord.objects.aget(worker_id=worker.worker_id)
    assert record.stopped_at is not None


def test_health_file_fresh_stale_and_missing(tmp_path):
    health = tmp_path / "alive"
    with pytest.raises(CommandError, match="does not exist"):
        call_command("werker_health", file=str(health))

    health.touch()
    call_command("werker_health", file=str(health), max_age=60)

    old = time.time() - 120
    os.utime(health, (old, old))
    with pytest.raises(CommandError, match=r"12\ds old"):
        call_command("werker_health", file=str(health), max_age=60)


@pytest.mark.django_db
def test_health_db_needs_a_live_worker():
    with pytest.raises(CommandError, match="no live"):
        call_command("werker_health", db=True)

    now = timezone.now()
    WorkerRecord.objects.create(
        worker_id="gone", hostname="h", pid=1, started_at=now, last_seen_at=now - timedelta(hours=1)
    )
    WorkerRecord.objects.create(
        worker_id="stopped", hostname="h", pid=2, started_at=now, last_seen_at=now, stopped_at=now
    )
    with pytest.raises(CommandError):
        call_command("werker_health", db=True)

    WorkerRecord.objects.create(
        worker_id="live", hostname="h", pid=3, started_at=now, last_seen_at=now
    )
    call_command("werker_health", db=True)
    with pytest.raises(CommandError):
        call_command("werker_health", db=True, hostname="other-host")


def test_taskworker_exits_nonzero_via_manage_py(tmp_path):
    # CommandError maps to exit code 1 when run as a process.
    import subprocess
    import sys

    proc = subprocess.run(  # noqa: S603 - fixed arguments
        [sys.executable, "example/manage.py", "werker_health", "--file", str(tmp_path / "nope")],
        capture_output=True,
        text=True,
        env={**os.environ, "PYTHONPATH": "."},
        check=False,
    )
    assert proc.returncode == 1
    assert "unhealthy" in proc.stderr


@pytest.mark.django_db
def test_admin_lists_workers(admin_client):
    now = timezone.now()
    WorkerRecord.objects.create(
        worker_id="w-1", hostname="h", pid=3, started_at=now, last_seen_at=now
    )
    response = admin_client.get("/admin/werker/workerrecord/")
    assert response.status_code == 200
    assert "w-1" in response.content.decode()
