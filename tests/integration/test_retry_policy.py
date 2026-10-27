"""@retry: per-task retry budget, exception filter, and backoff."""

import asyncio
from datetime import timedelta

import pytest
from django.tasks import task
from django.tasks.base import TaskResultStatus
from django.utils import timezone

from werker.decorators import retry
from werker.models import DBTaskResult
from werker.worker.core import Worker


class Transient(Exception):
    pass


@retry(max_retries=5, on=Transient, jitter=False, backoff_base=3)
@task
def flaky_transient() -> None:
    raise Transient("try again")


@retry(max_retries=5, on=Transient)
@task
def broken_permanently() -> None:
    raise KeyError("bug")


@pytest.mark.django_db(transaction=True)
async def test_per_task_max_retries_is_stored_on_the_row():
    result = await flaky_transient.aenqueue()
    row = await DBTaskResult.objects.aget(id=result.id)
    assert row.max_retries == 5


@pytest.mark.django_db(transaction=True)
async def test_matching_exception_is_retried_with_task_backoff():
    result = await flaky_transient.aenqueue()
    before = timezone.now()
    await asyncio.wait_for(Worker(once=True).run(), timeout=10)

    row = await DBTaskResult.objects.aget(id=result.id)
    assert row.status == TaskResultStatus.READY
    # backoff_base=3, attempt 1, no jitter: 3 seconds.
    assert before + timedelta(seconds=2.5) < row.run_after < before + timedelta(seconds=4)


@pytest.mark.django_db(transaction=True)
async def test_other_exception_fails_without_retrying():
    result = await broken_permanently.aenqueue()
    await asyncio.wait_for(Worker(once=True).run(), timeout=10)

    await result.arefresh()
    assert result.status == TaskResultStatus.FAILED
    assert result.attempts == 1
    assert result.errors[0].exception_class is KeyError


@pytest.mark.django_db(transaction=True)
async def test_retry_delay_grows_per_attempt_and_uses_task_policy():
    worker = Worker()
    worker.retry_jitter = False
    path = flaky_transient.module_path
    assert worker.retry_delay(path, 1) == 3
    assert worker.retry_delay(path, 2) == 9
    # A task without @retry uses the backend options (base 2).
    assert worker.retry_delay("example.demoapp.tasks.always_fails", 2) == 4
