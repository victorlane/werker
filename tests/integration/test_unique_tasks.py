"""@unique: one READY/RUNNING row per key."""

import asyncio
from concurrent.futures import ThreadPoolExecutor

import pytest
from django.db import close_old_connections, transaction
from django.tasks import task
from django.tasks.base import TaskResultStatus
from django.tasks.signals import task_enqueued

from werker import api
from werker.decorators import unique
from werker.models import DBTaskResult
from werker.worker.core import Worker


@unique
@task
def rebuild(section: str) -> str:
    return f"rebuilt {section}"


@unique(key=lambda user_id, **_: f"user-{user_id}")
@task
def sync_user(user_id: int, force: bool = False) -> int:
    return user_id


@task
def not_unique(section: str) -> str:
    return section


@pytest.mark.django_db(transaction=True)
def test_duplicate_enqueue_returns_the_queued_result():
    signals = []

    def on_enqueued(sender, task_result, **kwargs):
        signals.append(task_result.id)

    task_enqueued.connect(on_enqueued)
    try:
        first = rebuild.enqueue("docs")
        second = rebuild.enqueue("docs")
        other = rebuild.enqueue("blog")
    finally:
        task_enqueued.disconnect(on_enqueued)

    assert second.id == first.id
    assert other.id != first.id
    assert DBTaskResult.objects.count() == 2
    assert signals == [first.id, other.id]


@pytest.mark.django_db(transaction=True)
def test_tasks_without_unique_are_never_deduped():
    not_unique.enqueue("a")
    not_unique.enqueue("a")
    assert DBTaskResult.objects.count() == 2
    assert not DBTaskResult.objects.exclude(unique_key=None).exists()


@pytest.mark.django_db(transaction=True)
def test_custom_key_ignores_other_arguments():
    first = sync_user.enqueue(7)
    second = sync_user.enqueue(7, force=True)
    assert first.id == second.id
    assert DBTaskResult.objects.get(id=first.id).unique_key.endswith(":user-7")


@pytest.mark.django_db(transaction=True)
def test_key_is_released_once_the_task_finished():
    first = rebuild.enqueue("docs")
    asyncio.run(Worker(once=True).run())
    first.refresh()
    assert first.status == TaskResultStatus.SUCCESSFUL

    second = rebuild.enqueue("docs")
    assert second.id != first.id


@pytest.mark.django_db(transaction=True)
def test_dedupe_inside_a_caller_transaction():
    with transaction.atomic():
        first = rebuild.enqueue("docs")
        second = rebuild.enqueue("docs")
    assert first.id == second.id
    assert DBTaskResult.objects.count() == 1


@pytest.mark.django_db(transaction=True)
def test_concurrent_enqueues_create_one_row():
    def enqueue(_):
        try:
            return rebuild.enqueue("race").id
        finally:
            close_old_connections()

    with ThreadPoolExecutor(max_workers=10) as pool:
        ids = set(pool.map(enqueue, range(10)))

    assert len(ids) == 1
    assert DBTaskResult.objects.count() == 1


@pytest.mark.django_db(transaction=True)
def test_requeue_refuses_when_the_key_is_already_queued():
    failed = rebuild.enqueue("docs")
    assert api.cancel(failed.id) is True  # FAILED, key released
    queued = rebuild.enqueue("docs")
    assert queued.id != failed.id

    assert api.requeue(failed.id) is False
    assert DBTaskResult.objects.get(id=failed.id).status == "FAILED"
