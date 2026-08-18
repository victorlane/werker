from django.tasks import task

from werker.decorators import at_most_once


@task
def say_hello(name: str) -> str:
    return f"hello, {name}"


@task
async def say_hello_async(name: str) -> str:
    return f"hello (async), {name}"


@at_most_once
@task
def send_notification(user_id: int) -> str:
    return f"notified user {user_id}"


@task
def always_fails() -> None:
    raise ValueError("boom")
