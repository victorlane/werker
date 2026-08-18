from django.tasks import task


@task
def say_hello(name: str) -> str:
    return f"hello, {name}"


@task
async def say_hello_async(name: str) -> str:
    return f"hello (async), {name}"


@task
def always_fails() -> None:
    raise ValueError("boom")
