"""Bounded thread pool for sync task functions, plus the mandatory
per-execution connection cleanup.

A long-lived worker process reusing a bounded pool of threads for sync
Django ORM/task work will otherwise leak DB connections indefinitely until
max_connections is exhausted — the single most common real-world failure
mode of hand-rolled DB workers. Every call routed through this pool closes
old connections on its thread when done, success or failure.
"""

from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from django.db import close_old_connections


def create_executor(concurrency: int) -> ThreadPoolExecutor:
    return ThreadPoolExecutor(max_workers=concurrency, thread_name_prefix="werker-sync")


def run_with_connection_cleanup(func: Callable[..., Any], *args, **kwargs) -> Any:
    try:
        return func(*args, **kwargs)
    finally:
        close_old_connections()
