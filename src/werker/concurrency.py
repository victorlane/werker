"""Shared thread-pool helpers. Dependency-free within werker so worker,
broker, and results can all use them without depending on each other.
"""

from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from django.db import close_old_connections

#: Default size for a Broker/ResultStore's dedicated bookkeeping executor.
#: Configurable via OPTIONS["BOOKKEEPING_CONCURRENCY"].
DEFAULT_BOOKKEEPING_CONCURRENCY = 4


def create_bounded_executor(size: int, thread_name_prefix: str) -> ThreadPoolExecutor:
    return ThreadPoolExecutor(max_workers=size, thread_name_prefix=thread_name_prefix)


def run_with_connection_cleanup[**P, R](
    func: Callable[P, R], *args: P.args, **kwargs: P.kwargs
) -> R:
    try:
        return func(*args, **kwargs)
    finally:
        close_old_connections()


def get_option(options: Any, key: str, default: Any) -> Any:
    """Reads one key out of a backend's raw OPTIONS dict. Tolerant of
    options being None or not a dict, since the real settings object
    doesn't exist yet and tests construct Broker/ResultStore directly."""
    if isinstance(options, dict):
        return options.get(key, default)
    return default
