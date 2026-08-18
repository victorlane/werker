"""Shared thread-pool helpers — deliberately dependency-free within werker
(no imports from werker.worker/werker.broker/werker.results) so both the
worker's user-task executor and the Broker/ResultStore ABCs' own dedicated
bookkeeping executors (see werker.broker's module docstring) can use the
same small helpers without either package depending on the other.
"""

from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from django.db import close_old_connections
from typing_extensions import ParamSpec, TypeVar

_P = ParamSpec("_P")
_R = TypeVar("_R")

#: Default size for a Broker/ResultStore's dedicated bookkeeping executor
#: (see werker.broker's module docstring) — deliberately small; this is for
#: fast, frequent DB bookkeeping calls (claim/ack/heartbeat), not long-
#: running work. Configurable per backend alias via OPTIONS["BOOKKEEPING_CONCURRENCY"].
DEFAULT_BOOKKEEPING_CONCURRENCY = 4


def create_bounded_executor(size: int, thread_name_prefix: str) -> ThreadPoolExecutor:
    return ThreadPoolExecutor(max_workers=size, thread_name_prefix=thread_name_prefix)


def run_with_connection_cleanup(func: Callable[_P, _R], *args: _P.args, **kwargs: _P.kwargs) -> _R:
    try:
        return func(*args, **kwargs)
    finally:
        close_old_connections()


def get_option(options: Any, key: str, default: Any) -> Any:
    """Reads one key out of a backend's raw OPTIONS dict. Deliberately
    tolerant of options being None or not a dict — werker's real lazy
    settings/validation object (DRF api_settings-style) is phase 9's job;
    until then, Broker/ResultStore need to work when constructed directly
    with options=None, as the test suite does."""
    if isinstance(options, dict):
        return options.get(key, default)
    return default
