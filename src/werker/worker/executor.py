"""Backwards-compatible re-export. Implementation lives in werker.concurrency."""

from functools import partial

from werker.concurrency import create_bounded_executor, run_with_connection_cleanup

create_executor = partial(create_bounded_executor, thread_name_prefix="werker-sync")

__all__ = ["create_executor", "run_with_connection_cleanup"]
