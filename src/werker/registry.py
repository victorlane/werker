"""Module-level registry populated at import time by werker's decorators.
A plain side-table keyed by task path, not attributes on Task, since Task
is a frozen dataclass.

Every TaskPolicy field defaults to None, meaning "use the backend option".
Stacked decorators merge into one policy.
"""

import dataclasses
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from werker.models import DeliveryGuarantee


@dataclass(frozen=True)
class TaskPolicy:
    delivery: DeliveryGuarantee | None = None
    max_retries: int | None = None
    retry_on: tuple[type[BaseException], ...] | None = None
    backoff_base: float | None = None
    backoff_max: float | None = None
    jitter: bool | None = None
    timeout: float | None = None
    #: True dedupes on task path + args + kwargs, a callable builds the key.
    unique: bool | Callable[..., Any] | None = None
    #: Tokens per second, parsed from e.g. "10/m".
    rate: float | None = None
    #: Burst size for `rate`.
    rate_capacity: float | None = None
    concurrency: int | None = None


_POLICIES: dict[str, TaskPolicy] = {}


def set_policy(task_path: str, **changes: Any) -> TaskPolicy:
    policy = dataclasses.replace(_POLICIES.get(task_path, TaskPolicy()), **changes)
    _POLICIES[task_path] = policy
    return policy


def policy_for(task_path: str) -> TaskPolicy:
    return _POLICIES.get(task_path, TaskPolicy())


def all_policies() -> dict[str, TaskPolicy]:
    return dict(_POLICIES)


def limited_policies() -> dict[str, TaskPolicy]:
    """Task paths with a @rate_limit, for the broker's claim."""
    return {
        path: policy
        for path, policy in _POLICIES.items()
        if policy.rate is not None or policy.concurrency is not None
    }


def mark_at_most_once(task_path: str) -> None:
    set_policy(task_path, delivery=DeliveryGuarantee.AT_MOST_ONCE)


def delivery_guarantee_for(task_path: str) -> DeliveryGuarantee:
    return policy_for(task_path).delivery or DeliveryGuarantee.AT_LEAST_ONCE
