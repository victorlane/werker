"""Per-task policy registry, @retry validation, and backoff timing."""

import random

import pytest
from django.core.exceptions import ImproperlyConfigured

from werker import registry
from werker.models import DeliveryGuarantee
from werker.retry import backoff_delay


def test_stacked_decorators_merge_into_one_policy():
    path = "tests.unit.test_policies.merged"
    registry.set_policy(path, delivery=DeliveryGuarantee.AT_MOST_ONCE)
    registry.set_policy(path, max_retries=5)

    policy = registry.policy_for(path)
    assert policy.delivery == DeliveryGuarantee.AT_MOST_ONCE
    assert policy.max_retries == 5
    assert registry.delivery_guarantee_for(path) == DeliveryGuarantee.AT_MOST_ONCE


def test_unknown_task_gets_the_default_policy():
    policy = registry.policy_for("tests.unit.test_policies.nothing")
    assert policy.max_retries is None
    assert registry.delivery_guarantee_for("x") == DeliveryGuarantee.AT_LEAST_ONCE


@pytest.mark.parametrize(
    "kwargs",
    [
        {"max_retries": -1},
        {"max_retries": True},
        {"backoff_base": 0},
        {"backoff_max": -5},
        {"on": (ValueError, "nope")},
    ],
)
def test_retry_rejects_bad_arguments(kwargs):
    from werker.decorators import retry

    with pytest.raises(ImproperlyConfigured):
        retry(**kwargs)


def test_backoff_without_jitter_is_exponential_and_capped():
    assert backoff_delay(1, base=2, maximum=300, jitter=False) == 2
    assert backoff_delay(3, base=2, maximum=300, jitter=False) == 8
    assert backoff_delay(20, base=2, maximum=300, jitter=False) == 300
    assert backoff_delay(5000, base=2.0, maximum=300, jitter=False) == 300


def test_backoff_jitter_stays_between_half_and_full_delay():
    random.seed(1)
    delays = [backoff_delay(4, base=2, maximum=300, jitter=True) for _ in range(200)]
    assert all(8 <= d <= 16 for d in delays)
    assert len(set(delays)) > 1


@pytest.mark.parametrize("seconds", [0, -1, True, "5"])
def test_timeout_rejects_bad_seconds(seconds):
    from werker.decorators import timeout

    with pytest.raises(ImproperlyConfigured):
        timeout(seconds)


def test_task_timeout_option_accepts_none_but_not_zero():
    from werker.config import validate_options

    validate_options({"TASK_TIMEOUT": None})
    validate_options({"TASK_TIMEOUT": 2.5})
    with pytest.raises(ImproperlyConfigured):
        validate_options({"TASK_TIMEOUT": 0})
    with pytest.raises(ImproperlyConfigured):
        validate_options({"RETRY_JITTER": "yes"})


@pytest.mark.parametrize(
    "kwargs",
    [
        {},
        {"rate": "ten/s"},
        {"rate": "10/day"},
        {"rate": "0/s"},
        {"rate": "5/s", "burst": 0},
        {"concurrency": 0},
        {"concurrency": True},
    ],
)
def test_rate_limit_rejects_bad_arguments(kwargs):
    from werker.decorators import rate_limit

    with pytest.raises(ImproperlyConfigured):
        rate_limit(**kwargs)


def test_parse_rate():
    from werker.decorators import parse_rate

    assert parse_rate("10/s") == (10, 10)
    assert parse_rate("120/m") == (2, 120)
    assert parse_rate("36 / h") == (0.01, 36)
