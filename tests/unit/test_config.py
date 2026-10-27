"""Worker configuration validation is deterministic and needs no database."""

import pytest
from django.core.exceptions import ImproperlyConfigured

from werker.config import validate_options


@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("CONCURRENCY", 0),
        ("CLAIM_BATCH_SIZE", -1),
        ("MAX_RETRIES", -1),
        ("POLL_INTERVAL", 0),
        ("HEARTBEAT_INTERVAL", True),
        ("STALE_SCHEDULE_CLAIM_TIMEOUT", 0),
        ("BOOKKEEPING_CONCURRENCY", 1.5),
    ],
)
def test_validate_options_rejects_invalid_worker_controls(key, value):
    with pytest.raises(ImproperlyConfigured, match=key):
        validate_options({key: value})


def test_validate_options_accepts_valid_controls_and_unknown_custom_options():
    validate_options(
        {
            "CONCURRENCY": 1,
            "MAX_RETRIES": 0,
            "POLL_INTERVAL": 0.1,
            "CUSTOM_BROKER_OPTION": object(),
        }
    )