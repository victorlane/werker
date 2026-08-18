"""Unit tests for werker.concurrency's generic helper — no DB needed."""

from unittest.mock import patch

import pytest

from werker.concurrency import (
    DEFAULT_BOOKKEEPING_CONCURRENCY,
    get_option,
    run_with_connection_cleanup,
)


def test_run_with_connection_cleanup_returns_value_and_cleans_up():
    with patch("werker.concurrency.close_old_connections") as mock_close:
        result = run_with_connection_cleanup(lambda x, y: x + y, 2, 3)

    assert result == 5
    mock_close.assert_called_once()


def test_run_with_connection_cleanup_cleans_up_even_on_exception():
    def boom():
        raise ValueError("x")

    with patch("werker.concurrency.close_old_connections") as mock_close:
        with pytest.raises(ValueError, match="x"):
            run_with_connection_cleanup(boom)

    mock_close.assert_called_once()


def test_get_option_reads_from_dict():
    assert get_option({"FOO": 7}, "FOO", 1) == 7
    assert get_option({}, "FOO", 1) == 1


def test_get_option_tolerates_none_and_non_dict():
    assert (
        get_option(None, "FOO", DEFAULT_BOOKKEEPING_CONCURRENCY)
        == DEFAULT_BOOKKEEPING_CONCURRENCY
    )
    assert get_option("not-a-dict", "FOO", 9) == 9
