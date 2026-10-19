"""Unit tests for the backfill and lookback date-range generators and their dispatcher.

Neither function takes an injectable reference date, so expected values are
computed against the same UTC "today" the functions use. This is reliable
except across an exact UTC-midnight boundary during a run.
"""

from datetime import UTC, datetime, timedelta

import pytest

from data_pipeline_prototype.utils.python import (
    generate_backfill_date_range,
    generate_lookback_date_range,
    resolve_processing_dates,
)


def _day(offset: int) -> str:
    """Return the UTC date `offset` days from today as a YYYY-MM-DD string."""
    return (datetime.now(UTC).date() + timedelta(days=offset)).strftime("%Y-%m-%d")


# --- generate_backfill_date_range ---


def test_backfill_happy_path():
    """An inclusive past range returns each day as a YYYY-MM-DD string."""
    assert generate_backfill_date_range(_day(-5), _day(-3)) == [_day(-5), _day(-4), _day(-3)]


def test_backfill_missing_date_raises():
    """Omitting either bound raises — a backfill must name both dates explicitly."""
    with pytest.raises(ValueError, match="backfill requires both"):
        generate_backfill_date_range(_day(-5), None)


def test_backfill_start_after_end_raises():
    """A start later than the end raises."""
    with pytest.raises(ValueError, match="earlier than or the same as"):
        generate_backfill_date_range(_day(-3), _day(-5))


def test_backfill_future_date_raises():
    """A date later than today raises."""
    with pytest.raises(ValueError, match="later than the current day"):
        generate_backfill_date_range(_day(-1), _day(5))


# --- generate_lookback_date_range ---


def test_lookback_happy_path_single_day():
    """A one-day lookback returns yesterday."""
    assert generate_lookback_date_range(1) == [_day(-1)]


def test_lookback_zero_returns_today():
    """A zero-day lookback returns today, even with the default flag."""
    assert generate_lookback_date_range(0) == [_day(0)]


def test_lookback_multi_day_ends_yesterday():
    """N days looks back N days, ending on yesterday."""
    assert generate_lookback_date_range(3) == [_day(-3), _day(-2), _day(-1)]


def test_lookback_include_current_date_extends_to_today():
    """include_current_date extends the window through today."""
    assert generate_lookback_date_range(1, include_current_date=True) == [_day(-1), _day(0)]


def test_lookback_negative_raises():
    """A negative lookback raises."""
    with pytest.raises(ValueError, match="must not be negative"):
        generate_lookback_date_range(-1)


# --- resolve_processing_dates ---


def test_resolve_lookback_dispatches_to_lookback():
    """lookback returns the lookback window for the given number of days."""
    assert resolve_processing_dates("lookback", lookback_days=2) == [_day(-2), _day(-1)]


def test_resolve_backfill_dispatches_to_backfill():
    """backfill returns the inclusive range between the given dates."""
    assert resolve_processing_dates("backfill", start_date=_day(-5), end_date=_day(-4)) == [
        _day(-5),
        _day(-4),
    ]


def test_resolve_lookback_without_days_raises():
    """lookback with no lookback_days raises instead of silently choosing a window."""
    with pytest.raises(ValueError, match="requires an integer value for lookback_days"):
        resolve_processing_dates("lookback")


def test_resolve_backfill_without_dates_raises():
    """backfill with no dates raises: the backfill generator's own check applies."""
    with pytest.raises(ValueError, match="backfill requires both"):
        resolve_processing_dates("backfill")


def test_resolve_unknown_processing_type_raises():
    """Any processing type other than lookback or backfill raises and names the value."""
    with pytest.raises(ValueError, match="must be 'lookback' or 'backfill', not 'daily'"):
        resolve_processing_dates("daily")
