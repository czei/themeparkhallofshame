"""
Unit Tests: PartitionAwareDateRange helpers (Feature 004)

Tests for partition-aware date range helpers that ensure queries
use explicit bounds for MySQL partition pruning.

Feature: 004-themeparks-data-collection
Task: T041
"""

import pytest
from datetime import datetime, timedelta
from freezegun import freeze_time

from utils.query_helpers import PartitionAwareDateRange


class TestPartitionAwareDateRangeToday:
    """Tests for today period bounds."""

    @freeze_time("2025-06-15 14:30:00")
    def test_for_today_returns_midnight_bounds(self):
        """Today bounds start at midnight and end at next midnight."""
        bounds = PartitionAwareDateRange.for_today()

        assert bounds.start == datetime(2025, 6, 15, 0, 0, 0)
        assert bounds.end == datetime(2025, 6, 16, 0, 0, 0)
        assert bounds.period_name == 'today'

    @freeze_time("2025-06-15 14:30:00")
    def test_for_today_with_reference_time(self):
        """Today bounds can use a custom reference time."""
        ref_time = datetime(2025, 3, 20, 10, 0, 0)
        bounds = PartitionAwareDateRange.for_today(reference_time=ref_time)

        assert bounds.start == datetime(2025, 3, 20, 0, 0, 0)
        assert bounds.end == datetime(2025, 3, 21, 0, 0, 0)

    def test_for_today_clears_microseconds(self):
        """Today bounds have zero microseconds."""
        ref_time = datetime(2025, 6, 15, 14, 30, 45, 123456)
        bounds = PartitionAwareDateRange.for_today(reference_time=ref_time)

        assert bounds.start.microsecond == 0
        assert bounds.end.microsecond == 0


class TestPartitionAwareDateRangeYesterday:
    """Tests for yesterday period bounds."""

    @freeze_time("2025-06-15 14:30:00")
    def test_for_yesterday_returns_previous_day_bounds(self):
        """Yesterday bounds are previous day midnight to today midnight."""
        bounds = PartitionAwareDateRange.for_yesterday()

        assert bounds.start == datetime(2025, 6, 14, 0, 0, 0)
        assert bounds.end == datetime(2025, 6, 15, 0, 0, 0)
        assert bounds.period_name == 'yesterday'

    @freeze_time("2025-01-01 08:00:00")
    def test_for_yesterday_crosses_year_boundary(self):
        """Yesterday bounds work across year boundary."""
        bounds = PartitionAwareDateRange.for_yesterday()

        assert bounds.start == datetime(2024, 12, 31, 0, 0, 0)
        assert bounds.end == datetime(2025, 1, 1, 0, 0, 0)


class TestPartitionAwareDateRangeLastWeek:
    """Tests for last_week period bounds."""

    @freeze_time("2025-06-15 14:30:00")
    def test_for_last_week_returns_7_day_bounds(self):
        """Last week bounds are 7 days ago to today midnight."""
        bounds = PartitionAwareDateRange.for_last_week()

        assert bounds.start == datetime(2025, 6, 8, 0, 0, 0)
        assert bounds.end == datetime(2025, 6, 15, 0, 0, 0)
        assert bounds.period_name == 'last_week'

    @freeze_time("2025-06-15 14:30:00")
    def test_for_last_week_spans_exactly_7_days(self):
        """Last week bounds span exactly 7 days."""
        bounds = PartitionAwareDateRange.for_last_week()

        duration = bounds.end - bounds.start
        assert duration == timedelta(days=7)


class TestPartitionAwareDateRangeLastMonth:
    """Tests for last_month period bounds."""

    @freeze_time("2025-06-15 14:30:00")
    def test_for_last_month_returns_30_day_bounds(self):
        """Last month bounds are 30 days ago to today midnight."""
        bounds = PartitionAwareDateRange.for_last_month()

        assert bounds.start == datetime(2025, 5, 16, 0, 0, 0)
        assert bounds.end == datetime(2025, 6, 15, 0, 0, 0)
        assert bounds.period_name == 'last_month'

    @freeze_time("2025-06-15 14:30:00")
    def test_for_last_month_spans_exactly_30_days(self):
        """Last month bounds span exactly 30 days."""
        bounds = PartitionAwareDateRange.for_last_month()

        duration = bounds.end - bounds.start
        assert duration == timedelta(days=30)


class TestPartitionAwareDateRangeSpecificMonth:
    """Tests for specific calendar month bounds."""

    def test_for_specific_month_january(self):
        """Specific month bounds for January."""
        bounds = PartitionAwareDateRange.for_specific_month(2025, 1)

        assert bounds.start == datetime(2025, 1, 1, 0, 0, 0)
        assert bounds.end == datetime(2025, 2, 1, 0, 0, 0)
        assert bounds.period_name == '2025-01'

    def test_for_specific_month_december(self):
        """Specific month bounds for December cross year boundary."""
        bounds = PartitionAwareDateRange.for_specific_month(2025, 12)

        assert bounds.start == datetime(2025, 12, 1, 0, 0, 0)
        assert bounds.end == datetime(2026, 1, 1, 0, 0, 0)
        assert bounds.period_name == '2025-12'

    def test_for_specific_month_february_leap_year(self):
        """Specific month bounds for February in a leap year."""
        bounds = PartitionAwareDateRange.for_specific_month(2024, 2)

        assert bounds.start == datetime(2024, 2, 1, 0, 0, 0)
        assert bounds.end == datetime(2024, 3, 1, 0, 0, 0)


class TestPartitionAwareDateRangeSpecificYear:
    """Tests for specific calendar year bounds."""

    def test_for_specific_year_2025(self):
        """Specific year bounds for 2025."""
        bounds = PartitionAwareDateRange.for_specific_year(2025)

        assert bounds.start == datetime(2025, 1, 1, 0, 0, 0)
        assert bounds.end == datetime(2026, 1, 1, 0, 0, 0)
        assert bounds.period_name == '2025'

    def test_for_specific_year_spans_365_or_366_days(self):
        """Specific year bounds span a full year."""
        # Regular year (365 days)
        bounds_2025 = PartitionAwareDateRange.for_specific_year(2025)
        duration_2025 = bounds_2025.end - bounds_2025.start
        assert duration_2025 == timedelta(days=365)

        # Leap year (366 days)
        bounds_2024 = PartitionAwareDateRange.for_specific_year(2024)
        duration_2024 = bounds_2024.end - bounds_2024.start
        assert duration_2024 == timedelta(days=366)


class TestPartitionAwareDateRangeForPeriod:
    """Tests for the for_period convenience method."""

    @freeze_time("2025-06-15 14:30:00")
    def test_for_period_today(self):
        """for_period('today') returns today bounds."""
        bounds = PartitionAwareDateRange.for_period('today')
        assert bounds.period_name == 'today'
        assert bounds.start == datetime(2025, 6, 15, 0, 0, 0)

    @freeze_time("2025-06-15 14:30:00")
    def test_for_period_live_same_as_today(self):
        """for_period('live') returns today bounds."""
        bounds = PartitionAwareDateRange.for_period('live')
        assert bounds.period_name == 'today'

    @freeze_time("2025-06-15 14:30:00")
    def test_for_period_yesterday(self):
        """for_period('yesterday') returns yesterday bounds."""
        bounds = PartitionAwareDateRange.for_period('yesterday')
        assert bounds.period_name == 'yesterday'
        assert bounds.start == datetime(2025, 6, 14, 0, 0, 0)

    @freeze_time("2025-06-15 14:30:00")
    def test_for_period_last_week(self):
        """for_period('last_week') returns last week bounds."""
        bounds = PartitionAwareDateRange.for_period('last_week')
        assert bounds.period_name == 'last_week'

    @freeze_time("2025-06-15 14:30:00")
    def test_for_period_last_month(self):
        """for_period('last_month') returns last month bounds."""
        bounds = PartitionAwareDateRange.for_period('last_month')
        assert bounds.period_name == 'last_month'

    @freeze_time("2025-06-15 14:30:00")
    def test_for_period_handles_hyphen_format(self):
        """for_period handles hyphenated period names."""
        bounds = PartitionAwareDateRange.for_period('last-week')
        assert bounds.period_name == 'last_week'

    @freeze_time("2025-06-15 14:30:00")
    def test_for_period_handles_aliases(self):
        """for_period handles common period aliases."""
        # '7d' alias for last_week
        bounds_7d = PartitionAwareDateRange.for_period('7d')
        assert bounds_7d.period_name == 'last_week'

        # '30d' alias for last_month
        bounds_30d = PartitionAwareDateRange.for_period('30d')
        assert bounds_30d.period_name == 'last_month'

    def test_for_period_raises_on_unknown_period(self):
        """for_period raises ValueError for unknown periods."""
        with pytest.raises(ValueError) as exc_info:
            PartitionAwareDateRange.for_period('unknown_period')

        assert "Unknown period" in str(exc_info.value)


class TestPartitionAwareDateRangeCustomRange:
    """Tests for custom date range bounds."""

    def test_for_custom_range(self):
        """Custom range creates bounds from provided datetimes."""
        start = datetime(2025, 3, 1, 10, 0, 0)
        end = datetime(2025, 3, 15, 18, 0, 0)

        bounds = PartitionAwareDateRange.for_custom_range(start, end)

        assert bounds.start == start
        assert bounds.end == end
        assert bounds.period_name == 'custom'

    def test_for_custom_range_with_name(self):
        """Custom range can have a custom period name."""
        start = datetime(2025, 3, 1, 0, 0, 0)
        end = datetime(2025, 3, 31, 23, 59, 59)

        bounds = PartitionAwareDateRange.for_custom_range(start, end, 'march_2025')

        assert bounds.period_name == 'march_2025'


class TestPartitionAwareDateRangeBoundsInvariant:
    """Tests that verify partition-pruning invariants are maintained."""

    @freeze_time("2025-06-15 14:30:00")
    def test_start_is_always_before_end(self):
        """All bounds have start < end."""
        for period in ['today', 'yesterday', 'last_week', 'last_month']:
            bounds = PartitionAwareDateRange.for_period(period)
            assert bounds.start < bounds.end, f"start >= end for period '{period}'"

    @freeze_time("2025-06-15 14:30:00")
    def test_bounds_use_explicit_datetimes_not_sql_functions(self):
        """Bounds are Python datetime objects, not SQLAlchemy functions."""
        bounds = PartitionAwareDateRange.for_period('yesterday')

        # These should be actual datetime objects, not SQL expressions
        assert isinstance(bounds.start, datetime)
        assert isinstance(bounds.end, datetime)

    def test_specific_month_aligns_with_partition_boundaries(self):
        """Specific month bounds align with partition boundaries (1st of month)."""
        for month in range(1, 13):
            bounds = PartitionAwareDateRange.for_specific_month(2025, month)

            # Start should be 1st of month at midnight
            assert bounds.start.day == 1
            assert bounds.start.hour == 0
            assert bounds.start.minute == 0
            assert bounds.start.second == 0

            # End should be 1st of next month at midnight
            assert bounds.end.day == 1
            assert bounds.end.hour == 0
            assert bounds.end.minute == 0
            assert bounds.end.second == 0
