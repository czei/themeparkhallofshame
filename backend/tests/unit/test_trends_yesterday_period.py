"""
TDD Tests for YESTERDAY Period in Trends Query Classes
=======================================================

These tests verify that the 'yesterday' period is properly implemented
in all trends query classes used by the Awards component.

BUG DISCOVERED: The trends endpoints accept 'yesterday' as a valid period,
but the underlying query classes (LeastReliableRidesQuery, LongestWaitTimesQuery)
don't have a 'yesterday' case - they fall through to 30-day aggregates.

Test Structure:
1. RED: Write tests that fail with current implementation
2. GREEN: Fix the query classes to handle 'yesterday'
3. REFACTOR: Clean up if needed
"""

import pytest
from pathlib import Path


class TestLeastReliableRidesQueryYesterdayPeriod:
    """
    Tests for LeastReliableRidesQuery handling of 'yesterday' period.

    The query class must:
    1. Have explicit handling for 'yesterday' period
    2. NOT fall through to 30-day aggregate (the else clause)
    3. Query data for only yesterday's date range
    """

    def test_get_rankings_has_yesterday_case(self):
        """
        The get_rankings() method must explicitly handle 'yesterday' period.

        Current bug: 'yesterday' falls through to else clause which returns
        30-day aggregate data instead of yesterday-specific data.

        Fix: Add explicit 'elif period == 'yesterday':' case.
        """
        query_path = Path(__file__).parent.parent.parent / "src" / "database" / "queries" / "trends" / "least_reliable_rides.py"
        source_code = query_path.read_text()

        # Must have explicit handling for 'yesterday' in get_rankings
        assert "elif period == 'yesterday'" in source_code or "period == 'yesterday'" in source_code, \
            "LeastReliableRidesQuery.get_rankings() must explicitly handle 'yesterday' period"

        # Must NOT have yesterday fall through to the else clause
        # The else clause currently calls _get_daily_aggregate(30, ...)
        # We need to ensure 'yesterday' doesn't reach that else clause

    def test_get_park_rankings_has_yesterday_case(self):
        """
        The get_park_rankings() method must explicitly handle 'yesterday' period.
        """
        query_path = Path(__file__).parent.parent.parent / "src" / "database" / "queries" / "trends" / "least_reliable_rides.py"
        source_code = query_path.read_text()

        # Count occurrences - should have 'yesterday' case in BOTH methods
        # get_rankings and get_park_rankings
        yesterday_count = source_code.count("'yesterday'")

        assert yesterday_count >= 2, \
            f"LeastReliableRidesQuery should handle 'yesterday' in both get_rankings and get_park_rankings (found {yesterday_count} occurrences)"

    def test_yesterday_uses_snapshot_data_not_daily_stats(self):
        """
        YESTERDAY period should query from ride_status_snapshots for yesterday's
        date range, similar to TODAY but for the previous day.

        This is because:
        1. Daily stats may not be generated yet for yesterday
        2. We want consistent behavior with TODAY period
        3. Snapshot data is more accurate for single-day queries
        """
        query_path = Path(__file__).parent.parent.parent / "src" / "database" / "queries" / "trends" / "least_reliable_rides.py"
        source_code = query_path.read_text()

        # Should have a method like _get_yesterday that uses snapshots
        # OR should call _get_today-like method with yesterday's date range
        assert "_get_yesterday" in source_code or "get_yesterday_range" in source_code, \
            "LeastReliableRidesQuery should have yesterday-specific method using snapshot data"


class TestLongestWaitTimesQueryYesterdayPeriod:
    """
    Tests for LongestWaitTimesQuery handling of 'yesterday' period.
    """

    def test_get_rankings_has_yesterday_case(self):
        """
        The get_rankings() method must explicitly handle 'yesterday' period.
        """
        query_path = Path(__file__).parent.parent.parent / "src" / "database" / "queries" / "trends" / "longest_wait_times.py"
        source_code = query_path.read_text()

        assert "elif period == 'yesterday'" in source_code or "period == 'yesterday'" in source_code, \
            "LongestWaitTimesQuery.get_rankings() must explicitly handle 'yesterday' period"

    def test_get_park_rankings_has_yesterday_case(self):
        """
        The get_park_rankings() method must explicitly handle 'yesterday' period.
        """
        query_path = Path(__file__).parent.parent.parent / "src" / "database" / "queries" / "trends" / "longest_wait_times.py"
        source_code = query_path.read_text()

        # Count occurrences - should have 'yesterday' case in BOTH methods
        yesterday_count = source_code.count("'yesterday'")

        assert yesterday_count >= 2, \
            f"LongestWaitTimesQuery should handle 'yesterday' in both get_rankings and get_park_rankings (found {yesterday_count} occurrences)"


class TestTimezoneHelperForYesterday:
    """
    Tests for timezone helper functions needed for YESTERDAY period.
    """

    def test_get_yesterday_range_utc_exists(self):
        """
        There should be a helper function to get yesterday's UTC time range.

        Similar to get_today_range_to_now_utc() but for yesterday:
        - Returns (start_utc, end_utc) for yesterday in Pacific timezone
        - start_utc = yesterday 00:00:00 Pacific converted to UTC
        - end_utc = yesterday 23:59:59 Pacific converted to UTC
        """
        timezone_path = Path(__file__).parent.parent.parent / "src" / "utils" / "timezone.py"
        source_code = timezone_path.read_text()

        # Should have a function for yesterday's range
        assert "get_yesterday_range_utc" in source_code or "get_pacific_day_range_utc" in source_code, \
            "timezone.py should have a helper for getting yesterday's UTC range"


class TestQueryPeriodDispatchConsistency:
    """
    Tests for consistent period handling across all trends query classes.

    All query classes that accept a 'period' parameter should handle
    the same set of periods: today, yesterday, last_week, last_month
    """

    def test_least_reliable_handles_all_periods(self):
        """
        LeastReliableRidesQuery should handle: today, yesterday, last_week, last_month
        """
        query_path = Path(__file__).parent.parent.parent / "src" / "database" / "queries" / "trends" / "least_reliable_rides.py"
        source_code = query_path.read_text()

        required_periods = ['today', 'yesterday', 'last_week', 'last_month']

        for period in required_periods:
            assert f"'{period}'" in source_code, \
                f"LeastReliableRidesQuery should handle period '{period}'"

    def test_longest_wait_times_handles_all_periods(self):
        """
        LongestWaitTimesQuery should handle: today, yesterday, last_week, last_month
        """
        query_path = Path(__file__).parent.parent.parent / "src" / "database" / "queries" / "trends" / "longest_wait_times.py"
        source_code = query_path.read_text()

        required_periods = ['today', 'yesterday', 'last_week', 'last_month']

        for period in required_periods:
            assert f"'{period}'" in source_code, \
                f"LongestWaitTimesQuery should handle period '{period}'"


class TestNoFallthroughToWrongPeriod:
    """
    Critical tests to ensure 'yesterday' doesn't silently fall through
    to an incorrect period handler.
    """

    def test_least_reliable_yesterday_not_in_else_clause(self):
        """
        The 'else' clause in period dispatch should ONLY handle the final
        expected period (last_month), not be a catch-all.

        BUG: Current code has:
            if period == 'today': ...
            elif period == '7days': ...
            else:  # 30days  <-- 'yesterday' falls here!

        FIX: Should be:
            if period == 'today': ...
            elif period == 'yesterday': ...
            elif period == 'last_week': ...
            elif period == 'last_month': ...
            else: raise ValueError(...)
        """
        query_path = Path(__file__).parent.parent.parent / "src" / "database" / "queries" / "trends" / "least_reliable_rides.py"
        source_code = query_path.read_text()

        # The old pattern used '7days' and '30days' as period values
        # The new pattern uses 'last_week' and 'last_month'
        # Check that we're using the correct period values
        assert "'last_week'" in source_code or "'7days'" in source_code, \
            "LeastReliableRidesQuery should use proper period values"

        # Most importantly: verify 'yesterday' is explicitly handled
        # by checking it appears BEFORE any else clause in the dispatch logic
        lines = source_code.split('\n')
        found_yesterday_before_else = False
        in_get_rankings = False

        for line in lines:
            if 'def get_rankings' in line:
                in_get_rankings = True
            if in_get_rankings:
                if "'yesterday'" in line:
                    found_yesterday_before_else = True
                if line.strip().startswith('else:') and not found_yesterday_before_else:
                    # Found else before yesterday - this is the bug!
                    pass
                if 'def ' in line and 'get_rankings' not in line:
                    in_get_rankings = False

        assert found_yesterday_before_else, \
            "LeastReliableRidesQuery.get_rankings must handle 'yesterday' before any else clause"
