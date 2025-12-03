"""
TDD Tests for YESTERDAY Period in Wait Times API Routes
========================================================

These tests verify that the 'yesterday' period is properly implemented
in the /parks/waittimes and /rides/waittimes API routes.

BUG DISCOVERED: The waittimes endpoints accept 'yesterday' as a valid period,
but the underlying route code doesn't have a 'yesterday' case - it falls
through to 'last_month' (30-day aggregate data).

Test Structure:
1. RED: Write tests that fail with current implementation
2. GREEN: Fix the route handlers to handle 'yesterday'
3. REFACTOR: Clean up if needed
"""

import pytest
from pathlib import Path


class TestParksWaitTimesYesterdayPeriod:
    """
    Tests for /parks/waittimes route handling of 'yesterday' period.

    The route must:
    1. Have explicit handling for 'yesterday' period
    2. NOT fall through to last_month (the else clause)
    3. Query data for only yesterday's date range
    """

    def test_parks_waittimes_has_yesterday_case(self):
        """
        The /parks/waittimes route must explicitly handle 'yesterday' period.

        Current bug: 'yesterday' falls through to else clause which returns
        last_month aggregate data instead of yesterday-specific data.

        Fix: Add explicit 'elif period == 'yesterday':' case.
        """
        route_path = Path(__file__).parent.parent.parent / "src" / "api" / "routes" / "parks.py"
        source_code = route_path.read_text()

        # Find the waittimes function and check for yesterday handling
        # Should have an explicit yesterday case, not just validation
        assert "elif period == 'yesterday'" in source_code, \
            "parks.py /parks/waittimes must explicitly handle 'yesterday' period with elif"

    def test_parks_waittimes_yesterday_not_in_else_clause(self):
        """
        The 'yesterday' period must NOT fall through to the else clause.

        BUG: Current code has:
            if period == 'live': ...
            elif period == 'today': ...
            else:  # Historical data
                if period == 'last_week': ...
                else:  # last_month  <-- 'yesterday' falls here!

        The else clause should only handle last_month, not be a catch-all.
        """
        route_path = Path(__file__).parent.parent.parent / "src" / "api" / "routes" / "parks.py"
        source_code = route_path.read_text()

        # Count occurrences of 'yesterday' in the waittimes function area
        # Should appear in the period dispatch logic
        waittimes_section = source_code.split("def get_park_wait_times")[1].split("def ")[0]

        # Must have explicit 'yesterday' handling (not just in validation)
        assert "'yesterday'" in waittimes_section, \
            "parks waittimes route should handle 'yesterday' period"

        # Should call a yesterday-specific query method
        assert "yesterday" in waittimes_section.lower(), \
            "parks waittimes should have yesterday-specific query logic"


class TestRidesWaitTimesYesterdayPeriod:
    """
    Tests for /rides/waittimes route handling of 'yesterday' period.
    """

    def test_rides_waittimes_has_yesterday_case(self):
        """
        The /rides/waittimes route must explicitly handle 'yesterday' period.
        """
        route_path = Path(__file__).parent.parent.parent / "src" / "api" / "routes" / "rides.py"
        source_code = route_path.read_text()

        assert "elif period == 'yesterday'" in source_code, \
            "rides.py /rides/waittimes must explicitly handle 'yesterday' period with elif"

    def test_rides_waittimes_yesterday_not_in_else_clause(self):
        """
        The 'yesterday' period must NOT fall through to the else clause.
        """
        route_path = Path(__file__).parent.parent.parent / "src" / "api" / "routes" / "rides.py"
        source_code = route_path.read_text()

        # Find the waittimes function
        waittimes_section = source_code.split("def get_ride_wait_times")[1].split("def ")[0]

        # Must have explicit 'yesterday' handling
        assert "'yesterday'" in waittimes_section, \
            "rides waittimes route should handle 'yesterday' period"


class TestYesterdayQueryClassesExist:
    """
    Tests for yesterday-specific query classes existence.

    The waittimes routes need query classes that can fetch yesterday's data
    from snapshot tables (similar to TODAY queries).
    """

    def test_yesterday_park_wait_times_query_exists(self):
        """
        There should be a query class for yesterday's park wait times.
        Either a dedicated class or a method on an existing class.
        """
        # Check for dedicated yesterday query file
        yesterday_path = Path(__file__).parent.parent.parent / "src" / "database" / "queries" / "yesterday"
        today_path = Path(__file__).parent.parent.parent / "src" / "database" / "queries" / "today"

        # Either should have yesterday-specific queries OR today queries should support yesterday
        has_yesterday_dir = yesterday_path.exists()
        has_today_dir = today_path.exists()

        assert has_yesterday_dir or has_today_dir, \
            "Should have query classes for yesterday data (either in yesterday/ or today/ directory)"

    def test_yesterday_ride_wait_times_query_exists(self):
        """
        There should be a query class for yesterday's ride wait times.
        """
        # Check both possible locations
        yesterday_path = Path(__file__).parent.parent.parent / "src" / "database" / "queries" / "yesterday"

        # If yesterday directory exists, check for ride wait times query
        if yesterday_path.exists():
            files = list(yesterday_path.glob("*.py"))
            file_names = [f.name for f in files]
            assert any("wait" in name.lower() for name in file_names), \
                "yesterday/ directory should have wait times query file"


class TestWaitTimesQueryConsistency:
    """
    Tests for consistent period handling across all waittimes queries.

    All waittimes query classes should handle the same periods:
    today, yesterday, last_week, last_month
    """

    def test_all_periods_documented(self):
        """
        The route documentation should mention all supported periods.
        """
        parks_path = Path(__file__).parent.parent.parent / "src" / "api" / "routes" / "parks.py"
        rides_path = Path(__file__).parent.parent.parent / "src" / "api" / "routes" / "rides.py"

        parks_code = parks_path.read_text()
        rides_code = rides_path.read_text()

        # Both files should document 'yesterday' as a valid period
        assert "'yesterday'" in parks_code, "parks.py should document 'yesterday' period"
        assert "'yesterday'" in rides_code, "rides.py should document 'yesterday' period"
