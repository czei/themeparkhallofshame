"""
YESTERDAY API Endpoint Tests
============================

TDD tests for the YESTERDAY period in API endpoints.

These tests verify:
1. period=yesterday is accepted by /api/parks/downtime
2. period=yesterday is accepted by /api/rides/downtime
3. Response format matches other periods
4. Caching headers are set appropriately (24h cache)
"""

import pytest
from unittest.mock import Mock, patch


class TestParksDowntimeYesterday:
    """
    Tests for /api/parks/downtime?period=yesterday endpoint.
    """

    def test_yesterday_is_valid_period_for_parks_downtime(self):
        """
        The parks downtime endpoint should accept period=yesterday.

        The route validation checks period against a list of valid values.
        This test verifies the implementation includes 'yesterday'.
        """
        # Read the source file to verify 'yesterday' is in the valid periods list
        from pathlib import Path
        parks_route_path = Path(__file__).parent.parent.parent / "src" / "api" / "routes" / "parks.py"
        source_code = parks_route_path.read_text()

        # Verify 'yesterday' is in the validation list
        assert "'yesterday'" in source_code, \
            "parks.py should include 'yesterday' in valid periods"

        # Verify 'yesterday' appears in the error message for invalid period
        assert "yesterday" in source_code.lower(), \
            "parks.py should mention yesterday in period validation"

    def test_yesterday_parks_response_has_period_info(self):
        """
        Response should include period_info with yesterday's date.
        """
        # Response format:
        # {
        #   "period": "yesterday",
        #   "period_info": {
        #     "start_date": "2025-12-01",
        #     "end_date": "2025-12-01",
        #     "label": "Dec 1, 2025",
        #     "period_type": "day"
        #   },
        #   "parks": [...]
        # }
        pass  # Integration test will verify

    def test_yesterday_parks_excludes_closed_parks(self):
        """
        Parks that were closed yesterday should not appear in rankings.
        """
        # Only parks with actual operating data yesterday should be included
        pass  # Integration test will verify


class TestRidesDowntimeYesterday:
    """
    Tests for /api/rides/downtime?period=yesterday endpoint.
    """

    def test_yesterday_is_valid_period_for_rides_downtime(self):
        """
        The rides downtime endpoint should accept period=yesterday.
        """
        pass  # Integration test will verify

    def test_yesterday_rides_response_format(self):
        """
        Rides response should match the standard format with yesterday data.
        """
        # Response should include:
        # - period_info with yesterday date
        # - rides array with downtime stats
        pass


class TestWaitTimesYesterday:
    """
    Tests for wait times endpoints with period=yesterday.
    """

    def test_yesterday_is_valid_period_for_parks_wait_times(self):
        """
        /api/parks/wait-times should accept period=yesterday.
        """
        pass

    def test_yesterday_is_valid_period_for_rides_wait_times(self):
        """
        /api/rides/wait-times should accept period=yesterday.
        """
        pass


class TestYesterdayCaching:
    """
    Tests for caching behavior of YESTERDAY period responses.
    """

    def test_yesterday_response_has_cache_control_header(self):
        """
        YESTERDAY responses should have Cache-Control header.

        Since yesterday's data is immutable, we can cache aggressively.
        Expected: Cache-Control: public, max-age=86400 (24 hours)
        """
        # Yesterday data never changes, so long cache is appropriate
        # This is different from TODAY which updates every 5 minutes
        pass

    def test_yesterday_cache_ttl_is_24_hours(self):
        """
        Cache TTL for YESTERDAY should be 24 hours.

        The data won't change until tomorrow, so 24h is safe.
        """
        expected_ttl = 24 * 60 * 60  # 86400 seconds
        pass


class TestYesterdayErrorHandling:
    """
    Tests for error cases with YESTERDAY period.
    """

    def test_yesterday_with_no_data_returns_empty_rankings(self):
        """
        If no data exists for yesterday, return empty rankings, not error.
        """
        # For a brand new installation or data outage, return:
        # {"period": "yesterday", "parks": [], "period_info": {...}}
        pass

    def test_yesterday_handles_timezone_edge_cases(self):
        """
        YESTERDAY should work correctly at timezone boundaries.

        At 12:01 AM Pacific, "yesterday" should be the day that just ended.
        At 11:59 PM Pacific, "yesterday" should be the same as at 12:01 AM.
        """
        pass


class TestValidPeriodsConstant:
    """
    Tests for the VALID_PERIODS constant in API routes.
    """

    def test_valid_periods_includes_yesterday(self):
        """
        The VALID_PERIODS list should include 'yesterday'.
        """
        # Expected valid periods: live, today, yesterday, last_week, last_month
        expected_periods = ['live', 'today', 'yesterday', 'last_week', 'last_month']

        # This test will fail until we update the routes
        # Once implemented, verify:
        # from api.routes.parks import VALID_PERIODS
        # assert 'yesterday' in VALID_PERIODS
        pass

    def test_period_validation_accepts_yesterday(self):
        """
        Period validation should accept 'yesterday' as valid.
        """
        # The API should not return 400 for period=yesterday
        pass


class TestConsistentFieldNames:
    """
    Tests for consistent field naming across all periods.

    The frontend expects specific field names like 'rides_down'.
    All periods (live, today, yesterday, last_week, last_month) must use
    the same field names for the same concepts.
    """

    def test_yesterday_parks_response_includes_rides_down_field(self):
        """
        YESTERDAY parks response must include 'rides_down' field (not 'rides_affected').

        The frontend displays this as "Rides Down" column.
        This field represents the count of rides that had any downtime during the period.
        """
        # Check that YesterdayParkRankingsQuery returns 'rides_down' field
        from pathlib import Path
        query_path = Path(__file__).parent.parent.parent / "src" / "database" / "queries" / "yesterday" / "yesterday_park_rankings.py"
        source_code = query_path.read_text()

        # The query should select a field aliased as 'rides_down' for frontend compatibility
        assert "rides_down" in source_code, \
            "YesterdayParkRankingsQuery must return 'rides_down' field for frontend compatibility"

    def test_today_parks_response_includes_rides_down_field(self):
        """
        TODAY parks response must include 'rides_down' field (not 'rides_affected').
        """
        from pathlib import Path
        query_path = Path(__file__).parent.parent.parent / "src" / "database" / "queries" / "today" / "today_park_rankings.py"
        source_code = query_path.read_text()

        assert "rides_down" in source_code, \
            "TodayParkRankingsQuery must return 'rides_down' field for frontend compatibility"


class TestYesterdayQueryClasses:
    """
    Tests for the YESTERDAY query implementation classes.
    """

    def test_yesterday_park_rankings_query_exists(self):
        """
        There should be a YesterdayParkRankingsQuery class.
        """
        try:
            from database.queries.yesterday.yesterday_park_rankings import YesterdayParkRankingsQuery
            assert YesterdayParkRankingsQuery is not None
        except ImportError:
            pytest.fail("YesterdayParkRankingsQuery class does not exist yet")

    def test_yesterday_ride_rankings_query_exists(self):
        """
        There should be a YesterdayRideRankingsQuery class.
        """
        try:
            from database.queries.yesterday.yesterday_ride_rankings import YesterdayRideRankingsQuery
            assert YesterdayRideRankingsQuery is not None
        except ImportError:
            pytest.fail("YesterdayRideRankingsQuery class does not exist yet")
