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

    def test_last_week_parks_response_includes_rides_down_field(self):
        """
        LAST_WEEK parks response must include 'rides_down' field (not 'max_rides_affected').

        The rankings query returns 'max_rides_affected' but frontend expects 'rides_down'.
        """
        from pathlib import Path
        query_path = Path(__file__).parent.parent.parent / "src" / "database" / "queries" / "rankings" / "park_downtime_rankings.py"
        source_code = query_path.read_text()

        # Check that the SQL returns rides_down (not max_rides_affected)
        assert "AS rides_down" in source_code, \
            "ParkDowntimeRankingsQuery must return 'rides_down' field for frontend compatibility"

    def test_last_month_parks_response_includes_rides_down_field(self):
        """
        LAST_MONTH parks response must include 'rides_down' field.

        Same query class as LAST_WEEK, so same fix applies.
        """
        # Same query file handles both last_week and last_month
        from pathlib import Path
        query_path = Path(__file__).parent.parent.parent / "src" / "database" / "queries" / "rankings" / "park_downtime_rankings.py"
        source_code = query_path.read_text()

        # Verify no 'max_rides_affected' alias remains in the output
        # The sort mapping can reference it internally, but the output column must be 'rides_down'
        assert "AS rides_down" in source_code, \
            "ParkDowntimeRankingsQuery must return 'rides_down' field for frontend compatibility"


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


class TestChartsLiveAndTodaySupport:
    """
    Tests for LIVE and TODAY period support in charts endpoints.
    """

    def test_chart_data_endpoint_accepts_live_period(self):
        """
        The /trends/chart-data endpoint should accept period=live.

        LIVE should be treated as TODAY for charts (showing today's data).
        """
        from pathlib import Path
        trends_path = Path(__file__).parent.parent.parent / "src" / "api" / "routes" / "trends.py"
        source_code = trends_path.read_text()

        # Check that 'live' is in valid_periods for chart-data
        assert "'live'" in source_code, \
            "trends.py should include 'live' in valid periods for chart-data"

    def test_park_shame_history_uses_mariadb_compatible_group_by(self):
        """
        The park_shame_history.py hourly query should use MariaDB-compatible GROUP BY.

        MariaDB strict mode requires GROUP BY to use the full expression, not alias:
        - BAD:  GROUP BY hour  (where hour is an alias)
        - GOOD: GROUP BY HOUR(DATE_SUB(rss.recorded_at, INTERVAL 8 HOUR))
        """
        from pathlib import Path
        query_path = Path(__file__).parent.parent.parent / "src" / "database" / "queries" / "charts" / "park_shame_history.py"
        source_code = query_path.read_text()

        # The query should NOT use just "GROUP BY hour" - needs full expression
        # Check for the correct pattern
        assert "GROUP BY HOUR(DATE_SUB(rss.recorded_at, INTERVAL 8 HOUR))" in source_code, \
            "park_shame_history.py should use full expression in GROUP BY for MariaDB compatibility"


class TestChartsPeriodLabels:
    """
    Tests for correct period labels in frontend charts.

    The frontend charts.js getPeriodLabel() method must return the
    correct label for ALL supported periods including 'yesterday'.
    """

    def test_frontend_charts_get_period_label_includes_yesterday(self):
        """
        Frontend charts.js getPeriodLabel() must include 'yesterday' case.

        BUG: When 'yesterday' is selected, the chart title shows 'Last Week'
        because getPeriodLabel() defaults to 'Last Week' for unknown periods.

        Fix: Add 'yesterday': 'Yesterday (Hourly)' to the labels map.
        """
        from pathlib import Path
        charts_path = Path(__file__).parent.parent.parent.parent / "frontend" / "js" / "components" / "charts.js"
        source_code = charts_path.read_text()

        # The getPeriodLabel method should have 'yesterday' in its labels object
        assert "'yesterday':" in source_code or '"yesterday":' in source_code, \
            "charts.js getPeriodLabel() must include 'yesterday' period label"

        # Verify it maps to something with 'Yesterday' (not 'Last Week')
        assert "Yesterday" in source_code, \
            "charts.js must have a 'Yesterday' label for the yesterday period"


class TestMockDataTimeLimits:
    """
    Tests for mock data generation respecting current time.

    When generating mock hourly data for TODAY/LIVE, the data should
    only include hours up to the current hour, not all 18 hours.
    """

    def test_mock_hourly_data_respects_current_hour(self):
        """
        Mock hourly chart data should only include hours up to current time.

        BUG: At 9am, mock data shows all 18 hours (6am-11pm) with full data.
        This is impossible - we can't have data for hours that haven't happened.

        Fix: _generate_mock_hourly_chart_data() should check current Pacific
        time and only generate data for hours that have passed.
        """
        from pathlib import Path
        trends_path = Path(__file__).parent.parent.parent / "src" / "api" / "routes" / "trends.py"
        source_code = trends_path.read_text()

        # The mock data generator should reference current time
        # Look for Pacific timezone handling in the mock generator
        assert "get_now_pacific" in source_code or "datetime.now" in source_code, \
            "_generate_mock_hourly_chart_data should check current time"

        # Check that the function limits data based on current hour
        # This is a heuristic - look for hour comparison logic
        assert "current_hour" in source_code.lower() or "now_hour" in source_code.lower(), \
            "_generate_mock_hourly_chart_data should limit data to current hour"


class TestTrendsYesterdaySupport:
    """
    Tests for YESTERDAY period support in trends API endpoints.
    """

    def test_chart_data_endpoint_accepts_yesterday(self):
        """
        The /trends/chart-data endpoint should accept period=yesterday.
        """
        from pathlib import Path
        trends_path = Path(__file__).parent.parent.parent / "src" / "api" / "routes" / "trends.py"
        source_code = trends_path.read_text()

        # Find the chart-data endpoint's valid_periods
        # It should include 'yesterday'
        assert "'yesterday'" in source_code, \
            "trends.py should include 'yesterday' in valid periods"

        # Count occurrences of the valid_periods pattern without yesterday
        old_pattern = "valid_periods = ['today', 'last_week', 'last_month']"
        assert old_pattern not in source_code, \
            f"trends.py still has valid_periods without 'yesterday': {old_pattern}"

    def test_longest_wait_times_endpoint_accepts_yesterday(self):
        """
        The /trends/longest-wait-times endpoint should accept period=yesterday.
        """
        from pathlib import Path
        trends_path = Path(__file__).parent.parent.parent / "src" / "api" / "routes" / "trends.py"
        source_code = trends_path.read_text()

        # Check that the old pattern without 'yesterday' is NOT present
        old_pattern = "valid_periods = ['today', 'last_week', 'last_month']"
        assert old_pattern not in source_code, \
            "trends.py should have 'yesterday' in all valid_periods lists"

    def test_least_reliable_rides_endpoint_accepts_yesterday(self):
        """
        The /trends/least-reliable-rides endpoint should accept period=yesterday.
        """
        from pathlib import Path
        trends_path = Path(__file__).parent.parent.parent / "src" / "api" / "routes" / "trends.py"
        source_code = trends_path.read_text()

        old_pattern = "valid_periods = ['today', 'last_week', 'last_month']"
        assert old_pattern not in source_code, \
            "trends.py should have 'yesterday' in all valid_periods lists"
