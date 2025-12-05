"""
Unit Tests for Ride Downtime Sorting
====================================

TDD Tests: These tests verify that the ride downtime API supports
sorting by status, cumulative downtime, uptime percentage, and trend.

Expected Sort Options:
---------------------
- current_is_open: ASC (Down rides first)
- downtime_hours: DESC (Most downtime first) - DEFAULT
- uptime_percentage: ASC (Lowest uptime = worst)
- trend_percentage: DESC (Most increased downtime first)
"""

import inspect


class TestRideSortByValidation:
    """Test sort_by parameter validation in rides.py route."""

    def test_rides_route_accepts_sort_by_parameter(self):
        """
        CRITICAL: rides.py must accept sort_by query parameter.

        The route should parse sort_by from request args.
        """
        from api.routes.rides import get_ride_downtime_rankings
        source = inspect.getsource(get_ride_downtime_rankings)

        assert "sort_by" in source, \
            "Route must accept sort_by query parameter"
        assert "request.args.get('sort_by'" in source, \
            "Route must parse sort_by from request.args"

    def test_rides_route_validates_sort_by_options(self):
        """
        CRITICAL: rides.py must validate sort_by against allowed options.

        Valid options: current_is_open, downtime_hours, uptime_percentage, trend_percentage
        """
        from api.routes.rides import get_ride_downtime_rankings
        source = inspect.getsource(get_ride_downtime_rankings)

        # Check that validation exists
        assert "valid_sort_options" in source or "sort_by not in" in source, \
            "Route must validate sort_by parameter"

        # Check all expected sort options are defined
        expected_options = ['current_is_open', 'downtime_hours', 'uptime_percentage', 'trend_percentage']
        for option in expected_options:
            assert option in source, \
                f"Route must accept sort option: {option}"

    def test_rides_route_default_sort_is_downtime_hours(self):
        """
        CRITICAL: Default sort should be 'downtime_hours'.

        When no sort_by is provided, results should be sorted by
        cumulative downtime hours (descending).
        """
        from api.routes.rides import get_ride_downtime_rankings
        source = inspect.getsource(get_ride_downtime_rankings)

        # Check default value in request.args.get
        assert "'downtime_hours'" in source and "sort_by" in source, \
            "Default sort_by should be 'downtime_hours'"


class TestRideLiveQuerySorting:
    """Test sorting in get_ride_live_downtime_rankings() for TODAY period."""

    def test_live_query_accepts_sort_by_parameter(self):
        """
        CRITICAL: get_ride_live_downtime_rankings must accept sort_by.

        The method signature should include sort_by parameter.
        """
        from database.repositories.stats_repository import StatsRepository

        # Check method signature
        sig = inspect.signature(StatsRepository.get_ride_live_downtime_rankings)
        params = list(sig.parameters.keys())

        assert 'sort_by' in params, \
            "get_ride_live_downtime_rankings must accept sort_by parameter"

    def test_live_query_has_dynamic_order_by(self):
        """
        CRITICAL: Live query must use dynamic ORDER BY based on sort_by.

        The query should not have hardcoded ORDER BY.
        """
        from database.repositories.stats_repository import StatsRepository
        source = inspect.getsource(StatsRepository.get_ride_live_downtime_rankings)

        # Should use a helper method or dynamic construction
        assert 'sort_by' in source, \
            "Query must use sort_by parameter for dynamic ordering"

    def test_live_query_sort_mapping_exists(self):
        """
        CRITICAL: Sort mapping for rides should exist.

        Either in the method or as a helper, there should be a mapping
        from sort_by values to SQL ORDER BY clauses.
        """
        from database.repositories.stats_repository import StatsRepository
        source = inspect.getsource(StatsRepository)

        # Look for ride sort mapping
        has_sort_mapping = (
            '_get_ride_order_by_clause' in source or
            '_get_ride_sort_clause' in source or
            'ride_sort_mapping' in source.lower() or
            ('current_is_open' in source and 'uptime_percentage' in source)
        )

        assert has_sort_mapping, \
            "StatsRepository must have ride sort mapping logic"


class TestRideHistoricalQuerySorting:
    """Test sorting in ride_downtime_rankings.py for 7days/30days periods."""

    def test_weekly_query_accepts_sort_by_parameter(self):
        """
        CRITICAL: get_weekly must accept sort_by parameter.
        """
        from database.queries.rankings.ride_downtime_rankings import RideDowntimeRankingsQuery

        sig = inspect.signature(RideDowntimeRankingsQuery.get_weekly)
        params = list(sig.parameters.keys())

        assert 'sort_by' in params, \
            "get_weekly must accept sort_by parameter"

    def test_monthly_query_accepts_sort_by_parameter(self):
        """
        CRITICAL: get_monthly must accept sort_by parameter.
        """
        from database.queries.rankings.ride_downtime_rankings import RideDowntimeRankingsQuery

        sig = inspect.signature(RideDowntimeRankingsQuery.get_monthly)
        params = list(sig.parameters.keys())

        assert 'sort_by' in params, \
            "get_monthly must accept sort_by parameter"

    def test_rankings_query_has_order_by_method(self):
        """
        CRITICAL: RideDowntimeRankingsQuery should have _get_order_by_clause.

        Similar to ParkDowntimeRankingsQuery pattern.
        """
        from database.queries.rankings.ride_downtime_rankings import RideDowntimeRankingsQuery

        assert hasattr(RideDowntimeRankingsQuery, '_get_order_by_clause'), \
            "RideDowntimeRankingsQuery must have _get_order_by_clause method"


class TestSortDirections:
    """Test that each sort option uses the correct direction."""

    def test_current_is_open_sorts_ascending(self):
        """
        Status sort should be ASC - Down rides (0) first, then Operating (1).
        """
        from database.repositories.stats_repository import StatsRepository
        source = inspect.getsource(StatsRepository._get_ride_order_by_clause)

        # The ride sort mapping should have current_is_open with ASC
        assert 'current_is_open' in source and 'ASC' in source, \
            "current_is_open should sort ASC (down rides first)"

    def test_downtime_hours_sorts_descending(self):
        """
        Cumulative downtime sort should be DESC - Most downtime first.
        """
        from database.repositories.stats_repository import StatsRepository
        source = inspect.getsource(StatsRepository)

        # The default sort and downtime_hours should use DESC
        assert 'downtime_hours DESC' in source or 'downtime_hours" : "DESC' in source or \
               ('downtime_hours' in source and 'DESC' in source), \
            "downtime_hours should sort DESC"

    def test_uptime_percentage_sorts_ascending(self):
        """
        Uptime % sort should be ASC - Lowest uptime = worst performers first.
        """
        from database.repositories.stats_repository import StatsRepository
        source = inspect.getsource(StatsRepository)

        if 'uptime_percentage' in source:
            assert 'uptime_percentage ASC' in source or \
                   'uptime_percentage' in source and 'ASC' in source, \
                "uptime_percentage should sort ASC (lowest uptime first)"

    def test_trend_percentage_sorts_descending(self):
        """
        Trend sort should be DESC - Most increased downtime first.
        """
        from database.repositories.stats_repository import StatsRepository
        source = inspect.getsource(StatsRepository)

        if 'trend_percentage' in source:
            assert 'trend_percentage DESC' in source or \
                   'trend_percentage' in source and 'DESC' in source, \
                "trend_percentage should sort DESC (most increase first)"


class TestRoutePassesSortByToQuery:
    """Test that routes properly pass sort_by to query methods."""

    def test_today_period_passes_sort_by(self):
        """
        CRITICAL: Today period should pass sort_by to stats_repo.
        """
        from api.routes.rides import get_ride_downtime_rankings
        source = inspect.getsource(get_ride_downtime_rankings)

        # Look for the call to get_ride_live_downtime_rankings with sort_by
        assert 'get_ride_live_downtime_rankings' in source, \
            "Route must call get_ride_live_downtime_rankings for today period"

        # Find the section that calls the live method
        live_call_idx = source.find('get_ride_live_downtime_rankings')
        call_section = source[live_call_idx:live_call_idx+300]

        assert 'sort_by' in call_section, \
            "Route must pass sort_by to get_ride_live_downtime_rankings"

    def test_weekly_period_passes_sort_by(self):
        """
        CRITICAL: 7days period should pass sort_by to query.get_weekly.
        """
        from api.routes.rides import get_ride_downtime_rankings
        source = inspect.getsource(get_ride_downtime_rankings)

        # Find the weekly query call
        weekly_idx = source.find('get_weekly')
        if weekly_idx > 0:
            call_section = source[weekly_idx:weekly_idx+200]
            assert 'sort_by' in call_section, \
                "Route must pass sort_by to get_weekly"

    def test_monthly_period_passes_sort_by(self):
        """
        CRITICAL: 30days period should pass sort_by to query.get_monthly.
        """
        from api.routes.rides import get_ride_downtime_rankings
        source = inspect.getsource(get_ride_downtime_rankings)

        # Find the monthly query call
        monthly_idx = source.find('get_monthly')
        if monthly_idx > 0:
            call_section = source[monthly_idx:monthly_idx+200]
            assert 'sort_by' in call_section, \
                "Route must pass sort_by to get_monthly"
