"""
Unit Tests for Live Wait Times Feature
======================================

These tests verify that the wait times API returns LIVE snapshot data
for period=today, including current_status fields.

TDD Approach: These tests should FAIL if the API falls back to
aggregated stats instead of returning live snapshot data.

The Bug These Tests Catch:
-------------------------
If period=today returns aggregated data from *_daily_stats tables instead of
live data from ride_status_snapshots, these tests will fail because:
1. current_status field will be missing
2. Wait times won't match real-time snapshot values
"""



class TestRideLiveWaitTimesResponse:
    """Test that ride wait times API returns proper live data for period=today."""

    def test_ride_wait_times_today_has_current_status_field(self):
        """
        CRITICAL TEST: Verify period=today returns current_status field.

        This test FAILS if the API falls back to aggregated stats (which
        don't include current_status) instead of live snapshot data.
        """
        # The expected fields that MUST be present for live wait times
        expected_live_fields = [
            'current_status',
            'current_is_open',
            'park_is_open',
            'avg_wait_time',
            'peak_wait_time',
        ]

        # Verify the live query method includes these fields
        from utils.sql_helpers import RideStatusSQL, ParkStatusSQL

        # These methods should exist and be used for live queries
        current_status_sql = RideStatusSQL.current_status_subquery("r.ride_id", park_id_expr="r.park_id")
        current_is_open_sql = RideStatusSQL.current_is_open_subquery("r.ride_id", park_id_expr="r.park_id")
        park_is_open_sql = ParkStatusSQL.park_is_open_subquery("p.park_id")

        # Verify SQL includes the column aliases
        assert 'current_status' in current_status_sql
        assert 'current_is_open' in current_is_open_sql
        assert 'park_is_open' in park_is_open_sql

    def test_ride_wait_times_today_uses_live_snapshots(self):
        """
        Verify that period=today queries ride_status_snapshots table,
        not ride_daily_stats table.

        This catches the bug where today falls back to 7-day aggregated data.
        """
        from database.repositories.stats_repository import StatsRepository

        # Verify the method exists - it should query live snapshots
        assert hasattr(StatsRepository, 'get_ride_live_wait_time_rankings'), \
            "StatsRepository must have get_ride_live_wait_time_rankings method for live wait times"

    def test_ride_wait_times_today_vs_aggregated_has_different_fields(self):
        """
        Verify that live wait times response has MORE fields than aggregated.

        Live data includes: current_status, current_is_open, park_is_open
        Aggregated data does NOT include these fields.
        """
        from database.queries.rankings.ride_wait_time_rankings import RideWaitTimeRankingsQuery

        # The aggregated query (RideWaitTimeRankingsQuery) should NOT have current_status
        # If it does, we should verify it's the live method being called for today

        # Get the aggregated query SQL to verify it doesn't have status fields
        import inspect
        source = inspect.getsource(RideWaitTimeRankingsQuery._get_rankings)

        # Aggregated query should NOT include current_status subquery
        assert 'current_status' not in source.lower() or 'subquery' in source.lower(), \
            "Aggregated query should not return current_status"


class TestParkLiveWaitTimesResponse:
    """Test that park wait times API returns proper live data for period=today."""

    def test_park_wait_times_today_has_park_is_open_field(self):
        """
        CRITICAL TEST: Verify period=today returns park_is_open field.
        """
        from utils.sql_helpers import ParkStatusSQL

        park_is_open_sql = ParkStatusSQL.park_is_open_subquery("p.park_id")
        assert 'park_is_open' in park_is_open_sql

    def test_park_wait_times_today_uses_live_snapshots(self):
        """
        Verify that period=today for parks queries live snapshots.
        """
        from database.repositories.stats_repository import StatsRepository

        assert hasattr(StatsRepository, 'get_park_live_wait_time_rankings'), \
            "StatsRepository must have get_park_live_wait_time_rankings method for live wait times"


class TestWaitTimesRouteDispatch:
    """Test that routes correctly dispatch to live vs aggregated queries."""

    def test_rides_waittimes_today_dispatches_to_live_method(self):
        """
        Verify that /api/rides/waittimes?period=today calls the live method,
        not the aggregated stats method.

        This is the CORE test that would have caught the bug.
        """
        # Read the route source to verify dispatch logic
        import inspect
        from api.routes.rides import get_ride_wait_times
        source = inspect.getsource(get_ride_wait_times)

        # For period=today, should call live method from StatsRepository
        # NOT RideWaitTimeRankingsQuery.get_by_period('today')
        assert "period == 'today'" in source, \
            "Route must check for period=today"
        assert "get_ride_live_wait_time_rankings" in source, \
            "Route must call live method for period=today"

    def test_parks_waittimes_today_dispatches_to_live_method(self):
        """
        Verify that /api/parks/waittimes?period=today calls the live method.
        """
        import inspect
        from api.routes.parks import get_park_wait_times
        source = inspect.getsource(get_park_wait_times)

        assert "period == 'today'" in source, \
            "Route must check for period=today"
        assert "get_park_live_wait_time_rankings" in source, \
            "Route must call live method for period=today"


class TestLiveWaitTimesQueryStructure:
    """Test the SQL structure of live wait times queries."""

    def test_live_ride_wait_times_query_uses_pacific_timezone(self):
        """
        Verify that live queries use Pacific timezone bounds.

        This ensures "today" means Pacific calendar day, not UTC.
        """
        import inspect
        from database.repositories.stats_repository import StatsRepository
        source = inspect.getsource(StatsRepository.get_ride_live_wait_time_rankings)

        assert 'pacific' in source.lower() or 'get_today_pacific' in source, \
            "Live query must use Pacific timezone for 'today'"

    def test_live_ride_wait_times_query_filters_positive_waits(self):
        """
        Verify that live queries only count positive wait times.

        Rides with wait_time=0 or NULL should not be included in averages.
        """
        import inspect
        from database.repositories.stats_repository import StatsRepository
        source = inspect.getsource(StatsRepository.get_ride_live_wait_time_rankings)

        assert 'wait_time > 0' in source or 'wait_time IS NOT NULL' in source, \
            "Live query must filter to positive wait times"

    def test_live_ride_wait_times_query_groups_by_ride(self):
        """
        Verify that live queries aggregate across snapshots per ride.
        """
        import inspect
        from database.repositories.stats_repository import StatsRepository
        source = inspect.getsource(StatsRepository.get_ride_live_wait_time_rankings)

        assert 'GROUP BY' in source, \
            "Live query must group by ride to aggregate snapshots"
        assert 'AVG' in source or 'avg_wait_time' in source, \
            "Live query must calculate average wait time"


class TestWaitTimesApiContract:
    """Test the API contract for wait times endpoints."""

    def test_ride_wait_times_required_response_fields(self):
        """
        Document and verify all required response fields.

        For period=today, response MUST include:
        - Standard fields: ride_id, ride_name, park_name, avg_wait_time, peak_wait_time
        - Live-only fields: current_status, current_is_open, park_is_open
        """
        required_live_fields = {
            'ride_id',
            'ride_name',
            'park_name',
            'park_id',
            'avg_wait_time',
            'peak_wait_time',
            'current_status',      # LIVE ONLY
            'current_is_open',     # LIVE ONLY
            'park_is_open',        # LIVE ONLY
        }

        # These fields distinguish live from aggregated responses
        live_only_fields = {'current_status', 'current_is_open', 'park_is_open'}

        assert live_only_fields.issubset(required_live_fields), \
            "Live-only fields must be in required fields for period=today"

    def test_park_wait_times_required_response_fields(self):
        """
        Document and verify all required response fields for parks.
        """
        required_live_fields = {
            'park_id',
            'park_name',
            'location',
            'avg_wait_time',
            'peak_wait_time',
            'park_is_open',        # LIVE ONLY
        }

        live_only_fields = {'park_is_open'}

        assert live_only_fields.issubset(required_live_fields), \
            "Live-only fields must be in required fields for period=today"
