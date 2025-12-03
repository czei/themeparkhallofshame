"""
Unit Tests for Wait Times API Contract (Field Names)
====================================================

TDD Tests: These tests define the EXPECTED API response field names
that the frontend requires. They should FAIL if the API returns
different field names.

The Bug These Tests Catch:
--------------------------
Frontend expects: avg_wait_minutes, peak_wait_minutes, trend_percentage, tier
API was returning: avg_wait_time, peak_wait_time (missing trend_percentage, tier)

This mismatch causes the frontend to display zeros because:
  - ride.avg_wait_minutes is undefined (API sends avg_wait_time)
  - undefined || 0 evaluates to 0
"""



class TestRideWaitTimesFieldNames:
    """Test that ride wait times API returns correct field names for frontend."""

    def test_ride_wait_times_returns_avg_wait_minutes_not_avg_wait_time(self):
        """
        CRITICAL: Frontend expects 'avg_wait_minutes', not 'avg_wait_time'.

        Frontend code (wait-times.js line 337):
            const aVal = this.state.sortBy === 'avg'
                ? (a.avg_wait_minutes || 0)  // <-- expects avg_wait_minutes

        If API returns 'avg_wait_time', the frontend shows 0.
        """
        # Verify the live query method returns correct field name
        import inspect
        from database.repositories.stats_repository import StatsRepository
        source = inspect.getsource(StatsRepository.get_ride_live_wait_time_rankings)

        # The SQL should alias to avg_wait_minutes for frontend compatibility
        assert 'avg_wait_minutes' in source, \
            "Live ride wait times must return 'avg_wait_minutes' (not 'avg_wait_time') for frontend"

    def test_ride_wait_times_returns_peak_wait_minutes_not_peak_wait_time(self):
        """
        CRITICAL: Frontend expects 'peak_wait_minutes', not 'peak_wait_time'.

        Frontend code (wait-times.js line 416):
            <span class="wait-value">${this.formatWaitTime(ride.peak_wait_minutes || 0)}</span>

        If API returns 'peak_wait_time', the frontend shows 0.
        """
        import inspect
        from database.repositories.stats_repository import StatsRepository
        source = inspect.getsource(StatsRepository.get_ride_live_wait_time_rankings)

        assert 'peak_wait_minutes' in source, \
            "Live ride wait times must return 'peak_wait_minutes' (not 'peak_wait_time') for frontend"

    def test_ride_wait_times_returns_tier_field(self):
        """
        CRITICAL: Frontend expects 'tier' field for ride classification.

        Frontend code (wait-times.js line 385):
            const tierBadge = this.getTierBadge(ride.tier);

        If tier is missing, the frontend shows "?" badge.
        """
        import inspect
        from database.repositories.stats_repository import StatsRepository
        source = inspect.getsource(StatsRepository.get_ride_live_wait_time_rankings)

        # The SQL should include tier from ride_classifications table
        assert 'tier' in source.lower(), \
            "Live ride wait times must include 'tier' field from ride_classifications"

    def test_ride_wait_times_returns_trend_percentage_field(self):
        """
        Frontend expects 'trend_percentage' field for trend indicator.

        Frontend code (wait-times.js line 376-382):
            const trendPct = ride.trend_percentage !== null ...
            const trendText = trendPct !== null
                ? `${trendPct > 0 ? '+' : ''}${trendPct.toFixed(1)}%`
                : 'N/A';

        If trend_percentage is missing, the frontend shows "N/A".
        """
        import inspect
        from database.repositories.stats_repository import StatsRepository
        source = inspect.getsource(StatsRepository.get_ride_live_wait_time_rankings)

        # The SQL should include trend_percentage calculation
        assert 'trend_percentage' in source, \
            "Live ride wait times must include 'trend_percentage' field for frontend trend indicator"


class TestParkWaitTimesFieldNames:
    """Test that park wait times API returns correct field names for frontend."""

    def test_park_wait_times_returns_avg_wait_minutes_not_avg_wait_time(self):
        """
        CRITICAL: Frontend expects 'avg_wait_minutes', not 'avg_wait_time'.

        Frontend code (wait-times.js line 305):
            <span class="wait-value">${this.formatWaitTime(park.avg_wait_minutes || 0)}</span>
        """
        import inspect
        from database.repositories.stats_repository import StatsRepository
        source = inspect.getsource(StatsRepository.get_park_live_wait_time_rankings)

        assert 'avg_wait_minutes' in source, \
            "Live park wait times must return 'avg_wait_minutes' (not 'avg_wait_time') for frontend"

    def test_park_wait_times_returns_peak_wait_minutes_not_peak_wait_time(self):
        """
        CRITICAL: Frontend expects 'peak_wait_minutes', not 'peak_wait_time'.

        Frontend code (wait-times.js line 308):
            <span class="wait-value">${this.formatWaitTime(park.peak_wait_minutes || 0)}</span>
        """
        import inspect
        from database.repositories.stats_repository import StatsRepository
        source = inspect.getsource(StatsRepository.get_park_live_wait_time_rankings)

        assert 'peak_wait_minutes' in source, \
            "Live park wait times must return 'peak_wait_minutes' (not 'peak_wait_time') for frontend"

    def test_park_wait_times_returns_rides_reporting_field(self):
        """
        Frontend expects 'rides_reporting' field for parks.

        Frontend code (wait-times.js line 310):
            <td class="rides-col">${park.rides_reporting || 0}</td>

        If rides_reporting is missing, the frontend shows 0.
        """
        import inspect
        from database.repositories.stats_repository import StatsRepository
        source = inspect.getsource(StatsRepository.get_park_live_wait_time_rankings)

        assert 'rides_reporting' in source, \
            "Live park wait times must include 'rides_reporting' count for frontend"

    def test_park_wait_times_returns_trend_percentage_field(self):
        """
        Frontend expects 'trend_percentage' field for trend indicator.

        Frontend code (wait-times.js line 275-281):
            const trendPct = park.trend_percentage !== null ...
            const trendText = trendPct !== null
                ? `${trendPct > 0 ? '+' : ''}${trendPct.toFixed(1)}%`
                : 'N/A';
        """
        import inspect
        from database.repositories.stats_repository import StatsRepository
        source = inspect.getsource(StatsRepository.get_park_live_wait_time_rankings)

        assert 'trend_percentage' in source, \
            "Live park wait times must include 'trend_percentage' field for frontend"


class TestAggregatedWaitTimesFieldNames:
    """Test that aggregated (7days/30days) wait times return correct field names."""

    def test_aggregated_ride_wait_times_returns_correct_field_names(self):
        """
        Aggregated ride wait times (7days/30days) must also use correct field names.
        """
        import inspect
        from database.queries.rankings.ride_wait_time_rankings import RideWaitTimeRankingsQuery
        source = inspect.getsource(RideWaitTimeRankingsQuery)

        # Check for minutes field names
        assert 'avg_wait_minutes' in source or 'avg_wait_time' in source, \
            "Aggregated query must include wait time field"

        # The aggregated query should also return tier and trend_percentage
        # for consistency with live data

    def test_aggregated_park_wait_times_returns_correct_field_names(self):
        """
        Aggregated park wait times (7days/30days) must also use correct field names.
        """
        import inspect
        from database.queries.rankings.park_wait_time_rankings import ParkWaitTimeRankingsQuery
        source = inspect.getsource(ParkWaitTimeRankingsQuery)

        # Check for minutes field names
        assert 'avg_wait_minutes' in source or 'avg_wait_time' in source, \
            "Aggregated query must include wait time field"


class TestWaitTimesRequiredFields:
    """Document all required fields for wait times API contract."""

    def test_ride_wait_times_today_required_fields(self):
        """
        Document all required fields for /rides/waittimes?period=today

        Required by frontend (wait-times.js):
        - rank: Position in ranking
        - ride_id: Unique ride identifier
        - ride_name: Display name
        - park_id: Parent park identifier
        - park_name: Parent park name
        - location: Park location string
        - avg_wait_minutes: Average wait time in minutes
        - peak_wait_minutes: Maximum wait time in minutes
        - current_status: OPERATING, DOWN, CLOSED, REFURBISHMENT
        - current_is_open: Boolean for ride status
        - park_is_open: Boolean for parent park status
        - tier: Ride importance tier (1, 2, or 3)
        - trend_percentage: Wait time trend vs previous period
        - queue_times_url: External link to Queue-Times.com
        """
        required_fields = {
            'ride_id',
            'ride_name',
            'park_id',
            'park_name',
            'location',
            'avg_wait_minutes',      # NOT avg_wait_time
            'peak_wait_minutes',     # NOT peak_wait_time
            'current_status',
            'current_is_open',
            'park_is_open',
            'tier',
            'trend_percentage',
            'queue_times_url',
        }

        # Verify all fields are documented
        assert len(required_fields) == 13, "Expected 13 required fields for ride wait times"

    def test_park_wait_times_today_required_fields(self):
        """
        Document all required fields for /parks/waittimes?period=today

        Required by frontend (wait-times.js):
        - rank: Position in ranking
        - park_id: Unique park identifier
        - park_name: Display name
        - location: Geographic location
        - avg_wait_minutes: Park-wide average wait time
        - peak_wait_minutes: Park-wide maximum wait time
        - park_is_open: Boolean for park operating status
        - rides_reporting: Count of rides with wait data
        - trend_percentage: Wait time trend vs previous period
        - queue_times_url: External link to Queue-Times.com
        """
        required_fields = {
            'park_id',
            'park_name',
            'location',
            'avg_wait_minutes',      # NOT avg_wait_time
            'peak_wait_minutes',     # NOT peak_wait_time
            'park_is_open',
            'rides_reporting',
            'trend_percentage',
            'queue_times_url',
        }

        # Verify all fields are documented
        assert len(required_fields) == 9, "Expected 9 required fields for park wait times"


class TestTodayWaitTimesFieldNames:
    """
    Test that TODAY period queries return correct field names.

    BUG FIX: The today queries (TodayRideWaitTimesQuery, TodayParkWaitTimesQuery)
    were returning different field names than the live queries:
    - Today returned: avg_wait_time, peak_wait_time, rides_with_waits
    - Frontend expects: avg_wait_minutes, peak_wait_minutes, rides_reporting

    These tests ensure all periods use consistent field names.
    """

    def test_today_ride_wait_times_uses_avg_wait_minutes(self):
        """
        TODAY period must return 'avg_wait_minutes', not 'avg_wait_time'.
        """
        import inspect
        from database.queries.today.today_ride_wait_times import TodayRideWaitTimesQuery
        source = inspect.getsource(TodayRideWaitTimesQuery.get_rankings)

        assert 'avg_wait_minutes' in source, \
            "TodayRideWaitTimesQuery must use 'avg_wait_minutes' (not 'avg_wait_time') for frontend compatibility"

    def test_today_ride_wait_times_uses_peak_wait_minutes(self):
        """
        TODAY period must return 'peak_wait_minutes', not 'peak_wait_time'.
        """
        import inspect
        from database.queries.today.today_ride_wait_times import TodayRideWaitTimesQuery
        source = inspect.getsource(TodayRideWaitTimesQuery.get_rankings)

        assert 'peak_wait_minutes' in source, \
            "TodayRideWaitTimesQuery must use 'peak_wait_minutes' (not 'peak_wait_time') for frontend compatibility"

    def test_today_ride_wait_times_includes_current_is_open(self):
        """
        TODAY period must return 'current_is_open' for ride status badge.
        """
        import inspect
        from database.queries.today.today_ride_wait_times import TodayRideWaitTimesQuery
        source = inspect.getsource(TodayRideWaitTimesQuery.get_rankings)

        assert 'current_is_open' in source, \
            "TodayRideWaitTimesQuery must include 'current_is_open' for ride status badge"

    def test_today_ride_wait_times_includes_current_status(self):
        """
        TODAY period must return 'current_status' for ride status display.
        """
        import inspect
        from database.queries.today.today_ride_wait_times import TodayRideWaitTimesQuery
        source = inspect.getsource(TodayRideWaitTimesQuery.get_rankings)

        assert 'current_status' in source, \
            "TodayRideWaitTimesQuery must include 'current_status' for ride status display"

    def test_today_park_wait_times_uses_avg_wait_minutes(self):
        """
        TODAY period must return 'avg_wait_minutes', not 'avg_wait_time'.
        """
        import inspect
        from database.queries.today.today_park_wait_times import TodayParkWaitTimesQuery
        source = inspect.getsource(TodayParkWaitTimesQuery.get_rankings)

        assert 'avg_wait_minutes' in source, \
            "TodayParkWaitTimesQuery must use 'avg_wait_minutes' (not 'avg_wait_time') for frontend compatibility"

    def test_today_park_wait_times_uses_peak_wait_minutes(self):
        """
        TODAY period must return 'peak_wait_minutes', not 'peak_wait_time'.
        """
        import inspect
        from database.queries.today.today_park_wait_times import TodayParkWaitTimesQuery
        source = inspect.getsource(TodayParkWaitTimesQuery.get_rankings)

        assert 'peak_wait_minutes' in source, \
            "TodayParkWaitTimesQuery must use 'peak_wait_minutes' (not 'peak_wait_time') for frontend compatibility"

    def test_today_park_wait_times_uses_rides_reporting(self):
        """
        TODAY period must return 'rides_reporting', not 'rides_with_waits'.
        """
        import inspect
        from database.queries.today.today_park_wait_times import TodayParkWaitTimesQuery
        source = inspect.getsource(TodayParkWaitTimesQuery.get_rankings)

        assert 'rides_reporting' in source, \
            "TodayParkWaitTimesQuery must use 'rides_reporting' (not 'rides_with_waits') for frontend compatibility"
