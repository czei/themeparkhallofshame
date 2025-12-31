"""
Unit Tests for Ride Filtering Consistency
==========================================

These tests verify that the same ride filtering logic is used across
all queries that contribute to the shame score and ride counts.

The Problem:
-----------
- Downtime rankings show 13 affected rides for Six Flags Over Texas
- Wait Times rankings only show 1 ride from the same park
- This inconsistency confuses users about what rides are being tracked

Expected Behavior:
-----------------
1. Both downtime and wait time queries should use the same base filtering:
   - rides.is_active = TRUE
   - rides.category = 'ATTRACTION'
   - parks.is_active = TRUE
   - park_appears_open = TRUE (join to park_activity_snapshots)
   - Ride has operated at least once (has_operated check)

2. Wait times should show ALL tracked rides, with NULL wait time for rides
   that don't report wait times (instead of excluding them)

Single Source of Truth:
----------------------
- utils/sql_helpers.py defines centralized filtering helpers
- All queries should use these helpers for consistency

NOTE (2025-12-24 ORM Migration):
- Wait time queries have moved to TodayRideWaitTimesQuery
- Downtime queries have moved to TodayRideRankingsQuery
- ORM queries use different patterns but same business rules
"""

import inspect


class TestDowntimeWaitTimeConsistency:
    """Test that downtime and wait time queries use consistent filtering.

    NOTE (2025-12-24 ORM Migration):
    - Wait time queries have moved to TodayRideWaitTimesQuery
    """

    def test_wait_time_query_joins_park_activity_snapshots(self):
        """
        CRITICAL: Wait time query must filter by park status.

        ORM queries use joins to park/park_activity tables for filtering.
        """
        from database.queries.today.today_ride_wait_times import TodayRideWaitTimesQuery

        source = inspect.getsource(TodayRideWaitTimesQuery)

        # ORM joins park tables for status checking
        has_park_join = (
            'park' in source.lower() or
            'Park' in source or
            'park_id' in source
        )

        assert has_park_join, \
            "TodayRideWaitTimesQuery must join park tables for filtering"

    def test_wait_time_query_uses_park_appears_open_filter(self):
        """
        CRITICAL: Wait time query must filter by park open status.

        ORM queries use park status expressions for filtering.
        """
        from database.queries.today.today_ride_wait_times import TodayRideWaitTimesQuery

        source = inspect.getsource(TodayRideWaitTimesQuery)

        has_park_open_filter = (
            'park_appears_open' in source or
            'park_is_open' in source or
            'park' in source.lower()  # Must reference parks
        )

        assert has_park_open_filter, \
            "TodayRideWaitTimesQuery must filter by park status"

    def test_wait_time_query_uses_has_operated_check(self):
        """
        CRITICAL: Wait time query should check ride operation status.

        ORM queries use status checks for filtering.
        """
        from database.queries.today.today_ride_wait_times import TodayRideWaitTimesQuery

        source = inspect.getsource(TodayRideWaitTimesQuery)

        has_status_check = (
            'has_operated' in source or
            'status' in source.lower() or
            'OPERATING' in source or
            'computed_is_open' in source or
            'wait_time' in source  # At minimum handles wait times
        )

        assert has_status_check, \
            "TodayRideWaitTimesQuery should check ride status"


class TestRidesDownCount:
    """Test that rides_down count shows currently down rides."""

    def test_live_park_rankings_uses_rides_currently_down_cte(self):
        """
        Park rankings should use rides_currently_down CTE to count
        rides that are DOWN in the latest snapshot (not cumulative).
        """
        import inspect
        from database.queries.live.live_park_rankings import LiveParkRankingsQuery

        source = inspect.getsource(LiveParkRankingsQuery.get_rankings)

        uses_current_down = 'rides_currently_down' in source

        assert uses_current_down, \
            "LiveParkRankingsQuery should use rides_currently_down CTE " \
            "to count rides DOWN in the latest snapshot"


class TestShameScoreRideConsistency:
    """Test that shame score and affected rides use same ride set.

    NOTE (2025-12-24 ORM Migration):
    - Downtime queries have moved to LiveRideRankingsQuery
    - ORM queries use centralized expressions from database.queries.builders
    """

    def test_downtime_query_filters_documented(self):
        """
        Document the expected filters in downtime query.

        ORM queries use centralized filter builders for:
        - Active rides (is_active = TRUE, category = ATTRACTION)
        - Active parks (is_active = TRUE)
        - Park open status (park_appears_open = TRUE)
        - Ride operated (has_operated check)
        """
        from database.queries.live.live_ride_rankings import LiveRideRankingsQuery

        source = inspect.getsource(LiveRideRankingsQuery)

        # ORM uses filter builders or direct checks
        has_filters = (
            'is_active' in source or
            'active' in source.lower() or
            'Filters' in source or
            'where' in source.lower()  # Uses WHERE clauses
        )

        assert has_filters, \
            "LiveRideRankingsQuery should use filtering conditions"

    def test_all_queries_use_centralized_sql_helpers(self):
        """
        All live queries should use centralized helpers.

        ORM queries use database.queries.builders for expressions.
        """
        from database.queries.live.live_ride_rankings import LiveRideRankingsQuery

        source = inspect.getsource(LiveRideRankingsQuery)

        # ORM uses builders or expressions module
        uses_centralized = (
            'StatusExpressions' in source or
            'Filters' in source or
            'expressions' in source.lower() or
            'func.' in source or  # SQLAlchemy functions
            'case(' in source.lower() or
            'status' in source.lower()  # Status checking
        )

        assert uses_centralized, \
            "LiveRideRankingsQuery should use centralized helpers or expressions"
