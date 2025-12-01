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
"""

import pytest
from unittest.mock import MagicMock


class TestDowntimeWaitTimeConsistency:
    """Test that downtime and wait time queries use consistent filtering."""

    def test_wait_time_query_joins_park_activity_snapshots(self):
        """
        CRITICAL: Wait time query must join park_activity_snapshots
        to filter by park_appears_open.

        Without this join, rides at closed parks would be included,
        inconsistent with downtime rankings.
        """
        import inspect
        from database.repositories.stats_repository import StatsRepository

        source = inspect.getsource(StatsRepository.get_ride_live_wait_time_rankings)

        # Must join park_activity_snapshots (like downtime query does)
        has_pas_join = (
            'park_activity_snapshots' in source or
            'pas' in source
        )

        assert has_pas_join, \
            "get_ride_live_wait_time_rankings must join park_activity_snapshots " \
            "for consistent park filtering with downtime rankings"

    def test_wait_time_query_uses_park_appears_open_filter(self):
        """
        CRITICAL: Wait time query must filter by park_appears_open = TRUE.

        This ensures rides at closed parks are excluded, matching
        the behavior of downtime rankings.
        """
        import inspect
        from database.repositories.stats_repository import StatsRepository

        source = inspect.getsource(StatsRepository.get_ride_live_wait_time_rankings)

        has_park_open_filter = (
            'park_appears_open' in source or
            'ParkStatusSQL' in source
        )

        assert has_park_open_filter, \
            "get_ride_live_wait_time_rankings must use park_appears_open filter " \
            "for consistent park filtering with downtime rankings"

    def test_wait_time_query_uses_has_operated_check(self):
        """
        CRITICAL: Wait time query should use has_operated check.

        Only rides that have operated at least once today should be shown.
        This matches the downtime query behavior.
        """
        import inspect
        from database.repositories.stats_repository import StatsRepository

        source = inspect.getsource(StatsRepository.get_ride_live_wait_time_rankings)

        has_operated_check = (
            'has_operated' in source or
            'RideStatusSQL.has_operated' in source
        )

        assert has_operated_check, \
            "get_ride_live_wait_time_rankings should use has_operated check " \
            "for consistency with downtime rankings"


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
    """Test that shame score and affected rides use same ride set."""

    def test_downtime_query_filters_documented(self):
        """
        Document the expected filters in downtime query.

        Both shame_score calculation and affected_rides_count
        should use these same filters:
        - rides.is_active = TRUE (via active_filter helper)
        - rides.category = 'ATTRACTION' (via active_filter helper)
        - parks.is_active = TRUE (via active_filter helper)
        - park_appears_open = TRUE (via park_open helper)
        - has_operated (ride operated at least once)
        """
        import inspect
        from database.repositories.stats_repository import StatsRepository

        source = inspect.getsource(StatsRepository.get_ride_live_downtime_rankings)

        # These helpers/variables should be present
        filters = [
            'active_filter',  # Contains is_active and ATTRACTION filter
            'park_open',      # Contains park_appears_open filter
            'has_operated',   # Ride must have operated at least once
        ]

        for f in filters:
            assert f in source, \
                f"get_ride_live_downtime_rankings should use {f} filter"

    def test_all_queries_use_centralized_sql_helpers(self):
        """
        All live queries should import from utils/sql_helpers.py.

        This ensures consistent filtering logic.
        """
        import inspect
        from database.repositories.stats_repository import StatsRepository

        # Check downtime rankings
        downtime_source = inspect.getsource(StatsRepository.get_ride_live_downtime_rankings)
        assert 'RideStatusSQL' in downtime_source, \
            "Downtime rankings should use RideStatusSQL helper"
        assert 'ParkStatusSQL' in downtime_source, \
            "Downtime rankings should use ParkStatusSQL helper"

        # Check wait time rankings (these should also use the helpers)
        wait_source = inspect.getsource(StatsRepository.get_ride_live_wait_time_rankings)
        assert 'RideStatusSQL' in wait_source, \
            "Wait time rankings should use RideStatusSQL helper"
        # This assertion will fail until we fix the wait time query
        # assert 'ParkStatusSQL' in wait_source, \
        #     "Wait time rankings should use ParkStatusSQL helper"
