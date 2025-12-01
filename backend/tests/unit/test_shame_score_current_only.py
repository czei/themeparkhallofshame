"""
Unit Tests for Current-Only Shame Score Calculation
====================================================

TDD Tests: Verify that shame score only counts rides that are CURRENTLY down,
not rides that had downtime earlier but are now operating.

The Problem:
-----------
- "Affected Rides: 13" suggests 13 rides are currently down
- But those rides may have been down earlier and are now operating
- User expects shame score to reflect CURRENT state, not cumulative

Expected Behavior:
-----------------
1. Shame score only counts downtime for rides that are CURRENTLY DOWN
2. "Rides Down" (renamed from "Affected Rides") shows count of currently down rides
3. Rides that were down earlier but are now operating don't contribute to shame

Single Source of Truth:
----------------------
- Use latest snapshot to determine "currently down" status
- Filter by park_appears_open = TRUE (exclude closed parks)
- Filter by has_operated (ride must have operated at least once)
"""

import pytest
from unittest.mock import MagicMock


class TestShameScoreCurrentOnly:
    """Test that shame score only counts currently down rides."""

    def test_live_park_rankings_uses_current_status_for_shame(self):
        """
        CRITICAL: Shame score should be based on rides CURRENTLY down,
        not cumulative downtime throughout the day.

        The query must check if ride is down in the LATEST snapshot,
        not just any snapshot from today.
        """
        import inspect
        from database.repositories.stats_repository import StatsRepository

        source = inspect.getsource(StatsRepository.get_park_live_downtime_rankings)

        # Should reference "latest" or "current" snapshot logic
        uses_current_status = (
            'current_status' in source.lower() or
            'latest' in source.lower() or
            'currently_down' in source.lower() or
            'is_currently_down' in source.lower()
        )

        assert uses_current_status, \
            "get_park_live_downtime_rankings should check current status, " \
            "not cumulative downtime throughout the day"

    def test_shame_score_filters_by_currently_down_rides(self):
        """
        Shame score calculation should only include downtime
        for rides that are DOWN in the latest snapshot.
        """
        import inspect
        from database.repositories.stats_repository import StatsRepository

        source = inspect.getsource(StatsRepository.get_park_live_downtime_rankings)

        # The query should have a CTE or subquery that identifies currently down rides
        has_current_check = (
            'rides_currently_down' in source or
            'current_ride_status' in source or
            'latest_snapshot' in source
        )

        assert has_current_check, \
            "Shame score should filter to only count currently down rides"


class TestRidesDownCount:
    """Test that rides_down count shows currently down, not cumulative."""

    def test_response_uses_rides_down_not_affected_rides(self):
        """
        API response should use 'rides_down' instead of 'affected_rides_count'.

        The column is renamed to make clear it shows CURRENT status.
        """
        import inspect
        from database.repositories.stats_repository import StatsRepository

        source = inspect.getsource(StatsRepository.get_park_live_downtime_rankings)

        # Should have rides_down in the SELECT
        has_rides_down = 'rides_down' in source

        assert has_rides_down, \
            "API should return 'rides_down' showing currently down rides"

    def test_rides_down_counts_latest_snapshot_only(self):
        """
        rides_down should count rides DOWN in the LATEST snapshot only,
        not distinct rides that were ever down today.
        """
        import inspect
        from database.repositories.stats_repository import StatsRepository

        source = inspect.getsource(StatsRepository.get_park_live_downtime_rankings)

        # Should NOT use COUNT(DISTINCT ...) for all snapshots
        # Should use latest snapshot logic
        uses_latest = (
            'latest_status' in source or
            'current_snapshot' in source or
            'most_recent' in source or
            'rides_currently_down' in source
        )

        assert uses_latest, \
            "rides_down should be based on latest snapshot, not all snapshots today"


class TestLatestSnapshotLogic:
    """Test that latest snapshot is correctly identified."""

    def test_query_identifies_latest_snapshot_per_ride(self):
        """
        Query should identify the most recent snapshot for each ride
        to determine current status.
        """
        import inspect
        from database.repositories.stats_repository import StatsRepository

        source = inspect.getsource(StatsRepository.get_park_live_downtime_rankings)

        # Should have MAX(recorded_at) or similar to find latest
        finds_latest = (
            'MAX(rss.recorded_at)' in source or
            'MAX(recorded_at)' in source or
            'latest_snapshot' in source or
            'ORDER BY recorded_at DESC' in source
        )

        assert finds_latest, \
            "Query should identify latest snapshot to determine current status"
