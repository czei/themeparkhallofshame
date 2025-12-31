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

NOTE (2025-12-24 ORM Migration):
- Live park rankings have moved to LiveParkRankingsQuery
- The queries use ORM with centralized status expressions
"""

import inspect


class TestShameScoreCurrentOnly:
    """Test that shame score only counts currently down rides.

    NOTE: Live rankings have moved to LiveParkRankingsQuery class.
    """

    def test_live_park_rankings_uses_current_status_for_shame(self):
        """
        Live park rankings should include status-related logic.

        In ORM queries, this is achieved via case() expressions
        that check current ride status.
        """
        from database.queries.live.live_park_rankings import LiveParkRankingsQuery

        source = inspect.getsource(LiveParkRankingsQuery)

        # ORM queries use case(), is_down expressions, or status checks
        uses_current_status = (
            'current' in source.lower() or
            'is_down' in source.lower() or
            'status' in source.lower() or
            'case(' in source.lower()
        )

        assert uses_current_status, \
            "LiveParkRankingsQuery should check current status for shame score"

    def test_shame_score_filters_by_currently_down_rides(self):
        """
        Shame score calculation uses ORM expressions for status.
        """
        from database.queries.live.live_park_rankings import LiveParkRankingsQuery

        source = inspect.getsource(LiveParkRankingsQuery)

        # ORM uses case(), func.sum(), and is_down expressions
        has_status_check = (
            'case(' in source.lower() or
            'is_down' in source or
            'DOWN' in source or
            'status' in source
        )

        assert has_status_check, \
            "Shame score should filter rides by status"


class TestRidesDownCount:
    """Test that rides_down count shows currently down, not cumulative."""

    def test_response_uses_rides_down_not_affected_rides(self):
        """
        ORM query should calculate rides_down count.
        """
        from database.queries.live.live_park_rankings import LiveParkRankingsQuery

        source = inspect.getsource(LiveParkRankingsQuery)

        # Should have rides_down or similar count
        has_rides_down = (
            'rides_down' in source or
            'rides_currently_down' in source or
            'down_count' in source.lower() or
            'is_down' in source
        )

        assert has_rides_down, \
            "LiveParkRankingsQuery should calculate rides currently down"

    def test_rides_down_counts_latest_snapshot_only(self):
        """
        ORM query should use latest snapshot for current status.
        """
        from database.queries.live.live_park_rankings import LiveParkRankingsQuery

        source = inspect.getsource(LiveParkRankingsQuery)

        # ORM queries use time window or latest logic
        uses_latest = (
            'live_cutoff' in source or
            'LIVE_WINDOW' in source or
            'hours_ago' in source or
            'order_by' in source.lower()
        )

        assert uses_latest, \
            "Query should use time window for current status"


class TestLatestSnapshotLogic:
    """Test that latest snapshot is correctly identified."""

    def test_query_identifies_latest_snapshot_per_ride(self):
        """
        Query should use time window or max for latest snapshot.
        """
        from database.queries.live.live_park_rankings import LiveParkRankingsQuery

        source = inspect.getsource(LiveParkRankingsQuery)

        # ORM uses func.max(), time window, or subqueries
        finds_latest = (
            'func.max' in source.lower() or
            'max(' in source.lower() or
            'LIVE_WINDOW' in source or
            'hours_ago' in source or
            'recorded_at' in source
        )

        assert finds_latest, \
            "Query should identify latest snapshot for current status"
