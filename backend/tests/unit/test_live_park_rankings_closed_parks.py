"""
Unit Tests: Live Park Rankings - Closed Parks Filter
=====================================================

CRITICAL BUG FIX: Closed parks with stale shame scores should NOT appear
in the live park downtime rankings.

Business Rules:
1. LIVE rankings show parks with rides down RIGHT NOW
2. If a park is CLOSED (park_is_open=0), its downtime does NOT count
3. Parks with zero downtime should NOT appear in rankings
4. Parks that are closed should NOT appear in rankings regardless of shame_score

Bug Reproduced:
- Kennywood showing in LIVE rankings with open=0, rides_down=0, downtime=0, shame=31.2
- This is WRONG - closed parks should be filtered out

TDD: This test MUST FAIL before the fix, and PASS after.
"""

import pytest
from unittest.mock import MagicMock, Mock
from datetime import datetime, timezone


class TestLiveParkRankingsClosedParksFilter:
    """Test that closed parks are excluded from live rankings."""

    def test_closed_parks_should_not_appear_in_rankings(self):
        """
        CRITICAL: Parks with park_is_open=FALSE should NOT appear in rankings.

        This test verifies that FastLiveParkRankingsQuery properly filters
        out closed parks, even if they have a non-zero shame_score in the cache.
        """
        from database.queries.live.fast_live_park_rankings import FastLiveParkRankingsQuery
        import inspect

        # Get the source of the get_rankings method
        source = inspect.getsource(FastLiveParkRankingsQuery.get_rankings)

        # The query MUST filter by park_is_open
        assert 'park_is_open' in source.lower(), \
            "FastLiveParkRankingsQuery MUST filter by park_is_open to exclude closed parks"

        # Specifically, should be filtering for open parks only
        assert 'park_is_open = true' in source.lower() or 'park_is_open = 1' in source.lower(), \
            "Query must filter for park_is_open = TRUE"

    def test_query_filters_by_park_is_open_true(self):
        """
        Verify the SQL query explicitly requires park_is_open = TRUE.
        """
        from database.queries.live.fast_live_park_rankings import FastLiveParkRankingsQuery
        import inspect

        source = inspect.getsource(FastLiveParkRankingsQuery.get_rankings)

        # The WHERE clause should include park_is_open filter
        # Check for plr.park_is_open = TRUE or similar
        has_open_filter = (
            'plr.park_is_open = true' in source.lower() or
            'plr.park_is_open = 1' in source.lower() or
            'park_is_open = true' in source.lower() or
            'park_is_open = 1' in source.lower()
        )

        assert has_open_filter, \
            "Query WHERE clause must include filter for open parks: park_is_open = TRUE"

    def test_zero_downtime_parks_should_not_appear(self):
        """
        Parks with total_downtime_hours = 0 should not appear in rankings.

        Why would a park with zero downtime be in a "downtime rankings" list?
        """
        from database.queries.live.fast_live_park_rankings import FastLiveParkRankingsQuery
        import inspect

        source = inspect.getsource(FastLiveParkRankingsQuery.get_rankings)

        # The query should filter for parks with actual downtime
        # Either total_downtime_hours > 0 OR rides_down > 0
        has_downtime_filter = (
            'total_downtime_hours > 0' in source.lower() or
            'rides_down > 0' in source.lower()
        )

        assert has_downtime_filter, \
            "Query must filter for parks with actual downtime (total_downtime_hours > 0 or rides_down > 0)"


class TestLiveParkRankingsBusinessRules:
    """Test business rules for live park rankings."""

    def test_shame_score_alone_is_not_sufficient_filter(self):
        """
        Having shame_score > 0 is NOT sufficient to include a park.

        The shame_score might be stale (calculated when park was open earlier).
        Must ALSO verify park is currently open AND has current downtime.
        """
        from database.queries.live.fast_live_park_rankings import FastLiveParkRankingsQuery
        import inspect

        source = inspect.getsource(FastLiveParkRankingsQuery.get_rankings)

        # Find the WHERE clause
        where_start = source.lower().find('where')
        assert where_start != -1, "Query must have a WHERE clause"

        where_clause = source.lower()[where_start:]

        # Verify shame_score > 0 is NOT the only filter
        # There should be additional filters for park_is_open and downtime
        filters_count = (
            ('park_is_open' in where_clause) +
            ('shame_score' in where_clause) +
            ('downtime' in where_clause or 'rides_down' in where_clause)
        )

        assert filters_count >= 2, \
            "WHERE clause must have multiple filters, not just shame_score > 0. " \
            f"Found conditions for: {filters_count} criteria"
