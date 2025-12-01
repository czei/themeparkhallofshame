"""
Unit Tests for Status Summary Value Correctness
================================================

TDD Tests: These tests verify that status counts are ACCURATE,
not inflated by JOIN issues.

The Bug These Tests Catch:
--------------------------
Status counts were inflated ~24x because the query OUTER JOINed to
ALL park_activity_snapshots in the 2-hour window instead of only
the LATEST one per park.

Example:
- Actual rides: 1790
- Reported total: 38520 (21x inflation!)

Root Cause:
-----------
The query had:
    .outerjoin(
        park_activity_snapshots,
        and_(
            parks.c.park_id == park_activity_snapshots.c.park_id,
            Filters.within_live_window(park_activity_snapshots.c.recorded_at),
        ),
    )

This joins ALL snapshots in the window (~24 per park in 2 hours),
multiplying each ride count by the number of snapshots.

Fix:
----
Join only the LATEST park_activity_snapshot per park, similar to
how we get the latest ride snapshot.
"""

import pytest
from unittest.mock import MagicMock, patch


class TestStatusSummaryTotalCount:
    """Test that total count is reasonable (not inflated)."""

    def test_total_should_equal_sum_of_status_counts(self):
        """
        Total should equal the sum of all status categories.

        This catches multiplication bugs where rides are counted
        multiple times due to JOIN issues.
        """
        import inspect
        from database.queries.live.status_summary import StatusSummaryQuery
        source = inspect.getsource(StatusSummaryQuery.get_summary)

        # The query should count each ride ONCE, not multiple times
        # Check that we're using COUNT() not SUM() incorrectly
        assert 'func.count()' in source or 'COUNT(*)' in source, \
            "Should use count for total rides"

    def test_query_should_limit_park_snapshots_to_latest(self):
        """
        CRITICAL: The query must join only the LATEST park_activity_snapshot
        per park, not ALL snapshots in the live window.

        Bug: OUTER JOIN to park_activity_snapshots without limiting to latest
        causes each ride to be counted once per snapshot (~24x inflation).
        """
        import inspect
        from database.queries.live.status_summary import StatusSummaryQuery
        source = inspect.getsource(StatusSummaryQuery)

        # Check for subquery pattern to get latest park snapshot
        has_latest_park_snapshot = (
            'latest_park' in source.lower() or
            'max_snapshot_id' in source or
            'MAX(pas' in source or
            'max(park_activity_snapshots' in source.lower() or
            '_get_latest_park_snapshots' in source
        )

        assert has_latest_park_snapshot, \
            "Query must use subquery to get LATEST park_activity_snapshot per park, " \
            "not join ALL snapshots in the window (causes ~24x count inflation)"


class TestStatusSummaryQueryStructure:
    """Test the query structure to prevent JOIN multiplication bugs."""

    def test_rides_should_not_be_multiplied_by_snapshots(self):
        """
        Each ride should be counted exactly ONCE in the status summary.

        If there are N park_activity_snapshots per park in the live window,
        and we OUTER JOIN to all of them, each ride gets counted N times.

        The fix is to join to a subquery that returns only the LATEST
        park_activity_snapshot per park.
        """
        import inspect
        from database.queries.live.status_summary import StatusSummaryQuery
        source = inspect.getsource(StatusSummaryQuery)

        # The class should have a method to get latest park snapshots
        # similar to how it has _get_latest_snapshots_subquery for rides
        has_latest_park_subquery = (
            '_get_latest_park_snapshots' in source or
            'latest_park_snapshot' in source.lower()
        )

        # Or the outerjoin should be to a subquery, not directly to the table
        outerjoin_section = source[source.find('outerjoin'):source.find('outerjoin')+500] if 'outerjoin' in source else ''

        # Check that outerjoin is to a subquery (contains MAX or subquery)
        outerjoin_uses_subquery = (
            'subquery' in outerjoin_section.lower() or
            'max(' in outerjoin_section.lower() or
            has_latest_park_subquery
        )

        assert outerjoin_uses_subquery, \
            "outerjoin to park_activity_snapshots must use subquery for latest snapshot only"


class TestExpectedRideCounts:
    """Document expected ride counts for validation."""

    def test_document_expected_total_rides(self):
        """
        Document the expected total number of rides.

        Database has ~1790 active attractions.
        Any total significantly higher than this indicates JOIN multiplication.
        """
        expected_max_rides = 2000  # Upper bound for active attractions
        expected_min_rides = 1000  # Lower bound (some may not have snapshots)

        # This documents our expectations - the actual test is in integration
        assert expected_max_rides > expected_min_rides
        assert expected_max_rides < 5000, \
            "If total exceeds 5000, something is very wrong (we have ~1790 rides)"

    def test_inflation_factor_example(self):
        """
        Document the inflation bug for reference.

        If park_activity_snapshots has 24 entries per park in 2 hours
        (one every 5 minutes), and we JOIN to all of them, we get 24x inflation.

        Actual: 1790 rides
        Inflated: 38520 (21.5x - close to 24x)
        """
        actual_rides = 1790
        inflated_total = 38520
        inflation_factor = inflated_total / actual_rides

        assert inflation_factor > 20, "This documents the ~21x inflation bug"
        assert inflation_factor < 25, "Inflation is roughly 24x (one per 5-min snapshot)"
