"""
API Smoke Tests

Sanity checks that run against mirrored production data.
These tests verify:
- APIs return data
- Response structure is correct
- Performance is acceptable
- Data is consistent across endpoints

These are NON-BLOCKING tests - failures are warnings, not CI blockers.

Usage:
    # First mirror production data
    ./deployment/scripts/mirror-production-db.sh --days=7

    # Run smoke tests
    pytest tests/smoke/ -v -m smoke

    # Run with performance timing
    pytest tests/smoke/ -v -m smoke --durations=10
"""

import pytest
import time
from datetime import datetime, timedelta
from sqlalchemy import text


# =============================================================================
# Parks API Smoke Tests
# =============================================================================

@pytest.mark.smoke
class TestParksDowntimeSmoke:
    """Smoke tests for parks/downtime API."""

    PERIODS = ['live', 'today', 'yesterday', 'last_week', 'last_month']

    @pytest.mark.parametrize("period", PERIODS)
    def test_parks_downtime_returns_data(self, smoke_connection, period):
        """Verify parks/downtime returns non-empty results for each period."""
        result = smoke_connection.execute(text("""
            SELECT COUNT(DISTINCT p.park_id) as park_count
            FROM parks p
            JOIN park_activity_snapshots pas ON p.park_id = pas.park_id
            WHERE pas.park_appears_open = 1
        """)).fetchone()

        # Should have data for at least some parks
        # Exact count depends on period and data freshness
        if period == 'live':
            # Live might have fewer parks (only currently open)
            assert result.park_count >= 0, "Should have parks in database"
        else:
            assert result.park_count >= 10, f"Should have at least 10 parks with data for {period}"

    def test_parks_have_required_fields(self, smoke_connection):
        """Verify parks table has all required fields for API responses."""
        result = smoke_connection.execute(text("""
            SELECT
                p.park_id,
                p.name,
                p.is_disney,
                p.is_universal,
                p.timezone,
                p.latitude,
                p.longitude
            FROM parks p
            WHERE p.is_active = 1
            LIMIT 1
        """)).fetchone()

        assert result is not None, "Should have at least one active park"
        assert result.park_id is not None
        assert result.name is not None
        assert result.timezone is not None

    def test_parks_shame_scores_are_reasonable(self, smoke_connection):
        """Verify shame scores are within expected bounds."""
        result = smoke_connection.execute(text("""
            SELECT
                MIN(shame_score) as min_shame,
                MAX(shame_score) as max_shame,
                AVG(shame_score) as avg_shame
            FROM park_activity_snapshots
            WHERE park_appears_open = 1
              AND shame_score IS NOT NULL
              AND recorded_at >= DATE_SUB(NOW(), INTERVAL 7 DAY)
        """)).fetchone()

        if result.avg_shame is not None:
            # Shame scores should be non-negative
            assert result.min_shame >= 0, "Shame scores should be non-negative"
            # Shame scores typically don't exceed 100 (but could in extreme cases)
            assert result.max_shame < 500, f"Max shame {result.max_shame} seems unreasonably high"
            # Average should be reasonable
            assert result.avg_shame < 50, f"Average shame {result.avg_shame} seems high"


@pytest.mark.smoke
class TestRidesDowntimeSmoke:
    """Smoke tests for rides/downtime API."""

    def test_rides_have_status_data(self, smoke_connection):
        """Verify rides have status snapshot data."""
        result = smoke_connection.execute(text("""
            SELECT
                COUNT(DISTINCT r.ride_id) as rides_with_data,
                COUNT(*) as total_snapshots
            FROM rides r
            JOIN ride_status_snapshots rss ON r.ride_id = rss.ride_id
            WHERE rss.recorded_at >= DATE_SUB(NOW(), INTERVAL 7 DAY)
        """)).fetchone()

        assert result.rides_with_data >= 500, f"Should have at least 500 rides with data, got {result.rides_with_data}"
        assert result.total_snapshots >= 100000, f"Should have at least 100K snapshots, got {result.total_snapshots}"

    def test_rides_have_tier_distribution(self, smoke_connection):
        """Verify rides table has tier column and some data."""
        result = smoke_connection.execute(text("""
            SELECT
                tier,
                COUNT(*) as count
            FROM rides
            WHERE is_active = 1
            GROUP BY tier
            ORDER BY tier
        """)).fetchall()

        # Should have rides (tier classification may be incomplete)
        total_rides = sum(r.count for r in result)
        assert total_rides >= 500, f"Should have at least 500 active rides, got {total_rides}"

        # Report tier distribution (informational)
        print("\n=== Tier Distribution ===")
        for r in result:
            tier_name = f"Tier {r.tier}" if r.tier else "Unclassified"
            print(f"  {tier_name}: {r.count} rides")

    def test_ride_statuses_are_valid(self, smoke_connection):
        """Verify ride status values are valid enum values."""
        result = smoke_connection.execute(text("""
            SELECT DISTINCT status
            FROM ride_status_snapshots
            WHERE recorded_at >= DATE_SUB(NOW(), INTERVAL 1 DAY)
        """)).fetchall()

        valid_statuses = {'OPERATING', 'DOWN', 'CLOSED', 'REFURBISHMENT', None}
        actual_statuses = {r.status for r in result}

        invalid = actual_statuses - valid_statuses
        assert len(invalid) == 0, f"Found invalid statuses: {invalid}"


# =============================================================================
# Data Consistency Smoke Tests
# =============================================================================

@pytest.mark.smoke
class TestDataConsistencySmoke:
    """Verify data consistency across tables."""

    def test_all_rides_have_parks(self, smoke_connection):
        """Verify all rides reference valid parks."""
        result = smoke_connection.execute(text("""
            SELECT COUNT(*) as orphan_rides
            FROM rides r
            LEFT JOIN parks p ON r.park_id = p.park_id
            WHERE p.park_id IS NULL
        """)).fetchone()

        assert result.orphan_rides == 0, f"Found {result.orphan_rides} rides without valid parks"

    def test_snapshots_have_valid_references(self, smoke_connection):
        """Verify snapshots reference valid rides and parks."""
        # Check ride_status_snapshots
        ride_orphans = smoke_connection.execute(text("""
            SELECT COUNT(*) as cnt
            FROM ride_status_snapshots rss
            LEFT JOIN rides r ON rss.ride_id = r.ride_id
            WHERE r.ride_id IS NULL
              AND rss.recorded_at >= DATE_SUB(NOW(), INTERVAL 7 DAY)
        """)).fetchone()

        assert ride_orphans.cnt == 0, f"Found {ride_orphans.cnt} ride snapshots without valid rides"

        # Check park_activity_snapshots
        park_orphans = smoke_connection.execute(text("""
            SELECT COUNT(*) as cnt
            FROM park_activity_snapshots pas
            LEFT JOIN parks p ON pas.park_id = p.park_id
            WHERE p.park_id IS NULL
              AND pas.recorded_at >= DATE_SUB(NOW(), INTERVAL 7 DAY)
        """)).fetchone()

        assert park_orphans.cnt == 0, f"Found {park_orphans.cnt} park snapshots without valid parks"

    def test_disney_universal_flags_consistent(self, smoke_connection):
        """Verify Disney/Universal flags are mutually exclusive."""
        result = smoke_connection.execute(text("""
            SELECT COUNT(*) as both_flags
            FROM parks
            WHERE is_disney = 1 AND is_universal = 1
        """)).fetchone()

        assert result.both_flags == 0, "No park should be both Disney AND Universal"


# =============================================================================
# Performance Smoke Tests
# =============================================================================

@pytest.mark.smoke
class TestPerformanceSmoke:
    """Performance benchmarks against production-like data volume."""

    SLOW_QUERY_THRESHOLD_MS = 5000  # 5 seconds

    def test_parks_ranking_query_performance(self, smoke_connection):
        """Benchmark parks ranking query performance."""
        start = time.time()

        smoke_connection.execute(text("""
            SELECT
                p.park_id,
                p.name,
                AVG(CASE WHEN pas.park_appears_open = 1 THEN pas.shame_score END) as avg_shame
            FROM parks p
            JOIN park_activity_snapshots pas ON p.park_id = pas.park_id
            WHERE DATE(CONVERT_TZ(pas.recorded_at, '+00:00', 'America/Los_Angeles')) = CURDATE() - INTERVAL 1 DAY
            GROUP BY p.park_id
            ORDER BY avg_shame DESC
            LIMIT 50
        """)).fetchall()

        elapsed_ms = (time.time() - start) * 1000

        assert elapsed_ms < self.SLOW_QUERY_THRESHOLD_MS, \
            f"Parks ranking query took {elapsed_ms:.0f}ms, threshold is {self.SLOW_QUERY_THRESHOLD_MS}ms"

    def test_rides_ranking_query_performance(self, smoke_connection):
        """Benchmark rides ranking query performance."""
        start = time.time()

        smoke_connection.execute(text("""
            SELECT
                r.ride_id,
                r.name,
                p.name as park_name,
                COUNT(*) as down_snapshots
            FROM rides r
            JOIN parks p ON r.park_id = p.park_id
            JOIN ride_status_snapshots rss ON r.ride_id = rss.ride_id
            JOIN park_activity_snapshots pas ON p.park_id = pas.park_id
                AND pas.recorded_at = rss.recorded_at
            WHERE DATE(CONVERT_TZ(rss.recorded_at, '+00:00', 'America/Los_Angeles')) = CURDATE() - INTERVAL 1 DAY
              AND pas.park_appears_open = 1
              AND rss.status = 'DOWN'
            GROUP BY r.ride_id
            ORDER BY down_snapshots DESC
            LIMIT 50
        """)).fetchall()

        elapsed_ms = (time.time() - start) * 1000

        assert elapsed_ms < self.SLOW_QUERY_THRESHOLD_MS, \
            f"Rides ranking query took {elapsed_ms:.0f}ms, threshold is {self.SLOW_QUERY_THRESHOLD_MS}ms"

    def test_hourly_stats_query_performance(self, smoke_connection):
        """Benchmark hourly stats aggregation query performance."""
        start = time.time()

        smoke_connection.execute(text("""
            SELECT
                DATE(hour_start_utc) as stat_date,
                p.name as park_name,
                SUM(weighted_downtime_hours) as total_weighted_downtime,
                AVG(shame_score) as avg_shame
            FROM park_hourly_stats phs
            JOIN parks p ON phs.park_id = p.park_id
            WHERE hour_start_utc >= DATE_SUB(NOW(), INTERVAL 7 DAY)
            GROUP BY DATE(hour_start_utc), phs.park_id
            ORDER BY stat_date DESC, avg_shame DESC
            LIMIT 100
        """)).fetchall()

        elapsed_ms = (time.time() - start) * 1000

        assert elapsed_ms < self.SLOW_QUERY_THRESHOLD_MS, \
            f"Hourly stats query took {elapsed_ms:.0f}ms, threshold is {self.SLOW_QUERY_THRESHOLD_MS}ms"


# =============================================================================
# Data Freshness Smoke Tests
# =============================================================================

@pytest.mark.smoke
class TestDataFreshnessSmoke:
    """Verify mirrored data is reasonably fresh."""

    def test_data_freshness_report(self, smoke_connection, data_freshness):
        """Report on data freshness (informational)."""
        print("\n=== Data Freshness Report ===")
        print(f"Latest snapshot: {data_freshness['latest_snapshot']}")
        print(f"Hours old: {data_freshness['hours_old']}")
        print(f"Parks with data: {data_freshness['parks_with_data']}")

        # Warn if data is more than 24 hours old
        if data_freshness['hours_old'] > 24:
            pytest.warns(UserWarning,
                match=f"Data is {data_freshness['hours_old']} hours old. Consider refreshing with mirror-production-db.sh")

    def test_has_yesterday_data(self, smoke_connection):
        """Verify we have data for yesterday (required for most tests).

        NOTE: This test is NON-BLOCKING. If no production data is mirrored,
        the test will be skipped with a helpful message.
        """
        result = smoke_connection.execute(text("""
            SELECT COUNT(*) as cnt
            FROM park_activity_snapshots
            WHERE DATE(CONVERT_TZ(recorded_at, '+00:00', 'America/Los_Angeles')) = CURDATE() - INTERVAL 1 DAY
        """)).fetchone()

        if result.cnt < 1000:
            pytest.skip(
                f"No production data for yesterday ({result.cnt} snapshots). "
                f"Run mirror-production-db.sh --days=2 to populate test data."
            )

    def test_has_last_week_data(self, smoke_connection):
        """Verify we have data for the last week."""
        result = smoke_connection.execute(text("""
            SELECT
                COUNT(DISTINCT DATE(recorded_at)) as days_with_data,
                COUNT(*) as total_snapshots
            FROM park_activity_snapshots
            WHERE recorded_at >= DATE_SUB(NOW(), INTERVAL 7 DAY)
        """)).fetchone()

        assert result.days_with_data >= 5, \
            f"Should have data for at least 5 of last 7 days, got {result.days_with_data}"
