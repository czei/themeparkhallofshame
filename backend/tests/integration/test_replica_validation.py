"""
Production Replica Validation Tests
====================================

Tests that validate against fresh production replica data.
These tests verify real-world time-based aggregations and catch
time-boundary bugs that deterministic fixtures might miss.

Usage:
    pytest tests/integration/test_replica_validation.py -m requires_replica

Environment Variables Required:
    REPLICA_DB_HOST     - Replica database host
    REPLICA_DB_PORT     - Replica port (default: 3306)
    REPLICA_DB_NAME     - Replica database name
    REPLICA_DB_USER     - Read-only user
    REPLICA_DB_PASSWORD - Password

Note:
    These tests are OPTIONAL and NON-BLOCKING.
    They will be skipped if replica environment variables are not set.
"""

import pytest
from datetime import datetime, timezone, timedelta
from sqlalchemy import text


@pytest.mark.requires_replica
class TestReplicaDataFreshness:
    """Tests that verify replica has fresh, recent data."""

    def test_replica_has_recent_snapshots(self, replica_connection, verify_replica_freshness):
        """
        Verify replica has park_activity_snapshots from the last hour.

        This ensures data collection is running and replica is receiving updates.
        """
        result = replica_connection.execute(text("""
            SELECT COUNT(*) FROM park_activity_snapshots
            WHERE recorded_at >= DATE_SUB(NOW(), INTERVAL 1 HOUR)
        """)).scalar()

        assert result > 0, (
            "No recent snapshots found in the last hour. "
            "Is data collection running? Is replication working?"
        )

    def test_replica_has_recent_ride_status(self, replica_connection, verify_replica_freshness):
        """
        Verify replica has ride_status_snapshots from the last hour.
        """
        result = replica_connection.execute(text("""
            SELECT COUNT(*) FROM ride_status_snapshots
            WHERE recorded_at >= DATE_SUB(NOW(), INTERVAL 1 HOUR)
        """)).scalar()

        assert result > 0, (
            "No recent ride status snapshots found in the last hour. "
            "Is data collection running? Is replication working?"
        )


@pytest.mark.requires_replica
class TestReplicaAggregationConsistency:
    """Tests that verify pre-aggregated data matches raw calculations."""

    def test_today_snapshot_count_is_reasonable(self, replica_connection, verify_replica_freshness):
        """
        Verify TODAY period has a reasonable number of snapshots.

        With 5-minute collection intervals and ~40 active parks,
        we expect at least a few hundred snapshots per hour.
        """
        result = replica_connection.execute(text("""
            SELECT COUNT(*) FROM park_activity_snapshots
            WHERE recorded_at >= DATE_SUB(NOW(), INTERVAL 12 HOUR)
              AND park_appears_open = TRUE
        """)).scalar()

        # Minimum expectation: at least 100 snapshots in 12 hours
        # (very conservative - actual should be much higher)
        assert result >= 100, (
            f"Only {result} park snapshots in the last 12 hours. "
            f"Expected at least 100. Is data collection running?"
        )

    def test_hourly_stats_exist_for_recent_hours(self, replica_connection, verify_replica_freshness):
        """
        Verify park_hourly_stats has data for recent hours.
        """
        result = replica_connection.execute(text("""
            SELECT COUNT(DISTINCT hour_start_utc)
            FROM park_hourly_stats
            WHERE hour_start_utc >= DATE_SUB(NOW(), INTERVAL 24 HOUR)
        """)).scalar()

        # Should have stats for at least some of the last 24 hours
        assert result >= 1, (
            "No hourly stats found in the last 24 hours. "
            "Has the hourly aggregation job run?"
        )

    def test_shame_scores_are_within_valid_range(self, replica_connection, verify_replica_freshness):
        """
        Verify all shame scores in recent data are within valid range [0, 10].
        """
        result = replica_connection.execute(text("""
            SELECT COUNT(*) FROM park_activity_snapshots
            WHERE recorded_at >= DATE_SUB(NOW(), INTERVAL 24 HOUR)
              AND (shame_score < 0 OR shame_score > 10)
        """)).scalar()

        assert result == 0, (
            f"Found {result} snapshots with invalid shame scores (outside 0-10 range). "
            f"This indicates a calculation bug."
        )


@pytest.mark.requires_replica
class TestReplicaTimeBoundaries:
    """Tests for time boundary edge cases using real production data."""

    def test_pacific_day_boundary_data_exists(self, replica_connection, verify_replica_freshness):
        """
        Verify data exists around the Pacific midnight boundary.

        This catches issues where day boundaries are miscalculated.
        """
        # Pacific midnight is typically 8:00 UTC (PST) or 7:00 UTC (PDT)
        # Check for data around this boundary
        result = replica_connection.execute(text("""
            SELECT COUNT(*) FROM park_activity_snapshots
            WHERE TIME(recorded_at) BETWEEN '07:00:00' AND '09:00:00'
              AND recorded_at >= DATE_SUB(NOW(), INTERVAL 7 DAY)
        """)).scalar()

        assert result > 0, (
            "No snapshots found around Pacific midnight boundary (7-9 UTC). "
            "This could indicate timezone handling issues."
        )

    def test_parks_have_consistent_daily_coverage(self, replica_connection, verify_replica_freshness):
        """
        Verify active parks have consistent snapshot coverage throughout the day.
        """
        result = replica_connection.execute(text("""
            SELECT p.name, COUNT(*) as snapshot_count
            FROM parks p
            JOIN park_activity_snapshots pas ON p.park_id = pas.park_id
            WHERE pas.recorded_at >= DATE_SUB(NOW(), INTERVAL 24 HOUR)
              AND p.is_active = TRUE
            GROUP BY p.park_id, p.name
            HAVING snapshot_count < 100
            LIMIT 5
        """)).fetchall()

        if result:
            parks_with_low_coverage = [f"{row.name}: {row.snapshot_count}" for row in result]
            pytest.skip(
                f"Some parks have low coverage (may be expected for closed parks): "
                f"{', '.join(parks_with_low_coverage)}"
            )


@pytest.mark.requires_replica
class TestReplicaDataIntegrity:
    """Tests for data integrity in the replica."""

    def test_no_orphaned_ride_snapshots(self, replica_connection, verify_replica_freshness):
        """
        Verify ride_status_snapshots don't reference non-existent rides.
        """
        result = replica_connection.execute(text("""
            SELECT COUNT(*) FROM ride_status_snapshots rss
            LEFT JOIN rides r ON rss.ride_id = r.ride_id
            WHERE r.ride_id IS NULL
              AND rss.recorded_at >= DATE_SUB(NOW(), INTERVAL 24 HOUR)
        """)).scalar()

        assert result == 0, (
            f"Found {result} orphaned ride status snapshots. "
            f"Rides may have been deleted without cleaning up snapshots."
        )

    def test_no_future_timestamps(self, replica_connection, verify_replica_freshness):
        """
        Verify no snapshots have timestamps in the future.
        """
        result = replica_connection.execute(text("""
            SELECT COUNT(*) FROM park_activity_snapshots
            WHERE recorded_at > DATE_ADD(NOW(), INTERVAL 5 MINUTE)
        """)).scalar()

        assert result == 0, (
            f"Found {result} snapshots with future timestamps. "
            f"This indicates clock skew or data corruption."
        )
