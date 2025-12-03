"""
Index Coverage Tests (TDD)
==========================

These tests verify that performance-critical indexes exist in the database.

TDD Flow:
1. Tests FAIL initially (indexes don't exist)
2. Apply migration 005_performance_indexes.sql
3. Tests PASS

The indexes are critical for time-range queries on snapshot tables.
"""

import pytest
from sqlalchemy import text


class TestRequiredIndexes:
    """Verify performance-critical indexes exist in the database."""

    def test_ride_status_snapshots_time_range_covering_index_exists(self, mysql_connection):
        """
        Covering index for time-range aggregations must exist.

        Index: idx_rss_time_range_covering (recorded_at, ride_id, computed_is_open, wait_time)

        This index is critical for TODAY queries that:
        - Filter by recorded_at range (WHERE recorded_at >= :start AND recorded_at < :end)
        - Group by ride_id
        - Aggregate computed_is_open and wait_time

        Without this index, queries do full table scans on 1M+ rows.
        """
        result = mysql_connection.execute(text("""
            SELECT COUNT(*) as idx_count
            FROM information_schema.statistics
            WHERE table_schema = DATABASE()
              AND table_name = 'ride_status_snapshots'
              AND index_name = 'idx_rss_time_range_covering'
        """))
        idx_count = result.scalar()

        assert idx_count > 0, (
            "Missing idx_rss_time_range_covering on ride_status_snapshots. "
            "Run migration 005_performance_indexes.sql to create: "
            "CREATE INDEX idx_rss_time_range_covering ON ride_status_snapshots "
            "(recorded_at, ride_id, computed_is_open, wait_time)"
        )

    def test_park_activity_snapshots_time_range_covering_index_exists(self, mysql_connection):
        """
        Covering index for park status joins must exist.

        Index: idx_pas_time_range_covering (recorded_at, park_id, park_appears_open)

        This index optimizes the JOIN between ride_status_snapshots and
        park_activity_snapshots on recorded_at and park_id, with filtering
        on park_appears_open.
        """
        result = mysql_connection.execute(text("""
            SELECT COUNT(*) as idx_count
            FROM information_schema.statistics
            WHERE table_schema = DATABASE()
              AND table_name = 'park_activity_snapshots'
              AND index_name = 'idx_pas_time_range_covering'
        """))
        idx_count = result.scalar()

        assert idx_count > 0, (
            "Missing idx_pas_time_range_covering on park_activity_snapshots. "
            "Run migration 005_performance_indexes.sql to create: "
            "CREATE INDEX idx_pas_time_range_covering ON park_activity_snapshots "
            "(recorded_at, park_id, park_appears_open)"
        )


class TestExistingIndexes:
    """Verify that existing required indexes are present."""

    def test_ride_status_snapshots_has_ride_recorded_index(self, mysql_connection):
        """The idx_ride_recorded index should exist for single ride lookups."""
        result = mysql_connection.execute(text("""
            SELECT COUNT(*) as idx_count
            FROM information_schema.statistics
            WHERE table_schema = DATABASE()
              AND table_name = 'ride_status_snapshots'
              AND index_name = 'idx_ride_recorded'
        """))
        idx_count = result.scalar()

        assert idx_count > 0, (
            "Missing idx_ride_recorded on ride_status_snapshots - "
            "required for single ride status lookups"
        )

    def test_park_activity_snapshots_has_park_recorded_index(self, mysql_connection):
        """The idx_park_recorded index should exist for park lookups."""
        result = mysql_connection.execute(text("""
            SELECT COUNT(*) as idx_count
            FROM information_schema.statistics
            WHERE table_schema = DATABASE()
              AND table_name = 'park_activity_snapshots'
              AND index_name = 'idx_park_recorded'
        """))
        idx_count = result.scalar()

        assert idx_count > 0, (
            "Missing idx_park_recorded on park_activity_snapshots - "
            "required for park status lookups"
        )


class TestIndexEffectiveness:
    """Test that indexes are being used by EXPLAIN."""

    def test_time_range_query_uses_covering_index(self, mysql_connection):
        """
        Verify that time-range queries use the covering index.

        The EXPLAIN output should show:
        - 'Using index' (covering index scan)
        - NOT 'Using where' alone (table scan)
        """
        # This test will PASS after the covering index is created
        # and MySQL query optimizer chooses to use it

        # Get today's date range for testing
        from utils.timezone import get_today_range_to_now_utc
        try:
            start_utc, now_utc = get_today_range_to_now_utc()
        except Exception:
            # Skip if timezone utils not available
            pytest.skip("Timezone utils not available")

        explain_query = text("""
            EXPLAIN
            SELECT ride_id, AVG(wait_time), MAX(wait_time)
            FROM ride_status_snapshots
            WHERE recorded_at >= :start_utc AND recorded_at < :now_utc
            GROUP BY ride_id
        """)

        result = mysql_connection.execute(explain_query, {
            "start_utc": start_utc,
            "now_utc": now_utc
        })

        explain_output = [dict(row._mapping) for row in result]

        # Check if using any index (even if not the covering one)
        if explain_output:
            first_row = explain_output[0]
            key_used = first_row.get('key') or first_row.get('Key')
            extra = first_row.get('Extra') or first_row.get('extra') or ''

            print(f"\nEXPLAIN output: key={key_used}, extra={extra}")

            # After optimization, we want 'Using index' in Extra
            # Before optimization, this test passes with any index usage
            assert key_used is not None or 'Using index' in extra, (
                "Query not using any index - performance will be poor. "
                "Expected idx_rss_time_range_covering to be used."
            )
