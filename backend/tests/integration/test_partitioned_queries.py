"""
Integration Tests: Partitioned Query Optimization (Feature 004)

Verifies that queries on partitioned ride_status_snapshots table
properly use partition pruning for efficient data access.

Feature: 004-themeparks-data-collection
Task: T040

NOTE: These tests require the partition migration to have been applied.
If the table is not partitioned, tests will be skipped.
"""

import pytest
from datetime import datetime, timedelta
from sqlalchemy import text


class TestPartitionPruning:
    """
    Tests to verify MySQL partition pruning is working correctly.

    Partition scheme: RANGE by YEAR(recorded_at) * 100 + MONTH(recorded_at)
    - p_before_2024: All data before January 2024
    - p202401 through p203012: Monthly partitions
    - p_future: Data after 2030
    """

    @pytest.fixture
    def check_partitioned(self, mysql_session):
        """Check if table is partitioned, skip if not."""
        result = mysql_session.execute(text("""
            SELECT COUNT(*) as partition_count
            FROM information_schema.partitions
            WHERE table_schema = DATABASE()
            AND table_name = 'ride_status_snapshots'
            AND partition_name IS NOT NULL
        """))
        count = result.scalar()

        if count == 0:
            pytest.skip("ride_status_snapshots is not partitioned - run partition migration first")

        return count

    def test_partition_exists(self, mysql_session, check_partitioned):
        """Verify partitions exist on ride_status_snapshots."""
        partition_count = check_partitioned
        # Expect ~85 partitions: 1 (before_2024) + 84 (7 years × 12 months) + 1 (future)
        assert partition_count >= 10, f"Expected at least 10 partitions, got {partition_count}"

    def test_explain_shows_partition_pruning(self, mysql_session, check_partitioned):
        """EXPLAIN shows partition pruning for date-bounded queries."""
        # Query for a specific month
        explain_result = mysql_session.execute(text("""
            EXPLAIN SELECT COUNT(*)
            FROM ride_status_snapshots
            WHERE recorded_at >= '2025-01-01 00:00:00'
            AND recorded_at < '2025-02-01 00:00:00'
        """))
        rows = explain_result.fetchall()

        # Check that only the relevant partition is accessed
        # MySQL EXPLAIN shows partition info in the partitions column
        found_partition_info = False
        for row in rows:
            row_str = str(row)
            # Look for partition pruning evidence
            if 'p202501' in row_str or 'partitions' in row_str.lower():
                found_partition_info = True

        # Alternative: Check EXPLAIN PARTITIONS
        explain_partitions = mysql_session.execute(text("""
            EXPLAIN PARTITIONS SELECT COUNT(*)
            FROM ride_status_snapshots
            WHERE recorded_at >= '2025-01-01 00:00:00'
            AND recorded_at < '2025-02-01 00:00:00'
        """))
        partition_rows = explain_partitions.fetchall()

        # The partitions column should show only relevant partitions
        for row in partition_rows:
            if hasattr(row, 'partitions') and row.partitions:
                # Should only have p202501, not all partitions
                assert 'p202501' in row.partitions or len(row.partitions.split(',')) < 5

    def test_single_month_query_performance(self, mysql_session, check_partitioned):
        """Single month query should only access one partition."""
        # Get partition access stats before query
        result = mysql_session.execute(text("""
            EXPLAIN PARTITIONS SELECT snapshot_id, ride_id, recorded_at, wait_time
            FROM ride_status_snapshots
            WHERE recorded_at >= '2025-06-01 00:00:00'
            AND recorded_at < '2025-07-01 00:00:00'
            LIMIT 100
        """))
        row = result.fetchone()

        if row and hasattr(row, 'partitions') and row.partitions:
            partitions_accessed = row.partitions.split(',')
            # Should access only 1-2 partitions (maybe overlap at boundary)
            assert len(partitions_accessed) <= 2, f"Query accessed {len(partitions_accessed)} partitions"

    def test_year_range_query_pruning(self, mysql_session, check_partitioned):
        """Year-long query should only access 12 partitions."""
        result = mysql_session.execute(text("""
            EXPLAIN PARTITIONS SELECT COUNT(*)
            FROM ride_status_snapshots
            WHERE recorded_at >= '2025-01-01 00:00:00'
            AND recorded_at < '2026-01-01 00:00:00'
        """))
        row = result.fetchone()

        if row and hasattr(row, 'partitions') and row.partitions:
            partitions_accessed = row.partitions.split(',')
            # Should access approximately 12 partitions for a year
            assert len(partitions_accessed) <= 15, f"Query accessed {len(partitions_accessed)} partitions"


class TestPartitionedQueryPatterns:
    """
    Tests for common query patterns that should benefit from partitioning.
    """

    @pytest.fixture
    def check_partitioned(self, mysql_session):
        """Check if table is partitioned."""
        result = mysql_session.execute(text("""
            SELECT COUNT(*) as partition_count
            FROM information_schema.partitions
            WHERE table_schema = DATABASE()
            AND table_name = 'ride_status_snapshots'
            AND partition_name IS NOT NULL
        """))
        count = result.scalar()

        if count == 0:
            pytest.skip("ride_status_snapshots is not partitioned")

        return count

    @pytest.fixture
    def setup_test_data(self, mysql_session, check_partitioned):
        """Create test park and ride for query tests."""
        # Create park
        mysql_session.execute(text("""
            INSERT INTO parks (queue_times_id, name, timezone, is_active)
            VALUES (99010, 'Partition Test Park', 'America/Los_Angeles', 1)
        """))
        mysql_session.flush()

        result = mysql_session.execute(text(
            "SELECT park_id FROM parks WHERE queue_times_id = 99010"
        ))
        park_id = result.scalar()

        # Create ride
        mysql_session.execute(text("""
            INSERT INTO rides (queue_times_id, park_id, name, is_active)
            VALUES (990010, :park_id, 'Partition Test Ride', 1)
        """), {'park_id': park_id})
        mysql_session.flush()

        result = mysql_session.execute(text(
            "SELECT ride_id FROM rides WHERE queue_times_id = 990010"
        ))
        ride_id = result.scalar()

        return {'park_id': park_id, 'ride_id': ride_id}

    def test_today_query_pattern(self, mysql_session, setup_test_data):
        """TODAY period query uses proper date bounds."""
        ride_id = setup_test_data['ride_id']
        today_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        tomorrow_start = today_start + timedelta(days=1)

        # Insert test data
        mysql_session.execute(text("""
            INSERT INTO ride_status_snapshots
            (ride_id, recorded_at, wait_time, is_open, computed_is_open, status, last_updated_api, data_source)
            VALUES (:ride_id, :recorded_at, 30, 1, 1, 'OPERATING', :recorded_at, 'LIVE')
        """), {'ride_id': ride_id, 'recorded_at': datetime.now()})
        mysql_session.flush()

        # Query with date bounds (partition-friendly)
        result = mysql_session.execute(text("""
            SELECT COUNT(*) FROM ride_status_snapshots
            WHERE ride_id = :ride_id
            AND recorded_at >= :today_start
            AND recorded_at < :tomorrow_start
        """), {
            'ride_id': ride_id,
            'today_start': today_start,
            'tomorrow_start': tomorrow_start
        })
        count = result.scalar()

        assert count >= 1

    def test_yesterday_query_pattern(self, mysql_session, setup_test_data):
        """YESTERDAY period query uses proper date bounds."""
        ride_id = setup_test_data['ride_id']
        today_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        yesterday_start = today_start - timedelta(days=1)

        # Insert test data for yesterday
        mysql_session.execute(text("""
            INSERT INTO ride_status_snapshots
            (ride_id, recorded_at, wait_time, is_open, computed_is_open, status, last_updated_api, data_source)
            VALUES (:ride_id, :recorded_at, 25, 1, 1, 'OPERATING', :recorded_at, 'LIVE')
        """), {'ride_id': ride_id, 'recorded_at': yesterday_start + timedelta(hours=12)})
        mysql_session.flush()

        # Query with date bounds
        result = mysql_session.execute(text("""
            SELECT COUNT(*) FROM ride_status_snapshots
            WHERE ride_id = :ride_id
            AND recorded_at >= :yesterday_start
            AND recorded_at < :today_start
        """), {
            'ride_id': ride_id,
            'yesterday_start': yesterday_start,
            'today_start': today_start
        })
        count = result.scalar()

        assert count >= 1

    def test_week_query_pattern(self, mysql_session, setup_test_data):
        """last_week period query uses proper date bounds."""
        ride_id = setup_test_data['ride_id']
        today_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        week_ago = today_start - timedelta(days=7)

        # Insert test data for 3 days ago
        mysql_session.execute(text("""
            INSERT INTO ride_status_snapshots
            (ride_id, recorded_at, wait_time, is_open, computed_is_open, status, last_updated_api, data_source)
            VALUES (:ride_id, :recorded_at, 35, 1, 1, 'OPERATING', :recorded_at, 'LIVE')
        """), {'ride_id': ride_id, 'recorded_at': today_start - timedelta(days=3)})
        mysql_session.flush()

        # Query with date bounds
        result = mysql_session.execute(text("""
            SELECT COUNT(*) FROM ride_status_snapshots
            WHERE ride_id = :ride_id
            AND recorded_at >= :week_ago
            AND recorded_at < :today_start
        """), {
            'ride_id': ride_id,
            'week_ago': week_ago,
            'today_start': today_start
        })
        count = result.scalar()

        assert count >= 1

    def test_year_over_year_comparison(self, mysql_session, setup_test_data):
        """Year-over-year comparison query targets specific partitions."""
        ride_id = setup_test_data['ride_id']

        # Insert data for this year and last year
        this_year = datetime(2025, 6, 15, 12, 0, 0)
        last_year = datetime(2024, 6, 15, 12, 0, 0)

        for ts, wait in [(this_year, 30), (last_year, 25)]:
            mysql_session.execute(text("""
                INSERT INTO ride_status_snapshots
                (ride_id, recorded_at, wait_time, is_open, computed_is_open, status, last_updated_api, data_source)
                VALUES (:ride_id, :recorded_at, :wait_time, 1, 1, 'OPERATING', :recorded_at, 'ARCHIVE')
            """), {'ride_id': ride_id, 'recorded_at': ts, 'wait_time': wait})
        mysql_session.flush()

        # Year-over-year comparison query
        result = mysql_session.execute(text("""
            SELECT
                YEAR(recorded_at) as year,
                AVG(wait_time) as avg_wait
            FROM ride_status_snapshots
            WHERE ride_id = :ride_id
            AND (
                (recorded_at >= '2025-06-01' AND recorded_at < '2025-07-01')
                OR
                (recorded_at >= '2024-06-01' AND recorded_at < '2024-07-01')
            )
            GROUP BY YEAR(recorded_at)
            ORDER BY year
        """), {'ride_id': ride_id})
        rows = result.fetchall()

        # Should have data from both years
        assert len(rows) == 2
        assert rows[0][0] == 2024
        assert rows[1][0] == 2025


class TestPartitionMetadata:
    """
    Tests for partition metadata and information_schema queries.
    """

    @pytest.fixture
    def check_partitioned(self, mysql_session):
        """Check if table is partitioned."""
        result = mysql_session.execute(text("""
            SELECT COUNT(*) as partition_count
            FROM information_schema.partitions
            WHERE table_schema = DATABASE()
            AND table_name = 'ride_status_snapshots'
            AND partition_name IS NOT NULL
        """))
        count = result.scalar()

        if count == 0:
            pytest.skip("ride_status_snapshots is not partitioned")

        return count

    def test_partition_list_query(self, mysql_session, check_partitioned):
        """Can query partition metadata from information_schema."""
        result = mysql_session.execute(text("""
            SELECT
                partition_name,
                partition_description,
                table_rows
            FROM information_schema.partitions
            WHERE table_schema = DATABASE()
            AND table_name = 'ride_status_snapshots'
            AND partition_name IS NOT NULL
            ORDER BY partition_name
            LIMIT 10
        """))
        rows = result.fetchall()

        assert len(rows) > 0
        # First partition should be p_before_2024 or p202401
        assert rows[0][0].startswith('p')

    def test_partition_row_distribution(self, mysql_session, check_partitioned):
        """Can query row counts per partition."""
        result = mysql_session.execute(text("""
            SELECT
                partition_name,
                table_rows
            FROM information_schema.partitions
            WHERE table_schema = DATABASE()
            AND table_name = 'ride_status_snapshots'
            AND partition_name IS NOT NULL
            AND table_rows > 0
            ORDER BY table_rows DESC
            LIMIT 5
        """))
        rows = result.fetchall()

        # This is informational - may have no data in test db
        # Just verify query works
        assert True

    def test_partition_data_length(self, mysql_session, check_partitioned):
        """Can query storage size per partition."""
        result = mysql_session.execute(text("""
            SELECT
                partition_name,
                data_length,
                index_length,
                (data_length + index_length) as total_size
            FROM information_schema.partitions
            WHERE table_schema = DATABASE()
            AND table_name = 'ride_status_snapshots'
            AND partition_name IS NOT NULL
            ORDER BY total_size DESC
            LIMIT 5
        """))
        rows = result.fetchall()

        # Verify query executes successfully
        assert True
