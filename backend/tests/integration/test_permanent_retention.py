"""
Integration Tests: Permanent Data Retention (Feature 004)

Verifies that ride_status_snapshots data is retained permanently
for historical analysis and archive import.

Feature: 004-themeparks-data-collection
Task: T038
"""

import pytest
from datetime import datetime, timedelta
from sqlalchemy import text, select, func
from sqlalchemy.orm import Session


class TestPermanentRetention:
    """
    Integration tests for permanent data retention.

    Validates that:
    1. Snapshots with data_source='LIVE' are created correctly
    2. Snapshots with data_source='ARCHIVE' are created correctly
    3. Data older than 24 hours is NOT deleted
    4. Both LIVE and ARCHIVE data coexist without conflicts
    """

    @pytest.fixture
    def setup_test_park(self, mysql_session):
        """Create test park for snapshot tests."""
        mysql_session.execute(text("""
            INSERT INTO parks (queue_times_id, name, city, state_province, country, timezone, is_active)
            VALUES (99001, 'Retention Test Park', 'Test City', 'CA', 'US', 'America/Los_Angeles', 1)
        """))
        mysql_session.flush()

        result = mysql_session.execute(text(
            "SELECT park_id FROM parks WHERE queue_times_id = 99001"
        ))
        park_id = result.scalar()
        return park_id

    @pytest.fixture
    def setup_test_ride(self, mysql_session, setup_test_park):
        """Create test ride for snapshot tests."""
        park_id = setup_test_park

        mysql_session.execute(text("""
            INSERT INTO rides (queue_times_id, park_id, name, is_active)
            VALUES (990001, :park_id, 'Retention Test Ride', 1)
        """), {'park_id': park_id})
        mysql_session.flush()

        result = mysql_session.execute(text(
            "SELECT ride_id FROM rides WHERE queue_times_id = 990001"
        ))
        ride_id = result.scalar()
        return ride_id

    def test_live_snapshot_includes_data_source(self, mysql_session, setup_test_ride):
        """LIVE snapshots are tagged with data_source='LIVE'."""
        ride_id = setup_test_ride
        now = datetime.utcnow()

        # Insert LIVE snapshot
        mysql_session.execute(text("""
            INSERT INTO ride_status_snapshots
            (ride_id, recorded_at, wait_time, is_open, computed_is_open, status, last_updated_api, data_source)
            VALUES (:ride_id, :recorded_at, 30, 1, 1, 'OPERATING', :recorded_at, 'LIVE')
        """), {'ride_id': ride_id, 'recorded_at': now})
        mysql_session.flush()

        # Verify data_source is set
        result = mysql_session.execute(text("""
            SELECT data_source FROM ride_status_snapshots
            WHERE ride_id = :ride_id ORDER BY snapshot_id DESC LIMIT 1
        """), {'ride_id': ride_id})
        data_source = result.scalar()

        assert data_source == 'LIVE'

    def test_archive_snapshot_includes_data_source(self, mysql_session, setup_test_ride):
        """ARCHIVE snapshots are tagged with data_source='ARCHIVE'."""
        ride_id = setup_test_ride
        # Historical timestamp from 6 months ago
        historical_time = datetime.utcnow() - timedelta(days=180)

        # Insert ARCHIVE snapshot
        mysql_session.execute(text("""
            INSERT INTO ride_status_snapshots
            (ride_id, recorded_at, wait_time, is_open, computed_is_open, status, last_updated_api, data_source)
            VALUES (:ride_id, :recorded_at, 45, 1, 1, 'OPERATING', :recorded_at, 'ARCHIVE')
        """), {'ride_id': ride_id, 'recorded_at': historical_time})
        mysql_session.flush()

        # Verify data_source is set
        result = mysql_session.execute(text("""
            SELECT data_source FROM ride_status_snapshots
            WHERE ride_id = :ride_id AND recorded_at = :recorded_at
        """), {'ride_id': ride_id, 'recorded_at': historical_time})
        data_source = result.scalar()

        assert data_source == 'ARCHIVE'

    def test_old_data_persists_beyond_24_hours(self, mysql_session, setup_test_ride):
        """Snapshots older than 24 hours are NOT deleted (permanent retention)."""
        ride_id = setup_test_ride

        # Insert snapshot from 48 hours ago
        old_time = datetime.utcnow() - timedelta(hours=48)
        mysql_session.execute(text("""
            INSERT INTO ride_status_snapshots
            (ride_id, recorded_at, wait_time, is_open, computed_is_open, status, last_updated_api, data_source)
            VALUES (:ride_id, :recorded_at, 25, 1, 1, 'OPERATING', :recorded_at, 'LIVE')
        """), {'ride_id': ride_id, 'recorded_at': old_time})
        mysql_session.flush()

        # Verify the old snapshot exists
        result = mysql_session.execute(text("""
            SELECT COUNT(*) FROM ride_status_snapshots
            WHERE ride_id = :ride_id AND recorded_at = :recorded_at
        """), {'ride_id': ride_id, 'recorded_at': old_time})
        count = result.scalar()

        assert count == 1, "Old snapshot should persist (permanent retention)"

    def test_live_and_archive_coexist(self, mysql_session, setup_test_ride):
        """LIVE and ARCHIVE data can coexist for the same ride."""
        ride_id = setup_test_ride
        now = datetime.utcnow()
        historical_time = datetime.utcnow() - timedelta(days=180)

        # Insert both LIVE and ARCHIVE snapshots
        mysql_session.execute(text("""
            INSERT INTO ride_status_snapshots
            (ride_id, recorded_at, wait_time, is_open, computed_is_open, status, last_updated_api, data_source)
            VALUES
            (:ride_id, :now, 30, 1, 1, 'OPERATING', :now, 'LIVE'),
            (:ride_id, :historical, 45, 1, 1, 'OPERATING', :historical, 'ARCHIVE')
        """), {'ride_id': ride_id, 'now': now, 'historical': historical_time})
        mysql_session.flush()

        # Count by data_source
        result = mysql_session.execute(text("""
            SELECT data_source, COUNT(*) as cnt
            FROM ride_status_snapshots
            WHERE ride_id = :ride_id
            GROUP BY data_source
            ORDER BY data_source
        """), {'ride_id': ride_id})
        rows = result.fetchall()

        data_sources = {row[0]: row[1] for row in rows}
        assert data_sources.get('ARCHIVE') == 1, "Should have 1 ARCHIVE snapshot"
        assert data_sources.get('LIVE') == 1, "Should have 1 LIVE snapshot"

    def test_data_source_query_filtering(self, mysql_session, setup_test_ride):
        """Queries can filter by data_source for analysis."""
        ride_id = setup_test_ride
        now = datetime.utcnow()

        # Insert multiple snapshots with different sources
        mysql_session.execute(text("""
            INSERT INTO ride_status_snapshots
            (ride_id, recorded_at, wait_time, is_open, computed_is_open, status, last_updated_api, data_source)
            VALUES
            (:ride_id, :t1, 30, 1, 1, 'OPERATING', :t1, 'LIVE'),
            (:ride_id, :t2, 35, 1, 1, 'OPERATING', :t2, 'LIVE'),
            (:ride_id, :t3, 40, 1, 1, 'OPERATING', :t3, 'ARCHIVE'),
            (:ride_id, :t4, 45, 1, 1, 'OPERATING', :t4, 'ARCHIVE')
        """), {
            'ride_id': ride_id,
            't1': now - timedelta(hours=1),
            't2': now,
            't3': now - timedelta(days=30),
            't4': now - timedelta(days=60)
        })
        mysql_session.flush()

        # Query LIVE only
        result = mysql_session.execute(text("""
            SELECT COUNT(*) FROM ride_status_snapshots
            WHERE ride_id = :ride_id AND data_source = 'LIVE'
        """), {'ride_id': ride_id})
        live_count = result.scalar()

        # Query ARCHIVE only
        result = mysql_session.execute(text("""
            SELECT COUNT(*) FROM ride_status_snapshots
            WHERE ride_id = :ride_id AND data_source = 'ARCHIVE'
        """), {'ride_id': ride_id})
        archive_count = result.scalar()

        assert live_count == 2, "Should have 2 LIVE snapshots"
        assert archive_count == 2, "Should have 2 ARCHIVE snapshots"

    def test_old_archive_data_persists(self, mysql_session, setup_test_ride):
        """Archive data from months ago persists (for historical analysis)."""
        ride_id = setup_test_ride

        # Insert snapshots from various historical periods
        historical_dates = [
            datetime.utcnow() - timedelta(days=30),   # 1 month ago
            datetime.utcnow() - timedelta(days=90),   # 3 months ago
            datetime.utcnow() - timedelta(days=180),  # 6 months ago
            datetime.utcnow() - timedelta(days=365),  # 1 year ago
        ]

        for idx, ts in enumerate(historical_dates):
            mysql_session.execute(text("""
                INSERT INTO ride_status_snapshots
                (ride_id, recorded_at, wait_time, is_open, computed_is_open, status, last_updated_api, data_source)
                VALUES (:ride_id, :recorded_at, :wait_time, 1, 1, 'OPERATING', :recorded_at, 'ARCHIVE')
            """), {'ride_id': ride_id, 'recorded_at': ts, 'wait_time': 10 + idx * 5})
        mysql_session.flush()

        # Verify all historical snapshots exist
        result = mysql_session.execute(text("""
            SELECT COUNT(*) FROM ride_status_snapshots
            WHERE ride_id = :ride_id AND data_source = 'ARCHIVE'
        """), {'ride_id': ride_id})
        count = result.scalar()

        assert count == 4, "All historical ARCHIVE snapshots should persist"


class TestRetentionDataQuality:
    """
    Tests for data quality in retained snapshots.
    """

    @pytest.fixture
    def setup_test_park(self, mysql_session):
        """Create test park."""
        mysql_session.execute(text("""
            INSERT INTO parks (queue_times_id, name, city, state_province, country, timezone, is_active)
            VALUES (99002, 'Data Quality Test Park', 'Test City', 'CA', 'US', 'America/Los_Angeles', 1)
        """))
        mysql_session.flush()

        result = mysql_session.execute(text(
            "SELECT park_id FROM parks WHERE queue_times_id = 99002"
        ))
        return result.scalar()

    @pytest.fixture
    def setup_test_ride(self, mysql_session, setup_test_park):
        """Create test ride."""
        park_id = setup_test_park

        mysql_session.execute(text("""
            INSERT INTO rides (queue_times_id, park_id, name, is_active)
            VALUES (990002, :park_id, 'Data Quality Test Ride', 1)
        """), {'park_id': park_id})
        mysql_session.flush()

        result = mysql_session.execute(text(
            "SELECT ride_id FROM rides WHERE queue_times_id = 990002"
        ))
        return result.scalar()

    def test_data_source_default_value(self, mysql_session, setup_test_ride):
        """Default data_source is 'LIVE' if not specified."""
        ride_id = setup_test_ride
        now = datetime.utcnow()

        # Insert without explicit data_source (should default to LIVE)
        mysql_session.execute(text("""
            INSERT INTO ride_status_snapshots
            (ride_id, recorded_at, wait_time, is_open, computed_is_open, status, last_updated_api)
            VALUES (:ride_id, :recorded_at, 30, 1, 1, 'OPERATING', :recorded_at)
        """), {'ride_id': ride_id, 'recorded_at': now})
        mysql_session.flush()

        # Verify default value
        result = mysql_session.execute(text("""
            SELECT data_source FROM ride_status_snapshots
            WHERE ride_id = :ride_id ORDER BY snapshot_id DESC LIMIT 1
        """), {'ride_id': ride_id})
        data_source = result.scalar()

        assert data_source == 'LIVE', "Default data_source should be 'LIVE'"

    def test_data_source_enum_validation(self, mysql_session, setup_test_ride):
        """Only valid data_source values are accepted."""
        ride_id = setup_test_ride
        now = datetime.utcnow()

        # Try to insert with invalid data_source
        with pytest.raises(Exception):
            mysql_session.execute(text("""
                INSERT INTO ride_status_snapshots
                (ride_id, recorded_at, wait_time, is_open, computed_is_open, status, last_updated_api, data_source)
                VALUES (:ride_id, :recorded_at, 30, 1, 1, 'OPERATING', :recorded_at, 'INVALID')
            """), {'ride_id': ride_id, 'recorded_at': now})
            mysql_session.flush()

    def test_archive_preserves_original_timestamps(self, mysql_session, setup_test_ride):
        """Archive imports preserve original recorded_at timestamps."""
        ride_id = setup_test_ride

        # Historical timestamp with specific time
        original_time = datetime(2024, 7, 15, 14, 30, 0)

        mysql_session.execute(text("""
            INSERT INTO ride_status_snapshots
            (ride_id, recorded_at, wait_time, is_open, computed_is_open, status, last_updated_api, data_source)
            VALUES (:ride_id, :recorded_at, 45, 1, 1, 'OPERATING', :recorded_at, 'ARCHIVE')
        """), {'ride_id': ride_id, 'recorded_at': original_time})
        mysql_session.flush()

        # Verify timestamp is preserved exactly
        result = mysql_session.execute(text("""
            SELECT recorded_at FROM ride_status_snapshots
            WHERE ride_id = :ride_id AND data_source = 'ARCHIVE'
        """), {'ride_id': ride_id})
        stored_time = result.scalar()

        assert stored_time == original_time, "Original timestamp should be preserved exactly"
