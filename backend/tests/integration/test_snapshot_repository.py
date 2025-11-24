"""
Theme Park Downtime Tracker - Snapshot Repository Unit Tests

Tests RideStatusSnapshotRepository and ParkActivitySnapshotRepository:
- Insert operations
- Latest snapshot queries
- Historical snapshot queries

Priority: P1 - Time-series data critical for tracking
"""

import pytest
import sys
from pathlib import Path
from datetime import datetime

backend_src = Path(__file__).parent.parent.parent / 'src'
sys.path.insert(0, str(backend_src.absolute()))

from database.repositories.snapshot_repository import (
    RideStatusSnapshotRepository,
    ParkActivitySnapshotRepository
)


class TestRideStatusSnapshotRepository:
    """Test ride status snapshot operations."""

    def test_insert_snapshot(self, mysql_connection, sample_park_data, sample_ride_data):
        """Insert ride status snapshot."""
        from tests.conftest import insert_sample_park, insert_sample_ride

        park_id = insert_sample_park(mysql_connection, sample_park_data)
        sample_ride_data['park_id'] = park_id
        ride_id = insert_sample_ride(mysql_connection, sample_ride_data)

        repo = RideStatusSnapshotRepository(mysql_connection)
        snapshot_data = {
            'ride_id': ride_id,
            'recorded_at': datetime.now(),
            'wait_time': 45,
            'is_open': True,
            'computed_is_open': True
        }

        snapshot_id = repo.insert(snapshot_data)

        assert snapshot_id is not None
        assert snapshot_id > 0

    def test_get_latest_by_ride(self, mysql_connection, sample_park_data, sample_ride_data):
        """Get most recent snapshot for a ride."""
        from tests.conftest import insert_sample_park, insert_sample_ride

        park_id = insert_sample_park(mysql_connection, sample_park_data)
        sample_ride_data['park_id'] = park_id
        ride_id = insert_sample_ride(mysql_connection, sample_ride_data)

        repo = RideStatusSnapshotRepository(mysql_connection)

        # Insert 2 snapshots with distinct timestamps
        from datetime import timedelta
        base_time = datetime.now()
        repo.insert({'ride_id': ride_id, 'recorded_at': base_time - timedelta(seconds=10),
                    'wait_time': 30, 'is_open': 1, 'computed_is_open': 1})
        repo.insert({'ride_id': ride_id, 'recorded_at': base_time,
                    'wait_time': 45, 'is_open': 1, 'computed_is_open': 1})

        latest = repo.get_latest_by_ride(ride_id)

        assert latest is not None
        assert latest['wait_time'] == 45  # Latest snapshot

    def test_get_latest_by_ride_not_found(self, mysql_connection):
        """Get latest snapshot for nonexistent ride."""
        repo = RideStatusSnapshotRepository(mysql_connection)

        result = repo.get_latest_by_ride(999)

        assert result is None

    def test_insert_snapshot_database_error(self, mysql_connection):
        """Insert should raise exception on database error."""
        repo = RideStatusSnapshotRepository(mysql_connection)

        # Invalid data that will cause database error (missing required fields)
        invalid_data = {
            'ride_id': None,  # NULL constraint violation
            'recorded_at': datetime.now(),
            'wait_time': 45
        }

        with pytest.raises(Exception):
            repo.insert(invalid_data)


class TestParkActivitySnapshotRepository:
    """Test park activity snapshot operations."""

    def test_insert_activity(self, mysql_connection, sample_park_data):
        """Insert park activity snapshot."""
        from tests.conftest import insert_sample_park

        park_id = insert_sample_park(mysql_connection, sample_park_data)

        repo = ParkActivitySnapshotRepository(mysql_connection)
        activity_data = {
            'park_id': park_id,
            'recorded_at': datetime.now(),
            'park_appears_open': True,
            'rides_open': 25,
            'rides_closed': 5,
            'total_rides_tracked': 30,
            'avg_wait_time': 42.5,
            'max_wait_time': 90
        }

        activity_id = repo.insert(activity_data)

        assert activity_id is not None
        assert activity_id > 0

    def test_get_latest_by_park(self, mysql_connection, sample_park_data):
        """Get most recent activity snapshot for a park."""
        from tests.conftest import insert_sample_park

        park_id = insert_sample_park(mysql_connection, sample_park_data)

        repo = ParkActivitySnapshotRepository(mysql_connection)

        # Insert 2 snapshots with distinct timestamps
        from datetime import timedelta
        base_time = datetime.now()
        repo.insert({'park_id': park_id, 'recorded_at': base_time - timedelta(seconds=10),
                    'park_appears_open': 1, 'rides_open': 20, 'rides_closed': 10,
                    'total_rides_tracked': 30, 'avg_wait_time': 35.0, 'max_wait_time': 60})
        repo.insert({'park_id': park_id, 'recorded_at': base_time,
                    'park_appears_open': 1, 'rides_open': 25, 'rides_closed': 5,
                    'total_rides_tracked': 30, 'avg_wait_time': 42.5, 'max_wait_time': 90})

        latest = repo.get_latest_by_park(park_id)

        assert latest is not None
        assert latest['rides_open'] == 25  # Latest snapshot

    def test_get_latest_by_park_not_found(self, mysql_connection):
        """Get latest snapshot for nonexistent park."""
        repo = ParkActivitySnapshotRepository(mysql_connection)

        result = repo.get_latest_by_park(999)

        assert result is None

    def test_insert_activity_database_error(self, mysql_connection):
        """Insert should raise exception on database error."""
        repo = ParkActivitySnapshotRepository(mysql_connection)

        # Invalid data that will cause database error (missing required fields)
        invalid_data = {
            'park_id': None,  # NULL constraint violation
            'recorded_at': datetime.now(),
            'park_appears_open': True
        }

        with pytest.raises(Exception):
            repo.insert(invalid_data)
