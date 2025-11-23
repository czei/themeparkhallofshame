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

    def test_insert_snapshot(self, sqlite_connection, sample_park_data, sample_ride_data):
        """Insert ride status snapshot."""
        from tests.conftest import insert_sample_park, insert_sample_ride

        park_id = insert_sample_park(sqlite_connection, sample_park_data)
        sample_ride_data['park_id'] = park_id
        ride_id = insert_sample_ride(sqlite_connection, sample_ride_data)

        repo = RideStatusSnapshotRepository(sqlite_connection)
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

    def test_get_latest_by_ride(self, sqlite_connection, sample_park_data, sample_ride_data):
        """Get most recent snapshot for a ride."""
        from tests.conftest import insert_sample_park, insert_sample_ride

        park_id = insert_sample_park(sqlite_connection, sample_park_data)
        sample_ride_data['park_id'] = park_id
        ride_id = insert_sample_ride(sqlite_connection, sample_ride_data)

        repo = RideStatusSnapshotRepository(sqlite_connection)

        # Insert 2 snapshots
        repo.insert({'ride_id': ride_id, 'recorded_at': datetime.now(),
                    'wait_time': 30, 'is_open': 1, 'computed_is_open': 1})
        repo.insert({'ride_id': ride_id, 'recorded_at': datetime.now(),
                    'wait_time': 45, 'is_open': 1, 'computed_is_open': 1})

        latest = repo.get_latest_by_ride(ride_id)

        assert latest is not None
        assert latest['wait_time'] == 45  # Latest snapshot

    def test_get_latest_by_ride_not_found(self, sqlite_connection):
        """Get latest snapshot for nonexistent ride."""
        repo = RideStatusSnapshotRepository(sqlite_connection)

        result = repo.get_latest_by_ride(999)

        assert result is None


class TestParkActivitySnapshotRepository:
    """Test park activity snapshot operations."""

    def test_insert_activity(self, sqlite_connection, sample_park_data):
        """Insert park activity snapshot."""
        from tests.conftest import insert_sample_park

        park_id = insert_sample_park(sqlite_connection, sample_park_data)

        repo = ParkActivitySnapshotRepository(sqlite_connection)
        activity_data = {
            'park_id': park_id,
            'recorded_at': datetime.now(),
            'park_appears_open': True,
            'active_rides_count': 25,
            'total_rides_count': 30
        }

        activity_id = repo.insert(activity_data)

        assert activity_id is not None
        assert activity_id > 0

    def test_get_latest_by_park(self, sqlite_connection, sample_park_data):
        """Get most recent activity snapshot for a park."""
        from tests.conftest import insert_sample_park

        park_id = insert_sample_park(sqlite_connection, sample_park_data)

        repo = ParkActivitySnapshotRepository(sqlite_connection)

        # Insert 2 snapshots
        repo.insert({'park_id': park_id, 'recorded_at': datetime.now(),
                    'park_appears_open': 1, 'active_rides_count': 20, 'total_rides_count': 30})
        repo.insert({'park_id': park_id, 'recorded_at': datetime.now(),
                    'park_appears_open': 1, 'active_rides_count': 25, 'total_rides_count': 30})

        latest = repo.get_latest_by_park(park_id)

        assert latest is not None
        assert latest['active_rides_count'] == 25  # Latest snapshot

    def test_get_latest_by_park_not_found(self, sqlite_connection):
        """Get latest snapshot for nonexistent park."""
        repo = ParkActivitySnapshotRepository(sqlite_connection)

        result = repo.get_latest_by_park(999)

        assert result is None
