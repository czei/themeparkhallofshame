"""
Theme Park Downtime Tracker - Snapshot Repository Unit Tests

Tests RideStatusSnapshotRepository and ParkActivitySnapshotRepository:
- Insert operations
- Latest snapshot queries
- Historical snapshot queries

Priority: P1 - Time-series data critical for tracking

NOTE (2025-12-24 ORM Migration):
- Repositories now use SQLAlchemy ORM Session
- Tests updated to use mysql_session fixture instead of mysql_connection
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

    def test_insert_snapshot(self, mysql_session):
        """Insert ride status snapshot."""
        from src.models import Park, Ride, RideStatusSnapshot

        # Create test park
        park = Park(
            queue_times_id=101,
            name='Magic Kingdom',
            city='Orlando',
            country='US',
            timezone='America/New_York',
            is_active=True
        )
        mysql_session.add(park)
        mysql_session.flush()

        # Create test ride
        ride = Ride(
            queue_times_id=1001,
            park_id=park.park_id,
            name='Space Mountain',
            is_active=True,
            category='ATTRACTION'
        )
        mysql_session.add(ride)
        mysql_session.flush()

        repo = RideStatusSnapshotRepository(mysql_session)
        snapshot_data = {
            'ride_id': ride.ride_id,
            'recorded_at': datetime.now(),
            'wait_time': 45,
            'is_open': True,
            'computed_is_open': True,
            'status': 'OPERATING',
            'last_updated_api': datetime.now()
        }

        snapshot_id = repo.insert(snapshot_data)

        assert snapshot_id is not None
        assert snapshot_id > 0

    def test_get_latest_by_ride(self, mysql_session):
        """Get most recent snapshot for a ride."""
        from src.models import Park, Ride
        from datetime import timedelta

        # Create test park and ride
        park = Park(
            queue_times_id=102,
            name='Disneyland',
            city='Anaheim',
            country='US',
            timezone='America/Los_Angeles',
            is_active=True
        )
        mysql_session.add(park)
        mysql_session.flush()

        ride = Ride(
            queue_times_id=1002,
            park_id=park.park_id,
            name='Matterhorn',
            is_active=True,
            category='ATTRACTION'
        )
        mysql_session.add(ride)
        mysql_session.flush()

        repo = RideStatusSnapshotRepository(mysql_session)

        # Insert 2 snapshots with distinct timestamps
        base_time = datetime.now()
        repo.insert({'ride_id': ride.ride_id, 'recorded_at': base_time - timedelta(seconds=10),
                    'wait_time': 30, 'is_open': 1, 'computed_is_open': 1,
                    'status': 'OPERATING', 'last_updated_api': base_time - timedelta(seconds=10)})
        repo.insert({'ride_id': ride.ride_id, 'recorded_at': base_time,
                    'wait_time': 45, 'is_open': 1, 'computed_is_open': 1,
                    'status': 'OPERATING', 'last_updated_api': base_time})

        latest = repo.get_latest_by_ride(ride.ride_id)

        assert latest is not None
        assert latest['wait_time'] == 45  # Latest snapshot

    def test_get_latest_by_ride_not_found(self, mysql_session):
        """Get latest snapshot for nonexistent ride."""
        repo = RideStatusSnapshotRepository(mysql_session)

        result = repo.get_latest_by_ride(999999)

        assert result is None

    def test_insert_snapshot_database_error(self, mysql_session):
        """Insert should raise exception on database error."""
        repo = RideStatusSnapshotRepository(mysql_session)

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

    def test_insert_activity(self, mysql_session):
        """Insert park activity snapshot."""
        from src.models import Park

        # Create test park
        park = Park(
            queue_times_id=103,
            name='EPCOT',
            city='Orlando',
            country='US',
            timezone='America/New_York',
            is_active=True
        )
        mysql_session.add(park)
        mysql_session.flush()

        repo = ParkActivitySnapshotRepository(mysql_session)
        activity_data = {
            'park_id': park.park_id,
            'recorded_at': datetime.now(),
            'park_appears_open': True,
            'rides_open': 25,
            'rides_closed': 5,
            'total_rides_tracked': 30,
            'avg_wait_time': 42.5,
            'max_wait_time': 90,
            'shame_score': 1.5
        }

        activity_id = repo.insert(activity_data)

        assert activity_id is not None
        assert activity_id > 0

    def test_get_latest_by_park(self, mysql_session):
        """Get most recent activity snapshot for a park."""
        from src.models import Park
        from datetime import timedelta

        # Create test park
        park = Park(
            queue_times_id=104,
            name='Hollywood Studios',
            city='Orlando',
            country='US',
            timezone='America/New_York',
            is_active=True
        )
        mysql_session.add(park)
        mysql_session.flush()

        repo = ParkActivitySnapshotRepository(mysql_session)

        # Insert 2 snapshots with distinct timestamps
        base_time = datetime.now()
        repo.insert({'park_id': park.park_id, 'recorded_at': base_time - timedelta(seconds=10),
                    'park_appears_open': 1, 'rides_open': 20, 'rides_closed': 10,
                    'total_rides_tracked': 30, 'avg_wait_time': 35.0, 'max_wait_time': 60,
                    'shame_score': 1.0})
        repo.insert({'park_id': park.park_id, 'recorded_at': base_time,
                    'park_appears_open': 1, 'rides_open': 25, 'rides_closed': 5,
                    'total_rides_tracked': 30, 'avg_wait_time': 42.5, 'max_wait_time': 90,
                    'shame_score': 1.5})

        latest = repo.get_latest_by_park(park.park_id)

        assert latest is not None
        assert latest['rides_open'] == 25  # Latest snapshot

    def test_get_latest_by_park_not_found(self, mysql_session):
        """Get latest snapshot for nonexistent park."""
        repo = ParkActivitySnapshotRepository(mysql_session)

        result = repo.get_latest_by_park(999999)

        assert result is None

    def test_insert_activity_database_error(self, mysql_session):
        """Insert should raise exception on database error."""
        repo = ParkActivitySnapshotRepository(mysql_session)

        # Invalid data that will cause database error (missing required fields)
        invalid_data = {
            'park_id': None,  # NULL constraint violation
            'recorded_at': datetime.now(),
            'park_appears_open': True
        }

        with pytest.raises(Exception):
            repo.insert(invalid_data)
