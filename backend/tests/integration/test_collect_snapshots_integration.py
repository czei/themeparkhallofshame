"""
Theme Park Downtime Tracker - Snapshot Collection Integration Tests

Tests the complete snapshot collection pipeline:
- Fetches wait times from Queue-Times API (mocked)
- Stores ride snapshots in MySQL database
- Detects status changes
- Records park activity

Priority: P0 - Critical production code (runs every 10 minutes)
Coverage: 0% → Target 80%+
"""

import pytest
import sys
from pathlib import Path
from datetime import datetime, timedelta
from unittest.mock import Mock, patch, MagicMock
from typing import Dict, List

backend_src = Path(__file__).parent.parent.parent / 'src'
sys.path.insert(0, str(backend_src.absolute()))

from scripts.collect_snapshots import SnapshotCollector
from database.repositories.park_repository import ParkRepository
from database.repositories.ride_repository import RideRepository
from database.repositories.snapshot_repository import RideStatusSnapshotRepository, ParkActivitySnapshotRepository
from database.repositories.status_change_repository import RideStatusChangeRepository


# ============================================================================
# FIXTURES - Sample API Responses
# ============================================================================

@pytest.fixture
def sample_api_response_open_park():
    """Queue-Times API response for park with multiple operating rides."""
    return {
        'id': 101,
        'name': 'Magic Kingdom',
        'rides': [
            {
                'id': 1001,
                'name': 'Space Mountain',
                'wait_time': 45,
                'is_open': True,
                'land': 'Tomorrowland'
            },
            {
                'id': 1002,
                'name': 'Haunted Mansion',
                'wait_time': 30,
                'is_open': True,
                'land': 'Liberty Square'
            },
            {
                'id': 1003,
                'name': 'Big Thunder Mountain',
                'wait_time': 0,
                'is_open': False,  # Closed for maintenance
                'land': 'Frontierland'
            },
            {
                'id': 1004,
                'name': 'Pirates of the Caribbean',
                'wait_time': 25,
                'is_open': True,
                'land': 'Adventureland'
            }
        ]
    }


@pytest.fixture
def sample_api_response_closed_park():
    """Queue-Times API response for park that appears closed."""
    return {
        'id': 101,
        'name': 'Magic Kingdom',
        'rides': [
            {
                'id': 1001,
                'name': 'Space Mountain',
                'wait_time': 0,
                'is_open': False,
                'land': 'Tomorrowland'
            },
            {
                'id': 1002,
                'name': 'Haunted Mansion',
                'wait_time': 0,
                'is_open': False,
                'land': 'Liberty Square'
            }
        ]
    }


@pytest.fixture
def sample_api_response_empty():
    """Queue-Times API response with no rides."""
    return {
        'id': 101,
        'name': 'Magic Kingdom',
        'rides': []
    }


# ============================================================================
# FIXTURES - Database Setup
# ============================================================================

@pytest.fixture
def setup_test_park(mysql_connection):
    """Create a test park in database and return park_id."""
    park_repo = ParkRepository(mysql_connection)

    park_data = {
        'queue_times_id': 101,
        'name': 'Magic Kingdom',
        'city': 'Orlando',
        'state_province': 'FL',
        'country': 'US',
        'latitude': 28.417663,
        'longitude': -81.581213,
        'timezone': 'America/New_York',
        'operator': 'Disney',
        'is_disney': True,
        'is_universal': False,
        'is_active': True
    }

    park = park_repo.create(park_data)
    # park_repo.create() returns a Park object, extract park_id
    return park.park_id if hasattr(park, 'park_id') else park['park_id']


@pytest.fixture
def setup_test_rides(mysql_connection, setup_test_park):
    """Create test rides in database and return list of ride_ids."""
    park_id = setup_test_park
    ride_repo = RideRepository(mysql_connection)

    rides_data = [
        {
            'queue_times_id': 1001,
            'park_id': park_id,
            'name': 'Space Mountain',
            'land_area': 'Tomorrowland',
            'tier': 1,
            'is_active': True
        },
        {
            'queue_times_id': 1002,
            'park_id': park_id,
            'name': 'Haunted Mansion',
            'land_area': 'Liberty Square',
            'tier': 1,
            'is_active': True
        },
        {
            'queue_times_id': 1003,
            'park_id': park_id,
            'name': 'Big Thunder Mountain',
            'land_area': 'Frontierland',
            'tier': 1,
            'is_active': True
        },
        {
            'queue_times_id': 1004,
            'park_id': park_id,
            'name': 'Pirates of the Caribbean',
            'land_area': 'Adventureland',
            'tier': 2,
            'is_active': True
        }
    ]

    ride_ids = []
    for ride_data in rides_data:
        ride = ride_repo.create(ride_data)
        # ride_repo.create() returns a Ride object, extract ride_id
        ride_ids.append(ride.ride_id if hasattr(ride, 'ride_id') else ride['ride_id'])

    return ride_ids


# ============================================================================
# TEST: Basic Collection Flow
# ============================================================================

class TestSnapshotCollectionBasicFlow:
    """Test basic snapshot collection workflow."""

    @patch('scripts.collect_snapshots.get_db_connection')
    @patch('scripts.collect_snapshots.QueueTimesClient')
    def test_collect_snapshots_creates_snapshots_in_database(
        self, mock_client_class, mock_get_db, mysql_connection,
        setup_test_rides, sample_api_response_open_park
    ):
        """Snapshot collection should create ride snapshots in database."""
        # Setup mocks
        mock_get_db.return_value.__enter__.return_value = mysql_connection
        mock_get_db.return_value.__exit__.return_value = None

        mock_client = Mock()
        mock_client_class.return_value = mock_client
        mock_client.get_park_wait_times.return_value = sample_api_response_open_park

        # Run collection
        collector = SnapshotCollector()
        collector.run()

        # Verify snapshots created
        snapshot_repo = RideStatusSnapshotRepository(mysql_connection)

        # Check Space Mountain snapshot (45 min wait)
        from database.repositories.ride_repository import RideRepository
        ride_repo = RideRepository(mysql_connection)
        space_mountain = ride_repo.get_by_queue_times_id(1001)
        snapshot = snapshot_repo.get_latest_by_ride(space_mountain.ride_id)

        assert snapshot is not None
        assert snapshot['wait_time'] == 45
        assert snapshot['is_open'] == True
        assert snapshot['computed_is_open'] == True

    @patch('scripts.collect_snapshots.get_db_connection')
    @patch('scripts.collect_snapshots.QueueTimesClient')
    def test_collect_snapshots_creates_park_activity_snapshot(
        self, mock_client_class, mock_get_db, mysql_connection,
        setup_test_rides, sample_api_response_open_park
    ):
        """Snapshot collection should create park activity snapshot."""
        # Setup mocks
        mock_get_db.return_value.__enter__.return_value = mysql_connection
        mock_get_db.return_value.__exit__.return_value = None

        mock_client = Mock()
        mock_client_class.return_value = mock_client
        mock_client.get_park_wait_times.return_value = sample_api_response_open_park

        # Run collection
        collector = SnapshotCollector()
        collector.run()

        # Verify park activity snapshot
        from database.repositories.park_repository import ParkRepository
        park_repo = ParkRepository(mysql_connection)
        park = park_repo.get_by_queue_times_id(101)

        park_activity_repo = ParkActivitySnapshotRepository(mysql_connection)
        activity = park_activity_repo.get_latest_by_park(park.park_id)

        assert activity is not None
        assert activity['park_appears_open'] == True
        assert activity['rides_open'] == 3  # 3 rides with wait_time > 0 or is_open
        assert activity['rides_closed'] == 1  # 1 ride closed
        assert activity['total_rides_tracked'] == 4


# ============================================================================
# TEST: Status Change Detection
# ============================================================================

class TestStatusChangeDetection:
    """Test ride status change detection."""

    @patch('scripts.collect_snapshots.get_db_connection')
    @patch('scripts.collect_snapshots.QueueTimesClient')
    def test_detects_ride_going_down(
        self, mock_client_class, mock_get_db, mysql_connection, setup_test_rides
    ):
        """Should detect and record when ride goes from open to closed."""
        # Setup mocks
        mock_get_db.return_value.__enter__.return_value = mysql_connection
        mock_get_db.return_value.__exit__.return_value = None

        mock_client = Mock()
        mock_client_class.return_value = mock_client

        # First collection - ride is open
        mock_client.get_park_wait_times.return_value = {
            'id': 101,
            'name': 'Magic Kingdom',
            'rides': [
                {'id': 1001, 'name': 'Space Mountain', 'wait_time': 45, 'is_open': True}
            ]
        }

        collector1 = SnapshotCollector()
        collector1.run()

        # Second collection - ride is closed
        mock_client.get_park_wait_times.return_value = {
            'id': 101,
            'name': 'Magic Kingdom',
            'rides': [
                {'id': 1001, 'name': 'Space Mountain', 'wait_time': 0, 'is_open': False}
            ]
        }

        collector2 = SnapshotCollector()
        collector2.run()

        # Verify status change recorded
        status_change_repo = RideStatusChangeRepository(mysql_connection)
        from database.repositories.ride_repository import RideRepository
        ride_repo = RideRepository(mysql_connection)
        space_mountain = ride_repo.get_by_queue_times_id(1001)

        # Get recent status changes for this ride
        from sqlalchemy import text
        result = mysql_connection.execute(
            text("SELECT * FROM ride_status_changes WHERE ride_id = :ride_id ORDER BY changed_at DESC LIMIT 1"),
            {'ride_id': space_mountain.ride_id}
        )
        row = result.fetchone()

        assert row is not None
        change = dict(row._mapping)  # Convert Row to dict
        assert change['previous_status'] == True  # Was open
        assert change['new_status'] == False  # Now closed


# ============================================================================
# TEST: Error Handling
# ============================================================================

class TestErrorHandling:
    """Test error handling in snapshot collection."""

    @patch('scripts.collect_snapshots.get_db_connection')
    @patch('scripts.collect_snapshots.QueueTimesClient')
    def test_handles_api_timeout(
        self, mock_client_class, mock_get_db, mysql_connection, setup_test_park
    ):
        """Should handle API timeout gracefully."""
        # Setup mocks
        mock_get_db.return_value.__enter__.return_value = mysql_connection
        mock_get_db.return_value.__exit__.return_value = None

        mock_client = Mock()
        mock_client_class.return_value = mock_client
        mock_client.get_park_wait_times.side_effect = Exception("Connection timeout")

        # Run collection - should not crash
        collector = SnapshotCollector()
        collector.run()

        # Verify error was tracked
        assert collector.stats['errors'] == 1
        assert collector.stats['parks_processed'] == 1
        assert collector.stats['snapshots_created'] == 0

    @patch('scripts.collect_snapshots.get_db_connection')
    @patch('scripts.collect_snapshots.QueueTimesClient')
    def test_handles_empty_api_response(
        self, mock_client_class, mock_get_db, mysql_connection,
        setup_test_park, sample_api_response_empty
    ):
        """Should handle empty API response (no rides)."""
        # Setup mocks
        mock_get_db.return_value.__enter__.return_value = mysql_connection
        mock_get_db.return_value.__exit__.return_value = None

        mock_client = Mock()
        mock_client_class.return_value = mock_client
        mock_client.get_park_wait_times.return_value = sample_api_response_empty

        # Run collection
        collector = SnapshotCollector()
        collector.run()

        # Should complete without errors
        assert collector.stats['parks_processed'] == 1
        assert collector.stats['rides_processed'] == 0
        assert collector.stats['snapshots_created'] == 0


# ============================================================================
# TEST: Statistics Tracking
# ============================================================================

class TestStatisticsTracking:
    """Test collection statistics tracking."""

    @patch('scripts.collect_snapshots.get_db_connection')
    @patch('scripts.collect_snapshots.QueueTimesClient')
    def test_tracks_collection_statistics(
        self, mock_client_class, mock_get_db, mysql_connection,
        setup_test_rides, sample_api_response_open_park
    ):
        """Should track collection statistics correctly."""
        # Setup mocks
        mock_get_db.return_value.__enter__.return_value = mysql_connection
        mock_get_db.return_value.__exit__.return_value = None

        mock_client = Mock()
        mock_client_class.return_value = mock_client
        mock_client.get_park_wait_times.return_value = sample_api_response_open_park

        # Run collection
        collector = SnapshotCollector()
        collector.run()

        # Verify statistics
        assert collector.stats['parks_processed'] == 1
        assert collector.stats['rides_processed'] == 4  # 4 rides in sample data
        assert collector.stats['snapshots_created'] == 4
        assert collector.stats['errors'] == 0
