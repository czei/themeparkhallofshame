"""
Theme Park Downtime Tracker - Status Change Repository Unit Tests

Tests RideStatusChangeRepository:
- Insert status change events
- Query operations (latest, history, downtime events)
- Duration calculations

Priority: P1 - Critical for downtime tracking
"""

import pytest
import sys
from pathlib import Path
from datetime import datetime

backend_src = Path(__file__).parent.parent.parent / 'src'
sys.path.insert(0, str(backend_src.absolute()))

from database.repositories.status_change_repository import RideStatusChangeRepository


class TestRideStatusChangeRepository:
    """Test ride status change operations."""

    def test_insert_status_change(self, mysql_connection, sample_park_data, sample_ride_data):
        """Insert a status change event."""
        from tests.conftest import insert_sample_park, insert_sample_ride

        park_id = insert_sample_park(mysql_connection, sample_park_data)
        sample_ride_data['park_id'] = park_id
        ride_id = insert_sample_ride(mysql_connection, sample_ride_data)

        repo = RideStatusChangeRepository(mysql_connection)
        change_data = {
            'ride_id': ride_id,
            'changed_at': datetime.now(),
            'previous_status': True,
            'new_status': False,
            'duration_in_previous_status': 120, 'wait_time_at_change': None
        }

        change_id = repo.insert(change_data)

        assert change_id is not None
        assert change_id > 0

    def test_get_latest_by_ride(self, mysql_connection, sample_park_data, sample_ride_data):
        """Get most recent status change for a ride."""
        from tests.conftest import insert_sample_park, insert_sample_ride

        park_id = insert_sample_park(mysql_connection, sample_park_data)
        sample_ride_data['park_id'] = park_id
        ride_id = insert_sample_ride(mysql_connection, sample_ride_data)

        repo = RideStatusChangeRepository(mysql_connection)

        # Insert 2 status changes with distinct timestamps
        from datetime import timedelta
        base_time = datetime.now()
        repo.insert({'ride_id': ride_id, 'changed_at': base_time - timedelta(seconds=10),
                    'previous_status': 1, 'new_status': 0, 'duration_in_previous_status': 60, 'wait_time_at_change': None})
        repo.insert({'ride_id': ride_id, 'changed_at': base_time,
                    'previous_status': 0, 'new_status': 1, 'duration_in_previous_status': 120, 'wait_time_at_change': None})

        latest = repo.get_latest_by_ride(ride_id)

        assert latest is not None
        assert latest['duration_in_previous_status'] == 120  # Latest change

    def test_get_latest_by_ride_not_found(self, mysql_connection):
        """Get latest status change for nonexistent ride."""
        repo = RideStatusChangeRepository(mysql_connection)

        result = repo.get_latest_by_ride(999)

        assert result is None

    @pytest.mark.skip(reason="Requires MySQL-specific DATE_SUB() function")
    def test_count_changes_by_ride(self, mysql_connection, sample_park_data, sample_ride_data):
        """Count status changes for a ride."""
        from tests.conftest import insert_sample_park, insert_sample_ride

        park_id = insert_sample_park(mysql_connection, sample_park_data)
        sample_ride_data['park_id'] = park_id
        ride_id = insert_sample_ride(mysql_connection, sample_ride_data)

        repo = RideStatusChangeRepository(mysql_connection)

        # Insert changes: 2 to open, 1 to closed
        repo.insert({'ride_id': ride_id, 'changed_at': datetime.now(),
                    'previous_status': 0, 'new_status': 1, 'duration_in_previous_status': 0, 'wait_time_at_change': None})
        repo.insert({'ride_id': ride_id, 'changed_at': datetime.now(),
                    'previous_status': 0, 'new_status': 1, 'duration_in_previous_status': 0, 'wait_time_at_change': None})
        repo.insert({'ride_id': ride_id, 'changed_at': datetime.now(),
                    'previous_status': 1, 'new_status': 0, 'duration_in_previous_status': 60, 'wait_time_at_change': None})

        counts = repo.count_changes_by_ride(ride_id)

        assert counts['total'] == 3
        assert counts['to_open'] == 2
        assert counts['to_closed'] == 1

    @pytest.mark.skip(reason="Requires MySQL-specific DATE_SUB() function")
    def test_count_changes_no_changes(self, mysql_connection):
        """Count changes for ride with no status changes."""
        repo = RideStatusChangeRepository(mysql_connection)

        counts = repo.count_changes_by_ride(999)

        assert counts['total'] == 0
        assert counts['to_open'] == 0
        assert counts['to_closed'] == 0
