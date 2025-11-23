"""
Theme Park Downtime Tracker - Operating Hours Detector Unit Tests

Tests OperatingHoursDetector:
- Detect operating sessions from ride activity
- Timezone handling (UTC ↔ local time)
- Calculate operating hours duration
- Multi-park detection

Priority: P1 - Critical for operating hours tracking
"""

import pytest
from datetime import datetime, date, time, timedelta
from zoneinfo import ZoneInfo

from processor.operating_hours_detector import OperatingHoursDetector


class TestDetectOperatingSession:
    """Test operating session detection logic."""

    def test_detect_no_activity(self, sqlite_connection, sample_park_data):
        """detect_operating_session() should return None when no ride activity exists."""
        from tests.conftest import insert_sample_park

        park_id = insert_sample_park(sqlite_connection, sample_park_data)

        detector = OperatingHoursDetector(sqlite_connection)
        operating_date = date(2024, 1, 1)
        park_timezone = 'America/New_York'

        session = detector.detect_operating_session(park_id, operating_date, park_timezone)

        assert session is None

    def test_detect_single_day_activity(self, sqlite_connection, sample_park_data, sample_ride_data):
        """detect_operating_session() should detect operating hours from ride snapshots."""
        from tests.conftest import insert_sample_park, insert_sample_ride
        from sqlalchemy import text

        park_id = insert_sample_park(sqlite_connection, sample_park_data)
        sample_ride_data['park_id'] = park_id
        ride_id = insert_sample_ride(sqlite_connection, sample_ride_data)

        # Insert snapshots for a day: 9am - 5pm EST
        # Use UTC times (EST is UTC-5)
        operating_date = date(2024, 1, 15)
        snapshots = [
            datetime(2024, 1, 15, 14, 0, 0),  # 9am EST = 2pm UTC
            datetime(2024, 1, 15, 18, 0, 0),  # 1pm EST = 6pm UTC
            datetime(2024, 1, 15, 22, 0, 0)   # 5pm EST = 10pm UTC
        ]

        for recorded_at in snapshots:
            sqlite_connection.execute(text("""
                INSERT INTO ride_status_snapshots (ride_id, recorded_at, wait_time, is_open, computed_is_open)
                VALUES (:ride_id, :recorded_at, :wait_time, :is_open, :computed_is_open)
            """), {
                'ride_id': ride_id,
                'recorded_at': recorded_at,
                'wait_time': 30,
                'is_open': 1,
                'computed_is_open': 1
            })
        sqlite_connection.commit()

        detector = OperatingHoursDetector(sqlite_connection)
        session = detector.detect_operating_session(park_id, operating_date, 'America/New_York')

        assert session is not None
        assert session['park_id'] == park_id
        assert session['operating_date'] == operating_date
        assert session['total_operating_hours'] == 8.0  # 9am to 5pm = 8 hours
        assert session['active_rides_count'] == 1

    def test_detect_timezone_conversion(self, sqlite_connection, sample_park_data, sample_ride_data):
        """detect_operating_session() should handle timezone conversion correctly."""
        from tests.conftest import insert_sample_park, insert_sample_ride
        from sqlalchemy import text

        park_id = insert_sample_park(sqlite_connection, sample_park_data)
        sample_ride_data['park_id'] = park_id
        ride_id = insert_sample_ride(sqlite_connection, sample_ride_data)

        # Pacific time test: 10am PST = 6pm UTC on same day
        operating_date = date(2024, 1, 15)
        sqlite_connection.execute(text("""
            INSERT INTO ride_status_snapshots (ride_id, recorded_at, wait_time, is_open, computed_is_open)
            VALUES (:ride_id, :recorded_at, :wait_time, :is_open, :computed_is_open)
        """), {
            'ride_id': ride_id,
            'recorded_at': datetime(2024, 1, 15, 18, 0, 0),  # 10am PST = 6pm UTC
            'wait_time': 30,
            'is_open': 1,
            'computed_is_open': 1
        })
        sqlite_connection.commit()

        detector = OperatingHoursDetector(sqlite_connection)
        session = detector.detect_operating_session(park_id, operating_date, 'America/Los_Angeles')

        assert session is not None
        # Verify local time is 10am
        assert session['park_opened_at'].hour == 10

    def test_detect_counts_active_rides(self, sqlite_connection, sample_park_data):
        """detect_operating_session() should count unique active rides."""
        from tests.conftest import insert_sample_park, insert_sample_ride
        from sqlalchemy import text

        park_id = insert_sample_park(sqlite_connection, sample_park_data)

        # Create 3 rides
        ride_ids = []
        for i in range(3):
            ride_id = insert_sample_ride(sqlite_connection, {
                'queue_times_id': 1001 + i,
                'park_id': park_id,
                'name': f'Ride {i+1}',
                'land_area': f'Area {i+1}',
                'tier': 1,
                'is_active': 1
            })
            ride_ids.append(ride_id)

        # Insert snapshots for all 3 rides
        operating_date = date(2024, 1, 15)
        for ride_id in ride_ids:
            sqlite_connection.execute(text("""
                INSERT INTO ride_status_snapshots (ride_id, recorded_at, wait_time, is_open, computed_is_open)
                VALUES (:ride_id, :recorded_at, :wait_time, :is_open, :computed_is_open)
            """), {
                'ride_id': ride_id,
                'recorded_at': datetime(2024, 1, 15, 14, 0, 0),
                'wait_time': 30,
                'is_open': 1,
                'computed_is_open': 1
            })
        sqlite_connection.commit()

        detector = OperatingHoursDetector(sqlite_connection)
        session = detector.detect_operating_session(park_id, operating_date, 'America/New_York')

        assert session is not None
        assert session['active_rides_count'] == 3

    def test_detect_ignores_inactive_rides(self, sqlite_connection, sample_park_data):
        """detect_operating_session() should ignore inactive rides."""
        from tests.conftest import insert_sample_park, insert_sample_ride
        from sqlalchemy import text

        park_id = insert_sample_park(sqlite_connection, sample_park_data)

        # Create 1 active and 1 inactive ride
        active_ride_id = insert_sample_ride(sqlite_connection, {
            'queue_times_id': 1001,
            'park_id': park_id,
            'name': 'Active Ride',
            'land_area': 'Area 1',
            'tier': 1,
            'is_active': 1
        })
        inactive_ride_id = insert_sample_ride(sqlite_connection, {
            'queue_times_id': 1002,
            'park_id': park_id,
            'name': 'Inactive Ride',
            'land_area': 'Area 2',
            'tier': 1,
            'is_active': 0
        })

        # Insert snapshots for both rides
        operating_date = date(2024, 1, 15)
        for ride_id in [active_ride_id, inactive_ride_id]:
            sqlite_connection.execute(text("""
                INSERT INTO ride_status_snapshots (ride_id, recorded_at, wait_time, is_open, computed_is_open)
                VALUES (:ride_id, :recorded_at, :wait_time, :is_open, :computed_is_open)
            """), {
                'ride_id': ride_id,
                'recorded_at': datetime(2024, 1, 15, 14, 0, 0),
                'wait_time': 30,
                'is_open': 1,
                'computed_is_open': 1
            })
        sqlite_connection.commit()

        detector = OperatingHoursDetector(sqlite_connection)
        session = detector.detect_operating_session(park_id, operating_date, 'America/New_York')

        assert session is not None
        assert session['active_rides_count'] == 1  # Only active ride counted


class TestDetectAllParksForDate:
    """Test detecting operating sessions for all parks."""

    def test_detect_all_parks_no_parks(self, sqlite_connection):
        """detect_all_parks_for_date() should return empty list with no active parks."""
        detector = OperatingHoursDetector(sqlite_connection)
        operating_date = date(2024, 1, 15)

        sessions = detector.detect_all_parks_for_date(operating_date)

        assert sessions == []

    def test_detect_all_parks_multiple_parks(self, sqlite_connection, sample_park_data):
        """detect_all_parks_for_date() should detect sessions for all active parks."""
        from tests.conftest import insert_sample_park, insert_sample_ride
        from sqlalchemy import text

        # Create 2 parks
        park1_id = insert_sample_park(sqlite_connection, sample_park_data)

        park2_data = sample_park_data.copy()
        park2_data['queue_times_id'] = 102
        park2_data['name'] = 'Epcot'
        park2_id = insert_sample_park(sqlite_connection, park2_data)

        # Create rides for both parks
        ride1_id = insert_sample_ride(sqlite_connection, {
            'queue_times_id': 1001,
            'park_id': park1_id,
            'name': 'Ride 1',
            'land_area': 'Area 1',
            'tier': 1,
            'is_active': 1
        })
        ride2_id = insert_sample_ride(sqlite_connection, {
            'queue_times_id': 1002,
            'park_id': park2_id,
            'name': 'Ride 2',
            'land_area': 'Area 2',
            'tier': 1,
            'is_active': 1
        })

        # Insert snapshots for both rides
        operating_date = date(2024, 1, 15)
        for ride_id in [ride1_id, ride2_id]:
            sqlite_connection.execute(text("""
                INSERT INTO ride_status_snapshots (ride_id, recorded_at, wait_time, is_open, computed_is_open)
                VALUES (:ride_id, :recorded_at, :wait_time, :is_open, :computed_is_open)
            """), {
                'ride_id': ride_id,
                'recorded_at': datetime(2024, 1, 15, 14, 0, 0),
                'wait_time': 30,
                'is_open': 1,
                'computed_is_open': 1
            })
        sqlite_connection.commit()

        detector = OperatingHoursDetector(sqlite_connection)
        sessions = detector.detect_all_parks_for_date(operating_date)

        assert len(sessions) == 2
        park_ids = [s['park_id'] for s in sessions]
        assert park1_id in park_ids
        assert park2_id in park_ids


class TestSaveOperatingSession:
    """Test saving operating sessions - skipped due to MySQL-specific SQL."""

    @pytest.mark.skip(reason="Requires MySQL ON DUPLICATE KEY UPDATE syntax")
    def test_save_operating_session(self, sqlite_connection, sample_park_data):
        """save_operating_session() should save session to database (MySQL only)."""
        # This test requires MySQL-specific ON DUPLICATE KEY UPDATE
        # Will be tested in integration tests with real MySQL
        pass


class TestBackfillOperatingSessions:
    """Test backfilling operating sessions."""

    @pytest.mark.skip(reason="Depends on save_operating_session which uses MySQL syntax")
    def test_backfill_date_range(self, sqlite_connection, sample_park_data, sample_ride_data):
        """backfill_operating_sessions() should create sessions for date range (MySQL only)."""
        # This test requires MySQL-specific ON DUPLICATE KEY UPDATE in save_operating_session
        # Will be tested in integration tests with real MySQL
        pass
