"""
Theme Park Downtime Tracker - Status Change Detector Unit Tests

Tests StatusChangeDetector:
- Detect status transitions (open ↔ closed)
- Calculate downtime durations
- Downtime summary statistics
- Longest downtime events queries

Priority: P1 - Critical for downtime tracking
"""

import pytest
from datetime import datetime, timedelta

from processor.status_change_detector import StatusChangeDetector


class TestStatusChangeDetection:
    """Test status change detection logic."""

    def test_detect_status_changes_no_snapshots(self, sqlite_connection, sample_park_data, sample_ride_data):
        """detect_status_changes() should return empty list when no snapshots exist."""
        from tests.conftest import insert_sample_park, insert_sample_ride

        park_id = insert_sample_park(sqlite_connection, sample_park_data)
        sample_ride_data['park_id'] = park_id
        ride_id = insert_sample_ride(sqlite_connection, sample_ride_data)

        detector = StatusChangeDetector(sqlite_connection)
        start_time = datetime(2024, 1, 1, 9, 0, 0)
        end_time = datetime(2024, 1, 1, 17, 0, 0)

        changes = detector.detect_status_changes(ride_id, start_time, end_time)

        assert changes == []

    def test_detect_status_changes_single_snapshot(self, sqlite_connection, sample_park_data, sample_ride_data):
        """detect_status_changes() should return empty list with only 1 snapshot."""
        from tests.conftest import insert_sample_park, insert_sample_ride
        from sqlalchemy import text

        park_id = insert_sample_park(sqlite_connection, sample_park_data)
        sample_ride_data['park_id'] = park_id
        ride_id = insert_sample_ride(sqlite_connection, sample_ride_data)

        # Insert 1 snapshot
        sqlite_connection.execute(text("""
            INSERT INTO ride_status_snapshots (ride_id, recorded_at, wait_time, is_open, computed_is_open)
            VALUES (:ride_id, :recorded_at, :wait_time, :is_open, :computed_is_open)
        """), {
            'ride_id': ride_id,
            'recorded_at': datetime(2024, 1, 1, 10, 0, 0),
            'wait_time': 30,
            'is_open': 1,
            'computed_is_open': 1
        })
        sqlite_connection.commit()

        detector = StatusChangeDetector(sqlite_connection)
        start_time = datetime(2024, 1, 1, 9, 0, 0)
        end_time = datetime(2024, 1, 1, 17, 0, 0)

        changes = detector.detect_status_changes(ride_id, start_time, end_time)

        assert changes == []

    def test_detect_no_status_changes(self, sqlite_connection, sample_park_data, sample_ride_data):
        """detect_status_changes() should return empty list when status remains constant."""
        from tests.conftest import insert_sample_park, insert_sample_ride
        from sqlalchemy import text

        park_id = insert_sample_park(sqlite_connection, sample_park_data)
        sample_ride_data['park_id'] = park_id
        ride_id = insert_sample_ride(sqlite_connection, sample_ride_data)

        # Insert 3 snapshots with same status (all open)
        snapshots = [
            (datetime(2024, 1, 1, 10, 0, 0), 1),
            (datetime(2024, 1, 1, 11, 0, 0), 1),
            (datetime(2024, 1, 1, 12, 0, 0), 1)
        ]

        for recorded_at, is_open in snapshots:
            sqlite_connection.execute(text("""
                INSERT INTO ride_status_snapshots (ride_id, recorded_at, wait_time, is_open, computed_is_open)
                VALUES (:ride_id, :recorded_at, :wait_time, :is_open, :computed_is_open)
            """), {
                'ride_id': ride_id,
                'recorded_at': recorded_at,
                'wait_time': 30,
                'is_open': is_open,
                'computed_is_open': is_open
            })
        sqlite_connection.commit()

        detector = StatusChangeDetector(sqlite_connection)
        start_time = datetime(2024, 1, 1, 9, 0, 0)
        end_time = datetime(2024, 1, 1, 17, 0, 0)

        changes = detector.detect_status_changes(ride_id, start_time, end_time)

        assert changes == []

    def test_detect_single_transition_open_to_closed(self, sqlite_connection, sample_park_data, sample_ride_data):
        """detect_status_changes() should detect open → closed transition."""
        from tests.conftest import insert_sample_park, insert_sample_ride
        from sqlalchemy import text

        park_id = insert_sample_park(sqlite_connection, sample_park_data)
        sample_ride_data['park_id'] = park_id
        ride_id = insert_sample_ride(sqlite_connection, sample_ride_data)

        # Insert snapshots: open → closed
        snapshots = [
            (datetime(2024, 1, 1, 10, 0, 0), 1),  # Open
            (datetime(2024, 1, 1, 11, 0, 0), 0)   # Closed
        ]

        for recorded_at, is_open in snapshots:
            sqlite_connection.execute(text("""
                INSERT INTO ride_status_snapshots (ride_id, recorded_at, wait_time, is_open, computed_is_open)
                VALUES (:ride_id, :recorded_at, :wait_time, :is_open, :computed_is_open)
            """), {
                'ride_id': ride_id,
                'recorded_at': recorded_at,
                'wait_time': 30 if is_open else 0,
                'is_open': is_open,
                'computed_is_open': is_open
            })
        sqlite_connection.commit()

        detector = StatusChangeDetector(sqlite_connection)
        start_time = datetime(2024, 1, 1, 9, 0, 0)
        end_time = datetime(2024, 1, 1, 17, 0, 0)

        changes = detector.detect_status_changes(ride_id, start_time, end_time)

        assert len(changes) == 1
        assert changes[0]['ride_id'] == ride_id
        assert changes[0]['previous_status'] in (True, 1)
        assert changes[0]['new_status'] in (False, 0)
        # SQLite returns datetime as string, convert for comparison
        change_time = changes[0]['change_detected_at']
        if isinstance(change_time, str):
            change_time = datetime.fromisoformat(change_time.replace(' ', 'T'))
        assert change_time == datetime(2024, 1, 1, 11, 0, 0)
        assert changes[0]['downtime_duration_minutes'] is None  # No duration for open→closed

    def test_detect_single_transition_closed_to_open_with_duration(self, sqlite_connection, sample_park_data, sample_ride_data):
        """detect_status_changes() should detect closed → open and calculate downtime duration."""
        from tests.conftest import insert_sample_park, insert_sample_ride
        from sqlalchemy import text

        park_id = insert_sample_park(sqlite_connection, sample_park_data)
        sample_ride_data['park_id'] = park_id
        ride_id = insert_sample_ride(sqlite_connection, sample_ride_data)

        # Insert snapshots: closed → open (120 minutes downtime)
        snapshots = [
            (datetime(2024, 1, 1, 10, 0, 0), 0),  # Closed
            (datetime(2024, 1, 1, 12, 0, 0), 1)   # Open (2 hours later)
        ]

        for recorded_at, is_open in snapshots:
            sqlite_connection.execute(text("""
                INSERT INTO ride_status_snapshots (ride_id, recorded_at, wait_time, is_open, computed_is_open)
                VALUES (:ride_id, :recorded_at, :wait_time, :is_open, :computed_is_open)
            """), {
                'ride_id': ride_id,
                'recorded_at': recorded_at,
                'wait_time': 30 if is_open else 0,
                'is_open': is_open,
                'computed_is_open': is_open
            })
        sqlite_connection.commit()

        detector = StatusChangeDetector(sqlite_connection)
        start_time = datetime(2024, 1, 1, 9, 0, 0)
        end_time = datetime(2024, 1, 1, 17, 0, 0)

        changes = detector.detect_status_changes(ride_id, start_time, end_time)

        assert len(changes) == 1
        assert changes[0]['ride_id'] == ride_id
        assert changes[0]['previous_status'] in (False, 0)
        assert changes[0]['new_status'] in (True, 1)
        # SQLite returns datetime as string
        change_time = changes[0]['change_detected_at']
        if isinstance(change_time, str):
            change_time = datetime.fromisoformat(change_time.replace(' ', 'T'))
        assert change_time == datetime(2024, 1, 1, 12, 0, 0)
        assert changes[0]['downtime_duration_minutes'] == 120  # 2 hours

    def test_detect_multiple_transitions(self, sqlite_connection, sample_park_data, sample_ride_data):
        """detect_status_changes() should detect multiple transitions."""
        from tests.conftest import insert_sample_park, insert_sample_ride
        from sqlalchemy import text

        park_id = insert_sample_park(sqlite_connection, sample_park_data)
        sample_ride_data['park_id'] = park_id
        ride_id = insert_sample_ride(sqlite_connection, sample_ride_data)

        # Insert snapshots: open → closed → open → closed
        snapshots = [
            (datetime(2024, 1, 1, 10, 0, 0), 1),  # Open
            (datetime(2024, 1, 1, 11, 0, 0), 0),  # Closed (downtime starts)
            (datetime(2024, 1, 1, 11, 30, 0), 1), # Open (30 min downtime)
            (datetime(2024, 1, 1, 13, 0, 0), 0)   # Closed again
        ]

        for recorded_at, is_open in snapshots:
            sqlite_connection.execute(text("""
                INSERT INTO ride_status_snapshots (ride_id, recorded_at, wait_time, is_open, computed_is_open)
                VALUES (:ride_id, :recorded_at, :wait_time, :is_open, :computed_is_open)
            """), {
                'ride_id': ride_id,
                'recorded_at': recorded_at,
                'wait_time': 30 if is_open else 0,
                'is_open': is_open,
                'computed_is_open': is_open
            })
        sqlite_connection.commit()

        detector = StatusChangeDetector(sqlite_connection)
        start_time = datetime(2024, 1, 1, 9, 0, 0)
        end_time = datetime(2024, 1, 1, 17, 0, 0)

        changes = detector.detect_status_changes(ride_id, start_time, end_time)

        assert len(changes) == 3
        # First transition: open → closed
        assert changes[0]['previous_status'] in (True, 1)
        assert changes[0]['new_status'] in (False, 0)
        assert changes[0]['downtime_duration_minutes'] is None

        # Second transition: closed → open (30 minutes)
        assert changes[1]['previous_status'] in (False, 0)
        assert changes[1]['new_status'] in (True, 1)
        assert changes[1]['downtime_duration_minutes'] == 30

        # Third transition: open → closed
        assert changes[2]['previous_status'] in (True, 1)
        assert changes[2]['new_status'] in (False, 0)
        assert changes[2]['downtime_duration_minutes'] is None


class TestSaveStatusChange:
    """Test saving status changes to database."""

    def test_save_status_change(self, sqlite_connection, sample_park_data, sample_ride_data):
        """save_status_change() should insert change record and return ID."""
        from tests.conftest import insert_sample_park, insert_sample_ride

        park_id = insert_sample_park(sqlite_connection, sample_park_data)
        sample_ride_data['park_id'] = park_id
        ride_id = insert_sample_ride(sqlite_connection, sample_ride_data)

        detector = StatusChangeDetector(sqlite_connection)

        change_data = {
            'ride_id': ride_id,
            'previous_status': True,
            'new_status': False,
            'change_detected_at': datetime(2024, 1, 1, 11, 0, 0),
            'downtime_duration_minutes': None
        }

        change_id = detector.save_status_change(change_data)

        assert change_id is not None
        assert change_id > 0

    def test_save_status_change_with_duration(self, sqlite_connection, sample_park_data, sample_ride_data):
        """save_status_change() should save downtime duration."""
        from tests.conftest import insert_sample_park, insert_sample_ride
        from sqlalchemy import text

        park_id = insert_sample_park(sqlite_connection, sample_park_data)
        sample_ride_data['park_id'] = park_id
        ride_id = insert_sample_ride(sqlite_connection, sample_ride_data)

        detector = StatusChangeDetector(sqlite_connection)

        change_data = {
            'ride_id': ride_id,
            'previous_status': False,
            'new_status': True,
            'change_detected_at': datetime(2024, 1, 1, 12, 0, 0),
            'downtime_duration_minutes': 120
        }

        change_id = detector.save_status_change(change_data)

        # Verify saved data
        result = sqlite_connection.execute(text("""
            SELECT downtime_duration_minutes FROM ride_status_changes WHERE change_id = :change_id
        """), {'change_id': change_id})
        row = result.fetchone()

        assert row.downtime_duration_minutes == 120


class TestDowntimeSummary:
    """Test downtime summary calculations."""

    def test_calculate_downtime_summary_no_changes(self, sqlite_connection, sample_park_data, sample_ride_data):
        """calculate_downtime_summary() should return zero metrics when no changes."""
        from tests.conftest import insert_sample_park, insert_sample_ride

        park_id = insert_sample_park(sqlite_connection, sample_park_data)
        sample_ride_data['park_id'] = park_id
        ride_id = insert_sample_ride(sqlite_connection, sample_ride_data)

        detector = StatusChangeDetector(sqlite_connection)
        start_time = datetime(2024, 1, 1, 9, 0, 0)
        end_time = datetime(2024, 1, 1, 17, 0, 0)

        summary = detector.calculate_downtime_summary(ride_id, start_time, end_time)

        assert summary['ride_id'] == ride_id
        assert summary['downtime_event_count'] == 0
        assert summary['total_downtime_minutes'] == 0
        # No downtime = 100% uptime
        assert summary['uptime_percentage'] == 100.0

    def test_calculate_downtime_summary_with_downtime(self, sqlite_connection, sample_park_data, sample_ride_data):
        """calculate_downtime_summary() should calculate correct statistics."""
        from tests.conftest import insert_sample_park, insert_sample_ride
        from sqlalchemy import text

        park_id = insert_sample_park(sqlite_connection, sample_park_data)
        sample_ride_data['park_id'] = park_id
        ride_id = insert_sample_ride(sqlite_connection, sample_ride_data)

        # Insert snapshots: 8-hour period with 2 hours downtime
        # 9am open, 10am closed, 12pm open, 5pm still open
        snapshots = [
            (datetime(2024, 1, 1, 9, 0, 0), 1),   # Open
            (datetime(2024, 1, 1, 10, 0, 0), 0),  # Closed
            (datetime(2024, 1, 1, 12, 0, 0), 1),  # Open (2hr downtime)
            (datetime(2024, 1, 1, 17, 0, 0), 1)   # Still open
        ]

        for recorded_at, is_open in snapshots:
            sqlite_connection.execute(text("""
                INSERT INTO ride_status_snapshots (ride_id, recorded_at, wait_time, is_open, computed_is_open)
                VALUES (:ride_id, :recorded_at, :wait_time, :is_open, :computed_is_open)
            """), {
                'ride_id': ride_id,
                'recorded_at': recorded_at,
                'wait_time': 30 if is_open else 0,
                'is_open': is_open,
                'computed_is_open': is_open
            })
        sqlite_connection.commit()

        detector = StatusChangeDetector(sqlite_connection)
        start_time = datetime(2024, 1, 1, 9, 0, 0)
        end_time = datetime(2024, 1, 1, 17, 0, 0)

        summary = detector.calculate_downtime_summary(ride_id, start_time, end_time)

        assert summary['ride_id'] == ride_id
        assert summary['downtime_event_count'] == 1  # 1 open→closed transition
        assert summary['total_downtime_minutes'] == 120  # 2 hours
        # 8 hours = 480 minutes, 120 downtime = 75% uptime
        assert summary['uptime_percentage'] == 75.0


class TestDetectAllRides:
    """Test detecting changes for all active rides."""

    def test_detect_all_rides_for_period_no_rides(self, sqlite_connection):
        """detect_all_rides_for_period() should return empty dict with no active rides."""
        detector = StatusChangeDetector(sqlite_connection)
        start_time = datetime(2024, 1, 1, 9, 0, 0)
        end_time = datetime(2024, 1, 1, 17, 0, 0)

        all_changes = detector.detect_all_rides_for_period(start_time, end_time)

        assert all_changes == {}

    def test_detect_all_rides_for_period_multiple_rides(self, sqlite_connection, sample_park_data):
        """detect_all_rides_for_period() should detect changes for all active rides."""
        from tests.conftest import insert_sample_park, insert_sample_ride
        from sqlalchemy import text

        park_id = insert_sample_park(sqlite_connection, sample_park_data)

        # Create 2 active rides
        ride1_id = insert_sample_ride(sqlite_connection, {
            'queue_times_id': 1001, 'park_id': park_id, 'name': 'Ride 1',
            'land_area': 'Area 1', 'tier': 1, 'is_active': 1
        })
        ride2_id = insert_sample_ride(sqlite_connection, {
            'queue_times_id': 1002, 'park_id': park_id, 'name': 'Ride 2',
            'land_area': 'Area 2', 'tier': 1, 'is_active': 1
        })

        # Insert snapshots for ride 1: open → closed
        sqlite_connection.execute(text("""
            INSERT INTO ride_status_snapshots (ride_id, recorded_at, wait_time, is_open, computed_is_open)
            VALUES (:ride_id, :recorded_at, :wait_time, :is_open, :computed_is_open)
        """), {'ride_id': ride1_id, 'recorded_at': datetime(2024, 1, 1, 10, 0, 0), 'wait_time': 30, 'is_open': 1, 'computed_is_open': 1})
        sqlite_connection.execute(text("""
            INSERT INTO ride_status_snapshots (ride_id, recorded_at, wait_time, is_open, computed_is_open)
            VALUES (:ride_id, :recorded_at, :wait_time, :is_open, :computed_is_open)
        """), {'ride_id': ride1_id, 'recorded_at': datetime(2024, 1, 1, 11, 0, 0), 'wait_time': 0, 'is_open': 0, 'computed_is_open': 0})

        # Insert snapshots for ride 2: closed → open
        sqlite_connection.execute(text("""
            INSERT INTO ride_status_snapshots (ride_id, recorded_at, wait_time, is_open, computed_is_open)
            VALUES (:ride_id, :recorded_at, :wait_time, :is_open, :computed_is_open)
        """), {'ride_id': ride2_id, 'recorded_at': datetime(2024, 1, 1, 10, 0, 0), 'wait_time': 0, 'is_open': 0, 'computed_is_open': 0})
        sqlite_connection.execute(text("""
            INSERT INTO ride_status_snapshots (ride_id, recorded_at, wait_time, is_open, computed_is_open)
            VALUES (:ride_id, :recorded_at, :wait_time, :is_open, :computed_is_open)
        """), {'ride_id': ride2_id, 'recorded_at': datetime(2024, 1, 1, 12, 0, 0), 'wait_time': 30, 'is_open': 1, 'computed_is_open': 1})

        sqlite_connection.commit()

        detector = StatusChangeDetector(sqlite_connection)
        start_time = datetime(2024, 1, 1, 9, 0, 0)
        end_time = datetime(2024, 1, 1, 17, 0, 0)

        all_changes = detector.detect_all_rides_for_period(start_time, end_time)

        assert len(all_changes) == 2
        assert ride1_id in all_changes
        assert ride2_id in all_changes
        assert len(all_changes[ride1_id]) == 1
        assert len(all_changes[ride2_id]) == 1


class TestLongestDowntimeEvents:
    """Test longest downtime events query."""

    def test_get_longest_downtime_events_no_events(self, sqlite_connection):
        """get_longest_downtime_events() should return empty list with no downtime events."""
        detector = StatusChangeDetector(sqlite_connection)

        events = detector.get_longest_downtime_events(limit=10)

        assert events == []

    def test_get_longest_downtime_events_orders_by_duration(self, sqlite_connection, sample_park_data):
        """get_longest_downtime_events() should return events ordered by duration DESC."""
        from tests.conftest import insert_sample_park, insert_sample_ride
        from sqlalchemy import text

        park_id = insert_sample_park(sqlite_connection, sample_park_data)

        # Create 2 rides
        ride1_id = insert_sample_ride(sqlite_connection, {
            'queue_times_id': 1001, 'park_id': park_id, 'name': 'Ride 1',
            'land_area': 'Area 1', 'tier': 1, 'is_active': 1
        })
        ride2_id = insert_sample_ride(sqlite_connection, {
            'queue_times_id': 1002, 'park_id': park_id, 'name': 'Ride 2',
            'land_area': 'Area 2', 'tier': 1, 'is_active': 1
        })

        # Insert status changes with different durations
        sqlite_connection.execute(text("""
            INSERT INTO ride_status_changes
            (ride_id, previous_status, new_status, change_detected_at, downtime_duration_minutes)
            VALUES (:ride_id, :previous_status, :new_status, :change_detected_at, :downtime_duration_minutes)
        """), {'ride_id': ride1_id, 'previous_status': 0, 'new_status': 1, 'change_detected_at': datetime(2024, 1, 1, 12, 0, 0), 'downtime_duration_minutes': 120})  # 2 hours

        sqlite_connection.execute(text("""
            INSERT INTO ride_status_changes
            (ride_id, previous_status, new_status, change_detected_at, downtime_duration_minutes)
            VALUES (:ride_id, :previous_status, :new_status, :change_detected_at, :downtime_duration_minutes)
        """), {'ride_id': ride2_id, 'previous_status': 0, 'new_status': 1, 'change_detected_at': datetime(2024, 1, 1, 13, 0, 0), 'downtime_duration_minutes': 180})  # 3 hours

        sqlite_connection.commit()

        detector = StatusChangeDetector(sqlite_connection)
        events = detector.get_longest_downtime_events(limit=10)

        assert len(events) == 2
        # Longest first (180 minutes)
        assert events[0]['downtime_duration_minutes'] == 180
        assert events[0]['ride_name'] == 'Ride 2'
        # Second longest (120 minutes)
        assert events[1]['downtime_duration_minutes'] == 120
        assert events[1]['ride_name'] == 'Ride 1'

    def test_get_longest_downtime_events_respects_limit(self, sqlite_connection, sample_park_data):
        """get_longest_downtime_events() should respect limit parameter."""
        from tests.conftest import insert_sample_park, insert_sample_ride
        from sqlalchemy import text

        park_id = insert_sample_park(sqlite_connection, sample_park_data)

        # Create 3 rides with different downtime durations
        for i in range(3):
            ride_id = insert_sample_ride(sqlite_connection, {
                'queue_times_id': 1001 + i, 'park_id': park_id, 'name': f'Ride {i+1}',
                'land_area': f'Area {i+1}', 'tier': 1, 'is_active': 1
            })

            sqlite_connection.execute(text("""
                INSERT INTO ride_status_changes
                (ride_id, previous_status, new_status, change_detected_at, downtime_duration_minutes)
                VALUES (:ride_id, :previous_status, :new_status, :change_detected_at, :downtime_duration_minutes)
            """), {'ride_id': ride_id, 'previous_status': 0, 'new_status': 1, 'change_detected_at': datetime(2024, 1, 1, 12, 0, 0), 'downtime_duration_minutes': (i + 1) * 60})

        sqlite_connection.commit()

        detector = StatusChangeDetector(sqlite_connection)
        events = detector.get_longest_downtime_events(limit=2)

        assert len(events) == 2
        # Should return top 2 longest
        assert events[0]['downtime_duration_minutes'] == 180  # 3 hours
        assert events[1]['downtime_duration_minutes'] == 120  # 2 hours

    def test_get_longest_downtime_events_filters_by_park(self, sqlite_connection):
        """get_longest_downtime_events() should filter by park_id when provided."""
        from tests.conftest import insert_sample_park, insert_sample_ride
        from sqlalchemy import text

        # Create 2 parks
        park1_id = insert_sample_park(sqlite_connection, {
            'queue_times_id': 101, 'name': 'Park 1', 'city': 'Orlando',
            'state_province': 'FL', 'country': 'USA', 'latitude': 28.4177,
            'longitude': -81.5812, 'timezone': 'America/New_York',
            'operator': 'Operator 1', 'is_disney': 1, 'is_universal': 0, 'is_active': 1
        })
        park2_id = insert_sample_park(sqlite_connection, {
            'queue_times_id': 102, 'name': 'Park 2', 'city': 'Anaheim',
            'state_province': 'CA', 'country': 'USA', 'latitude': 33.8121,
            'longitude': -117.9190, 'timezone': 'America/Los_Angeles',
            'operator': 'Operator 2', 'is_disney': 1, 'is_universal': 0, 'is_active': 1
        })

        # Create rides in both parks
        ride1_id = insert_sample_ride(sqlite_connection, {
            'queue_times_id': 1001, 'park_id': park1_id, 'name': 'Ride 1',
            'land_area': 'Area 1', 'tier': 1, 'is_active': 1
        })
        ride2_id = insert_sample_ride(sqlite_connection, {
            'queue_times_id': 1002, 'park_id': park2_id, 'name': 'Ride 2',
            'land_area': 'Area 2', 'tier': 1, 'is_active': 1
        })

        # Insert status changes for both rides
        sqlite_connection.execute(text("""
            INSERT INTO ride_status_changes
            (ride_id, previous_status, new_status, change_detected_at, downtime_duration_minutes)
            VALUES (:ride_id, :previous_status, :new_status, :change_detected_at, :downtime_duration_minutes)
        """), {'ride_id': ride1_id, 'previous_status': 0, 'new_status': 1, 'change_detected_at': datetime(2024, 1, 1, 12, 0, 0), 'downtime_duration_minutes': 120})

        sqlite_connection.execute(text("""
            INSERT INTO ride_status_changes
            (ride_id, previous_status, new_status, change_detected_at, downtime_duration_minutes)
            VALUES (:ride_id, :previous_status, :new_status, :change_detected_at, :downtime_duration_minutes)
        """), {'ride_id': ride2_id, 'previous_status': 0, 'new_status': 1, 'change_detected_at': datetime(2024, 1, 1, 13, 0, 0), 'downtime_duration_minutes': 180})

        sqlite_connection.commit()

        detector = StatusChangeDetector(sqlite_connection)

        # Filter by park 1 only
        events = detector.get_longest_downtime_events(park_id=park1_id, limit=10)

        assert len(events) == 1
        assert events[0]['park_name'] == 'Park 1'
        assert events[0]['ride_name'] == 'Ride 1'

    def test_get_longest_downtime_events_filters_by_time_range(self, sqlite_connection, sample_park_data):
        """get_longest_downtime_events() should filter by start_time and end_time when provided."""
        from tests.conftest import insert_sample_park, insert_sample_ride
        from sqlalchemy import text

        park_id = insert_sample_park(sqlite_connection, sample_park_data)

        # Create 2 rides
        ride1_id = insert_sample_ride(sqlite_connection, {
            'queue_times_id': 1001, 'park_id': park_id, 'name': 'Ride 1',
            'land_area': 'Area 1', 'tier': 1, 'is_active': 1
        })
        ride2_id = insert_sample_ride(sqlite_connection, {
            'queue_times_id': 1002, 'park_id': park_id, 'name': 'Ride 2',
            'land_area': 'Area 2', 'tier': 1, 'is_active': 1
        })

        # Insert status changes at different times
        sqlite_connection.execute(text("""
            INSERT INTO ride_status_changes
            (ride_id, previous_status, new_status, change_detected_at, downtime_duration_minutes)
            VALUES (:ride_id, :previous_status, :new_status, :change_detected_at, :downtime_duration_minutes)
        """), {'ride_id': ride1_id, 'previous_status': 0, 'new_status': 1, 'change_detected_at': datetime(2024, 1, 1, 10, 0, 0), 'downtime_duration_minutes': 120})  # Before time range

        sqlite_connection.execute(text("""
            INSERT INTO ride_status_changes
            (ride_id, previous_status, new_status, change_detected_at, downtime_duration_minutes)
            VALUES (:ride_id, :previous_status, :new_status, :change_detected_at, :downtime_duration_minutes)
        """), {'ride_id': ride2_id, 'previous_status': 0, 'new_status': 1, 'change_detected_at': datetime(2024, 1, 1, 15, 0, 0), 'downtime_duration_minutes': 180})  # Within time range

        sqlite_connection.commit()

        detector = StatusChangeDetector(sqlite_connection)

        # Filter by time range (12pm - 6pm)
        events = detector.get_longest_downtime_events(
            start_time=datetime(2024, 1, 1, 12, 0, 0),
            end_time=datetime(2024, 1, 1, 18, 0, 0),
            limit=10
        )

        assert len(events) == 1
        assert events[0]['ride_name'] == 'Ride 2'
        assert events[0]['downtime_duration_minutes'] == 180
