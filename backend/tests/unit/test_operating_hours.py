"""
Theme Park Downtime Tracker - Operating Hours Detector Unit Tests

Tests OperatingHoursDetector:
- Timezone conversion (UTC <-> local)
- Operating session detection
- Edge cases: no activity, midnight crossings

Priority: P1 - Tests operating hours detection logic (T145)
"""

import pytest
from datetime import datetime, date, time, timedelta
from unittest.mock import Mock, MagicMock, patch
from zoneinfo import ZoneInfo

from processor.operating_hours_detector import OperatingHoursDetector


class TestTimezoneConversion:
    """Test timezone handling in operating hours detection."""

    def test_east_coast_timezone_conversion(self):
        """Verify UTC boundaries are calculated correctly for Eastern Time."""
        # Create mock connection
        mock_conn = Mock()
        mock_result = Mock()
        mock_result.fetchone.return_value = None  # No activity
        mock_conn.execute.return_value = mock_result

        detector = OperatingHoursDetector(mock_conn)

        # Test a date in Eastern Time
        operating_date = date(2024, 7, 15)  # Summer (EDT, UTC-4)
        park_timezone = "America/New_York"

        result = detector.detect_operating_session(
            park_id=1,
            operating_date=operating_date,
            park_timezone=park_timezone
        )

        # Verify the query was executed (even if no results)
        assert mock_conn.execute.called

        # Check that the UTC boundaries in the query params are correct
        call_args = mock_conn.execute.call_args
        params = call_args[0][1]  # Second positional arg is the params dict

        # July 15 00:00 EDT = July 15 04:00 UTC
        # July 15 23:59 EDT = July 16 03:59 UTC
        expected_utc_start = datetime(2024, 7, 15, 4, 0, 0, tzinfo=ZoneInfo('UTC'))
        expected_utc_end = datetime(2024, 7, 16, 3, 59, 59, tzinfo=ZoneInfo('UTC'))

        actual_start = params['utc_start']
        actual_end = params['utc_end']

        # Compare by removing timezone info for simpler comparison
        if hasattr(actual_start, 'tzinfo'):
            actual_start = actual_start.replace(tzinfo=None)
        if hasattr(actual_end, 'tzinfo'):
            actual_end = actual_end.replace(tzinfo=None)

        assert actual_start == expected_utc_start.replace(tzinfo=None), f"Expected {expected_utc_start}, got {actual_start}"
        assert actual_end == expected_utc_end.replace(tzinfo=None), f"Expected {expected_utc_end}, got {actual_end}"

    def test_west_coast_timezone_conversion(self):
        """Verify UTC boundaries are calculated correctly for Pacific Time."""
        mock_conn = Mock()
        mock_result = Mock()
        mock_result.fetchone.return_value = None
        mock_conn.execute.return_value = mock_result

        detector = OperatingHoursDetector(mock_conn)

        operating_date = date(2024, 7, 15)  # Summer (PDT, UTC-7)
        park_timezone = "America/Los_Angeles"

        detector.detect_operating_session(
            park_id=1,
            operating_date=operating_date,
            park_timezone=park_timezone
        )

        call_args = mock_conn.execute.call_args
        params = call_args[0][1]

        # July 15 00:00 PDT = July 15 07:00 UTC
        # July 15 23:59 PDT = July 16 06:59 UTC
        expected_utc_start = datetime(2024, 7, 15, 7, 0, 0)
        expected_utc_end = datetime(2024, 7, 16, 6, 59, 59)

        actual_start = params['utc_start']
        actual_end = params['utc_end']

        if hasattr(actual_start, 'tzinfo'):
            actual_start = actual_start.replace(tzinfo=None)
        if hasattr(actual_end, 'tzinfo'):
            actual_end = actual_end.replace(tzinfo=None)

        assert actual_start == expected_utc_start
        assert actual_end == expected_utc_end

    def test_winter_daylight_saving_time(self):
        """Verify timezone handling during winter (Standard Time)."""
        mock_conn = Mock()
        mock_result = Mock()
        mock_result.fetchone.return_value = None
        mock_conn.execute.return_value = mock_result

        detector = OperatingHoursDetector(mock_conn)

        operating_date = date(2024, 1, 15)  # Winter (EST, UTC-5)
        park_timezone = "America/New_York"

        detector.detect_operating_session(
            park_id=1,
            operating_date=operating_date,
            park_timezone=park_timezone
        )

        call_args = mock_conn.execute.call_args
        params = call_args[0][1]

        # January 15 00:00 EST = January 15 05:00 UTC (UTC-5 in winter)
        expected_utc_start = datetime(2024, 1, 15, 5, 0, 0)

        actual_start = params['utc_start']
        if hasattr(actual_start, 'tzinfo'):
            actual_start = actual_start.replace(tzinfo=None)

        assert actual_start == expected_utc_start


class TestOperatingSessionDetection:
    """Test detection of operating sessions."""

    def test_detect_session_with_activity(self):
        """Detector should return session data when activity exists."""
        mock_conn = Mock()
        mock_result = Mock()

        # Simulate activity from 9 AM to 10 PM UTC (13 hours)
        first_activity = datetime(2024, 7, 15, 13, 0, 0)  # 9 AM EDT
        last_activity = datetime(2024, 7, 16, 2, 0, 0)    # 10 PM EDT

        mock_row = Mock()
        mock_row.first_activity = first_activity
        mock_row.last_activity = last_activity
        mock_row.active_rides_count = 25
        mock_row.open_ride_snapshots = 1500

        mock_result.fetchone.return_value = mock_row
        mock_conn.execute.return_value = mock_result

        detector = OperatingHoursDetector(mock_conn)

        result = detector.detect_operating_session(
            park_id=1,
            operating_date=date(2024, 7, 15),
            park_timezone="America/New_York"
        )

        assert result is not None
        assert result['park_id'] == 1
        assert result['session_date'] == date(2024, 7, 15)
        assert result['session_start_utc'] == first_activity
        assert result['session_end_utc'] == last_activity
        assert result['active_rides_count'] == 25
        assert result['open_ride_snapshots'] == 1500

        # Check operating minutes calculation (13 hours = 780 minutes)
        expected_minutes = int((last_activity - first_activity).total_seconds() / 60)
        assert result['operating_minutes'] == expected_minutes

    def test_detect_session_no_activity(self):
        """Detector should return None when no activity exists."""
        mock_conn = Mock()
        mock_result = Mock()

        # No activity - first_activity is None
        mock_row = Mock()
        mock_row.first_activity = None
        mock_row.last_activity = None
        mock_row.active_rides_count = 0
        mock_row.open_ride_snapshots = 0

        mock_result.fetchone.return_value = mock_row
        mock_conn.execute.return_value = mock_result

        detector = OperatingHoursDetector(mock_conn)

        result = detector.detect_operating_session(
            park_id=1,
            operating_date=date(2024, 7, 15),
            park_timezone="America/New_York"
        )

        assert result is None

    def test_detect_session_query_returns_none(self):
        """Detector should return None when query returns no rows."""
        mock_conn = Mock()
        mock_result = Mock()
        mock_result.fetchone.return_value = None
        mock_conn.execute.return_value = mock_result

        detector = OperatingHoursDetector(mock_conn)

        result = detector.detect_operating_session(
            park_id=999,  # Non-existent park
            operating_date=date(2024, 7, 15),
            park_timezone="America/New_York"
        )

        assert result is None

    def test_detect_session_string_datetime_handling(self):
        """Detector should handle string datetime values (SQLite compatibility)."""
        mock_conn = Mock()
        mock_result = Mock()

        # Simulate string datetime (SQLite returns strings)
        mock_row = Mock()
        mock_row.first_activity = "2024-07-15 13:00:00"
        mock_row.last_activity = "2024-07-16 02:00:00"
        mock_row.active_rides_count = 25
        mock_row.open_ride_snapshots = 1500

        mock_result.fetchone.return_value = mock_row
        mock_conn.execute.return_value = mock_result

        detector = OperatingHoursDetector(mock_conn)

        result = detector.detect_operating_session(
            park_id=1,
            operating_date=date(2024, 7, 15),
            park_timezone="America/New_York"
        )

        assert result is not None
        assert isinstance(result['session_start_utc'], datetime)
        assert isinstance(result['session_end_utc'], datetime)


class TestOperatingMinutesCalculation:
    """Test operating minutes calculation."""

    def test_full_day_operation(self):
        """Calculate minutes for ~12-hour operation."""
        mock_conn = Mock()
        mock_result = Mock()

        # 9 AM to 9 PM = 12 hours = 720 minutes
        first_activity = datetime(2024, 7, 15, 13, 0, 0)  # 9 AM EDT
        last_activity = datetime(2024, 7, 16, 1, 0, 0)    # 9 PM EDT

        mock_row = Mock()
        mock_row.first_activity = first_activity
        mock_row.last_activity = last_activity
        mock_row.active_rides_count = 25
        mock_row.open_ride_snapshots = 1500

        mock_result.fetchone.return_value = mock_row
        mock_conn.execute.return_value = mock_result

        detector = OperatingHoursDetector(mock_conn)

        result = detector.detect_operating_session(
            park_id=1,
            operating_date=date(2024, 7, 15),
            park_timezone="America/New_York"
        )

        assert result['operating_minutes'] == 720

    def test_short_operation(self):
        """Calculate minutes for short operation (e.g., event night)."""
        mock_conn = Mock()
        mock_result = Mock()

        # 6 PM to 11 PM = 5 hours = 300 minutes
        first_activity = datetime(2024, 7, 15, 22, 0, 0)  # 6 PM EDT
        last_activity = datetime(2024, 7, 16, 3, 0, 0)    # 11 PM EDT

        mock_row = Mock()
        mock_row.first_activity = first_activity
        mock_row.last_activity = last_activity
        mock_row.active_rides_count = 15
        mock_row.open_ride_snapshots = 500

        mock_result.fetchone.return_value = mock_row
        mock_conn.execute.return_value = mock_result

        detector = OperatingHoursDetector(mock_conn)

        result = detector.detect_operating_session(
            park_id=1,
            operating_date=date(2024, 7, 15),
            park_timezone="America/New_York"
        )

        assert result['operating_minutes'] == 300


class TestSaveOperatingSession:
    """Test saving operating sessions to database."""

    def test_save_session_executes_insert(self):
        """save_operating_session should execute INSERT query."""
        mock_conn = Mock()
        mock_result = Mock()
        mock_result.lastrowid = 42
        mock_conn.execute.return_value = mock_result

        detector = OperatingHoursDetector(mock_conn)

        session_data = {
            'park_id': 1,
            'session_date': date(2024, 7, 15),
            'session_start_utc': datetime(2024, 7, 15, 13, 0, 0),
            'session_end_utc': datetime(2024, 7, 16, 1, 0, 0),
            'operating_minutes': 720
        }

        result = detector.save_operating_session(session_data)

        assert result == 42
        assert mock_conn.execute.called


class TestDetectAllParks:
    """Test detection for all parks."""

    def test_detect_all_parks_queries_park_list(self):
        """detect_all_parks_for_date should query active parks."""
        mock_conn = Mock()

        # First call: get parks list
        mock_parks_result = Mock()
        mock_parks_result.__iter__ = Mock(return_value=iter([
            Mock(_mapping={'park_id': 1, 'name': 'Magic Kingdom', 'timezone': 'America/New_York'}),
            Mock(_mapping={'park_id': 2, 'name': 'Disneyland', 'timezone': 'America/Los_Angeles'})
        ]))

        # Second+ calls: detect sessions (return no activity)
        mock_session_result = Mock()
        mock_session_row = Mock()
        mock_session_row.first_activity = None
        mock_session_result.fetchone.return_value = mock_session_row

        mock_conn.execute.side_effect = [mock_parks_result, mock_session_result, mock_session_result]

        detector = OperatingHoursDetector(mock_conn)

        result = detector.detect_all_parks_for_date(date(2024, 7, 15))

        # Should return empty list (no activity)
        assert result == []
        # Should have called execute 3 times (parks query + 2 session queries)
        assert mock_conn.execute.call_count == 3


class TestBackfillOperatingSessions:
    """Test backfill functionality."""

    def test_backfill_date_range(self):
        """backfill_operating_sessions should process date range."""
        mock_conn = Mock()

        # Mock: first call returns park data, subsequent calls return no activity
        def execute_side_effect(*args, **kwargs):
            result = Mock()
            row = Mock()
            row.first_activity = None
            result.fetchone.return_value = row
            return result

        mock_conn.execute.side_effect = execute_side_effect

        detector = OperatingHoursDetector(mock_conn)

        # Backfill 3 days
        start_date = date(2024, 7, 15)
        end_date = date(2024, 7, 17)

        result = detector.backfill_operating_sessions(
            park_id=1,
            start_date=start_date,
            end_date=end_date,
            park_timezone="America/New_York"
        )

        # No sessions created (no activity found)
        assert result == 0
        # Should have called execute 3 times (once per day)
        assert mock_conn.execute.call_count == 3
