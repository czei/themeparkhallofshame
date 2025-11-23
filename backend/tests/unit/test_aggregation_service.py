"""
Theme Park Downtime Tracker - Aggregation Service Unit Tests

Tests AggregationService:
- Daily aggregation orchestration
- Timezone-aware processing
- Park and ride statistics calculation
- Aggregation log tracking
- Error handling

Note: Most methods use MySQL-specific SQL (NOW(), ON DUPLICATE KEY UPDATE).
These are skipped in unit tests and covered in integration tests with real MySQL.

Priority: P1 - Critical business logic for statistics generation
"""

import pytest
from datetime import datetime, date, time, timedelta
from zoneinfo import ZoneInfo
from unittest.mock import Mock, MagicMock, patch

from processor.aggregation_service import AggregationService


class TestAggregationServiceInit:
    """Test initialization and dependencies."""

    def test_init_creates_detectors(self, sqlite_connection):
        """__init__() should create OperatingHoursDetector and StatusChangeDetector."""
        service = AggregationService(sqlite_connection)

        assert service.conn == sqlite_connection
        assert service.hours_detector is not None
        assert service.change_detector is not None


class TestGetDistinctTimezones:
    """Test getting distinct timezones from active parks."""

    def test_get_distinct_timezones_no_parks(self, sqlite_connection):
        """_get_distinct_timezones() should return empty list when no active parks."""
        service = AggregationService(sqlite_connection)

        timezones = service._get_distinct_timezones()

        assert timezones == []

    def test_get_distinct_timezones_multiple_parks(self, sqlite_connection, sample_park_data):
        """_get_distinct_timezones() should return unique timezones sorted."""
        from tests.conftest import insert_sample_park

        # Create 3 parks: 2 in America/New_York, 1 in America/Los_Angeles
        park1_data = sample_park_data.copy()
        park1_data['timezone'] = 'America/New_York'
        insert_sample_park(sqlite_connection, park1_data)

        park2_data = sample_park_data.copy()
        park2_data['queue_times_id'] = 102
        park2_data['name'] = 'Epcot'
        park2_data['timezone'] = 'America/New_York'
        insert_sample_park(sqlite_connection, park2_data)

        park3_data = sample_park_data.copy()
        park3_data['queue_times_id'] = 103
        park3_data['name'] = 'Disneyland'
        park3_data['timezone'] = 'America/Los_Angeles'
        insert_sample_park(sqlite_connection, park3_data)

        service = AggregationService(sqlite_connection)
        timezones = service._get_distinct_timezones()

        # Should return 2 unique timezones, sorted alphabetically
        assert timezones == ['America/Los_Angeles', 'America/New_York']

    def test_get_distinct_timezones_ignores_inactive_parks(self, sqlite_connection, sample_park_data):
        """_get_distinct_timezones() should ignore inactive parks."""
        from tests.conftest import insert_sample_park

        # Create 1 active park and 1 inactive park with different timezones
        active_park_data = sample_park_data.copy()
        active_park_data['timezone'] = 'America/New_York'
        active_park_data['is_active'] = 1
        insert_sample_park(sqlite_connection, active_park_data)

        inactive_park_data = sample_park_data.copy()
        inactive_park_data['queue_times_id'] = 102
        inactive_park_data['name'] = 'Closed Park'
        inactive_park_data['timezone'] = 'America/Los_Angeles'
        inactive_park_data['is_active'] = 0
        insert_sample_park(sqlite_connection, inactive_park_data)

        service = AggregationService(sqlite_connection)
        timezones = service._get_distinct_timezones()

        # Should only return timezone from active park
        assert timezones == ['America/New_York']


class TestCreateAggregationLog:
    """Test aggregation log creation - skipped due to MySQL NOW()."""

    @pytest.mark.skip(reason="Uses MySQL NOW() function")
    def test_create_aggregation_log(self, sqlite_connection):
        """_create_aggregation_log() should create log entry (MySQL only)."""
        # Requires MySQL NOW() function
        pass


class TestCompleteAggregationLog:
    """Test aggregation log completion - skipped due to MySQL NOW()."""

    @pytest.mark.skip(reason="Uses MySQL NOW() function")
    def test_complete_aggregation_log_success(self, sqlite_connection):
        """_complete_aggregation_log() should update log with success (MySQL only)."""
        # Requires MySQL NOW() function
        pass

    @pytest.mark.skip(reason="Uses MySQL NOW() function")
    def test_complete_aggregation_log_error(self, sqlite_connection):
        """_complete_aggregation_log() should update log with error (MySQL only)."""
        # Requires MySQL NOW() function
        pass


class TestGetLastSuccessfulAggregation:
    """Test getting last successful aggregation - skipped due to MySQL-specific SQL."""

    @pytest.mark.skip(reason="Depends on aggregation_log with MySQL NOW()")
    def test_get_last_successful_aggregation_found(self, sqlite_connection):
        """get_last_successful_aggregation() should return most recent success (MySQL only)."""
        # Requires aggregation_log with MySQL NOW()
        pass

    @pytest.mark.skip(reason="Depends on aggregation_log with MySQL NOW()")
    def test_get_last_successful_aggregation_not_found(self, sqlite_connection):
        """get_last_successful_aggregation() should return None when no success (MySQL only)."""
        # Requires aggregation_log with MySQL NOW()
        pass


class TestAggregateParkDailyStats:
    """Test park daily stats aggregation - skipped due to MySQL-specific SQL."""

    @pytest.mark.skip(reason="Uses MySQL ON DUPLICATE KEY UPDATE")
    def test_aggregate_park_daily_stats(self, sqlite_connection, sample_park_data):
        """_aggregate_park_daily_stats() should calculate and save park stats (MySQL only)."""
        # Requires MySQL ON DUPLICATE KEY UPDATE
        pass


class TestAggregateRidesDailyStats:
    """Test ride daily stats aggregation - skipped due to MySQL-specific SQL."""

    @pytest.mark.skip(reason="Uses MySQL ON DUPLICATE KEY UPDATE")
    def test_aggregate_rides_daily_stats(self, sqlite_connection):
        """_aggregate_rides_daily_stats() should calculate stats for all rides (MySQL only)."""
        # Requires MySQL ON DUPLICATE KEY UPDATE
        pass


class TestAggregateSingleRideDailyStats:
    """Test single ride daily stats aggregation - skipped due to MySQL-specific SQL."""

    @pytest.mark.skip(reason="Uses MySQL ON DUPLICATE KEY UPDATE")
    def test_aggregate_single_ride_daily_stats_with_data(self, sqlite_connection):
        """_aggregate_single_ride_daily_stats() should calculate ride stats (MySQL only)."""
        # Requires MySQL ON DUPLICATE KEY UPDATE
        pass

    @pytest.mark.skip(reason="Uses MySQL ON DUPLICATE KEY UPDATE")
    def test_aggregate_single_ride_daily_stats_no_data(self, sqlite_connection):
        """_aggregate_single_ride_daily_stats() should skip ride with no snapshots (MySQL only)."""
        # Requires MySQL ON DUPLICATE KEY UPDATE
        pass


class TestAggregateDailyForTimezone:
    """Test daily aggregation for a specific timezone - skipped due to MySQL dependencies."""

    @pytest.mark.skip(reason="Depends on save_operating_session and stats methods with MySQL SQL")
    def test_aggregate_daily_for_timezone_single_park(self, sqlite_connection):
        """_aggregate_daily_for_timezone() should process parks in timezone (MySQL only)."""
        # Depends on MySQL-specific methods
        pass

    @pytest.mark.skip(reason="Depends on save_operating_session and stats methods with MySQL SQL")
    def test_aggregate_daily_for_timezone_no_operating_session(self, sqlite_connection):
        """_aggregate_daily_for_timezone() should skip parks with no activity (MySQL only)."""
        # Depends on MySQL-specific methods
        pass


class TestAggregateDaily:
    """Test daily aggregation orchestration - skipped due to MySQL dependencies."""

    @pytest.mark.skip(reason="Depends on aggregation log and stats methods with MySQL SQL")
    def test_aggregate_daily_single_timezone(self, sqlite_connection):
        """aggregate_daily() should orchestrate aggregation for specific timezone (MySQL only)."""
        # Depends on MySQL-specific methods
        pass

    @pytest.mark.skip(reason="Depends on aggregation log and stats methods with MySQL SQL")
    def test_aggregate_daily_all_timezones(self, sqlite_connection):
        """aggregate_daily() should orchestrate aggregation for all timezones (MySQL only)."""
        # Depends on MySQL-specific methods
        pass

    @pytest.mark.skip(reason="Depends on aggregation log and stats methods with MySQL SQL")
    def test_aggregate_daily_handles_errors(self, sqlite_connection):
        """aggregate_daily() should log errors and re-raise (MySQL only)."""
        # Depends on MySQL-specific methods
        pass
