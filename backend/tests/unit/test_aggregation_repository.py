"""
Theme Park Downtime Tracker - Aggregation Repository Unit Tests

Tests AggregationLogRepository:
- Insert and update aggregation logs
- Query operations (by date/type, recent logs, failed aggregations)
- Mark complete/failed
- Aggregation verification (is_date_aggregated)

Priority: P1 - Critical for safe cleanup verification
"""

import pytest
import sys
from pathlib import Path
from datetime import datetime, date

backend_src = Path(__file__).parent.parent.parent / 'src'
sys.path.insert(0, str(backend_src.absolute()))

from database.repositories.aggregation_repository import AggregationLogRepository


class TestAggregationLogRepository:
    """Test aggregation log operations."""

    def test_insert_aggregation_log(self, sqlite_connection):
        """Insert a new aggregation log entry."""
        repo = AggregationLogRepository(sqlite_connection)

        log_data = {
            'aggregation_date': date.today(),
            'aggregation_type': 'daily',
            'started_at': datetime.now(),
            'status': 'running',
            'parks_processed': 0,
            'rides_processed': 0
        }

        log_id = repo.insert(log_data)

        assert log_id is not None
        assert log_id > 0

    def test_get_by_id(self, sqlite_connection):
        """Get aggregation log by ID."""
        repo = AggregationLogRepository(sqlite_connection)

        log_data = {
            'aggregation_date': date.today(),
            'aggregation_type': 'daily',
            'started_at': datetime.now(),
            'status': 'running',
            'parks_processed': 0,
            'rides_processed': 0
        }

        log_id = repo.insert(log_data)
        fetched_log = repo.get_by_id(log_id)

        assert fetched_log is not None
        assert fetched_log['log_id'] == log_id
        assert fetched_log['aggregation_type'] == 'daily'

    def test_get_by_id_not_found(self, sqlite_connection):
        """Get aggregation log for nonexistent ID."""
        repo = AggregationLogRepository(sqlite_connection)

        result = repo.get_by_id(999)

        assert result is None

    def test_update_aggregation_log(self, sqlite_connection):
        """Update an aggregation log entry."""
        repo = AggregationLogRepository(sqlite_connection)

        log_data = {
            'aggregation_date': date.today(),
            'aggregation_type': 'daily',
            'started_at': datetime.now(),
            'status': 'running',
            'parks_processed': 0,
            'rides_processed': 0
        }

        log_id = repo.insert(log_data)

        # Update parks_processed
        update_data = {
            'log_id': log_id,
            'parks_processed': 5,
            'rides_processed': 25
        }
        success = repo.update(update_data)

        assert success is True

        # Verify update
        fetched_log = repo.get_by_id(log_id)
        assert fetched_log['parks_processed'] == 5
        assert fetched_log['rides_processed'] == 25

    @pytest.mark.skip(reason="Requires MySQL-specific NOW() function")
    def test_mark_complete(self, sqlite_connection):
        """Mark an aggregation as successfully completed."""
        repo = AggregationLogRepository(sqlite_connection)

        log_data = {
            'aggregation_date': date.today(),
            'aggregation_type': 'daily',
            'started_at': datetime.now(),
            'status': 'running',
            'parks_processed': 0,
            'rides_processed': 0
        }

        log_id = repo.insert(log_data)
        success = repo.mark_complete(log_id, parks_processed=10, rides_processed=50)

        assert success is True

        # Verify status changed to 'success'
        fetched_log = repo.get_by_id(log_id)
        assert fetched_log['status'] == 'success'
        assert fetched_log['parks_processed'] == 10
        assert fetched_log['rides_processed'] == 50
        assert fetched_log['completed_at'] is not None

    @pytest.mark.skip(reason="Requires MySQL-specific NOW() function")
    def test_mark_failed(self, sqlite_connection):
        """Mark an aggregation as failed."""
        repo = AggregationLogRepository(sqlite_connection)

        log_data = {
            'aggregation_date': date.today(),
            'aggregation_type': 'daily',
            'started_at': datetime.now(),
            'status': 'running',
            'parks_processed': 0,
            'rides_processed': 0
        }

        log_id = repo.insert(log_data)
        success = repo.mark_failed(log_id, error_message='Test error')

        assert success is True

        # Verify status changed to 'failed'
        fetched_log = repo.get_by_id(log_id)
        assert fetched_log['status'] == 'failed'
        assert fetched_log['error_message'] == 'Test error'
        assert fetched_log['completed_at'] is not None

    @pytest.mark.skip(reason="Requires MySQL-specific NOW() function (calls mark_complete)")
    def test_is_date_aggregated_success(self, sqlite_connection):
        """Check if a date has been successfully aggregated."""
        repo = AggregationLogRepository(sqlite_connection)

        target_date = date.today()
        log_data = {
            'aggregation_date': target_date,
            'aggregation_type': 'daily',
            'started_at': datetime.now(),
            'status': 'running',
            'parks_processed': 0,
            'rides_processed': 0
        }

        log_id = repo.insert(log_data)
        repo.mark_complete(log_id, parks_processed=10, rides_processed=50)

        is_aggregated = repo.is_date_aggregated(target_date, 'daily')

        assert is_aggregated is True

    def test_is_date_aggregated_not_found(self, sqlite_connection):
        """Check if non-aggregated date returns False."""
        repo = AggregationLogRepository(sqlite_connection)

        is_aggregated = repo.is_date_aggregated(date(2099, 12, 31), 'daily')

        assert is_aggregated is False

    @pytest.mark.skip(reason="Requires MySQL-specific NOW() function (calls mark_failed)")
    def test_is_date_aggregated_failed_status(self, sqlite_connection):
        """Check that failed aggregations don't count as aggregated."""
        repo = AggregationLogRepository(sqlite_connection)

        target_date = date.today()
        log_data = {
            'aggregation_date': target_date,
            'aggregation_type': 'daily',
            'started_at': datetime.now(),
            'status': 'running',
            'parks_processed': 0,
            'rides_processed': 0
        }

        log_id = repo.insert(log_data)
        repo.mark_failed(log_id, error_message='Test failure')

        is_aggregated = repo.is_date_aggregated(target_date, 'daily')

        assert is_aggregated is False

    def test_get_aggregation_status(self, sqlite_connection):
        """Get the status of an aggregation for a specific date."""
        repo = AggregationLogRepository(sqlite_connection)

        target_date = date.today()
        log_data = {
            'aggregation_date': target_date,
            'aggregation_type': 'daily',
            'started_at': datetime.now(),
            'status': 'success',
            'parks_processed': 10,
            'rides_processed': 50
        }

        repo.insert(log_data)

        status = repo.get_aggregation_status(target_date, 'daily')

        assert status == 'success'

    def test_get_aggregation_status_not_found(self, sqlite_connection):
        """Get status for non-existent aggregation."""
        repo = AggregationLogRepository(sqlite_connection)

        status = repo.get_aggregation_status(date(2099, 12, 31), 'daily')

        assert status is None
