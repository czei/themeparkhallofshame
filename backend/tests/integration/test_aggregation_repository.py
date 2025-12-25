"""
Theme Park Downtime Tracker - Aggregation Repository Unit Tests

Tests AggregationLogRepository:
- Insert and update aggregation logs
- Query operations (by date/type, recent logs, failed aggregations)
- Mark complete/failed
- Aggregation verification (is_date_aggregated)

Priority: P1 - Critical for safe cleanup verification

NOTE (2025-12-24 ORM Migration):
- Repositories now use SQLAlchemy ORM Session
- Tests updated to use mysql_session fixture instead of mysql_session
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

    def test_insert_aggregation_log(self, mysql_session):
        """Insert a new aggregation log entry."""
        repo = AggregationLogRepository(mysql_session)

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

    def test_get_by_id(self, mysql_session):
        """Get aggregation log by ID."""
        repo = AggregationLogRepository(mysql_session)

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

    def test_get_by_id_not_found(self, mysql_session):
        """Get aggregation log for nonexistent ID."""
        repo = AggregationLogRepository(mysql_session)

        result = repo.get_by_id(999)

        assert result is None

    def test_update_aggregation_log(self, mysql_session):
        """Update an aggregation log entry."""
        repo = AggregationLogRepository(mysql_session)

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

    def test_mark_complete(self, mysql_session):
        """Mark an aggregation as successfully completed."""
        repo = AggregationLogRepository(mysql_session)

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

    def test_mark_failed(self, mysql_session):
        """Mark an aggregation as failed."""
        repo = AggregationLogRepository(mysql_session)

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

    def test_is_date_aggregated_success(self, mysql_session):
        """Check if a date has been successfully aggregated."""
        repo = AggregationLogRepository(mysql_session)

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

    def test_is_date_aggregated_not_found(self, mysql_session):
        """Check if non-aggregated date returns False."""
        repo = AggregationLogRepository(mysql_session)

        is_aggregated = repo.is_date_aggregated(date(2099, 12, 31), 'daily')

        assert is_aggregated is False

    def test_is_date_aggregated_failed_status(self, mysql_session):
        """Check that failed aggregations don't count as aggregated."""
        repo = AggregationLogRepository(mysql_session)

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

    def test_get_aggregation_status(self, mysql_session):
        """Get the status of an aggregation for a specific date."""
        repo = AggregationLogRepository(mysql_session)

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

    def test_get_aggregation_status_not_found(self, mysql_session):
        """Get status for non-existent aggregation."""
        repo = AggregationLogRepository(mysql_session)

        status = repo.get_aggregation_status(date(2099, 12, 31), 'daily')

        assert status is None
