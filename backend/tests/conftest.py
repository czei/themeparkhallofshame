"""
Theme Park Downtime Tracker - pytest Configuration and Fixtures

Provides shared test fixtures for:
- Sample data objects (Parks, Rides, etc.)
- Mock connections and repositories
- Helper functions for test data insertion

Note: Database connection fixtures are in tests/integration/conftest.py
"""

import pytest
from datetime import datetime, date, timedelta
from sqlalchemy import text
from sqlalchemy.engine import Connection
from unittest.mock import Mock

# Note: create_engine import removed - SQLite fixtures deleted


# ============================================================================
# Sample Data Fixtures
# ============================================================================

# SQLite fixtures removed - all repository tests now use MySQL integration tests
# See tests/integration/conftest.py for mysql_connection fixture

# ============================================================================
# Sample Data Fixtures
# ============================================================================

@pytest.fixture
def sample_park_data():
    """
    Sample park dictionary for testing.

    Returns:
        Dictionary with park data matching parks table schema
    """
    return {
        'queue_times_id': 101,
        'name': 'Magic Kingdom',
        'city': 'Orlando',
        'state_province': 'FL',
        'country': 'US',
        'latitude': 28.385233,
        'longitude': -81.563873,
        'timezone': 'America/New_York',
        'operator': 'Disney',
        'is_disney': True,
        'is_universal': False,
        'is_active': True
    }


@pytest.fixture
def sample_ride_data():
    """
    Sample ride dictionary for testing.

    Returns:
        Dictionary with ride data matching rides table schema
    """
    return {
        'queue_times_id': 1001,
        'park_id': 1,  # Assumes park with ID 1 exists
        'name': 'Space Mountain',
        'land_area': 'Tomorrowland',
        'tier': 1,
        'is_active': True
    }


@pytest.fixture
def sample_snapshot_data():
    """
    Sample ride status snapshot dictionary for testing.

    Returns:
        Dictionary with snapshot data matching ride_status_snapshots table
    """
    return {
        'ride_id': 1,
        'recorded_at': datetime.now(),
        'wait_time': 45,
        'is_open': True,
        'computed_is_open': True
    }


@pytest.fixture
def sample_status_change_data():
    """
    Sample status change dictionary for testing.

    Returns:
        Dictionary with status change data matching ride_status_changes table
    """
    return {
        'ride_id': 1,
        'changed_at': datetime.now(),
        'previous_status': True,
        'new_status': False,
        'duration_in_previous_status': 120,
        'wait_time_at_change': None
    }


@pytest.fixture
def sample_aggregation_log_data():
    """
    Sample aggregation log dictionary for testing.

    Returns:
        Dictionary with aggregation log data
    """
    return {
        'aggregation_date': date.today(),
        'aggregation_type': 'daily',
        'started_at': datetime.now(),
        'status': 'running',
        'parks_processed': 0,
        'rides_processed': 0
    }


# ============================================================================
# Mock Fixtures
# ============================================================================

@pytest.fixture
def mock_db_connection():
    """
    Mock SQLAlchemy connection for unit tests that don't need real DB.

    Returns:
        Mock Connection object
    """
    conn = Mock(spec=Connection)
    conn.execute = Mock()
    conn.commit = Mock()
    conn.close = Mock()
    return conn


@pytest.fixture
def mock_queue_times_client():
    """
    Mock Queue-Times API client for testing collectors.

    Returns:
        Mock QueueTimesClient
    """
    client = Mock()
    client.get_parks = Mock(return_value=[])
    client.get_park_wait_times = Mock(return_value={'rides': []})
    return client


# ============================================================================
# Helper Functions
# ============================================================================

def insert_sample_park(conn: Connection, park_data: dict) -> int:
    """
    Insert a sample park into the test database.

    Args:
        conn: SQLAlchemy connection
        park_data: Dictionary with park fields

    Returns:
        park_id of inserted record

    Note:
        Does NOT commit - relies on test fixture transaction management.
        For MySQL integration tests, transaction is rolled back after test.
    """
    result = conn.execute(text("""
        INSERT INTO parks (
            queue_times_id, name, city, state_province, country,
            latitude, longitude, timezone, operator,
            is_disney, is_universal, is_active
        )
        VALUES (
            :queue_times_id, :name, :city, :state_province, :country,
            :latitude, :longitude, :timezone, :operator,
            :is_disney, :is_universal, :is_active
        )
    """), park_data)
    # No commit - relies on fixture transaction management
    return result.lastrowid


def insert_sample_ride(conn: Connection, ride_data: dict) -> int:
    """
    Insert a sample ride into the test database.

    Args:
        conn: SQLAlchemy connection
        ride_data: Dictionary with ride fields

    Returns:
        ride_id of inserted record

    Note:
        Does NOT commit - relies on test fixture transaction management.
        For MySQL integration tests, transaction is rolled back after test.
    """
    result = conn.execute(text("""
        INSERT INTO rides (
            queue_times_id, park_id, name, land_area, tier, is_active
        )
        VALUES (
            :queue_times_id, :park_id, :name, :land_area, :tier, :is_active
        )
    """), ride_data)
    # No commit - relies on fixture transaction management
    return result.lastrowid
