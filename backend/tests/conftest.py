"""
Theme Park Downtime Tracker - pytest Configuration and Fixtures

Provides shared test fixtures for:
- In-memory SQLite database setup
- Sample data objects (Parks, Rides, etc.)
- Mock connections and repositories
"""

import pytest
from datetime import datetime, date, timedelta
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection
from unittest.mock import Mock


# ============================================================================
# Database Fixtures
# ============================================================================

@pytest.fixture
def sqlite_engine():
    """
    Create an in-memory SQLite database engine.

    Returns:
        SQLAlchemy Engine for in-memory SQLite database
    """
    engine = create_engine('sqlite:///:memory:', echo=False)
    return engine


@pytest.fixture
def sqlite_connection(sqlite_engine):
    """
    Create a database connection with schema initialized.

    This fixture:
    1. Creates all tables matching the MySQL schema
    2. Yields a connection for testing
    3. Rolls back and closes after test completes

    Returns:
        SQLAlchemy Connection object
    """
    connection = sqlite_engine.connect()

    # Create schema (simplified MySQL → SQLite mappings)
    _create_schema(connection)

    yield connection

    # Cleanup
    connection.close()


def _create_schema(conn: Connection):
    """
    Create database schema for testing.

    Converts MySQL schema to SQLite-compatible SQL.
    """
    # Parks table
    conn.execute(text("""
        CREATE TABLE parks (
            park_id INTEGER PRIMARY KEY AUTOINCREMENT,
            queue_times_id INTEGER UNIQUE NOT NULL,
            name TEXT NOT NULL,
            city TEXT NOT NULL,
            state_province TEXT,
            country TEXT NOT NULL,
            latitude REAL,
            longitude REAL,
            timezone TEXT NOT NULL DEFAULT 'UTC',
            operator TEXT,
            is_disney INTEGER NOT NULL DEFAULT 0,
            is_universal INTEGER NOT NULL DEFAULT 0,
            is_active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """))

    # Rides table
    conn.execute(text("""
        CREATE TABLE rides (
            ride_id INTEGER PRIMARY KEY AUTOINCREMENT,
            park_id INTEGER NOT NULL,
            queue_times_id INTEGER UNIQUE NOT NULL,
            name TEXT NOT NULL,
            land_area TEXT,
            tier INTEGER,
            is_active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (park_id) REFERENCES parks(park_id) ON DELETE CASCADE
        )
    """))

    # Ride Classifications table
    conn.execute(text("""
        CREATE TABLE ride_classifications (
            classification_id INTEGER PRIMARY KEY AUTOINCREMENT,
            ride_id INTEGER NOT NULL,
            tier INTEGER NOT NULL,
            classification_method TEXT NOT NULL,
            confidence_score REAL,
            classified_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (ride_id) REFERENCES rides(ride_id) ON DELETE CASCADE
        )
    """))

    # Ride Status Snapshots table
    conn.execute(text("""
        CREATE TABLE ride_status_snapshots (
            snapshot_id INTEGER PRIMARY KEY AUTOINCREMENT,
            ride_id INTEGER NOT NULL,
            recorded_at TEXT NOT NULL,
            wait_time INTEGER,
            is_open INTEGER,
            computed_is_open INTEGER NOT NULL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (ride_id) REFERENCES rides(ride_id) ON DELETE CASCADE
        )
    """))

    # Ride Status Changes table
    conn.execute(text("""
        CREATE TABLE ride_status_changes (
            change_id INTEGER PRIMARY KEY AUTOINCREMENT,
            ride_id INTEGER NOT NULL,
            changed_at TEXT NOT NULL,
            old_status INTEGER NOT NULL,
            new_status INTEGER NOT NULL,
            downtime_duration_minutes INTEGER,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (ride_id) REFERENCES rides(ride_id) ON DELETE CASCADE
        )
    """))

    # Park Activity Snapshots table
    conn.execute(text("""
        CREATE TABLE park_activity_snapshots (
            activity_id INTEGER PRIMARY KEY AUTOINCREMENT,
            park_id INTEGER NOT NULL,
            recorded_at TEXT NOT NULL,
            park_appears_open INTEGER NOT NULL,
            active_rides_count INTEGER NOT NULL DEFAULT 0,
            total_rides_count INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (park_id) REFERENCES parks(park_id) ON DELETE CASCADE
        )
    """))

    # Daily Stats table
    conn.execute(text("""
        CREATE TABLE daily_stats (
            stat_id INTEGER PRIMARY KEY AUTOINCREMENT,
            park_id INTEGER NOT NULL,
            ride_id INTEGER,
            stat_date TEXT NOT NULL,
            total_downtime_minutes INTEGER NOT NULL DEFAULT 0,
            downtime_percentage REAL NOT NULL DEFAULT 0.0,
            status_changes_count INTEGER NOT NULL DEFAULT 0,
            avg_wait_time REAL,
            max_wait_time INTEGER,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (park_id) REFERENCES parks(park_id) ON DELETE CASCADE,
            FOREIGN KEY (ride_id) REFERENCES rides(ride_id) ON DELETE CASCADE
        )
    """))

    # Weekly Stats table
    conn.execute(text("""
        CREATE TABLE weekly_stats (
            stat_id INTEGER PRIMARY KEY AUTOINCREMENT,
            park_id INTEGER NOT NULL,
            ride_id INTEGER,
            week_start_date TEXT NOT NULL,
            total_downtime_minutes INTEGER NOT NULL DEFAULT 0,
            downtime_percentage REAL NOT NULL DEFAULT 0.0,
            status_changes_count INTEGER NOT NULL DEFAULT 0,
            avg_wait_time REAL,
            max_wait_time INTEGER,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (park_id) REFERENCES parks(park_id) ON DELETE CASCADE,
            FOREIGN KEY (ride_id) REFERENCES rides(ride_id) ON DELETE CASCADE
        )
    """))

    # Monthly Stats table
    conn.execute(text("""
        CREATE TABLE monthly_stats (
            stat_id INTEGER PRIMARY KEY AUTOINCREMENT,
            park_id INTEGER NOT NULL,
            ride_id INTEGER,
            month_start_date TEXT NOT NULL,
            total_downtime_minutes INTEGER NOT NULL DEFAULT 0,
            downtime_percentage REAL NOT NULL DEFAULT 0.0,
            status_changes_count INTEGER NOT NULL DEFAULT 0,
            avg_wait_time REAL,
            max_wait_time INTEGER,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (park_id) REFERENCES parks(park_id) ON DELETE CASCADE,
            FOREIGN KEY (ride_id) REFERENCES rides(ride_id) ON DELETE CASCADE
        )
    """))

    # Yearly Stats table
    conn.execute(text("""
        CREATE TABLE yearly_stats (
            stat_id INTEGER PRIMARY KEY AUTOINCREMENT,
            park_id INTEGER NOT NULL,
            ride_id INTEGER,
            year INTEGER NOT NULL,
            total_downtime_minutes INTEGER NOT NULL DEFAULT 0,
            downtime_percentage REAL NOT NULL DEFAULT 0.0,
            status_changes_count INTEGER NOT NULL DEFAULT 0,
            avg_wait_time REAL,
            max_wait_time INTEGER,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (park_id) REFERENCES parks(park_id) ON DELETE CASCADE,
            FOREIGN KEY (ride_id) REFERENCES rides(ride_id) ON DELETE CASCADE
        )
    """))

    # Aggregation Log table
    conn.execute(text("""
        CREATE TABLE aggregation_log (
            log_id INTEGER PRIMARY KEY AUTOINCREMENT,
            aggregation_date TEXT NOT NULL,
            aggregation_type TEXT NOT NULL,
            started_at TEXT NOT NULL,
            completed_at TEXT,
            status TEXT NOT NULL,
            parks_processed INTEGER NOT NULL DEFAULT 0,
            rides_processed INTEGER NOT NULL DEFAULT 0,
            error_message TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """))

    conn.commit()


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
        'old_status': True,
        'new_status': False,
        'downtime_duration_minutes': 120
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
    conn.commit()
    return result.lastrowid


def insert_sample_ride(conn: Connection, ride_data: dict) -> int:
    """
    Insert a sample ride into the test database.

    Args:
        conn: SQLAlchemy connection
        ride_data: Dictionary with ride fields

    Returns:
        ride_id of inserted record
    """
    result = conn.execute(text("""
        INSERT INTO rides (
            queue_times_id, park_id, name, land_area, tier, is_active
        )
        VALUES (
            :queue_times_id, :park_id, :name, :land_area, :tier, :is_active
        )
    """), ride_data)
    conn.commit()
    return result.lastrowid
