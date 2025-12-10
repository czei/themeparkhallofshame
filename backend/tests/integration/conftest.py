"""
Integration test fixtures and configuration.

Provides MySQL database fixtures for integration testing.

Fixture Types:
- mysql_connection: Isolated test database with transaction rollback
- replica_connection: Read-only production replica for fresh data validation

Safety Features:
- Blocks running tests against production/dev databases
- Validates required environment variables
- Checks replica lag before running replica tests
"""

import pytest
import os
from sqlalchemy import create_engine, text


# =============================================================================
# Safety Constants
# =============================================================================

# Database names that should NEVER be used for automated tests
PROTECTED_DATABASE_NAMES = [
    'themepark_tracker',       # Production
    'themepark_tracker_dev',   # Development
    'themepark_tracker_prod',  # Production alias
    'themepark_prod',          # Production alias
]


# =============================================================================
# Test Database Connection
# =============================================================================

def get_mysql_connection_string() -> str:
    """
    Get MySQL connection string from environment variables.

    Returns:
        MySQL connection string

    Raises:
        ValueError: If required environment variables are not set
    """
    required_vars = ['TEST_DB_HOST', 'TEST_DB_NAME', 'TEST_DB_USER', 'TEST_DB_PASSWORD']
    missing_vars = [var for var in required_vars if os.getenv(var) is None]

    if missing_vars:
        raise ValueError(
            f"Missing required environment variables: {', '.join(missing_vars)}. "
            "Set these before running integration tests."
        )

    host = os.getenv('TEST_DB_HOST')
    port = os.getenv('TEST_DB_PORT', '3306')
    database = os.getenv('TEST_DB_NAME')
    user = os.getenv('TEST_DB_USER')
    password = os.getenv('TEST_DB_PASSWORD')

    return f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"


@pytest.fixture(scope='session')
def mysql_engine():
    """
    Create MySQL engine for integration tests.

    Safety Features:
        - Validates required environment variables are set
        - Blocks running against protected database names (prod/dev)

    Yields:
        SQLAlchemy engine connected to test database

    Note:
        Requires environment variables:
        - TEST_DB_HOST
        - TEST_DB_PORT (default: 3306)
        - TEST_DB_NAME
        - TEST_DB_USER
        - TEST_DB_PASSWORD
    """
    # SAFETY CHECK: Prevent running tests against production or dev databases
    db_name = os.getenv('TEST_DB_NAME')
    if db_name in PROTECTED_DATABASE_NAMES:
        pytest.fail(
            f"SAFETY ERROR: TEST_DB_NAME='{db_name}' is a protected database.\n"
            f"Protected databases: {PROTECTED_DATABASE_NAMES}\n"
            f"Use 'themepark_test' or another dedicated test database.\n"
            f"This check prevents accidental data corruption in production/dev."
        )

    try:
        connection_string = get_mysql_connection_string()
        engine = create_engine(connection_string, echo=False)

        # Test connection
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))

        yield engine

        # Cleanup
        engine.dispose()

    except ValueError as e:
        pytest.skip(f"MySQL integration tests skipped: {e}")


@pytest.fixture
def mysql_connection(mysql_engine):
    """
    Provide MySQL connection with transaction rollback.

    Each test gets a fresh connection with a transaction that's
    rolled back after the test completes.

    Args:
        mysql_engine: MySQL engine fixture

    Yields:
        SQLAlchemy connection within a transaction
    """
    connection = mysql_engine.connect()
    transaction = connection.begin()

    try:
        yield connection
    finally:
        transaction.rollback()
        connection.close()


@pytest.fixture
def mysql_with_schema(mysql_connection):
    """
    Provide MySQL connection with schema created.

    Creates all required tables in the test database.
    Tables are dropped after the test completes (via rollback).

    Args:
        mysql_connection: MySQL connection fixture

    Yields:
        SQLAlchemy connection with schema
    """
    # Create schema (read from migrations or schema.sql)
    # For now, this is a placeholder
    # TODO: Load schema from database/schema.sql or migrations

    # Example table creation (minimal for testing)
    mysql_connection.execute(text("""
        CREATE TABLE IF NOT EXISTS parks (
            park_id INT AUTO_INCREMENT PRIMARY KEY,
            queue_times_id INT UNIQUE NOT NULL,
            name VARCHAR(255) NOT NULL,
            timezone VARCHAR(50) NOT NULL,
            latitude DECIMAL(10, 8),
            longitude DECIMAL(11, 8),
            is_active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        )
    """))

    mysql_connection.commit()

    yield mysql_connection

    # Cleanup (drop tables)
    mysql_connection.execute(text("DROP TABLE IF EXISTS parks"))
    mysql_connection.commit()


# Sample data fixtures (similar to unit test fixtures)

@pytest.fixture
def sample_park_data_mysql():
    """Sample park data for MySQL integration tests."""
    return {
        'queue_times_id': 101,
        'name': 'Magic Kingdom',
        'timezone': 'America/New_York',
        'latitude': 28.4177,
        'longitude': -81.5812,
        'is_active': True
    }


@pytest.fixture
def sample_ride_data_mysql():
    """Sample ride data for MySQL integration tests."""
    return {
        'queue_times_id': 1001,
        'park_id': None,  # Set in test
        'name': 'Space Mountain',
        'land_area': 'Tomorrowland',
        'tier': 1,
        'is_active': True
    }


# =============================================================================
# Production Replica Connection (for fresh data validation)
# =============================================================================

def get_replica_connection_string() -> str:
    """
    Get read-only replica connection string from environment variables.

    Returns:
        MySQL connection string for replica

    Raises:
        ValueError: If required environment variables are not set
    """
    required_vars = ['REPLICA_DB_HOST', 'REPLICA_DB_NAME', 'REPLICA_DB_USER', 'REPLICA_DB_PASSWORD']
    missing_vars = [var for var in required_vars if os.getenv(var) is None]

    if missing_vars:
        raise ValueError(
            f"Missing required replica environment variables: {', '.join(missing_vars)}. "
            "Set these to run replica validation tests."
        )

    host = os.getenv('REPLICA_DB_HOST')
    port = os.getenv('REPLICA_DB_PORT', '3306')
    database = os.getenv('REPLICA_DB_NAME')
    user = os.getenv('REPLICA_DB_USER')  # Should be a read-only user
    password = os.getenv('REPLICA_DB_PASSWORD')

    return f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"


@pytest.fixture(scope='session')
def replica_engine():
    """
    Create read-only connection to production replica.

    Use this fixture for tests that need fresh production-like data
    to validate time-sensitive aggregations.

    Yields:
        SQLAlchemy engine connected to read-only replica

    Note:
        Requires environment variables:
        - REPLICA_DB_HOST
        - REPLICA_DB_PORT (default: 3306)
        - REPLICA_DB_NAME
        - REPLICA_DB_USER (should be read-only)
        - REPLICA_DB_PASSWORD
    """
    try:
        connection_string = get_replica_connection_string()
        engine = create_engine(connection_string, echo=False)

        # Test connection
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))

        yield engine

        # Cleanup
        engine.dispose()

    except ValueError as e:
        pytest.skip(f"Replica tests skipped: {e}")


@pytest.fixture
def replica_connection(replica_engine):
    """
    Provide read-only connection to production replica.

    Unlike mysql_connection, this does NOT use transactions because
    we want to see the actual production data state.

    Args:
        replica_engine: Replica engine fixture

    Yields:
        SQLAlchemy connection (read-only)
    """
    connection = replica_engine.connect()
    try:
        yield connection
    finally:
        connection.close()


@pytest.fixture
def verify_replica_freshness(replica_connection):
    """
    Verify replica is within acceptable lag (≤5 minutes).

    This fixture should be used with tests that require fresh data.
    If the replica is too stale, the test will be skipped.

    Args:
        replica_connection: Replica connection fixture

    Yields:
        The lag in seconds (for informational purposes)
    """
    MAX_LAG_SECONDS = 300  # 5 minutes

    result = replica_connection.execute(text("""
        SELECT TIMESTAMPDIFF(SECOND,
            (SELECT MAX(recorded_at) FROM park_activity_snapshots),
            NOW()
        ) as lag_seconds
    """)).scalar()

    if result is None:
        pytest.skip("Replica has no data in park_activity_snapshots")

    if result > MAX_LAG_SECONDS:
        pytest.skip(
            f"Replica lag too high: {result}s (max allowed: {MAX_LAG_SECONDS}s). "
            f"Wait for replication to catch up or check replication health."
        )

    yield result  # Return lag for informational purposes in tests
