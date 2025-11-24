"""
Integration test fixtures and configuration.

Provides MySQL database fixtures for integration testing.
"""

import pytest
import os
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


def get_mysql_connection_string() -> str:
    """
    Get MySQL connection string from environment variables.

    Returns:
        MySQL connection string

    Raises:
        ValueError: If required environment variables are not set
    """
    required_vars = ['TEST_DB_HOST', 'TEST_DB_NAME', 'TEST_DB_USER', 'TEST_DB_PASSWORD']
    missing_vars = [var for var in required_vars if not os.getenv(var)]

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
