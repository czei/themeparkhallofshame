"""
Performance test fixtures and configuration.

Imports mysql_connection fixture from integration tests and adds
proper skip handling for when database is not available.
"""

import os
import pytest
from sqlalchemy import create_engine, text


def _has_test_database_config():
    """Check if test database environment variables are configured."""
    required_vars = ['TEST_DB_HOST', 'TEST_DB_NAME', 'TEST_DB_USER', 'TEST_DB_PASSWORD']
    return all(os.getenv(var) for var in required_vars)


def get_mysql_connection_string() -> str:
    """Get MySQL connection string from environment variables."""
    host = os.getenv('TEST_DB_HOST')
    port = os.getenv('TEST_DB_PORT', '3306')
    database = os.getenv('TEST_DB_NAME')
    user = os.getenv('TEST_DB_USER')
    password = os.getenv('TEST_DB_PASSWORD')
    return f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"


@pytest.fixture(scope='session')
def mysql_engine():
    """
    Create MySQL engine for performance tests.

    Skips all tests if TEST_DB_* environment variables are not set.
    """
    if not _has_test_database_config():
        pytest.skip(
            "Performance tests require TEST_DB_* environment variables. "
            "Set TEST_DB_HOST, TEST_DB_NAME, TEST_DB_USER, TEST_DB_PASSWORD."
        )

    try:
        connection_string = get_mysql_connection_string()
        engine = create_engine(connection_string, echo=False)

        # Test connection
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))

        yield engine

        engine.dispose()

    except Exception as e:
        pytest.skip(f"Performance tests skipped - database connection failed: {e}")


@pytest.fixture
def mysql_connection(mysql_engine):
    """
    Provide MySQL connection for performance tests.

    Unlike integration tests, performance tests do NOT use transaction
    rollback since they need to see real data state for accurate timing.
    """
    connection = mysql_engine.connect()
    try:
        yield connection
    finally:
        connection.close()


@pytest.fixture
def mysql_session(mysql_engine):
    """
    Provide MySQL Session for performance tests.

    Required for query classes that use ORM Session.query() API
    like TodayParkWaitTimesQuery which uses StatsRepository.
    """
    from sqlalchemy.orm import Session

    with Session(mysql_engine) as session:
        yield session
