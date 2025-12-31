"""
Smoke Test Fixtures

Provides fixtures for running smoke tests against mirrored production data.
These tests connect to the DEV database (themepark_tracker_dev) which should
be populated using mirror-production-db.sh before running.

Usage:
    # First mirror production data
    ./deployment/scripts/mirror-production-db.sh --days=7

    # Then run smoke tests
    pytest tests/smoke/ -v -m smoke
"""

import os
import pytest
from datetime import datetime, timezone
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

# Smoke tests use the DEV database (mirrored from production)
# NOT the test database
DEV_DATABASE_NAME = 'themepark_tracker_dev'


def get_dev_db_connection_string() -> str:
    """
    Get connection string for dev database (mirrored production data).

    Uses SMOKE_DB_* env vars if set, otherwise defaults to local dev DB.
    """
    host = os.environ.get('SMOKE_DB_HOST', 'localhost')
    port = os.environ.get('SMOKE_DB_PORT', '3306')
    user = os.environ.get('SMOKE_DB_USER', 'root')
    password = os.environ.get('SMOKE_DB_PASSWORD', '294e043ww')
    database = os.environ.get('SMOKE_DB_NAME', DEV_DATABASE_NAME)

    return f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"


@pytest.fixture(scope="module")
def smoke_engine():
    """
    Create SQLAlchemy engine for smoke tests against dev database.

    Skips tests if dev database is not available or has no recent data.
    """
    conn_string = get_dev_db_connection_string()

    try:
        engine = create_engine(conn_string, echo=False)

        # Verify connection and check for recent data
        with engine.connect() as conn:
            # Check database exists and is accessible
            result = conn.execute(text("SELECT 1"))

            # Check for recent snapshot data (within last 7 days)
            freshness = conn.execute(text("""
                SELECT
                    MAX(recorded_at) as latest,
                    TIMESTAMPDIFF(HOUR, MAX(recorded_at), NOW()) as hours_old
                FROM park_activity_snapshots
            """)).fetchone()

            if freshness.latest is None:
                pytest.skip("Dev database has no snapshot data. Run mirror-production-db.sh first.")

            if freshness.hours_old and freshness.hours_old > 168:  # 7 days
                pytest.skip(f"Dev database data is {freshness.hours_old} hours old. Run mirror-production-db.sh to refresh.")

        yield engine

    except Exception as e:
        pytest.skip(f"Dev database connection failed: {e}")


@pytest.fixture(scope="function")
def smoke_session(smoke_engine):
    """
    Provide SQLAlchemy Session for smoke tests.

    Read-only - smoke tests should not modify data.
    """
    session = Session(bind=smoke_engine)
    try:
        yield session
    finally:
        # Rollback any accidental changes (should be none)
        session.rollback()
        session.close()


@pytest.fixture(scope="module")
def smoke_connection(smoke_engine):
    """
    Provide raw SQLAlchemy Connection for smoke tests.

    Read-only - smoke tests should not modify data.
    """
    with smoke_engine.connect() as conn:
        yield conn


@pytest.fixture(scope="module")
def data_freshness(smoke_engine):
    """
    Get information about the freshness of mirrored data.

    Returns dict with:
    - latest_snapshot: datetime of most recent snapshot
    - hours_old: how old the data is
    - parks_with_data: count of parks with recent snapshots
    """
    with smoke_engine.connect() as conn:
        result = conn.execute(text("""
            SELECT
                MAX(recorded_at) as latest_snapshot,
                TIMESTAMPDIFF(HOUR, MAX(recorded_at), NOW()) as hours_old,
                COUNT(DISTINCT park_id) as parks_with_data
            FROM park_activity_snapshots
            WHERE recorded_at >= DATE_SUB(NOW(), INTERVAL 7 DAY)
        """)).fetchone()

        return {
            'latest_snapshot': result.latest_snapshot,
            'hours_old': result.hours_old or 0,
            'parks_with_data': result.parks_with_data or 0
        }
