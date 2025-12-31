"""
Golden Data Test Fixtures

Provides fixtures for loading captured production data snapshots
for deterministic regression testing.

Note: Large SQL files are loaded via mysql CLI for performance.
"""

import json
import os
import subprocess
import pytest
from pathlib import Path
from datetime import datetime, timezone, timedelta
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

GOLDEN_DATA_DIR = Path(__file__).parent / "datasets"


# =============================================================================
# Database Fixtures (copied from integration/conftest.py)
# =============================================================================

def get_mysql_connection_string() -> str:
    """Get MySQL connection string from environment variables."""
    host = os.environ.get('TEST_DB_HOST', 'localhost')
    port = os.environ.get('TEST_DB_PORT', '3306')
    user = os.environ.get('TEST_DB_USER', 'themepark_test')
    password = os.environ.get('TEST_DB_PASSWORD', 'test_password')
    database = os.environ.get('TEST_DB_NAME', 'themepark_test')

    return f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"


@pytest.fixture(scope="module")
def mysql_engine():
    """Create MySQL engine for golden data tests."""
    conn_string = get_mysql_connection_string()

    try:
        engine = create_engine(conn_string, echo=False)
        # Test connection
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        yield engine
    except Exception as e:
        pytest.skip(f"MySQL connection failed: {e}")


@pytest.fixture(scope="module")
def mysql_session(mysql_engine):
    """
    Provide SQLAlchemy ORM Session for golden data tests.

    Module-scoped to allow golden data fixtures to load data once
    and share it across tests in the same module.
    """
    session = Session(bind=mysql_engine)
    try:
        yield session
    finally:
        session.close()


def get_dataset_path(date_str: str) -> Path:
    """Get the path to a golden dataset directory."""
    return GOLDEN_DATA_DIR / date_str


def load_sql_via_cli(sql_path: Path):
    """Load SQL file via mysql CLI for performance."""
    if not sql_path.exists():
        raise FileNotFoundError(f"Golden data SQL file not found: {sql_path}")

    # Get DB connection params from environment
    host = os.environ.get('TEST_DB_HOST', 'localhost')
    port = os.environ.get('TEST_DB_PORT', '3306')
    user = os.environ.get('TEST_DB_USER', 'themepark_test')
    password = os.environ.get('TEST_DB_PASSWORD', 'test_password')
    database = os.environ.get('TEST_DB_NAME', 'themepark_test')

    cmd = [
        'mysql',
        f'-h{host}',
        f'-P{port}',
        f'-u{user}',
        f'-p{password}',
        database
    ]

    with open(sql_path, 'r') as f:
        result = subprocess.run(cmd, stdin=f, capture_output=True, text=True)
        if result.returncode != 0:
            # Ignore duplicate key warnings
            if 'Duplicate entry' not in result.stderr:
                raise RuntimeError(f"Failed to load {sql_path}: {result.stderr}")


def load_sql_file(session, sql_path: Path):
    """Load SQL statements from a file into the database (for small files)."""
    if not sql_path.exists():
        raise FileNotFoundError(f"Golden data SQL file not found: {sql_path}")

    conn = session.connection()
    sql_content = sql_path.read_text()

    # Split on semicolons but handle edge cases
    for statement in sql_content.split(';'):
        statement = statement.strip()
        if statement and not statement.startswith('--'):
            try:
                conn.execute(text(statement))
            except Exception as e:
                if 'Duplicate entry' not in str(e):
                    raise

    session.commit()


def load_expected_results(date_str: str, result_name: str) -> dict:
    """Load expected results JSON for a golden dataset."""
    expected_path = get_dataset_path(date_str) / "expected" / f"{result_name}.json"
    if not expected_path.exists():
        raise FileNotFoundError(f"Expected results not found: {expected_path}")

    return json.loads(expected_path.read_text())


@pytest.fixture
def golden_data_2025_12_21(mysql_session):
    """
    Load golden dataset for December 21, 2025.

    High-activity day with:
    - 33 parks with data
    - Multiple multi-hour outages
    - Good mix of Disney/Universal and regional parks

    NOTE: This fixture requires the golden data SQL files to be present in
    tests/golden_data/datasets/2025-12-21/. If the data can't be loaded
    (e.g., foreign key errors), the test will be skipped.
    """
    dataset_path = get_dataset_path("2025-12-21")

    # Load park and ride reference data
    parks_sql = dataset_path / "parks.sql"
    rides_sql = dataset_path / "rides.sql"

    try:
        if parks_sql.exists():
            load_sql_file(mysql_session, parks_sql)
        if rides_sql.exists():
            load_sql_file(mysql_session, rides_sql)

        # Load snapshot data
        snapshots_sql = dataset_path / "snapshots.sql"
        if snapshots_sql.exists():
            load_sql_file(mysql_session, snapshots_sql)
    except Exception as e:
        mysql_session.rollback()
        pytest.skip(f"Golden data fixture could not be loaded: {e}")

    yield {
        "date": "2025-12-21",
        "dataset_path": dataset_path,
        "load_expected": lambda name: load_expected_results("2025-12-21", name)
    }


@pytest.fixture
def golden_data_2025_12_22(mysql_session):
    """
    Load golden dataset for December 22, 2025.

    Another high-activity day for comparison testing.

    NOTE: This fixture requires the golden data SQL files to be present in
    tests/golden_data/datasets/2025-12-22/. If the data can't be loaded
    (e.g., foreign key errors), the test will be skipped.
    """
    dataset_path = get_dataset_path("2025-12-22")

    parks_sql = dataset_path / "parks.sql"
    rides_sql = dataset_path / "rides.sql"

    try:
        if parks_sql.exists():
            load_sql_file(mysql_session, parks_sql)
        if rides_sql.exists():
            load_sql_file(mysql_session, rides_sql)

        snapshots_sql = dataset_path / "snapshots.sql"
        if snapshots_sql.exists():
            load_sql_file(mysql_session, snapshots_sql)
    except Exception as e:
        mysql_session.rollback()
        pytest.skip(f"Golden data fixture could not be loaded: {e}")

    yield {
        "date": "2025-12-22",
        "dataset_path": dataset_path,
        "load_expected": lambda name: load_expected_results("2025-12-22", name)
    }


# Time constants for frozen time tests
# Dec 21, 2025 at 11:59 PM PST = Dec 22, 2025 at 7:59 AM UTC
GOLDEN_2025_12_21_END_UTC = datetime(2025, 12, 22, 7, 59, 59, tzinfo=timezone.utc)
GOLDEN_2025_12_21_START_UTC = datetime(2025, 12, 21, 8, 0, 0, tzinfo=timezone.utc)  # Midnight PST

GOLDEN_2025_12_22_END_UTC = datetime(2025, 12, 23, 7, 59, 59, tzinfo=timezone.utc)
GOLDEN_2025_12_22_START_UTC = datetime(2025, 12, 22, 8, 0, 0, tzinfo=timezone.utc)
