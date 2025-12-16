# Test Configuration for Replica Database

## Overview

This guide configures integration tests to run against the production replica instead of an isolated test database.

## Why Use Replica for Tests?

**Current Problem:**
- Tests use isolated `themepark_test` database with minimal mock data
- Tests pass with mock data but fail with real production patterns
- Manual database mirroring becomes stale quickly
- Time-sensitive tests are flaky or require freezegun

**Solution with Replica:**
- Tests run against continuously-synced production data
- Catches edge cases from real-world data patterns
- Always fresh (<5 second lag)
- Time-sensitive tests see actual "today", "yesterday", "last_week" data

## Configuration Steps

### Step 1: Update Environment Variables

Create/update `.env` in backend directory:

```bash
cd backend
cp .env.example .env.replica
nano .env.replica
```

Add replica configuration:

```bash
# Development Database (Local)
DB_HOST=localhost
DB_PORT=3306
DB_NAME=themepark_tracker_dev
DB_USER=root
DB_PASSWORD=your_dev_password

# Integration Test Database (Replica - Read-Only)
TEST_DB_HOST=localhost
TEST_DB_PORT=3306
TEST_DB_NAME=themepark_tracker  # The replicated database
TEST_DB_USER=root
TEST_DB_PASSWORD=your_dev_password

# Flag to enable replica testing
USE_REPLICA_FOR_TESTS=true
REPLICA_MAX_LAG_SECONDS=300  # Skip test if replica is > 5 min behind
```

### Step 2: Update Test Configuration

Edit `backend/tests/integration/conftest.py`:

```python
import os
import pytest
from datetime import datetime, timezone
from sqlalchemy import text, create_engine
from database.connection import get_db_connection

# Determine which database to use for integration tests
USE_REPLICA = os.getenv('USE_REPLICA_FOR_TESTS', 'false').lower() == 'true'
MAX_REPLICA_LAG_SECONDS = int(os.getenv('REPLICA_MAX_LAG_SECONDS', 300))

if USE_REPLICA:
    # Use production replica (read-only)
    TEST_DB_NAME = os.getenv('TEST_DB_NAME', 'themepark_tracker')
    print(f"\n✓ Integration tests using REPLICA database: {TEST_DB_NAME}")
    print(f"  Max allowed lag: {MAX_REPLICA_LAG_SECONDS} seconds")
else:
    # Use isolated test database (read-write)
    TEST_DB_NAME = 'themepark_test'
    print(f"\n✓ Integration tests using ISOLATED database: {TEST_DB_NAME}")


@pytest.fixture(scope="session")
def verify_replica_freshness():
    """
    Verify replica is fresh before running tests.

    Checks replication lag and skips tests if replica is too stale.
    Only runs when USE_REPLICA_FOR_TESTS=true.
    """
    if not USE_REPLICA:
        yield
        return

    try:
        with get_db_connection() as conn:
            # Check if this is a replica
            result = conn.execute(text("SHOW SLAVE STATUS"))
            row = result.fetchone()

            if not row:
                # Not a replica - might be running tests on production accidentally!
                pytest.fail(
                    "USE_REPLICA_FOR_TESTS=true but database is not a replica! "
                    "Do NOT run integration tests against production master!"
                )

            # Check replication lag
            seconds_behind = row.Seconds_Behind_Master
            slave_io_running = row.Slave_IO_Running
            slave_sql_running = row.Slave_SQL_Running

            if slave_io_running != 'Yes' or slave_sql_running != 'Yes':
                pytest.skip(
                    f"Replication is stopped! "
                    f"IO: {slave_io_running}, SQL: {slave_sql_running}"
                )

            if seconds_behind is None or seconds_behind > MAX_REPLICA_LAG_SECONDS:
                pytest.skip(
                    f"Replica lag too high: {seconds_behind} seconds "
                    f"(max: {MAX_REPLICA_LAG_SECONDS})"
                )

            print(f"✓ Replica is fresh (lag: {seconds_behind} seconds)")

    except Exception as e:
        pytest.fail(f"Failed to verify replica status: {e}")

    yield


@pytest.fixture(scope="function")
def mysql_connection(verify_replica_freshness):
    """
    Provides a database connection for integration tests.

    If using replica:
    - Connection is read-only (can't write)
    - No transaction rollback (replica is for reading only)

    If using isolated test DB:
    - Connection is read-write
    - Transaction is rolled back after each test
    """
    with get_db_connection() as conn:
        if not USE_REPLICA:
            # Isolated test DB: use transaction rollback
            trans = conn.begin()
            try:
                yield conn
            finally:
                trans.rollback()
        else:
            # Replica: no transaction needed (read-only)
            yield conn


@pytest.fixture(scope="session", autouse=True)
def database_safety_check():
    """
    Safety check: Prevent accidentally running tests against production master.

    This fixture runs automatically before any test.
    """
    if not USE_REPLICA:
        return

    # Get database name from environment
    db_host = os.getenv('TEST_DB_HOST', 'localhost')
    db_name = os.getenv('TEST_DB_NAME', 'themepark_tracker')

    # Fail if trying to test against production server
    if 'webperformance.com' in db_host or 'production' in db_host.lower():
        pytest.fail(
            "DANGER: Trying to run tests against production master! "
            "Tests should only run against LOCAL replica, not production server."
        )

    print(f"✓ Safety check passed: Testing against {db_host}/{db_name}")
```

### Step 3: Run Tests Against Replica

```bash
cd backend

# Set environment variable for this session
export USE_REPLICA_FOR_TESTS=true

# Run integration tests
pytest tests/integration/ -v

# Or set in .env and run
source .env.replica
pytest tests/integration/ -v
```

### Step 4: Verify Tests Pass with Real Data

Some tests may fail when run against real production data. This is **good** - it means they were passing with mock data but failing with real-world edge cases!

Common failures to fix:

1. **Hardcoded IDs**: Tests assume specific park/ride IDs exist
2. **Time-sensitive logic**: Tests assume "today" has data
3. **NULL handling**: Production data may have NULLs that mock data didn't
4. **Edge cases**: Production has unusual data patterns

**Example fixes:**

```python
# BAD: Assumes park ID 1 exists
def test_park_stats():
    stats = get_park_stats(park_id=1)
    assert stats is not None

# GOOD: Query for any active park
def test_park_stats(mysql_connection):
    result = mysql_connection.execute(
        text("SELECT id FROM parks WHERE is_active = TRUE LIMIT 1")
    )
    park_id = result.scalar()

    if not park_id:
        pytest.skip("No active parks in database")

    stats = get_park_stats(park_id=park_id)
    assert stats is not None
```

## Testing Strategies

### Strategy 1: Replica for Integration Tests (Recommended)

```bash
# Integration tests use replica
export USE_REPLICA_FOR_TESTS=true
pytest tests/integration/ -v

# Unit tests use mocks (no database)
pytest tests/unit/ -v
```

### Strategy 2: Hybrid Approach

```bash
# Most integration tests use replica
export USE_REPLICA_FOR_TESTS=true

# But some tests that need to write use isolated DB
pytest tests/integration/test_write_operations.py --db=isolated -v
```

### Strategy 3: Replica + Freezegun for Time Tests

```python
from freezegun import freeze_time

@freeze_time("2025-12-06 20:00:00")  # 8 PM Pacific
def test_today_rankings(mysql_connection, verify_replica_freshness):
    """
    Test today rankings with fixed time.

    Replica has real production data, but we freeze time for determinism.
    """
    rankings = get_today_rankings()
    assert len(rankings) > 0
```

## Benefits

### Before (Isolated Test DB)
- ❌ Stale data (manually synced)
- ❌ Mock data misses edge cases
- ❌ Tests pass but production fails
- ❌ Time-sensitive tests are flaky

### After (Replica)
- ✅ Always fresh (<5 sec lag)
- ✅ Real production data patterns
- ✅ Tests catch real bugs
- ✅ Time-sensitive tests work naturally

## Trade-offs

**Pros:**
- Tests catch bugs that mock data misses
- No manual database mirroring
- Always fresh production data
- Time-accurate testing

**Cons:**
- Tests can't write to database (read-only replica)
- Tests may be slower (more data to query)
- Requires replication setup
- Tests depend on production data quality

## CI/CD Integration

For GitHub Actions, you'll need to set up replica credentials as secrets:

```yaml
# .github/workflows/test.yml
env:
  USE_REPLICA_FOR_TESTS: true
  TEST_DB_HOST: ${{ secrets.REPLICA_DB_HOST }}
  TEST_DB_NAME: themepark_tracker
  TEST_DB_USER: ${{ secrets.REPLICA_DB_USER }}
  TEST_DB_PASSWORD: ${{ secrets.REPLICA_DB_PASSWORD }}
```

## Troubleshooting

### Tests Skipping with "Replica lag too high"

```bash
# Check replica status
mysql -u root -p -e "SHOW SLAVE STATUS\G" | grep Seconds_Behind_Master

# If lag is high, wait or increase threshold:
export REPLICA_MAX_LAG_SECONDS=600  # 10 minutes
```

### "Database is not a replica" Error

You're trying to test against a non-replica database:

```bash
# Verify you're testing against the replica
mysql -u root -p -e "SHOW SLAVE STATUS\G"

# If empty, you're not connected to replica
# Check TEST_DB_HOST and TEST_DB_NAME
```

### Tests Fail with "Table doesn't exist"

Replication may not have started yet or schema is out of sync:

```bash
# Check replication status
mysql -u root -p -e "SHOW SLAVE STATUS\G"

# Compare schemas
mysqldump -u root -p --no-data themepark_tracker > /tmp/schema.sql
# Review for missing tables
```

## Safety Checklist

Before enabling replica testing:

- [ ] Replication is configured and working
- [ ] Replica has read_only = ON
- [ ] Replica lag is < 5 seconds
- [ ] TEST_DB_HOST points to localhost (not production!)
- [ ] Safety fixture prevents testing against production master
- [ ] Integration tests don't try to write data

## Next Steps

1. Set up replication (see `REPLICATION_SETUP.md`)
2. Configure test environment variables
3. Run integration tests against replica
4. Fix any tests that fail with real data
5. Update CI/CD to use replica
6. Document any test-specific quirks

## Reference

- Main replication guide: `REPLICATION_SETUP.md`
- Test fixtures: `backend/tests/integration/conftest.py`
- Environment config: `backend/.env.example`
