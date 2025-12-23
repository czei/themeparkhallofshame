# Testing Strategy: Hybrid Approach for Time-Based Data

## Overview

Theme Park Hall of Shame has time-sensitive data that spans multiple periods (LIVE, TODAY, YESTERDAY, last_week, last_month). This creates testing challenges because:

1. Real data changes constantly - tests can't rely on production data state
2. Time boundaries matter - bugs often hide at timezone/day boundaries
3. Synthetic data may miss real-world edge cases
4. Deterministic tests are required for CI/CD

This document outlines our **hybrid testing strategy** that provides both deterministic CI tests and real-world validation.

---

## The Four-Layer Testing Pyramid

```
                    ┌─────────────────────┐
                    │  Replica Smoke      │  ← Optional, non-blocking
                    │  (Real Data)        │     Pre-deploy sanity check
                    └─────────────────────┘
                   ┌───────────────────────┐
                   │  Golden Data Tests    │  ← Deterministic + real patterns
                   │  (Captured Snapshots) │     Regression testing
                   └───────────────────────┘
              ┌─────────────────────────────────┐
              │  Integration Tests               │  ← Deterministic, DB interaction
              │  (Frozen Time + Fixtures)        │     Query validation
              └─────────────────────────────────┘
         ┌───────────────────────────────────────────┐
         │  Unit Tests                                │  ← Fast, isolated
         │  (Mocked Dependencies)                     │     Business logic
         └───────────────────────────────────────────┘
```

| Layer | Data Source | Speed | CI Blocking | Purpose |
|-------|-------------|-------|-------------|---------|
| Unit | Mocked/synthetic | <5 sec | Yes | Business logic verification |
| Integration | Frozen time + fixtures | ~30 sec | Yes | Database query validation |
| Golden Data | Captured production snapshots | ~10 sec | Yes | Regression testing |
| Replica Smoke | Live production replica | ~60 sec | No | Pre-deploy sanity check |

---

## Layer 1: Unit Tests (Mocked Dependencies)

**Purpose**: Fast verification of business logic without external dependencies.

**Characteristics**:
- Use `mock_db_connection` fixture (MagicMock)
- No actual database calls
- Test pure logic: calculations, transformations, error handling
- Run in <5 seconds for all ~800 unit tests

**Example**:
```python
def test_shame_score_calculation(mock_db_connection):
    """Test shame score formula without database."""
    calculator = ShameScoreCalculator(mock_db_connection)

    # Pure logic test - no DB needed
    score = calculator.calculate(downtime_hours=10, tier=1)
    assert score == 100  # 10 hours * tier 1 weight (10)
```

**When to write unit tests**:
- Testing business logic (calculations, transformations)
- Testing pure functions without side effects
- Testing error handling and edge cases
- You need fast feedback during TDD (<5 second cycle)

---

## Layer 2: Integration Tests (Frozen Time + Fixtures)

**Purpose**: Verify database queries work correctly with deterministic test data.

**Characteristics**:
- Use `mysql_session` fixture (real database with transaction rollback)
- Use `freezegun.freeze_time()` to control "now"
- Create test data relative to the frozen time
- Deterministic - same results regardless of when tests run

**The Frozen Time Pattern**:
```python
from freezegun import freeze_time
from datetime import datetime, timezone, timedelta

# Define constants at module level
MOCKED_NOW_UTC = datetime(2025, 12, 6, 4, 0, 0, tzinfo=timezone.utc)  # 8 PM PST Dec 5th
TODAY_START_UTC = datetime(2025, 12, 5, 8, 0, 0, tzinfo=timezone.utc)  # Midnight PST Dec 5th
YESTERDAY_START_UTC = TODAY_START_UTC - timedelta(days=1)

class TestTodayRankings:

    @freeze_time(MOCKED_NOW_UTC)
    def test_today_returns_only_today_data(self, mysql_session):
        """Test TODAY period only includes today's data."""
        conn = mysql_session.connection()

        # Insert data RELATIVE to mocked time
        # Today data (should appear)
        insert_park_snapshot(conn,
            recorded_at=MOCKED_NOW_UTC - timedelta(hours=2),
            shame_score=50.0
        )
        # Yesterday data (should NOT appear)
        insert_park_snapshot(conn,
            recorded_at=YESTERDAY_START_UTC + timedelta(hours=12),
            shame_score=100.0
        )

        result = get_park_rankings(mysql_session, period='today')

        # Verify only today's data returned
        assert len(result) == 1
        assert result[0]['shame_score'] == 50.0
```

**Key Rules**:
1. Always use `@freeze_time()` for time-sensitive tests
2. Define time constants at module level for clarity
3. Create test data with timestamps relative to `MOCKED_NOW`
4. Never use `datetime.now()` directly - always use the frozen time

**When to write integration tests**:
- Testing SQL queries against real MySQL
- Testing database schema assumptions
- Testing aggregations that depend on actual data
- Testing API endpoints end-to-end
- Verifying complex joins, subqueries, or window functions

---

## Layer 3: Golden Data Tests (Captured Production Snapshots)

**Purpose**: Regression testing with real data patterns and hand-verified expected results.

**Characteristics**:
- Capture production data for specific "interesting" dates
- Hand-verify expected results once
- Tests compare against golden expected values
- Deterministic - same golden data always produces same results

**Directory Structure**:
```
tests/golden_data/
├── datasets/
│   ├── 2025-12-05/                    # High-activity day
│   │   ├── ride_status_snapshots.sql  # Raw snapshot data
│   │   ├── park_activity_snapshots.sql
│   │   └── expected/
│   │       ├── parks_downtime_today.json
│   │       ├── parks_downtime_yesterday.json
│   │       └── rides_downtime_today.json
│   ├── 2025-12-01/                    # Multi-hour outage day
│   │   └── ...
│   └── 2025-11-28/                    # Thanksgiving (edge case)
│       └── ...
├── conftest.py                        # Golden data fixtures
└── test_golden_rankings.py            # Golden data test cases
```

**How to capture golden data**:
```bash
# 1. Mirror production data for the target date
./deployment/scripts/mirror-production-db.sh --days=7

# 2. Export snapshots for the target date
mysqldump themepark_tracker_dev \
  --tables ride_status_snapshots park_activity_snapshots \
  --where="DATE(recorded_at) = '2025-12-05'" \
  > tests/golden_data/datasets/2025-12-05/snapshots.sql

# 3. Run queries and capture expected results
python scripts/generate_golden_expected.py --date=2025-12-05

# 4. Hand-verify the expected results are correct!
```

**Golden data test example**:
```python
import json
from freezegun import freeze_time
from datetime import datetime, timezone

# Freeze time to end of the golden data date
GOLDEN_DATE_END = datetime(2025, 12, 6, 7, 59, 59, tzinfo=timezone.utc)  # 11:59 PM PST Dec 5

class TestGoldenDataRankings:

    @pytest.fixture
    def load_golden_2025_12_05(self, mysql_session):
        """Load golden dataset for Dec 5, 2025."""
        conn = mysql_session.connection()

        # Load the captured production data
        with open('tests/golden_data/datasets/2025-12-05/snapshots.sql') as f:
            for statement in f.read().split(';'):
                if statement.strip():
                    conn.execute(text(statement))

        yield

    @freeze_time(GOLDEN_DATE_END)
    def test_today_rankings_match_golden(self, mysql_session, load_golden_2025_12_05):
        """Verify TODAY rankings match hand-verified expected results."""

        # Load expected results
        with open('tests/golden_data/datasets/2025-12-05/expected/parks_downtime_today.json') as f:
            expected = json.load(f)

        # Run the actual query
        result = get_park_rankings(mysql_session, period='today')

        # Compare against golden expected
        assert len(result) == len(expected['parks'])
        for actual, exp in zip(result, expected['parks']):
            assert actual['park_name'] == exp['park_name']
            assert abs(actual['shame_score'] - exp['shame_score']) < 0.1
            assert abs(actual['total_downtime_hours'] - exp['total_downtime_hours']) < 0.01
```

**What makes a good golden dataset**:
- **High-activity day**: Many parks open, lots of downtime events
- **Multi-hour outage**: Tests that outages persist across hours
- **Timezone boundary**: Data around midnight PST
- **Holiday/weekend**: Different operating patterns
- **Edge cases**: Park closures, seasonal rides, data gaps

**When to add new golden data**:
- After fixing a bug that wasn't caught by existing tests
- When adding new query types or periods
- When real-world data reveals patterns not in synthetic fixtures

---

## Layer 4: Replica Smoke Tests (Optional, Non-Blocking)

**Purpose**: Pre-deployment sanity check against real production data.

**Characteristics**:
- Run against read-only production replica
- Non-blocking - failures are warnings, not CI failures
- Verify APIs return reasonable data
- Catch time-boundary bugs that fixtures might miss

**Setup**:
```bash
# Environment variables for replica connection
export REPLICA_DB_HOST=replica.example.com
export REPLICA_DB_PORT=3306
export REPLICA_DB_NAME=themepark_tracker
export REPLICA_DB_USER=readonly
export REPLICA_DB_PASSWORD=xxx
```

**Replica test example**:
```python
@pytest.mark.requires_replica
@pytest.mark.nonblocking
class TestReplicaSmokeTests:

    def test_today_api_returns_data(self, replica_connection):
        """Sanity check: TODAY API returns non-empty results."""
        result = get_park_rankings(replica_connection, period='today')

        # Just verify we get some data back
        # Don't assert specific values - they change daily
        assert len(result) > 0, "TODAY should return at least one park"
        assert all('park_name' in p for p in result)
        assert all('shame_score' in p for p in result)

    def test_yesterday_has_more_data_than_today(self, replica_connection):
        """Sanity check: YESTERDAY (complete day) has >= TODAY data."""
        today = get_park_rankings(replica_connection, period='today')
        yesterday = get_park_rankings(replica_connection, period='yesterday')

        # Yesterday is a complete day, should have at least as much
        assert len(yesterday) >= len(today)
```

**When to use replica tests**:
- Pre-deployment validation
- Investigating production issues
- Validating timezone edge cases with real schedules
- Smoke testing after infrastructure changes

**Important**: Replica tests are **informational only**. A failure means "investigate before deploying" not "block the deployment".

---

## CI/CD Integration

### GitHub Actions Workflow

```yaml
name: Test Suite

on: [push, pull_request]

jobs:
  unit-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Run Unit Tests
        run: pytest tests/unit/ -v
        # Must pass - blocks merge

  integration-tests:
    runs-on: ubuntu-latest
    services:
      mysql:
        image: mysql:8.0
        env:
          MYSQL_DATABASE: themepark_test
          MYSQL_USER: test
          MYSQL_PASSWORD: test
          MYSQL_ROOT_PASSWORD: root
    steps:
      - uses: actions/checkout@v4
      - name: Run Integration Tests
        run: pytest tests/integration/ -v
        # Must pass - blocks merge

  golden-data-tests:
    runs-on: ubuntu-latest
    services:
      mysql:
        image: mysql:8.0
    steps:
      - uses: actions/checkout@v4
      - name: Load Golden Data
        run: mysql -h localhost -u root -proot themepark_test < tests/golden_data/datasets/2025-12-05/snapshots.sql
      - name: Run Golden Data Tests
        run: pytest tests/golden_data/ -v
        # Must pass - blocks merge

  replica-smoke-tests:
    runs-on: ubuntu-latest
    if: github.ref == 'refs/heads/main'  # Only on main branch
    continue-on-error: true  # Non-blocking
    steps:
      - uses: actions/checkout@v4
      - name: Run Replica Smoke Tests
        run: pytest tests/replica/ -v --tb=short
        env:
          REPLICA_DB_HOST: ${{ secrets.REPLICA_DB_HOST }}
        # Informational only - does not block merge
```

### Local Development Commands

```bash
# Run all blocking tests (what CI runs)
pytest tests/unit/ tests/integration/ tests/golden_data/ -v

# Run fast unit tests only (during TDD)
pytest tests/unit/ -v --tb=short

# Run integration tests with coverage
pytest tests/integration/ --cov=src --cov-report=term-missing

# Run golden data tests
pytest tests/golden_data/ -v

# Run replica smoke tests (optional, requires replica access)
pytest tests/replica/ -v --tb=short
```

---

## Summary: When to Use Each Layer

| Situation | Test Layer |
|-----------|------------|
| Testing pure calculation logic | Unit |
| Testing SQL query correctness | Integration |
| Regression testing after bug fix | Golden Data |
| Pre-deployment sanity check | Replica Smoke |
| TDD red-green-refactor cycle | Unit |
| Verifying timezone handling | Integration + Golden Data |
| Catching real-world edge cases | Golden Data |
| Investigating production issues | Replica Smoke |

---

## Migration Plan

### Phase 1: Document Current State (Complete)
- [x] Document hybrid testing strategy in this file

### Phase 2: Capture Initial Golden Datasets
- [ ] Identify 3-5 "interesting" dates from production
- [ ] Create `tests/golden_data/` directory structure
- [ ] Capture snapshot data for each date
- [ ] Hand-verify expected results
- [ ] Write golden data test cases

### Phase 3: Add Replica Test Infrastructure (Optional)
- [ ] Set up read-only replica access
- [ ] Create `tests/replica/` directory
- [ ] Write smoke test cases
- [ ] Add non-blocking CI job

### Phase 4: Enhance Existing Tests
- [ ] Audit existing integration tests for frozen time usage
- [ ] Add missing `@freeze_time` decorators
- [ ] Ensure all time constants are module-level
- [ ] Add golden data tests for edge cases found in production
