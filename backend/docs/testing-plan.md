# Testing Strategy: Hybrid Approach for Time-Based Data

## Required API Coverage Matrix

This section defines the **mandatory** test coverage for all public API endpoints. Every combination in this matrix MUST have an integration test.

### Primary API Endpoints

The application has two main data tables (Shame Score/Downtime, Wait Times), two entity types (Parks, Rides), five time periods (LIVE, TODAY, YESTERDAY, LAST_WEEK, LAST_MONTH), and two filter modes (All Parks, Disney & Universal).

#### Coverage Matrix: Rankings APIs

| Endpoint | Entity | Period | Filter | Test File | Status |
|----------|--------|--------|--------|-----------|--------|
| `/parks/downtime` | Parks | LIVE | all-parks | `test_api_endpoints_integration.py` | ✅ `test_parks_downtime_live_all_parks` |
| `/parks/downtime` | Parks | LIVE | disney-universal | `test_api_endpoints_integration.py` | ✅ `test_parks_downtime_live_disney_universal` |
| `/parks/downtime` | Parks | TODAY | all-parks | `test_api_endpoints_integration.py` | ✅ `test_parks_downtime_today_all_parks` |
| `/parks/downtime` | Parks | TODAY | disney-universal | `test_api_endpoints_integration.py` | ✅ `test_parks_downtime_today_disney_universal_filter` |
| `/parks/downtime` | Parks | YESTERDAY | all-parks | `test_api_endpoints_integration.py` | ✅ `test_parks_downtime_yesterday_all_parks` |
| `/parks/downtime` | Parks | YESTERDAY | disney-universal | `test_api_endpoints_integration.py` | ✅ `test_parks_downtime_yesterday_disney_universal` |
| `/parks/downtime` | Parks | LAST_WEEK | all-parks | `test_api_endpoints_integration.py` | ✅ `test_parks_downtime_last_week_all_parks` |
| `/parks/downtime` | Parks | LAST_WEEK | disney-universal | `test_api_endpoints_integration.py` | ✅ `test_parks_downtime_last_week_disney_universal` |
| `/parks/downtime` | Parks | LAST_MONTH | all-parks | `test_api_endpoints_integration.py` | ✅ `test_parks_downtime_last_month_all_parks` |
| `/parks/downtime` | Parks | LAST_MONTH | disney-universal | `test_api_endpoints_integration.py` | ✅ `test_parks_downtime_last_month_disney_universal` |
| `/rides/downtime` | Rides | LIVE | all-parks | `test_api_endpoints_integration.py` | ✅ `test_rides_downtime_live_all_parks` |
| `/rides/downtime` | Rides | LIVE | disney-universal | `test_api_endpoints_integration.py` | ✅ `test_rides_downtime_live_disney_universal` |
| `/rides/downtime` | Rides | TODAY | all-parks | `test_api_endpoints_integration.py` | ✅ `test_rides_downtime_today` |
| `/rides/downtime` | Rides | TODAY | disney-universal | `test_api_endpoints_integration.py` | ✅ `test_rides_downtime_disney_universal_filter` |
| `/rides/downtime` | Rides | YESTERDAY | all-parks | `test_api_endpoints_integration.py` | ✅ `test_rides_downtime_yesterday_all_parks` |
| `/rides/downtime` | Rides | YESTERDAY | disney-universal | `test_api_endpoints_integration.py` | ✅ `test_rides_downtime_yesterday_disney_universal` |
| `/rides/downtime` | Rides | LAST_WEEK | all-parks | `test_api_endpoints_integration.py` | ✅ `test_rides_downtime_last_week_all_parks` |
| `/rides/downtime` | Rides | LAST_WEEK | disney-universal | `test_api_endpoints_integration.py` | ✅ `test_rides_downtime_last_week_disney_universal` |
| `/rides/downtime` | Rides | LAST_MONTH | all-parks | `test_api_endpoints_integration.py` | ✅ `test_rides_downtime_last_month_all_parks` |
| `/rides/downtime` | Rides | LAST_MONTH | disney-universal | `test_api_endpoints_integration.py` | ✅ `test_rides_downtime_last_month_disney_universal` |
| `/parks/waittimes` | Parks | LIVE | all-parks | `test_api_endpoints_integration.py` | ✅ `test_parks_waittimes_live_all_parks` |
| `/parks/waittimes` | Parks | LIVE | disney-universal | `test_api_endpoints_integration.py` | ✅ `test_parks_waittimes_live_disney_universal` |
| `/parks/waittimes` | Parks | TODAY | all-parks | `test_api_endpoints_integration.py` | ✅ `test_parks_waittimes_today_all_parks` |
| `/parks/waittimes` | Parks | TODAY | disney-universal | `test_api_endpoints_integration.py` | ✅ `test_parks_waittimes_today_disney_universal` |
| `/parks/waittimes` | Parks | YESTERDAY | all-parks | `test_api_endpoints_integration.py` | ✅ `test_parks_waittimes_yesterday_all_parks` |
| `/parks/waittimes` | Parks | YESTERDAY | disney-universal | `test_api_endpoints_integration.py` | ✅ `test_parks_waittimes_yesterday_disney_universal` |
| `/parks/waittimes` | Parks | LAST_WEEK | all-parks | `test_api_endpoints_integration.py` | ✅ `test_parks_waittimes_last_week_all_parks` |
| `/parks/waittimes` | Parks | LAST_WEEK | disney-universal | `test_api_endpoints_integration.py` | ✅ `test_parks_waittimes_last_week_disney_universal` |
| `/parks/waittimes` | Parks | LAST_MONTH | all-parks | `test_api_endpoints_integration.py` | ✅ `test_parks_waittimes_last_month_all_parks` |
| `/parks/waittimes` | Parks | LAST_MONTH | disney-universal | `test_api_endpoints_integration.py` | ✅ `test_parks_waittimes_last_month_disney_universal` |
| `/rides/waittimes` | Rides | LIVE | all-parks | `test_api_endpoints_integration.py` | ⚠️ `test_rides_waittimes_live_mode` (API not implemented, returns empty) |
| `/rides/waittimes` | Rides | LIVE | disney-universal | `test_api_endpoints_integration.py` | ⚠️ `test_rides_waittimes_disney_universal_filter` (API not implemented) |
| `/rides/waittimes` | Rides | TODAY | all-parks | `test_api_endpoints_integration.py` | ✅ `test_rides_waittimes_today_all_parks` |
| `/rides/waittimes` | Rides | TODAY | disney-universal | `test_api_endpoints_integration.py` | ✅ `test_rides_waittimes_today_disney_universal` |
| `/rides/waittimes` | Rides | YESTERDAY | all-parks | `test_api_endpoints_integration.py` | ✅ `test_rides_waittimes_yesterday_all_parks` |
| `/rides/waittimes` | Rides | YESTERDAY | disney-universal | `test_api_endpoints_integration.py` | ✅ `test_rides_waittimes_yesterday_disney_universal` |
| `/rides/waittimes` | Rides | LAST_WEEK | all-parks | `test_api_endpoints_integration.py` | ✅ `test_rides_waittimes_last_week_all_parks` |
| `/rides/waittimes` | Rides | LAST_WEEK | disney-universal | `test_api_endpoints_integration.py` | ✅ `test_rides_waittimes_last_week_disney_universal` |
| `/rides/waittimes` | Rides | LAST_MONTH | all-parks | `test_api_endpoints_integration.py` | ✅ `test_rides_waittimes_last_month_all_parks` |
| `/rides/waittimes` | Rides | LAST_MONTH | disney-universal | `test_api_endpoints_integration.py` | ✅ `test_rides_waittimes_last_month_disney_universal` |

**Coverage: 40/40 (100%) - All implemented periods tested**

**Note**: `/rides/waittimes` with `mode=live`, `mode=7day-average`, and `mode=peak-times` are NOT YET IMPLEMENTED (API returns empty data). Tests exist but verify the stub behavior.

#### Coverage Matrix: Detail & Explanation APIs

| Endpoint | Description | Test File | Status |
|----------|-------------|-----------|--------|
| `/parks/<id>/details` | Park details with ride breakdown | `test_api_endpoints_integration.py` | ✅ `test_park_details_success/not_found` |
| `/parks/<id>/details?period=live` | Shame breakdown (live) | `test_api_endpoints_integration.py` | ✅ `test_park_details_live_shame_breakdown` |
| `/parks/<id>/details?period=today` | Shame breakdown (today) | `test_api_endpoints_integration.py` | ✅ `test_park_details_today_shame_breakdown` |
| `/parks/<id>/details?period=yesterday` | Shame breakdown (yesterday) | `test_api_endpoints_integration.py` | ✅ `test_park_details_yesterday_shame_breakdown` |
| `/parks/<id>/details?period=last_week` | Shame breakdown (weekly) | `test_api_endpoints_integration.py` | ✅ `test_park_details_last_week_shame_breakdown` |
| `/parks/<id>/details?period=last_month` | Shame breakdown (monthly) | `test_api_endpoints_integration.py` | ✅ `test_park_details_last_month_shame_breakdown` |
| `/rides/<id>/details` | Ride details with hourly breakdown | `test_ride_details_daily_aggregation_api.py` | ✅ 8 tests (periods: today, yesterday, last_week, last_month) |
| `/live/status-summary` | Live status counts (rides down/open) | `test_api_endpoints_integration.py` | ✅ `test_live_status_summary_*` (5 tests) |

**Coverage: 8/8 (100%) - All detail/explanation endpoints covered**

#### Coverage Matrix: Trends APIs

| Endpoint | Category | Period | Filter | Test File | Status |
|----------|----------|--------|--------|-----------|--------|
| `/trends/parks/improving` | improving | today | both | `test_api_endpoints_integration.py` | ✅ 2 tests |
| `/trends/parks/declining` | declining | today | all-parks | `test_api_endpoints_integration.py` | ✅ 1 test |
| `/trends/rides/improving` | improving | today | all-parks | `test_api_endpoints_integration.py` | ✅ 1 test |
| `/trends/rides/declining` | declining | today/7days | all-parks | `test_api_endpoints_integration.py` | ✅ 2 tests |

**Missing: YESTERDAY and LAST_MONTH periods for all trends categories**

**Coverage: 6/16 (38%) - yesterday, last_month periods not tested**

### COVERAGE SUMMARY (Updated 2025-12-25)

| Category | Coverage | Status |
|----------|----------|--------|
| Rankings APIs | 40/40 (100%) | ✅ Complete |
| Detail & Explanation APIs | 8/8 (100%) | ✅ Complete |
| Trends APIs | 6/16 (38%) | 🟡 Needs work |

### REMAINING GAPS

| Priority | Endpoint | Issue |
|----------|----------|-------|
| 🟡 MEDIUM | `/trends/*` | Missing YESTERDAY, LAST_MONTH periods |
| 🔵 LOW | `/rides/waittimes` mode=live | API not implemented (returns empty) |
| 🔵 LOW | `/rides/waittimes` mode=7day-average | API not implemented (returns empty) |
| 🔵 LOW | `/rides/waittimes` mode=peak-times | API not implemented (returns empty) |

**Total Missing Tests: 10 for trends (non-critical), 0 for core ranking APIs**

### RECENTLY ADDED TESTS (2025-12-25)

- `/rides/waittimes` - Added TODAY, YESTERDAY, LAST_WEEK, LAST_MONTH for both filters ✅
- `/parks/downtime` - Added disney-universal filter for LAST_WEEK, LAST_MONTH ✅
- `/rides/downtime` - Added disney-universal filter for LAST_WEEK ✅
- Full matrix coverage for all core ranking APIs complete ✅

### PREVIOUSLY ADDED (2025-12-24)

- `/parks/waittimes` - All 10 period/filter combinations ✅
- `/parks/downtime` - LIVE and YESTERDAY periods ✅
- `/rides/downtime` - LIVE, YESTERDAY, LAST_MONTH periods ✅
- `/live/status-summary` - 5 API integration tests ✅
- `/parks/<id>/details` - All 5 period variations with shame breakdown ✅

### Test Data Strategy

All integration tests MUST use one of these approaches:

#### Approach 1: Frozen Time with Fixtures (Preferred for CI)
```python
from freezegun import freeze_time
from datetime import datetime, timezone, timedelta

MOCKED_NOW_UTC = datetime(2025, 12, 24, 20, 0, 0, tzinfo=timezone.utc)  # 12 PM PST

@freeze_time(MOCKED_NOW_UTC)
def test_today_parks_downtime_all(self, mysql_session):
    """Test TODAY parks downtime with all-parks filter."""
    # Create test data relative to MOCKED_NOW_UTC
    create_test_park(mysql_session, name="Test Park", is_disney=False)
    create_test_snapshot(mysql_session, recorded_at=MOCKED_NOW_UTC - timedelta(hours=2))

    response = client.get('/api/parks/downtime?period=today&filter=all-parks')
    assert response.status_code == 200
    assert len(response.json['parks']) > 0
```

#### Approach 2: Mirrored Production Data (For Smoke Tests)
```python
@pytest.mark.requires_mirror
def test_today_with_real_data(self, mysql_session):
    """Smoke test with mirrored production data."""
    # Assumes mirror-production-db.sh was run recently
    response = client.get('/api/parks/downtime?period=today&filter=all-parks')

    # Sanity checks only - don't assert specific values
    assert response.status_code == 200
    assert 'parks' in response.json
    # Today should have some data during park hours
```

### Test File Organization

```
tests/integration/api/
├── conftest.py                    # Shared fixtures, Flask test client
├── test_live_rankings.py          # LIVE period: parks/rides × downtime/waittimes × filters
├── test_today_rankings.py         # TODAY period: parks/rides × downtime/waittimes × filters
├── test_yesterday_rankings.py     # YESTERDAY period: parks/rides × downtime/waittimes × filters
├── test_weekly_rankings.py        # LAST_WEEK period: parks/rides × downtime/waittimes × filters
├── test_monthly_rankings.py       # LAST_MONTH period: parks/rides × downtime/waittimes × filters
├── test_park_details.py           # Park details endpoint
├── test_ride_details.py           # Ride details endpoint
├── test_shame_breakdown.py        # Shame score explanation
├── test_trends.py                 # All trends endpoints
└── test_live_status.py            # Live status summary

tests/integration/api/smoke/       # Optional: requires mirrored data
├── test_smoke_all_periods.py      # Quick smoke test all periods with real data
└── test_smoke_details.py          # Smoke test detail pages
```

---

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
    """Test shame score formula without database.

    FORMULA: shame = (weighted_downtime / (effective_weight × operating_hours)) × 10
    This is a RATE (0-10 scale), not cumulative.
    """
    calculator = ShameScoreCalculator(mock_db_connection)

    # Pure logic test - no DB needed
    # weighted_downtime=10, effective_weight=50, operating_hours=10
    score = calculator.calculate(weighted_downtime=10, effective_weight=50, operating_hours=10)
    assert score == 0.2  # (10 / (50 × 10)) × 10 = 0.2
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
