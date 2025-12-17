# Development Process Guide

This guide covers the complete development workflow for Theme Park Hall of Shame.

## Quick Start

```bash
# 1. Clone and setup
git clone <repo-url>
cd ThemeParkHallOfShame/backend

# 2. Create virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Set up test database
./deployment/scripts/setup-test-database.sh

# 5. Run tests
pytest

# 6. Start development server
FLASK_ENV=development FLASK_APP=src.api.app:app flask run
```

---

## Table of Contents

1. [Development Workflow](#development-workflow)
2. [Testing Strategy](#testing-strategy)
3. [Writing Tests](#writing-tests)
4. [Before Committing](#before-committing)
5. [Working with the Replica Database](#working-with-the-replica-database)
6. [Adding New Features](#adding-new-features)
7. [Database Changes](#database-changes)
8. [Troubleshooting](#troubleshooting)

---

## Development Workflow

This project follows **Test-Driven Development (TDD)** principles.

### The TDD Cycle: Red-Green-Refactor

```
┌─────────┐
│   RED   │  Write a failing test first
└────┬────┘
     │
     ▼
┌─────────┐
│  GREEN  │  Write minimal code to pass
└────┬────┘
     │
     ▼
┌─────────┐
│REFACTOR │  Clean up, then repeat
└─────────┘
```

### Example TDD Workflow

```python
# 1. RED: Write the test first (it will fail)
def test_calculate_shame_score():
    calculator = ShameScoreCalculator()
    score = calculator.calculate(downtime_hours=10, tier=1)
    assert score == 100  # Expects 10 hours * tier 1 weight (10) = 100

# Run: pytest tests/unit/test_shame_score.py
# Result: FAIL - ShameScoreCalculator.calculate() doesn't exist

# 2. GREEN: Write minimal code to make it pass
class ShameScoreCalculator:
    def calculate(self, downtime_hours, tier):
        tier_weights = {1: 10, 2: 5, 3: 1}
        return downtime_hours * tier_weights[tier]

# Run: pytest tests/unit/test_shame_score.py
# Result: PASS

# 3. REFACTOR: Clean up the code
class ShameScoreCalculator:
    TIER_WEIGHTS = {1: 10, 2: 5, 3: 1}

    def calculate(self, downtime_hours: float, tier: int) -> float:
        """Calculate shame score based on downtime and ride tier."""
        if tier not in self.TIER_WEIGHTS:
            raise ValueError(f"Invalid tier: {tier}")
        return downtime_hours * self.TIER_WEIGHTS[tier]

# Run: pytest tests/unit/test_shame_score.py
# Result: PASS (add more tests for edge cases)
```

### Local Development Setup

```bash
# Install dependencies
pip install -r backend/requirements.txt

# Set up environment variables
cp backend/.env.example backend/.env
nano backend/.env  # Configure database credentials

# Set up test database (one-time)
./deployment/scripts/setup-test-database.sh

# Run development server
cd backend
FLASK_ENV=development FLASK_APP=src.api.app:app flask run

# Access API
curl http://localhost:5000/api/health
```

### Database Mirroring from Production

For testing against real production data patterns:

```bash
# Mirror production database to local dev
./deployment/scripts/mirror-production-db.sh --days=30

# This creates a local copy of production data for the last 30 days
# Use for testing time-sensitive features with real data
```

See [REPLICATION_SETUP.md](../deployment/database/REPLICATION_SETUP.md) for continuous replication setup.

---

## Testing Strategy

This project uses a **layered testing strategy** with 935+ tests across 64 files.

### Test Categories

| Type | Count | Purpose | Database | Speed |
|------|-------|---------|----------|-------|
| **Unit** | 43 files (~800 tests) | Business logic verification | Mocked | <5 sec |
| **Integration** | 21 files (~135 tests) | Database interactions | Real MySQL | ~30 sec |
| **Contract** | 1 file | API schema validation | None | <1 sec |
| **Golden Data** | 4 files | Regression testing | None | <1 sec |
| **Performance** | 1 file | Query timing baselines | Real MySQL | Variable |

### When to Use Each Test Type

#### Unit Tests (Fast, Isolated)

**Use when:**
- Testing business logic (calculations, transformations)
- Testing pure functions
- Testing error handling
- You need fast feedback during TDD (<5 seconds)

**Database:** Mocked with `mock_db_connection` fixture

**Example:**
```python
def test_shame_score_calculation(mock_db_connection):
    # Fast, isolated, tests pure calculation logic
    calculator = ShameScoreCalculator(mock_db_connection)
    score = calculator.calculate(downtime_hours=10, tier=1)
    assert score == 100  # 10 hours * tier 1 weight (10) = 100
```

**Why mocking is valuable:**
- **Speed:** Unit tests run in <5 seconds (all 800 tests)
- **Isolation:** Tests don't fail due to database issues
- **TDD-friendly:** Fast feedback during red-green-refactor cycle
- **Focus:** Tests pure logic, not infrastructure

#### Integration Tests (Real Database)

**Use when:**
- Testing SQL queries
- Testing database schema assumptions
- Testing aggregations that depend on real data
- Testing API endpoints end-to-end
- Verifying complex joins and subqueries

**Database:** Real MySQL with automatic transaction rollback

**Example:**
```python
def test_park_rankings_query(mysql_connection):
    # Real MySQL, verifies actual query results
    # Set up test data
    insert_test_park(mysql_connection, name="Test Park")
    insert_test_rides(mysql_connection, park_id=1, count=10)

    # Execute actual query
    result = execute_park_rankings_query(mysql_connection, period="today")

    # Verify results
    assert result[0]['park_name'] == "Test Park"
    assert result[0]['shame_score'] > 0
    assert result[0]['total_rides'] == 10
```

**Why integration tests matter:**
- **Catch SQL errors:** Syntax, schema mismatches, missing indexes
- **Verify real data patterns:** NULL handling, edge cases, time zones
- **End-to-end validation:** API → business logic → database → response
- **Transaction safety:** Each test rolls back automatically (no data pollution)

### Running Tests

```bash
cd backend

# Run all tests
pytest

# Run specific categories
pytest tests/unit/              # Fast unit tests only
pytest tests/integration/       # Integration tests (requires test DB)
pytest tests/contract/          # API contract validation

# Run with coverage report
pytest --cov=src --cov-report=term-missing
pytest --cov=src --cov-report=html  # Generate HTML report in htmlcov/

# Run specific test file
pytest tests/unit/test_shame_score.py

# Run specific test function
pytest tests/unit/test_shame_score.py::test_calculate_shame_score

# Run tests matching a pattern
pytest -k "shame_score"

# Run with verbose output
pytest -v

# Run and show print statements
pytest -s
```

### Test Markers

Tests are marked for filtering:

```python
@pytest.mark.unit
def test_business_logic():
    """Fast, isolated unit test"""

@pytest.mark.integration
def test_database_query(mysql_connection):
    """Real database test"""

@pytest.mark.slow
def test_expensive_operation():
    """Test that takes significant time"""

@pytest.mark.time_sensitive
@freeze_time("2025-12-06T04:00:00Z")
def test_today_aggregation():
    """Time-sensitive test with frozen time"""

@pytest.mark.requires_replica
def test_with_production_data(replica_connection):
    """Optional test requiring production replica"""
```

**Run specific markers:**
```bash
pytest -m unit               # Only unit tests
pytest -m integration        # Only integration tests
pytest -m "not slow"         # Skip slow tests
pytest -m time_sensitive     # Only time-sensitive tests
```

---

## Writing Tests

### Test Organization

```
backend/tests/
├── unit/              # Fast, isolated tests (mocks)
├── integration/       # Real database tests
├── contract/          # API schema validation
├── golden_data/       # Regression test data
├── performance/       # Query timing tests
└── conftest.py        # Shared fixtures
```

### Unit Test Patterns

#### Testing Business Logic

```python
# tests/unit/test_shame_score.py
import pytest
from src.business.shame_score import ShameScoreCalculator

def test_tier1_ride_doubles_downtime_weight(mock_db_connection):
    """Tier 1 rides have 2x weight (10 vs 5)"""
    calc = ShameScoreCalculator(mock_db_connection)

    tier1_score = calc.calculate(downtime_hours=10, tier=1)
    tier2_score = calc.calculate(downtime_hours=10, tier=2)

    assert tier1_score == 100  # 10 * 10
    assert tier2_score == 50   # 10 * 5
    assert tier1_score == 2 * tier2_score

def test_zero_downtime_returns_zero_shame():
    """Parks with zero downtime have zero shame"""
    calc = ShameScoreCalculator(None)  # No DB needed
    assert calc.calculate(downtime_hours=0, tier=1) == 0
    assert calc.calculate(downtime_hours=0, tier=2) == 0

def test_invalid_tier_raises_error():
    """Invalid tier raises ValueError"""
    calc = ShameScoreCalculator(None)
    with pytest.raises(ValueError, match="Invalid tier"):
        calc.calculate(downtime_hours=10, tier=99)
```

#### Testing with Mock Data

```python
from unittest.mock import MagicMock, patch

def test_query_uses_correct_period(mock_db_connection):
    """Verify query passes correct period parameter"""
    # Arrange
    mock_db_connection.execute.return_value.fetchall.return_value = []

    # Act
    service = ParkRankingsService(mock_db_connection)
    service.get_rankings(period="today")

    # Assert
    call_args = mock_db_connection.execute.call_args
    assert "today" in str(call_args)
```

### Integration Test Patterns

#### Testing Database Queries

```python
# tests/integration/test_park_rankings.py
import pytest
from datetime import datetime, timezone
from freezegun import freeze_time

MOCKED_NOW = datetime(2025, 12, 6, 4, 0, 0, tzinfo=timezone.utc)

@freeze_time(MOCKED_NOW)
def test_park_rankings_returns_correct_order(mysql_connection):
    """Parks ranked by shame score descending"""
    # Arrange: Insert test data
    insert_park(mysql_connection, id=1, name="Low Shame Park")
    insert_park(mysql_connection, id=2, name="High Shame Park")

    insert_ride(mysql_connection, park_id=1, downtime_hours=5, tier=1)
    insert_ride(mysql_connection, park_id=2, downtime_hours=20, tier=1)

    # Act: Execute query
    rankings = get_park_rankings(mysql_connection, period="today")

    # Assert: High shame park should be first
    assert rankings[0]['park_name'] == "High Shame Park"
    assert rankings[0]['shame_score'] > rankings[1]['shame_score']
    assert rankings[1]['park_name'] == "Low Shame Park"

def test_park_rankings_excludes_closed_parks(mysql_connection):
    """Closed parks don't appear in rankings"""
    # Arrange
    insert_park(mysql_connection, id=1, name="Open Park", is_active=True)
    insert_park(mysql_connection, id=2, name="Closed Park", is_active=False)

    # Act
    rankings = get_park_rankings(mysql_connection, period="today")

    # Assert
    park_names = [r['park_name'] for r in rankings]
    assert "Open Park" in park_names
    assert "Closed Park" not in park_names
```

#### Time-Sensitive Tests

Always use `freezegun` for tests involving dates/times:

```python
from freezegun import freeze_time
from datetime import datetime, timezone

# Define constants at module level
MOCKED_NOW_UTC = datetime(2025, 12, 6, 4, 0, 0, tzinfo=timezone.utc)  # 8 PM PST Dec 5
TODAY_START_UTC = datetime(2025, 12, 5, 8, 0, 0, tzinfo=timezone.utc)  # Midnight PST Dec 5

@freeze_time(MOCKED_NOW_UTC)
def test_today_aggregation(mysql_connection):
    """Today aggregation includes only current day in Pacific time"""
    # Insert snapshots for today and yesterday
    insert_snapshot(mysql_connection, timestamp=TODAY_START_UTC + timedelta(hours=2))
    insert_snapshot(mysql_connection, timestamp=TODAY_START_UTC - timedelta(hours=2))

    # Get today's aggregation
    result = aggregate_today(mysql_connection)

    # Should only include today's snapshot
    assert result['snapshot_count'] == 1
```

### Using Fixtures

#### Mock Database Connection (Unit Tests)

```python
def test_with_mock(mock_db_connection):
    """Use mock_db_connection for unit tests"""
    # mock_db_connection is a MagicMock
    # No real database connection
    service = MyService(mock_db_connection)
    # ... test logic
```

#### Real MySQL Connection (Integration Tests)

```python
def test_with_real_db(mysql_connection):
    """Use mysql_connection for integration tests"""
    # mysql_connection is a real SQLAlchemy Connection
    # Runs in a transaction that rolls back after test
    result = mysql_connection.execute(text("SELECT 1"))
    assert result.scalar() == 1
```

#### Optional Replica Connection

```python
@pytest.mark.requires_replica
def test_with_production_data(replica_connection):
    """Use replica_connection for fresh production data"""
    # Only runs if REPLICA_DB_* env vars are set
    # Read-only connection to production replica
    result = replica_connection.execute(text("SELECT COUNT(*) FROM parks"))
    assert result.scalar() > 0
```

---

## Before Committing

**CRITICAL: Follow this checklist before every commit.**

### 1. Run Full Test Suite

```bash
cd backend

# Run all tests
pytest

# Expected output:
# ===== 935 passed in 35.12s =====
```

**Requirements:**
- ✅ All tests must pass (935+ tests)
- ✅ No skipped tests without documented reason
- ✅ 80% minimum code coverage

### 2. Run Linting

```bash
cd backend
ruff check .

# Expected output:
# All checks passed!
```

If linting fails:
```bash
# Auto-fix many issues
ruff check . --fix

# Review remaining issues manually
```

### 3. Manual Browser Testing

See [CLAUDE.md](../CLAUDE.md#mandatory-local-testing-before-production-deployment) for detailed manual testing requirements.

**Required steps:**
1. Mirror production database: `./deployment/scripts/mirror-production-db.sh --days=7`
2. Start local server: `FLASK_ENV=development FLASK_APP=src.api.app:app flask run`
3. Open frontend: `http://localhost:8080`
4. Test ALL time periods (LIVE, TODAY, YESTERDAY, last_week, last_month)
5. Verify shame scores match between Rankings and Details modal
6. Check charts display correctly

### 4. Git Commit

Only commit after ALL tests and manual verification pass:

```bash
# Stage changes
git add .

# Commit with descriptive message
git commit -m "feat: add new feature with comprehensive tests

- Add ShameScoreCalculator with tier weighting
- Add unit tests for calculation logic
- Add integration tests for database queries
- Update API endpoint to use new calculator

🤖 Generated with [Claude Code](https://claude.com/claude-code)

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

**Commit message guidelines:**
- Use conventional commits: `feat:`, `fix:`, `docs:`, `test:`, `refactor:`
- First line: Brief summary (<72 chars)
- Body: Detailed explanation of changes
- Include co-authoring footer if AI-assisted

---

## Working with the Replica Database

For optional testing against fresh production data.

### Setup Local Replica

Follow [REPLICATION_SETUP.md](../deployment/database/REPLICATION_SETUP.md) to set up continuous replication from production to your local dev machine.

**Benefits:**
- Always fresh production data (<5 second lag)
- Test against real-world data patterns
- Catch edge cases that mock data misses
- Time-accurate testing (actual "today", "yesterday", etc.)

### Configure Replica Testing

```bash
# Add to backend/.env
REPLICA_DB_HOST=localhost
REPLICA_DB_PORT=3306
REPLICA_DB_NAME=themepark_tracker_replica
REPLICA_DB_USER=dev_user
REPLICA_DB_PASSWORD=your_password
```

### Run Tests Against Replica

```bash
# Run tests that require replica
pytest -m requires_replica

# These tests are optional and skipped if replica isn't configured
```

### When to Use Replica vs Isolated Tests

**Use isolated test database (`themepark_test`) when:**
- Writing new tests (faster, more control)
- Testing specific scenarios with crafted data
- Running in CI/CD pipeline
- Need write access (replica is read-only)

**Use replica database when:**
- Validating against real production data patterns
- Testing time-sensitive aggregations with actual dates
- Smoke testing before production deployment
- Investigating production-specific edge cases

---

## Adding New Features

### Step-by-Step TDD Workflow

#### 1. Write the Test First (RED)

```python
# tests/unit/test_new_feature.py
import pytest

def test_new_feature_returns_expected_result(mock_db_connection):
    """New feature should return expected result"""
    # Arrange
    feature = NewFeature(mock_db_connection)

    # Act
    result = feature.execute()

    # Assert
    assert result == "expected_value"
```

Run: `pytest tests/unit/test_new_feature.py`
Expected: **FAIL** (feature doesn't exist yet)

#### 2. Implement Minimal Code (GREEN)

```python
# src/business/new_feature.py
class NewFeature:
    def __init__(self, db_connection):
        self.db = db_connection

    def execute(self):
        return "expected_value"
```

Run: `pytest tests/unit/test_new_feature.py`
Expected: **PASS**

#### 3. Add Integration Test

```python
# tests/integration/test_new_feature_integration.py
def test_new_feature_with_real_database(mysql_connection):
    """Integration test with real MySQL"""
    # Arrange: Insert test data
    insert_test_data(mysql_connection)

    # Act
    feature = NewFeature(mysql_connection)
    result = feature.execute()

    # Assert
    assert result is not None
    assert len(result) > 0
```

Run: `pytest tests/integration/test_new_feature_integration.py`
Expected: **PASS** (or fix bugs until it passes)

#### 4. Add API Endpoint

```python
# src/api/routes/features.py
from flask import Blueprint, jsonify
from src.business.new_feature import NewFeature

features_bp = Blueprint('features', __name__)

@features_bp.route('/api/features/new', methods=['GET'])
def get_new_feature():
    """Get new feature data"""
    from database.connection import get_db_connection

    with get_db_connection() as conn:
        feature = NewFeature(conn)
        result = feature.execute()
        return jsonify(result)
```

#### 5. Test Manually

```bash
# Start dev server
FLASK_ENV=development FLASK_APP=src.api.app:app flask run

# Test endpoint
curl http://localhost:5000/api/features/new
```

#### 6. Update Documentation

- Update `docs/` if API changes
- Update `README.md` if user-facing
- Update `CLAUDE.md` if developer workflow changes

---

## Database Changes

### Schema Migrations

**NEVER modify the database schema directly.** Always create migration files.

#### 1. Create Migration File

```bash
# Create new migration file
touch backend/src/database/migrations/YYYY-MM-DD_description.sql

# Example: 2025-12-15_add_ride_tier_column.sql
```

#### 2. Write Migration SQL

```sql
-- Migration: Add tier column to rides table
-- Date: 2025-12-15
-- Author: Your Name

-- Add tier column (1=flagship, 2=major, 3=minor)
ALTER TABLE rides
ADD COLUMN tier INT NOT NULL DEFAULT 3
COMMENT 'Ride tier: 1=flagship, 2=major, 3=minor';

-- Add index for tier-based queries
CREATE INDEX idx_rides_tier ON rides(tier);

-- Update existing rides based on known data
UPDATE rides SET tier = 1 WHERE name LIKE '%Coaster%';
UPDATE rides SET tier = 2 WHERE name LIKE '%Tower%';
```

#### 3. Test Migration Locally

```bash
# Apply migration to test database
mysql -u root -p themepark_test < backend/src/database/migrations/2025-12-15_add_ride_tier_column.sql

# Verify schema
mysql -u root -p themepark_test -e "DESCRIBE rides;"
```

#### 4. Run Tests

```bash
# Integration tests will catch schema issues
pytest tests/integration/
```

#### 5. Deploy Migration

Migrations are applied automatically during deployment via `deployment/deploy.sh`.

---

## Troubleshooting

### Tests Failing Locally

**"Database connection failed"**
```bash
# Verify test database exists
mysql -u root -p -e "SHOW DATABASES;" | grep themepark_test

# If missing, set it up
./deployment/scripts/setup-test-database.sh

# Check environment variables
echo $TEST_DB_HOST
echo $TEST_DB_NAME
```

**"Import error" or "Module not found"**
```bash
# Ensure venv is activated
source venv/bin/activate

# Reinstall dependencies
pip install -r requirements.txt

# Verify PYTHONPATH
export PYTHONPATH=/path/to/ThemeParkHallOfShame/backend/src
```

**"Tests pass locally but fail in CI"**
- Check for hardcoded paths
- Ensure tests don't depend on local data
- Verify `.env` variables aren't required (use defaults)
- Check for time-zone issues (use UTC)

### Integration Tests Too Slow

```bash
# Run only unit tests during development
pytest tests/unit/  # <5 seconds

# Run integration tests before commit
pytest tests/integration/  # ~30 seconds

# Run specific integration test
pytest tests/integration/test_specific.py::test_function
```

### Coverage Below 80%

```bash
# Generate HTML coverage report
pytest --cov=src --cov-report=html

# Open in browser
open htmlcov/index.html

# Find uncovered lines
pytest --cov=src --cov-report=term-missing
```

---

## Additional Resources

- [Deployment Guide](deployment.md) - Production deployment procedures
- [Architecture](architecture.md) - System architecture and technology decisions
- [CLAUDE.md](../CLAUDE.md) - Development guidelines and mandatory practices
- [REPLICATION_SETUP.md](../deployment/database/REPLICATION_SETUP.md) - Database replication setup
- [TEST_CONFIGURATION.md](../deployment/database/TEST_CONFIGURATION.md) - Replica testing configuration

---

## Summary

**Key Principles:**
1. ✅ Write tests first (TDD)
2. ✅ Unit tests for logic, integration tests for database
3. ✅ All tests must pass before committing
4. ✅ Manual browser testing required for UI changes
5. ✅ Migrations for all schema changes
6. ✅ Never commit without testing

**Test Suite Overview:**
- **935+ tests** across 64 files
- **Unit tests:** Fast (<5 sec), mocked, business logic
- **Integration tests:** Real MySQL (~30 sec), database verification
- **80% coverage** requirement
- **Transaction rollback** for test isolation

**Questions?** See [CLAUDE.md](../CLAUDE.md) for detailed guidelines.
