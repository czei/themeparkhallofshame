# Research: ORM Refactoring Technical Decisions

**Date**: 2025-12-21
**Feature**: 003-orm-refactoring
**Purpose**: Resolve all technical uncertainties before implementation

---

## 1. SQLAlchemy 2.0 ORM Strategy

### Decision: Declarative Models with Type Annotations

**Chosen Approach**: Use SQLAlchemy 2.0 declarative models with Python type annotations (`Mapped[T]` syntax).

**Rationale**:
- **Type Safety**: Python 3.11+ type checkers (mypy, pyright) can validate model attribute access at development time
- **IDE Support**: Better autocomplete and refactoring tools with typed attributes
- **SQLAlchemy 2.0 Best Practice**: Declarative style is the recommended approach in SQLAlchemy 2.0 documentation
- **Migration Path**: Easier to migrate from raw SQL since table structures map 1:1 to class definitions

**Example**:
```python
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from datetime import datetime

class Base(DeclarativeBase):
    pass

class Ride(Base):
    __tablename__ = "rides"

    id: Mapped[int] = mapped_column(primary_key=True)
    park_id: Mapped[int] = mapped_column(ForeignKey("parks.id"))
    name: Mapped[str] = mapped_column(String(255))
    created_at: Mapped[datetime] = mapped_column(default=func.now())

    # Relationship with type hint
    park: Mapped["Park"] = relationship(back_populates="rides")
```

**Alternatives Considered**:
- **Imperative mapping** (separate mapper() calls): More flexible but loses type safety and IDE support
- **SQLAlchemy Core only** (no ORM): Rejected because Core still requires SQL-like query building, doesn't provide model methods for business logic centralization

---

### Decision: Context-Based Session Management with Flask-SQLAlchemy Pattern

**Chosen Approach**: Use `scoped_session` with Flask application context for automatic session cleanup.

**Rationale**:
- **Flask Integration**: `scoped_session` provides thread-local sessions tied to Flask request context
- **Automatic Cleanup**: Session closes automatically at end of request (prevents connection leaks)
- **Test Compatibility**: Easy to override in tests with transaction rollback for isolation
- **Repository Pattern**: Repositories can inject session without explicit passing

**Implementation**:
```python
# src/database/connection.py
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker

engine = create_engine(DATABASE_URL, pool_pre_ping=True, pool_recycle=3600)
SessionLocal = sessionmaker(bind=engine)
db_session = scoped_session(SessionLocal)

# In Flask app setup
@app.teardown_appcontext
def shutdown_session(exception=None):
    db_session.remove()
```

**Alternatives Considered**:
- **Manual session management** (`with Session() as session:`): Requires explicit passing to all functions, verbose
- **Flask-SQLAlchemy extension**: Adds unnecessary abstraction layer, not needed since we're migrating existing code

---

### Decision: Lazy Loading with Explicit Joins for Performance-Critical Queries

**Chosen Approach**: Use `lazy='select'` (default) for relationships, add explicit `joinedload()` for hot paths.

**Rationale**:
- **N+1 Query Prevention**: Explicit `joinedload()` makes performance intent clear in code review
- **Flexibility**: Lazy loading allows ORM to fetch related data only when needed (default case)
- **Performance Optimization**: Hot paths (e.g., park rankings with rides) use `joinedload()` for single query
- **Query Visibility**: Explicit joins in repository code document which queries need optimization

**Example**:
```python
# Default: lazy loading (simple queries)
rides = session.query(Ride).filter(Ride.park_id == park_id).all()
# Each ride.park access triggers separate query (OK for admin views)

# Optimized: eager loading (performance-critical API endpoints)
rides = session.query(Ride).options(joinedload(Ride.park)).filter(Ride.park_id == park_id).all()
# Single query with JOIN (required for rankings API)
```

**Alternatives Considered**:
- **Eager loading by default** (`lazy='joined'`): Can cause unnecessary JOINs for simple queries, harder to optimize
- **Selectinload** (`lazy='selectin'`): Adds complexity with separate SELECT IN queries, not needed for small datasets

---

## 2. Migration Framework Selection

### Decision: Alembic with Autogenerate for Schema Detection

**Chosen Approach**: Use Alembic 1.13+ with `alembic revision --autogenerate` for migration generation.

**Rationale**:
- **Industry Standard**: Alembic is the de facto migration tool for SQLAlchemy projects
- **Autogenerate**: Detects schema differences between ORM models and database automatically
- **Version Control**: Migration scripts are version-controlled, documenting schema evolution
- **Rollback Support**: Built-in `downgrade()` function for safe rollbacks
- **Production Safety**: Can test migrations on local copy before production deployment

**Alembic Configuration for Existing Database**:
```ini
# alembic.ini
[alembic]
script_location = src/database/migrations
sqlalchemy.url = driver://user:pass@localhost/dbname
file_template = %%(year)d%%(month).2d%%(day).2d_%%(hour).2d%%(minute).2d_%%(slug)s
```

**Migration Workflow**:
```bash
# 1. Create migration after ORM model changes
alembic revision --autogenerate -m "add composite indexes for time-series queries"

# 2. Review generated migration (MANUAL REVIEW REQUIRED)
# Edit src/database/migrations/versions/YYYYMMDD_HHMM_add_composite_indexes.py

# 3. Test on local copy of production database
alembic upgrade head

# 4. Verify schema with SHOW CREATE TABLE

# 5. Test rollback
alembic downgrade -1
alembic upgrade head

# 6. Deploy to production (after all tests pass)
```

**Alternatives Considered**:
- **Manual SQL migration scripts**: Error-prone, no autogenerate, harder to maintain
- **Flask-Migrate** (Alembic wrapper): Unnecessary abstraction, doesn't add value for our use case
- **No migrations** (manual ALTER TABLE): Rejected - violates Constitution Principle IX (production integrity)

---

### Decision: Zero-Downtime Migration for hourly_stats Drop

**Chosen Approach**: Multi-phase migration to drop hourly_stats without API downtime.

**Migration Phases**:

**Phase 1**: Add composite indexes (no downtime, background operation)
```sql
-- Migration 001: Add indexes
CREATE INDEX idx_ride_snapshots_ride_time ON ride_status_snapshots(ride_id, snapshot_time);
CREATE INDEX idx_park_snapshots_park_time ON park_activity_snapshots(park_id, snapshot_time);
-- Background index build: ~5-10 minutes on production database
```

**Phase 2**: Deploy ORM code with hourly queries (backward compatible)
```python
# Code can query either hourly_stats (old) or calculate on-the-fly (new)
# Deployment switches to on-the-fly queries but hourly_stats still exists
```

**Phase 3**: Drop hourly_stats table after validation (scheduled maintenance window)
```sql
-- Migration 003: Drop table (requires <1 second, minimal downtime)
DROP TABLE hourly_stats;
-- Stop hourly aggregation cron job
```

**Rollback Strategy**:
```sql
-- If ORM queries are slow, recreate hourly_stats and resume aggregation job
CREATE TABLE hourly_stats (...);  -- From backup schema
-- Backfill last 24 hours from snapshots
-- Re-enable cron job
```

**Alternatives Considered**:
- **Single-phase drop**: Risky - if ORM queries are slow, no fallback without restore from backup
- **Blue-green deployment**: Overkill for database migration, requires duplicate infrastructure

---

## 3. Index Design for Performance

### Decision: Composite Indexes with Leading Time Column

**Chosen Approach**: Create composite indexes with `(entity_id, snapshot_time)` for time-series queries.

**Rationale**:
- **Query Pattern**: Most queries filter by `ride_id` and order by `snapshot_time` (or vice versa)
- **Index Selectivity**: `ride_id` is high-cardinality (thousands of rides), `snapshot_time` is low-cardinality within ride scope
- **Covering Index**: Composite index can satisfy both filter and sort without table lookup
- **MySQL Optimization**: InnoDB B+tree indexes perform well with left-prefix matching

**Index Definitions**:
```sql
-- Ride snapshot queries: "Get snapshots for ride X between time Y and Z"
CREATE INDEX idx_ride_snapshots_ride_time
  ON ride_status_snapshots(ride_id, snapshot_time);

-- Park snapshot queries: "Get all park snapshots for park X today"
CREATE INDEX idx_park_snapshots_park_time
  ON park_activity_snapshots(park_id, snapshot_time);

-- Daily stats queries: "Get stats for ride X across date range"
CREATE INDEX idx_daily_stats_ride_date
  ON daily_stats(ride_id, date);

-- Metrics version queries: "Get all stats for a specific metrics version"
CREATE INDEX idx_daily_stats_version
  ON daily_stats(metrics_version, date);
```

**EXPLAIN Plan Validation**:
```sql
EXPLAIN SELECT AVG(wait_time)
FROM ride_status_snapshots
WHERE ride_id = 123
  AND snapshot_time BETWEEN '2025-12-20 08:00:00' AND '2025-12-20 09:00:00';

-- Expected: key=idx_ride_snapshots_ride_time, key_len=12 (4 bytes ride_id + 8 bytes timestamp)
-- Rows: ~12 (5-min snapshots for 1 hour = 12 rows)
```

**Naming Convention**:
- Format: `idx_{table}_{column1}_{column2}`
- Descriptive names help identify purpose in SHOW INDEX output
- Consistent with existing project naming (verified in current schema)

**Alternatives Considered**:
- **Single-column indexes**: Less efficient for range queries, index merge operations are slower
- **Reverse order** `(snapshot_time, ride_id)`: Poor selectivity - time column has low cardinality across all rides
- **Partial indexes**: Not supported in MySQL (PostgreSQL only)

---

### Decision: Regular ANALYZE TABLE for Query Optimizer

**Chosen Approach**: Run `ANALYZE TABLE` after index creation and schedule monthly via cron.

**Rationale**:
- **Optimizer Statistics**: MySQL query planner needs accurate cardinality estimates for index selection
- **Post-Index Creation**: New indexes don't have statistics until ANALYZE runs
- **Low Cost**: ANALYZE is non-blocking for InnoDB, completes in seconds
- **Production Safety**: Prevents query plan regressions from stale statistics

**Implementation**:
```bash
# After index migration
mysql -e "ANALYZE TABLE ride_status_snapshots, park_activity_snapshots, daily_stats;"

# Monthly cron job
0 3 1 * * mysql -e "ANALYZE TABLE ride_status_snapshots, park_activity_snapshots, daily_stats;"
```

**Alternatives Considered**:
- **Automatic statistics** (innodb_stats_auto_recalc=ON): Unreliable timing, may not run when needed
- **OPTIMIZE TABLE**: Rebuilds table (slow, locks table), unnecessary for InnoDB

---

## 4. Query Migration Patterns

### Decision: Translate CTEs to Subqueries with ORM Hybrid Methods

**Chosen Approach**: Convert SQL CTEs (Common Table Expressions) to ORM subqueries, encapsulate in model hybrid methods.

**Rationale**:
- **Reusability**: Hybrid methods work in both Python code and SQL queries
- **DRY Compliance**: Single source of truth for business logic (Constitution Principle VII)
- **Type Safety**: ORM models enforce return types, CTEs are string-based
- **Testability**: Can unit test hybrid methods without database queries

**Example Migration**:
```sql
-- OLD: Raw SQL CTE
WITH rides_operated_today AS (
  SELECT DISTINCT ride_id
  FROM ride_status_snapshots
  WHERE snapshot_time >= CURDATE()
    AND (status = 'OPERATING' OR computed_is_open = TRUE)
    AND park_appears_open = TRUE
)
SELECT r.* FROM rides r
JOIN rides_operated_today rot ON r.id = rot.ride_id;
```

```python
# NEW: ORM with hybrid method
class RideStatusSnapshot(Base):
    __tablename__ = "ride_status_snapshots"

    @hybrid_method
    def is_operating(self):
        """Ride is operating if status is OPERATING or computed_is_open is TRUE"""
        return (self.status == 'OPERATING') | (self.computed_is_open == True)

    @is_operating.expression
    def is_operating(cls):
        """SQL expression for is_operating (for WHERE clauses)"""
        return or_(cls.status == 'OPERATING', cls.computed_is_open == True)

# Usage in repository
def rides_that_operated_today(session, today_start):
    subquery = (
        session.query(RideStatusSnapshot.ride_id.distinct())
        .filter(RideStatusSnapshot.snapshot_time >= today_start)
        .filter(RideStatusSnapshot.is_operating())
        .filter(RideStatusSnapshot.park_appears_open == True)
        .subquery()
    )

    return session.query(Ride).join(subquery, Ride.id == subquery.c.ride_id).all()
```

**Benefits**:
- `is_operating()` is reusable in Python: `snapshot.is_operating()` (returns bool)
- `is_operating()` is reusable in SQL: `.filter(Snapshot.is_operating())` (generates SQL)
- Single definition prevents divergence between Python and SQL logic

**Alternatives Considered**:
- **Raw SQL in repositories**: Rejected - violates DRY, doesn't leverage ORM
- **Python-only methods**: Rejected - can't use in database queries, forces in-memory filtering

---

### Decision: UTC Storage with Pacific Time Query Helpers

**Chosen Approach**: Store all timestamps in UTC, convert to Pacific time in query layer using SQLAlchemy func.

**Rationale**:
- **Database Consistency**: All timestamps stored in single timezone (UTC), no ambiguity
- **Query Accuracy**: Pacific time conversions happen in database (respects DST transitions)
- **Constitution Compliance**: Principle IV requires timezone consistency for uptime calculations
- **SQLAlchemy Support**: `func.convert_tz()` works across MySQL/MariaDB

**Implementation**:
```python
# In query helpers
from sqlalchemy import func

class TimeHelper:
    @staticmethod
    def to_pacific(utc_column):
        """Convert UTC timestamp column to Pacific time"""
        return func.convert_tz(utc_column, '+00:00', 'America/Los_Angeles')

    @staticmethod
    def pacific_date(utc_column):
        """Get Pacific calendar date from UTC timestamp"""
        return func.date(TimeHelper.to_pacific(utc_column))

# Usage
today_pacific = TimeHelper.pacific_date(RideStatusSnapshot.snapshot_time) == date.today()
session.query(RideStatusSnapshot).filter(today_pacific).all()
```

**Validation**:
```python
# Ensure Pacific time conversion handles DST correctly
# 2025-03-09 02:00:00 PST → 2025-03-09 03:00:00 PDT (spring forward)
# Query spanning DST boundary should return correct hourly buckets
```

**Alternatives Considered**:
- **Store Pacific time in database**: Rejected - causes DST ambiguity (2am doesn't exist on spring forward day)
- **Python-side conversion**: Rejected - can't use in WHERE clauses, forces full table scans

---

### Decision: Explicit NULL Handling with Coalesce

**Chosen Approach**: Use `func.coalesce()` for aggregations with NULL values.

**Rationale**:
- **MySQL Behavior**: `AVG()` excludes NULL values, but `SUM()` returns NULL if all values NULL
- **Consistency**: Explicit coalesce makes NULL handling visible in code review
- **Test Coverage**: Can test NULL handling explicitly in unit tests

**Example**:
```python
# Average wait time (NULL if no snapshots)
avg_wait = func.avg(RideStatusSnapshot.wait_time)

# Average wait time (0 if no snapshots)
avg_wait_safe = func.coalesce(func.avg(RideStatusSnapshot.wait_time), 0)

# Count with NULL handling
total_downtime = func.coalesce(func.sum(RideStatusSnapshot.downtime_minutes), 0)
```

**Alternatives Considered**:
- **Implicit NULL behavior**: Rejected - leads to unexpected results, hard to debug
- **Python post-processing**: Rejected - can't use in ORDER BY clauses

---

## 5. Testing Strategy

### Decision: Golden Data Regression Tests with Hand-Computed Values

**Chosen Approach**: Create golden data test fixtures with hand-computed expected values, validate ORM queries match.

**Rationale**:
- **Migration Validation**: Proves ORM queries return identical results to raw SQL
- **Constitution Compliance**: Principle VI (TDD) requires comprehensive test coverage
- **Regression Prevention**: Catches calculation bugs before production deployment
- **Historical Accuracy**: Validates business logic preserved during migration

**Golden Data Test Pattern**:
```python
# tests/golden_data/test_hourly_aggregation_parity.py
import pytest
from freezegun import freeze_time
from datetime import datetime, timezone

# Hand-computed expected value from production snapshot data
GOLDEN_DATA_HOURLY_SHAME = {
    "2025-12-05-14": {  # Dec 5, 2025, 2pm-3pm Pacific (hour 14)
        "park_id": 1,
        "shame_score": 127.5,  # Manually calculated from snapshot data
        "total_rides_down": 5,
        "total_rides_operated": 42,
    }
}

@freeze_time("2025-12-05 22:00:00")  # 10pm UTC = 2pm Pacific
def test_hourly_shame_score_matches_golden_data(orm_session, production_snapshot_data):
    """Validate ORM hourly query matches hand-computed golden data"""
    # Load production snapshot data from fixture
    load_snapshots(orm_session, production_snapshot_data)

    # Execute ORM query (new implementation)
    result = StatsRepository(orm_session).get_hourly_shame(
        park_id=1,
        hour_start=datetime(2025, 12, 5, 14, 0, 0)  # 2pm Pacific
    )

    # Validate matches golden data
    expected = GOLDEN_DATA_HOURLY_SHAME["2025-12-05-14"]
    assert result.shame_score == expected["shame_score"], \
        f"ORM query returned {result.shame_score}, expected {expected['shame_score']}"
    assert result.total_rides_down == expected["total_rides_down"]
    assert result.total_rides_operated == expected["total_rides_operated"]
```

**Golden Data Source**:
1. Export production snapshot data for specific test periods
2. Hand-calculate expected aggregation values using SQL or spreadsheet
3. Document calculation methodology in test docstrings
4. Store golden data in `tests/golden_data/fixtures/`

**Alternatives Considered**:
- **Compare ORM vs. raw SQL results**: Rejected - if raw SQL has bug, ORM will replicate it
- **Synthetic test data**: Rejected - doesn't catch real-world edge cases (NULL values, DST boundaries)

---

### Decision: Locust for Load Testing with Production Traffic Patterns

**Chosen Approach**: Use Locust to simulate 20 concurrent users hitting hourly API endpoints.

**Rationale**:
- **Python-Based**: Easy to integrate with existing test suite
- **Realistic Scenarios**: Can replay production API call patterns
- **Performance Metrics**: Provides p50, p95, p99 response times automatically
- **CI Integration**: Can run in GitHub Actions with threshold validation

**Load Test Configuration**:
```python
# tests/performance/locustfile.py
from locust import HttpUser, task, between

class ThemeParkAPIUser(HttpUser):
    wait_time = between(1, 3)  # Simulate user think time

    @task(3)  # Weight: 3x more frequent than other tasks
    def get_hourly_park_rankings(self):
        """Simulate dashboard loading TODAY park rankings"""
        self.client.get("/api/parks/downtime?period=today")

    @task(2)
    def get_hourly_ride_stats(self):
        """Simulate ride detail modal"""
        self.client.get("/api/rides/downtime?period=today&filter=all-parks")

    @task(1)
    def get_yesterday_rankings(self):
        """Simulate historical view"""
        self.client.get("/api/parks/downtime?period=yesterday")
```

**Performance Targets** (from spec FR-010):
```bash
# Run load test with 20 concurrent users
locust -f tests/performance/locustfile.py --host=http://localhost:5001 --users=20 --spawn-rate=2

# Validate thresholds
# - p95 response time < 500ms
# - p99 response time < 1000ms
# - 0% error rate
```

**Alternatives Considered**:
- **ab (ApacheBench)**: Too simple, doesn't support complex scenarios
- **JMeter**: Java-based, harder to integrate with Python project
- **pytest-benchmark**: Good for micro-benchmarks, not realistic load testing

---

### Decision: Automatic Transaction Rollback for Integration Tests

**Chosen Approach**: Use pytest fixture with session-level transaction rollback.

**Rationale**:
- **Test Isolation**: Each test starts with clean database state
- **Performance**: Rollback is faster than recreating tables
- **No Test Pollution**: Tests can't affect each other through database state

**Implementation**:
```python
# tests/conftest.py
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from src.models.base import Base

@pytest.fixture(scope="function")
def orm_session(test_database_url):
    """Provide ORM session with automatic rollback after test"""
    engine = create_engine(test_database_url)
    connection = engine.connect()
    transaction = connection.begin()

    SessionLocal = sessionmaker(bind=connection)
    session = SessionLocal()

    yield session

    # Rollback transaction (undo all changes)
    session.close()
    transaction.rollback()
    connection.close()
```

**Alternatives Considered**:
- **Recreate tables per test**: Slow, defeats purpose of integration tests
- **Manual cleanup in teardown**: Error-prone, easy to miss cleanup

---

## 6. Recompute Job Design

### Decision: CLI Script with Date Range and Idempotent UPSERT

**Chosen Approach**: Build `recompute_daily_stats.py` script with `--start-date` and `--end-date` arguments, using UPSERT for idempotency.

**Rationale**:
- **Idempotency**: Can run multiple times without creating duplicate rows
- **Flexibility**: Recompute any date range (1 day for hotfix, 90 days for full backfill)
- **Progress Tracking**: Logs each day processed, can resume after interruption
- **Dry Run Mode**: `--dry-run` flag for validation without writes

**CLI Interface**:
```bash
# Recompute single day (hotfix scenario)
python src/scripts/recompute_daily_stats.py --start-date 2025-12-05 --end-date 2025-12-05

# Recompute last 90 days (full backfill)
python src/scripts/recompute_daily_stats.py --start-date 2025-09-22 --end-date 2025-12-20

# Dry run to preview changes
python src/scripts/recompute_daily_stats.py --start-date 2025-12-01 --end-date 2025-12-10 --dry-run

# Specify metrics version for side-by-side comparison
python src/scripts/recompute_daily_stats.py --start-date 2025-12-05 --metrics-version 2
```

**Idempotent UPSERT Implementation**:
```python
# src/scripts/recompute_daily_stats.py
from sqlalchemy.dialects.mysql import insert

def recompute_daily_stats(session, start_date, end_date, metrics_version=1, dry_run=False):
    for current_date in date_range(start_date, end_date):
        logger.info(f"Processing {current_date}")

        # Calculate stats from raw snapshots (ORM query)
        stats = calculate_daily_stats_for_date(session, current_date)

        # UPSERT: Insert or update if exists
        stmt = insert(DailyStats).values(
            date=current_date,
            ride_id=stats.ride_id,
            park_id=stats.park_id,
            shame_score=stats.shame_score,
            total_downtime_minutes=stats.downtime,
            uptime_percentage=stats.uptime,
            metrics_version=metrics_version,
        )

        # MySQL specific: ON DUPLICATE KEY UPDATE
        stmt = stmt.on_duplicate_key_update(
            shame_score=stmt.inserted.shame_score,
            total_downtime_minutes=stmt.inserted.total_downtime_minutes,
            uptime_percentage=stmt.inserted.uptime_percentage,
            metrics_version=stmt.inserted.metrics_version,
        )

        if not dry_run:
            session.execute(stmt)
            session.commit()
        else:
            logger.info(f"DRY RUN: Would upsert {stats}")
```

**Progress Tracking**:
```python
# Log progress every 10 days
if current_date.day % 10 == 0:
    days_processed = (current_date - start_date).days
    days_total = (end_date - start_date).days
    pct_complete = (days_processed / days_total) * 100
    logger.info(f"Progress: {pct_complete:.1f}% ({days_processed}/{days_total} days)")
```

**Error Handling**:
```python
try:
    stats = calculate_daily_stats_for_date(session, current_date)
except SnapshotDataMissingError as e:
    logger.warning(f"Missing snapshot data for {current_date}: {e}")
    # Continue processing next date (don't fail entire job)
    continue
```

**Alternatives Considered**:
- **Truncate and reload**: Not idempotent, data loss if job fails mid-run
- **DELETE then INSERT**: Race condition if aggregation job runs concurrently
- **Airflow/Luigi orchestration**: Overkill for simple batch job

---

### Decision: metrics_version Column for Side-by-Side Validation

**Chosen Approach**: Add `metrics_version INT DEFAULT 1` column to daily_stats, increment when calculation logic changes.

**Rationale**:
- **Safe Validation**: Run new calculation (version 2) alongside old (version 1), compare results
- **Gradual Migration**: Can switch production queries to new version after validation
- **Rollback Safety**: If new version has bugs, switch back to version 1 immediately
- **Historical Analysis**: Can compare how calculation changes affected results

**Usage Workflow**:
```sql
-- Step 1: Deploy code with new calculation logic
-- Step 2: Recompute with version 2 (doesn't affect production queries)
python recompute_daily_stats.py --start-date 2025-12-01 --metrics-version 2

-- Step 3: Compare versions for sample dates
SELECT date, metrics_version, shame_score, uptime_percentage
FROM daily_stats
WHERE ride_id = 123 AND date BETWEEN '2025-12-01' AND '2025-12-07'
ORDER BY date, metrics_version;

-- Step 4: If version 2 looks correct, update production queries
-- Change: WHERE metrics_version = 1 → WHERE metrics_version = 2

-- Step 5: Delete old version after 30 days
DELETE FROM daily_stats WHERE metrics_version = 1;
```

**Alternatives Considered**:
- **Separate table** (daily_stats_v2): Schema duplication, harder to query both versions
- **Timestamp-based versioning**: Ambiguous which calculation version was used

---

## Research Summary

**All technical decisions documented. Ready for Phase 1 (data model design).**

**Key Decisions**:
1. ✅ SQLAlchemy 2.0 declarative models with type annotations + scoped_session + lazy loading with explicit joins
2. ✅ Alembic with autogenerate + zero-downtime multi-phase migration
3. ✅ Composite indexes `(entity_id, snapshot_time)` + monthly ANALYZE TABLE
4. ✅ CTEs → hybrid methods + UTC storage with Pacific helpers + explicit NULL handling
5. ✅ Golden data regression tests + Locust load testing + automatic transaction rollback
6. ✅ CLI recompute script with date range + idempotent UPSERT + metrics_version column

**Next Step**: Proceed to Phase 1 (data-model.md generation) after Zen `thinkdeep` review validates these decisions.
