# Quickstart: ORM Layer Developer Guide

**Feature**: 003-orm-refactoring
**Date**: 2025-12-21
**Purpose**: How to use SQLAlchemy 2.0 ORM models and query layer

---

## Setup

### Database Connection

```python
# Flask app context (automatic session management)
from src.models.base import db_session

def get_park_by_id(park_id: int):
    """Query using Flask scoped_session"""
    from src.models.park import Park

    park = db_session.query(Park).filter(Park.id == park_id).first()
    return park

# Flask teardown handles session cleanup automatically
```

```python
# Cron jobs / scripts (manual session management)
from src.models.base import create_session

def aggregate_daily_stats():
    """Cron job using session factory"""
    session = create_session()
    try:
        # Do work
        stats = calculate_stats(session)
        session.commit()
    except Exception as e:
        session.rollback()
        raise
    finally:
        session.close()
```

### Enable SQL Logging (Development)

```bash
# In .env
SQL_ECHO=true  # Logs all SQL queries to console
```

---

## Querying with ORM

### Basic Queries

```python
from src.models.park import Park
from src.models.ride import Ride
from src.models.base import db_session

# Get all parks
parks = db_session.query(Park).all()

# Get specific park
park = db_session.query(Park).filter(Park.id == 1).first()

# Get park by name
disney = db_session.query(Park).filter(Park.name.like('%Disney%')).all()

# Get rides for park
rides = db_session.query(Ride).filter(Ride.park_id == park_id).all()

# Count rides
total_rides = db_session.query(Ride).filter(Ride.park_id == park_id).count()
```

### Relationships and Joins

```python
from sqlalchemy.orm import joinedload

# WRONG - N+1 queries (triggers separate query for each ride's park)
rides = db_session.query(Ride).all()
for ride in rides:
    print(ride.park.name)  # ❌ Separate SELECT per ride

# CORRECT - Single query with JOIN (use joinedload())
rides = db_session.query(Ride).options(joinedload(Ride.park)).all()
for ride in rides:
    print(ride.park.name)  # ✅ No additional queries
```

**Rule**: Always use `joinedload()` when accessing relationships in loops.

### Filtering with Hybrid Methods

```python
from src.models.snapshots import RideStatusSnapshot

# Use hybrid methods in filters (works in SQL)
operating_snapshots = (
    db_session.query(RideStatusSnapshot)
    .filter(RideStatusSnapshot.is_operating())  # ✅ Hybrid method
    .all()
)

# Hybrid methods also work in Python
snapshot = db_session.query(RideStatusSnapshot).first()
if snapshot.is_operating():  # ✅ Same method, Python context
    print("Ride is operating")
```

### Aggregations

```python
from sqlalchemy import func

# Count by park
park_ride_counts = (
    db_session.query(
        Park.name,
        func.count(Ride.id).label('ride_count')
    )
    .join(Ride)
    .group_by(Park.id, Park.name)
    .all()
)

# Average wait time
avg_wait = (
    db_session.query(func.avg(RideStatusSnapshot.wait_time))
    .filter(RideStatusSnapshot.ride_id == ride_id)
    .scalar()
)

# Sum with NULL handling
total_downtime = (
    db_session.query(
        func.coalesce(func.sum(RideStatusSnapshot.downtime_minutes), 0)
    )
    .filter(RideStatusSnapshot.ride_id == ride_id)
    .scalar()
)
```

### Time-Based Queries

```python
from datetime import datetime, timedelta
from src.utils.query_helpers import TimeHelper

# Filter by date range
today_start = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
today_end = today_start + timedelta(days=1)

snapshots_today = (
    db_session.query(RideStatusSnapshot)
    .filter(RideStatusSnapshot.snapshot_time.between(today_start, today_end))
    .all()
)

# Filter by Pacific calendar date
from sqlalchemy import func

snapshots = (
    db_session.query(RideStatusSnapshot)
    .filter(TimeHelper.pacific_date(RideStatusSnapshot.snapshot_time) == '2025-12-21')
    .all()
)
```

---

## Old vs. New Patterns

### Ride Status Query

```python
# OLD (raw SQL)
cursor.execute("""
    SELECT r.id, r.name, r.park_id
    FROM rides r
    WHERE r.park_id = %s AND r.is_active = TRUE
""", (park_id,))
rides = cursor.fetchall()

# NEW (ORM)
from src.models.ride import Ride

rides = (
    db_session.query(Ride)
    .filter(Ride.park_id == park_id)
    .filter(Ride.is_active == True)
    .all()
)
```

### Downtime Aggregation

```python
# OLD (raw SQL with CTE)
cursor.execute("""
    WITH rides_operated AS (
        SELECT DISTINCT ride_id
        FROM ride_status_snapshots
        WHERE snapshot_time >= %s
            AND (status = 'OPERATING' OR computed_is_open = TRUE)
            AND park_appears_open = TRUE
    )
    SELECT COUNT(DISTINCT r.id) as rides_down
    FROM rides r
    JOIN ride_status_snapshots rss ON r.id = rss.ride_id
    WHERE r.id IN (SELECT ride_id FROM rides_operated)
        AND rss.is_down = TRUE
""", (today_start,))
result = cursor.fetchone()

# NEW (ORM with hybrid methods)
from src.utils.query_helpers import RideStatusQuery

operated_rides = RideStatusQuery.rides_that_operated_today(
    db_session,
    today_start
)

rides_down = (
    db_session.query(func.count(Ride.id.distinct()))
    .join(RideStatusSnapshot)
    .filter(Ride.id.in_(operated_rides))
    .filter(RideStatusSnapshot.is_down())  # Hybrid method
    .scalar()
)
```

### Repository Pattern

```python
# src/database/repositories/stats_repository.py
from sqlalchemy.orm import Session
from src.models.daily_stats import DailyStats
from typing import List, Dict

class StatsRepository:
    def __init__(self, session: Session):
        self.session = session

    def get_daily_stats(self, park_id: int, start_date: date, end_date: date) -> List[Dict]:
        """
        Get daily stats for park across date range.

        Returns: List of dictionaries (for API compatibility)
        """
        results = (
            self.session.query(DailyStats)
            .filter(DailyStats.park_id == park_id)
            .filter(DailyStats.date.between(start_date, end_date))
            .filter(DailyStats.metrics_version == 1)  # Current version
            .all()
        )

        # Convert ORM objects to dictionaries (API contract)
        return [
            {
                'date': r.date.isoformat(),
                'ride_id': r.ride_id,
                'shame_score': r.shame_score,
                'uptime_percentage': r.uptime_percentage,
            }
            for r in results
        ]
```

---

## Migrations

### Running Migrations

```bash
# Apply all pending migrations
alembic upgrade head

# Rollback one migration
alembic downgrade -1

# View migration history
alembic history

# Check current revision
alembic current
```

### Creating New Migrations

```bash
# Auto-generate migration from model changes
alembic revision --autogenerate -m "add ride tier column"

# Review generated migration (ALWAYS REVIEW BEFORE RUNNING)
# Edit src/database/migrations/versions/YYYYMMDD_HHMM_add_ride_tier_column.py

# Test migration on local copy
alembic upgrade head

# Verify schema change
mysql -e "SHOW CREATE TABLE rides;"

# Test rollback
alembic downgrade -1
alembic upgrade head
```

**CRITICAL**: Always test migrations on local copy of production database before deploying.

---

## Testing

### Unit Test Pattern

```python
# tests/unit/test_ride_model.py
import pytest
from src.models.ride import Ride
from src.models.snapshots import RideStatusSnapshot

def test_ride_get_current_status(orm_session):
    """Test ride.get_current_status() method"""
    ride = Ride(name="Test Ride", park_id=1)
    snapshot = RideStatusSnapshot(
        ride=ride,
        snapshot_time=datetime.utcnow(),
        status='OPERATING',
        computed_is_open=True,
        park_appears_open=True
    )
    orm_session.add_all([ride, snapshot])
    orm_session.commit()

    # Test method
    current = ride.get_current_status()
    assert current.id == snapshot.id
    assert current.status == 'OPERATING'
```

### Integration Test Pattern

```python
# tests/integration/test_stats_repository.py
import pytest
from freezegun import freeze_time
from src.database.repositories.stats_repository import StatsRepository

@freeze_time("2025-12-21 10:00:00")
def test_get_daily_stats(mysql_connection):
    """Test stats repository with real database"""
    # Setup test data
    insert_test_parks(mysql_connection)
    insert_test_rides(mysql_connection)

    # Test query
    repo = StatsRepository(mysql_connection)
    stats = repo.get_daily_stats(park_id=1, start_date=date(2025, 12, 20), end_date=date(2025, 12, 21))

    # Validate results
    assert len(stats) > 0
    assert stats[0]['date'] == '2025-12-20'

    # mysql_connection auto-rolls back transaction after test
```

---

## Performance

### Validating Index Usage

```python
from sqlalchemy import text

# Check if query uses indexes
query = db_session.query(RideStatusSnapshot).filter(
    RideStatusSnapshot.ride_id == 123,
    RideStatusSnapshot.snapshot_time >= datetime(2025, 12, 21)
)

# Get SQL and run EXPLAIN
sql = str(query.statement.compile(compile_kwargs={"literal_binds": True}))
explain = db_session.execute(text(f"EXPLAIN {sql}")).fetchall()

# Validate index usage
# Look for: key=idx_ride_snapshots_ride_time, key_len=12
for row in explain:
    print(row)
```

Expected output:
```
id | select_type | table | key                         | key_len | rows | Extra
1  | SIMPLE      | r_s   | idx_ride_snapshots_ride_time | 12      | 144  | Using where
```

### Preventing N+1 Queries

```python
# Enable SQL logging to detect N+1
# In .env: SQL_ECHO=true

# BAD - Triggers N+1
parks = db_session.query(Park).all()
for park in parks:
    for ride in park.rides:  # ❌ SELECT for each park
        print(ride.name)

# GOOD - Single query
from sqlalchemy.orm import selectinload

parks = db_session.query(Park).options(selectinload(Park.rides)).all()
for park in parks:
    for ride in park.rides:  # ✅ No additional queries
        print(ride.name)
```

---

## Debugging

### SQL Logging

```python
import logging

# Enable SQLAlchemy logging
logging.basicConfig()
logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

# Now all queries are logged to console
parks = db_session.query(Park).all()
# Output: SELECT parks.id, parks.name, parks.park_type FROM parks
```

### Query Profiling

```python
from sqlalchemy import event
from sqlalchemy.engine import Engine
import time

# Log slow queries
@event.listens_for(Engine, "before_cursor_execute")
def before_cursor_execute(conn, cursor, statement, parameters, context, executemany):
    conn.info.setdefault('query_start_time', []).append(time.time())

@event.listens_for(Engine, "after_cursor_execute")
def after_cursor_execute(conn, cursor, statement, parameters, context, executemany):
    total = time.time() - conn.info['query_start_time'].pop(-1)
    if total > 1.0:  # Log queries > 1 second
        print(f"SLOW QUERY ({total:.2f}s): {statement}")
```

---

## Common Patterns

### Model Methods vs. Repository Methods

**Use Model Methods** when:
- Logic operates on single model instance
- Business logic is reusable across queries
- Example: `ride.get_current_status()`, `snapshot.is_operating()`

**Use Repository Methods** when:
- Logic spans multiple tables/models
- Complex aggregations or filtering
- API endpoint needs specific data shape
- Example: `StatsRepository.get_park_rankings()`, `RideStatusQuery.hourly_downtime_by_park()`

### Hybrid Properties vs. Hybrid Methods

```python
# Hybrid Property (attribute-like access)
@hybrid_property
def full_name(self):
    return f"{self.first_name} {self.last_name}"

@full_name.expression
def full_name(cls):
    return cls.first_name + ' ' + cls.last_name

# Usage
user.full_name  # Python: "John Doe"
query.filter(User.full_name == "John Doe")  # SQL

# Hybrid Method (function call)
@hybrid_method
def is_operating(self):
    return self.status == 'OPERATING'

@is_operating.expression
def is_operating(cls):
    return cls.status == 'OPERATING'

# Usage
snapshot.is_operating()  # Python: True/False
query.filter(Snapshot.is_operating())  # SQL
```

---

## Summary

**Key Takeaways**:
- ✅ Use `joinedload()` to prevent N+1 queries
- ✅ Use hybrid methods for reusable business logic (DRY principle)
- ✅ Use `TimeHelper` for Pacific timezone conversions
- ✅ Use `func.coalesce()` for explicit NULL handling
- ✅ Always test migrations on local copy before production
- ✅ Enable SQL logging during development to catch performance issues
- ✅ Use repository pattern to maintain API contracts (same function signatures)

**Migration Checklist**:
1. Read this guide
2. Review `data-model.md` for ORM schema
3. Run migrations: `alembic upgrade head`
4. Enable SQL logging: `SQL_ECHO=true`
5. Test queries with EXPLAIN to validate index usage
6. Run full test suite: `pytest tests/`
7. Validate API contracts unchanged: `pytest tests/contract/`

**Need Help?**
- See `data-model.md` for complete ORM schema
- See `research.md` for technical decisions and rationale
- See `contracts/api-preservation.md` for API compatibility rules
