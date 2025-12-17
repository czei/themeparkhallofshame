# Research: Hourly Aggregation Implementation

**Phase**: 0 (Research & Technology Choices)
**Date**: 2025-12-05
**Status**: Complete

## Overview

This document records technical decisions for adding hourly aggregation tables to the Theme Park Hall of Shame system. The goal is to eliminate slow GROUP BY HOUR queries on raw snapshots by pre-computing hourly buckets.

## Key Findings

### Finding 1: Existing Infrastructure is Proven

**Context**: The codebase already implements daily/weekly/monthly/yearly aggregation with:
- Atomic swap pattern for zero-downtime updates (park_live_rankings)
- Centralized calculators (ShameScoreCalculator)
- Idempotent aggregation jobs (aggregate_daily.py)
- Retention policies (90 days raw, indefinite daily)

**Decision**: **Extend existing patterns** rather than introduce new architecture.

**Rationale**:
- Proven in production (daily aggregates work reliably)
- Team familiar with pattern (maintenance easier)
- Consistent with DRY principle (reuse existing calculators)

**Alternatives Considered**:
- ❌ **ORM-based aggregation**: User explicitly rejected due to previous performance issues ("took several minutes")
- ❌ **Real-time streaming aggregation**: Overkill for 5-minute collection cadence
- ❌ **Specialized time-series DB (TimescaleDB)**: Out of scope per spec, adds operational complexity

**References**:
- `backend/src/database/migrations/011_live_rankings_tables.sql` (atomic swap pattern)
- `backend/src/scripts/aggregate_daily.py` (aggregation job pattern)
- `backend/src/database/calculators/shame_score.py` (centralized logic)

---

### Finding 2: Hourly Table Schema Design

**Context**: Need to store pre-computed hourly metrics for parks and rides to replace GROUP BY HOUR queries.

**Decision**: Create two tables following existing daily stats pattern:

```sql
CREATE TABLE park_hourly_stats (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    park_id INT NOT NULL,
    hour_start_utc DATETIME NOT NULL,
    shame_score DECIMAL(3,1),
    avg_wait_time_minutes DECIMAL(6,2),
    rides_operating INT,
    rides_down INT,
    total_downtime_hours DECIMAL(8,2),
    weighted_downtime_hours DECIMAL(8,2),
    effective_park_weight DECIMAL(10,2),
    snapshot_count INT NOT NULL,
    park_was_open BOOLEAN NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY unique_park_hour (park_id, hour_start_utc),
    INDEX idx_hour_start (hour_start_utc),
    INDEX idx_park_hour (park_id, hour_start_utc),
    FOREIGN KEY (park_id) REFERENCES parks(id)
);

CREATE TABLE ride_hourly_stats (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    ride_id INT NOT NULL,
    park_id INT NOT NULL,
    hour_start_utc DATETIME NOT NULL,
    avg_wait_time_minutes DECIMAL(6,2),
    operating_snapshots INT,
    down_snapshots INT,
    downtime_hours DECIMAL(6,2),
    uptime_percentage DECIMAL(5,2),
    snapshot_count INT NOT NULL,
    ride_operated BOOLEAN NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY unique_ride_hour (ride_id, hour_start_utc),
    INDEX idx_hour_start (hour_start_utc),
    INDEX idx_park_hour (park_id, hour_start_utc),
    INDEX idx_ride_hour (ride_id, hour_start_utc),
    FOREIGN KEY (ride_id) REFERENCES rides(id),
    FOREIGN KEY (park_id) REFERENCES parks(id)
);
```

**Rationale**:
- **UNIQUE KEY on (entity_id, hour_start_utc)**: Enables idempotent upsert (INSERT ... ON DUPLICATE KEY UPDATE)
- **UTC timestamps**: All aggregation uses UTC, converted to Pacific for display (matches existing pattern)
- **Indexes on hour_start_utc**: Chart queries filter by time range
- **Indexes on (park_id, hour_start_utc)**: Park detail queries need both dimensions
- **snapshot_count**: Enables data quality validation (should be ~12 per hour for 5-min collection)
- **park_was_open / ride_operated**: Respects constitutional principle (park status precedence, only count rides that operated)

**Alternatives Considered**:
- ❌ **Single combined table**: Denormalized but poor performance (park queries scan ride-level rows)
- ❌ **Separate staging tables**: Unnecessary complexity (hourly jobs run after hour complete, no concurrent access risk)
- ❌ **Partition by hour_start_utc**: Premature optimization (3 years × 8,760 hours = 26K rows per park, indexes sufficient)

**References**:
- `backend/src/database/migrations/003_aggregates_tables.sql` (daily stats schema pattern)
- Constitution Principle I: Data Accuracy First (park_was_open flag respects park status precedence)

---

### Finding 3: Aggregation Job Timing & Idempotency

**Context**: Hourly aggregation must run reliably without corrupting data if run multiple times.

**Decision**:
- **Schedule**: Run at :05 past each hour (e.g., 13:05 processes 12:00-12:59 UTC)
- **Partial hour handling**: Wait until hour fully complete before aggregating
- **Idempotency**: Use MySQL upsert pattern `INSERT ... ON DUPLICATE KEY UPDATE`

**Implementation Pattern**:
```python
# aggregate_hourly.py (new script, modeled after aggregate_daily.py)

def aggregate_hour(hour_start_utc: datetime):
    """Aggregate snapshots for completed hour into hourly stats tables."""
    hour_end_utc = hour_start_utc + timedelta(hours=1)

    # Verify hour is complete (all snapshots collected)
    latest_snapshot = get_latest_snapshot_time()
    if latest_snapshot < hour_end_utc:
        logger.info(f"Hour {hour_start_utc} not complete, skipping")
        return

    # Aggregate parks (idempotent upsert)
    conn.execute(text("""
        INSERT INTO park_hourly_stats (
            park_id, hour_start_utc, shame_score, avg_wait_time_minutes,
            rides_operating, rides_down, total_downtime_hours,
            weighted_downtime_hours, effective_park_weight,
            snapshot_count, park_was_open
        )
        SELECT
            p.id,
            :hour_start,
            AVG(pas.shame_score),
            AVG(pas.avg_wait_time_minutes),
            AVG(pas.rides_operating),
            AVG(pas.rides_down),
            AVG(pas.total_downtime_hours),
            AVG(pas.weighted_downtime_hours),
            MAX(pas.effective_park_weight),  -- MAX ensures we use most current weight from hour
            COUNT(*),
            MAX(pas.park_appears_open)  -- Any snapshot with park open = hour had activity
        FROM parks p
        LEFT JOIN park_activity_snapshots pas
            ON p.id = pas.park_id
            AND pas.recorded_at >= :hour_start
            AND pas.recorded_at < :hour_end
        GROUP BY p.id
        ON DUPLICATE KEY UPDATE
            shame_score = VALUES(shame_score),
            avg_wait_time_minutes = VALUES(avg_wait_time_minutes),
            -- ... update all columns
            snapshot_count = VALUES(snapshot_count);
    """), {"hour_start": hour_start_utc, "hour_end": hour_end_utc})

    # Log success to aggregation_log table
    log_aggregation("hourly", hour_start_utc, parks_processed, rides_processed)
```

**Rationale**:
- **:05 delay**: Allows 5 minutes for stragglers (collection runs every 5 min, some may be delayed)
- **Hour completeness check**: Prevents partial aggregation if collection failed
- **ON DUPLICATE KEY UPDATE**: MySQL-native idempotency (safe to re-run if job crashes mid-execution)
- **AVG() on pre-computed shame_score**: Snapshots already have shame scores calculated (during collection), just average them
- **MAX() on effective_park_weight**: Uses most current weight from hour (7-day window only grows/stays same within day, never shrinks)
- **Log to aggregation_log**: Enables monitoring and debugging (matches daily aggregation pattern)
- **Data quality warning**: Add logger.warning() if snapshot_count < 6 (indicates collection failures)

**Alternatives Considered**:
- ❌ **Atomic swap for hourly**: Unnecessary for most cases (hourly jobs sequential, no concurrent read risk)
- ⚠️ **Atomic swap for TODAY queries**: Could reconsider if users report seeing mixed data during aggregation (brief inconsistency ~10 seconds)
- ❌ **Transaction rollback on failure**: ON DUPLICATE KEY simpler (no transaction overhead, partial progress safe)
- ❌ **Backfill on first read**: Lazy loading adds latency, prefer batch job

**Note on aggregate_daily.py**:
**IMPORTANT**: `aggregate_daily.py` is **NOT MODIFIED** in this phase. It continues reading from raw snapshots to create daily stats. Refactoring it to read hourly tables is a **Phase 2 follow-up** (separate feature, 1+ week after hourly tables are stable). This decouples the two major changes and de-risks the deployment.

**References**:
- `backend/src/scripts/aggregate_daily.py` (existing aggregation job pattern)
- `backend/src/database/repositories/snapshot_repository.py:get_latest_snapshot_time()`

---

### Finding 4: Query Migration Strategy

**Context**: Chart queries currently use GROUP BY HOUR on raw snapshots. Need to migrate to hourly tables without breaking functionality.

**Decision**: **Parallel implementation with feature flag**, then cutover.

**Implementation Steps**:

1. **Add hourly tables** (migration 012)
2. **Deploy hourly aggregation job** (runs in background, populates historical data)
3. **Refactor query classes** to use parameter instead of separate V2 classes (DRY principle):
   ```python
   # backend/src/database/queries/charts/park_shame_history.py (modified, not new file)
   class ParkShameHistoryQuery:
       """Chart query with fallback to raw snapshots."""

       def __init__(self, conn, use_hourly_tables=None):
           """
           Initialize query with optional hourly table override.

           Args:
               use_hourly_tables: None=auto-detect from env, True=use hourly, False=use raw
           """
           self.conn = conn
           if use_hourly_tables is None:
               use_hourly_tables = os.getenv("USE_HOURLY_TABLES", "true").lower() == "true"
           self.use_hourly_tables = use_hourly_tables

       def execute(self, park_id: int, start_time: datetime, end_time: datetime):
           if self.use_hourly_tables:
               return self._query_hourly_tables(park_id, start_time, end_time)
           else:
               return self._query_raw_snapshots(park_id, start_time, end_time)  # Rollback path

       def _query_hourly_tables(self, park_id, start, end):
           """Fast path: Read from pre-aggregated hourly stats."""
           return self.conn.execute(text("""
               SELECT
                   hour_start_utc,
                   shame_score,
                   avg_wait_time_minutes,
                   rides_operating,
                   rides_down
               FROM park_hourly_stats
               WHERE park_id = :park_id
                 AND hour_start_utc >= :start
                 AND hour_start_utc < :end
                 AND park_was_open = TRUE
               ORDER BY hour_start_utc
           """), {"park_id": park_id, "start": start, "end": end})

       def _query_raw_snapshots(self, park_id, start, end):
           """Fallback path: Original GROUP BY HOUR query on raw snapshots."""
           # [Original GROUP BY logic remains unchanged for rollback]
   ```

4. **Feature flag in Flask routes**:
   ```python
   # backend/src/api/routes/parks.py
   # Single query class, behavior controlled by env var
   query = ParkShameHistoryQuery(conn)  # Auto-detects USE_HOURLY_TABLES env var
   results = query.execute(park_id, start_time, end_time)
   ```

5. **Test with mirrored production DB**, verify results match
6. **Deploy with flag enabled**, monitor performance/errors
7. **Remove flag and old query classes** after 1 week of stable operation

**Rationale**:
- **Zero downtime**: Feature flag allows instant rollback if issues found
- **Gradual cutover**: Test in production with real traffic before committing
- **Result validation**: Can compare old vs new queries side-by-side during transition
- **Constitutional compliance**: Follows Production Integrity principle (test locally first, safe rollback plan)

**Alternatives Considered**:
- ❌ **Big bang cutover**: Too risky (chart queries critical user-facing feature)
- ❌ **Modify existing query classes**: Hard to rollback (would need git revert + redeploy)
- ❌ **A/B testing**: Overkill (not a UX experiment, just performance refactor)

**References**:
- `backend/src/database/queries/charts/park_shame_history.py` (original GROUP BY query to replace)
- Constitution Principle IX: Production Integrity & Local-First Development

---

### Finding 5: Backfill Strategy

**Context**: Need to populate hourly tables with historical data before queries can use them.

**Decision**: **Incremental backfill script** that processes hours in reverse chronological order (newest first).

**Implementation**:
```python
# backend/src/scripts/backfill_hourly_stats.py (new)

def backfill_hourly_stats(start_date: datetime, end_date: datetime, batch_hours: int = 24):
    """Backfill hourly aggregates for date range, processing in reverse order."""

    current_hour = end_date.replace(minute=0, second=0, microsecond=0)
    start_hour = start_date.replace(minute=0, second=0, microsecond=0)

    while current_hour >= start_hour:
        batch_end = current_hour
        batch_start = current_hour - timedelta(hours=batch_hours)

        logger.info(f"Backfilling hours {batch_start} to {batch_end}")

        for hour in reversed(range(batch_hours)):
            hour_start = batch_start + timedelta(hours=hour)
            aggregate_hour(hour_start)  # Reuse hourly aggregation function

        conn.commit()
        current_hour = batch_start

    logger.info("Backfill complete")

# Run: python -m scripts.backfill_hourly_stats --start="2025-11-01" --days=90
```

**Rationale**:
- **Reverse order**: Recent data more important (users query last 24 hours most often)
- **Reuse aggregate_hour()**: DRY principle (same logic as scheduled job)
- **Batch commits**: 24-hour batches prevent long transactions
- **Idempotent**: Safe to stop/restart (ON DUPLICATE KEY handles already-processed hours)
- **90-day limit**: Only backfill available raw data (spec says 90-day retention)

**Alternatives Considered**:
- ❌ **Forward order (oldest first)**: Would leave recent charts broken longer
- ❌ **Parallel processing**: Risk of deadlocks on UNIQUE KEY, not worth complexity
- ❌ **Dump/load from daily tables**: Loses intra-day granularity, defeats purpose

**References**:
- `backend/src/scripts/backfill_shame_scores.py` (similar backfill pattern)
- Specification FR-016: 90 days raw snapshot retention

---

### Finding 6: Retention Automation

**Context**: Specification requires 3-year retention for hourly aggregates (vs 90 days for raw snapshots).

**Decision**: **Add cleanup step to existing aggregate_daily.py script**.

**Implementation**:
```python
# backend/src/scripts/aggregate_daily.py (modify existing)

def cleanup_old_hourly_stats():
    """Delete hourly stats older than 3 years."""
    cutoff_date = datetime.now(timezone.utc) - timedelta(days=3*365)

    conn.execute(text("""
        DELETE FROM park_hourly_stats
        WHERE hour_start_utc < :cutoff
    """), {"cutoff": cutoff_date})

    conn.execute(text("""
        DELETE FROM ride_hourly_stats
        WHERE hour_start_utc < :cutoff
    """), {"cutoff": cutoff_date})

    logger.info(f"Deleted hourly stats older than {cutoff_date}")

# Add to daily aggregation workflow:
# 1. Aggregate yesterday's hourly stats into daily stats
# 2. Cleanup raw snapshots older than 90 days
# 3. Cleanup hourly stats older than 3 years  <-- NEW
```

**Rationale**:
- **Piggyback on daily job**: Already runs once per day, no new cron entry needed
- **After daily aggregation**: Ensures daily stats computed before hourly data deleted
- **Simple DELETE**: No archive needed (daily aggregates preserve long-term trends)
- **3-year window**: Matches specification FR-017

**Alternatives Considered**:
- ❌ **Separate cleanup job**: Unnecessary (daily job already has cleanup logic)
- ❌ **Archive to S3**: Out of scope (daily aggregates sufficient for long-term analysis)
- ❌ **Partition pruning**: Overkill (DELETE is fast enough for 26K rows/year)

**References**:
- `backend/src/scripts/aggregate_daily.py:cleanup_old_snapshots()` (existing cleanup pattern)
- Specification FR-017: 3-year hourly retention

---

### Finding 7: TODAY Period Hybrid Query Strategy

**Context**: Hourly aggregation runs at :05 past each hour, creating a 5-65 minute data freshness gap for the current, partial hour. Users expect real-time data for TODAY period.

**Decision**: **Hybrid query strategy** that combines completed hours from hourly tables with current hour from live rankings.

**Implementation Pattern**:
```python
# backend/src/database/queries/today/today_park_rankings.py (modified)

def get_today_rankings(self, filter_disney_universal=False, limit=50, sort_by='shame_score'):
    """Get TODAY rankings using hybrid approach for real-time freshness."""

    start_of_day_utc, now_utc = get_today_range_to_now_utc()
    current_hour_start = now_utc.replace(minute=0, second=0, microsecond=0)

    # Query 1: Completed hours from hourly tables (fast)
    completed_hours = conn.execute(text("""
        SELECT
            park_id,
            AVG(shame_score) as avg_shame_score,
            AVG(avg_wait_time_minutes) as avg_wait_time,
            SUM(total_downtime_hours) as total_downtime,
            AVG(rides_operating) as avg_rides_operating,
            AVG(rides_down) as avg_rides_down
        FROM park_hourly_stats
        WHERE hour_start_utc >= :start_of_day
          AND hour_start_utc < :current_hour
          AND park_was_open = TRUE
        GROUP BY park_id
    """), {"start_of_day": start_of_day_utc, "current_hour": current_hour_start})

    # Query 2: Current partial hour from live rankings (real-time)
    current_hour = conn.execute(text("""
        SELECT
            park_id,
            shame_score,
            avg_wait_time_minutes,
            total_downtime_hours,
            rides_operating,
            rides_down
        FROM park_live_rankings
    """))

    # Merge: Weighted average of completed hours + current hour
    # Weight by hours: N completed hours + 1 current hour
    for park_id in all_parks:
        completed = completed_hours.get(park_id, {})
        current = current_hour.get(park_id, {})

        hours_completed = count_completed_hours(start_of_day_utc, current_hour_start)
        weight_completed = hours_completed / (hours_completed + 1)
        weight_current = 1 / (hours_completed + 1)

        final_shame_score = (
            completed['avg_shame_score'] * weight_completed +
            current['shame_score'] * weight_current
        )
        # ...combine other metrics similarly
```

**Rationale**:
- **Real-time freshness**: Current hour always shows latest data (no 5-65 minute lag)
- **Performance**: Completed hours from fast hourly tables (not GROUP BY on snapshots)
- **User experience**: Matches existing expectation of "real-time" TODAY view
- **Accurate averaging**: Weights by number of hours (early morning = mostly current, evening = mostly historical)
- **Reuses existing infrastructure**: `park_live_rankings` already updated every 10 minutes

**Alternatives Considered**:
- ❌ **Wait for hourly aggregation**: Creates frustrating data freshness gap
- ❌ **Fall back to GROUP BY for current hour**: Defeats performance benefit
- ❌ **Show only completed hours**: Confusing UX (last hour disappears until :05)

**Trade-offs**:
- **Pro**: Best user experience, maintains real-time expectation
- **Pro**: Leverages existing live rankings infrastructure
- **Con**: Slightly more complex query logic (two sources instead of one)
- **Con**: Need to handle weighted averaging of metrics

**References**:
- `backend/src/scripts/aggregate_live_rankings.py` (10-minute refresh pattern)
- `backend/src/database/queries/today/today_park_rankings.py` (existing TODAY logic)
- Specification FR-001: TODAY rankings < 1 second (hybrid approach maintains this)

---

## Technology Stack Summary

| Component | Technology | Rationale |
|-----------|-----------|-----------|
| **Database** | MySQL/MariaDB | Existing infrastructure, native upsert support |
| **Query Layer** | SQLAlchemy Core (text()) | Existing pattern, no ORM complexity |
| **Aggregation Jobs** | Python scripts + cron | Proven in production (aggregate_daily.py) |
| **Idempotency** | INSERT ... ON DUPLICATE KEY UPDATE | MySQL-native, no external locking needed |
| **Monitoring** | aggregation_log table | Existing pattern, integrates with current monitoring |
| **Rollback Strategy** | Feature flags + parallel queries | Zero-downtime deployment, instant rollback |

---

## Open Questions & Decisions

### Q1: Should hourly jobs use atomic swap like live rankings?
**Answer**: No. Live rankings need atomic swap because they're queried while being updated (10-minute refresh). Hourly jobs run sequentially after hour completes - no concurrent access risk.

### Q2: What if hourly job falls behind (e.g., during outage)?
**Answer**: Job processes all missing hours in sequence. Idempotency (ON DUPLICATE KEY) ensures safe catch-up. Monitoring alerts if lag exceeds 2 hours.

### Q3: Do charts need to handle missing hourly data gracefully?
**Answer**: Yes. Query should return available hours, frontend interpolates gaps. Add `COALESCE(shame_score, 0)` for safety.

### Q4: Should we cache chart query results?
**Answer**: Not yet. Once hourly tables populated, queries will be fast enough (<100ms). Add caching later if needed (YAGNI principle).

### Q5: How to validate hourly aggregates match original GROUP BY results?
**Answer**: Integration test compares both query approaches on same data. Run during backfill phase before cutover.

---

## Next Steps

Phase 0 complete. Proceed to Phase 1:
1. **data-model.md**: Formal entity definitions for hourly stats tables
2. **contracts/api/parks.yaml**: Document existing API response format (unchanged)
3. **quickstart.md**: Local testing guide for hourly aggregation
4. **Update agent context**: Add SQLAlchemy Core, MySQL upsert patterns to CLAUDE.md

