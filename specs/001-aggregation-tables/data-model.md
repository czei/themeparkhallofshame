# Data Model: Hourly Aggregation Tables

**Phase**: 1 (Design & Contracts)
**Date**: 2025-12-05
**Status**: Complete

## Overview

This document defines the data entities for hourly pre-computed aggregation tables. These tables extend the existing daily/weekly/monthly aggregation pattern to hourly granularity, eliminating slow GROUP BY HOUR queries on raw snapshots.

## Entities

### 1. ParkHourlyStat

**Purpose**: Pre-computed hourly park performance metrics for fast chart queries and TODAY period rankings.

**Table**: `park_hourly_stats`

**Schema**:
```sql
CREATE TABLE park_hourly_stats (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    park_id INT NOT NULL,
    hour_start_utc DATETIME NOT NULL COMMENT 'Start of hour bucket (e.g., 2025-12-05 13:00:00)',

    -- Aggregated metrics (averaged from snapshots)
    shame_score DECIMAL(3,1) COMMENT '0-10 shame score (averaged across hour)',
    avg_wait_time_minutes DECIMAL(6,2) COMMENT 'Average wait time across all operating rides',
    rides_operating INT COMMENT 'Average number of rides operating during hour',
    rides_down INT COMMENT 'Average number of rides down during hour',
    total_downtime_hours DECIMAL(8,2) COMMENT 'Total ride-hours of downtime (unweighted)',
    weighted_downtime_hours DECIMAL(8,2) COMMENT 'Tier-weighted ride-hours of downtime',
    effective_park_weight DECIMAL(10,2) COMMENT '7-day hybrid denominator (MAX from hour for accuracy - monotonic within day)',

    -- Quality metadata
    snapshot_count INT NOT NULL COMMENT 'Number of snapshots aggregated (expect ~12 for 5-min collection)',
    park_was_open BOOLEAN NOT NULL COMMENT 'True if park had any activity during hour',

    -- Audit
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    -- Constraints
    UNIQUE KEY unique_park_hour (park_id, hour_start_utc),
    INDEX idx_hour_start (hour_start_utc),
    INDEX idx_park_hour (park_id, hour_start_utc),
    FOREIGN KEY (park_id) REFERENCES parks(id) ON DELETE CASCADE
) ENGINE=InnoDB COMMENT='Hourly park performance aggregates (3-year retention)';
```

**Relationships**:
- Many-to-One with `parks` (one park has many hourly stats)
- Used by chart queries to display hourly shame score trends

**Validation Rules**:
- `shame_score`: NULL if park was closed entire hour, 0-10 range if open
- `snapshot_count`: Should be ~12 (5-min collection = 12 snapshots/hour), warn if <6
- `park_was_open`: Derived from `MAX(park_appears_open)` across snapshots in hour
- `hour_start_utc`: Always on hour boundary (minutes/seconds = 00:00)

**State Transitions**: None (immutable after creation, only updated if aggregation re-run)

**Business Logic**:
- Aggregation uses existing `ShameScoreCalculator` for consistency
- Shame score averaged across snapshots (already pre-computed during collection)
- Effective park weight uses 7-day hybrid denominator (prevents morning volatility)

**Example**:
```json
{
  "id": 12345,
  "park_id": 196,
  "hour_start_utc": "2025-12-05T21:00:00Z",  // 1 PM Pacific
  "shame_score": 6.8,
  "avg_wait_time_minutes": 42.5,
  "rides_operating": 48,
  "rides_down": 5,
  "total_downtime_hours": 0.42,  // 5 rides × 5 minutes × 12 snapshots / 60 = 5 hours downtime
  "weighted_downtime_hours": 1.25,  // Tier weights applied
  "effective_park_weight": 120.0,  // Sum of tier weights for rides that operated in last 7 days
  "snapshot_count": 12,
  "park_was_open": true,
  "created_at": "2025-12-05T22:05:00Z"
}
```

---

### 2. RideHourlyStat

**Purpose**: Pre-computed hourly ride downtime metrics for ride-level chart queries and rankings.

**Table**: `ride_hourly_stats`

**Schema**:
```sql
CREATE TABLE ride_hourly_stats (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    ride_id INT NOT NULL,
    park_id INT NOT NULL,
    hour_start_utc DATETIME NOT NULL COMMENT 'Start of hour bucket',

    -- Aggregated metrics
    avg_wait_time_minutes DECIMAL(6,2) COMMENT 'Average wait time when ride operating',
    operating_snapshots INT COMMENT 'Number of snapshots with ride operating',
    down_snapshots INT COMMENT 'Number of snapshots with ride down',
    downtime_hours DECIMAL(6,2) COMMENT 'Hours ride was down (down_snapshots × 5 / 60)',
    uptime_percentage DECIMAL(5,2) COMMENT 'Percentage of time ride was operating',

    -- Quality metadata
    snapshot_count INT NOT NULL COMMENT 'Total snapshots aggregated',
    ride_operated BOOLEAN NOT NULL COMMENT 'True if ride operated at all during hour',

    -- Audit
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    -- Constraints
    UNIQUE KEY unique_ride_hour (ride_id, hour_start_utc),
    INDEX idx_hour_start (hour_start_utc),
    INDEX idx_park_hour (park_id, hour_start_utc),
    INDEX idx_ride_hour (ride_id, hour_start_utc),
    FOREIGN KEY (ride_id) REFERENCES rides(id) ON DELETE CASCADE,
    FOREIGN KEY (park_id) REFERENCES parks(id) ON DELETE CASCADE
) ENGINE=InnoDB COMMENT='Hourly ride downtime aggregates (3-year retention)';
```

**Relationships**:
- Many-to-One with `rides` (one ride has many hourly stats)
- Many-to-One with `parks` (for cross-park ride comparisons)

**Validation Rules**:
- `uptime_percentage`: 0-100 range, NULL if ride never operated during hour
- `ride_operated`: Derived from Rule 2 (must have operated while park was open)
- `downtime_hours`: Calculated using `DowntimeSQL.downtime_hours_rounded(down_snapshots)`
- `operating_snapshots + down_snapshots <= snapshot_count` (invariant)

**State Transitions**: None (immutable)

**Business Logic**:
- Only aggregates rides that **operated** during hour (per Rule 2)
- Uses park-type-aware downtime logic via `RideStatusSQL.is_down()`
- Disney/Universal: Only count `status='DOWN'`, ignore `CLOSED`
- Other parks: Count DOWN or CLOSED as downtime

**Example**:
```json
{
  "id": 67890,
  "ride_id": 5823,
  "park_id": 196,
  "hour_start_utc": "2025-12-05T21:00:00Z",
  "avg_wait_time_minutes": 65.0,
  "operating_snapshots": 9,
  "down_snapshots": 3,
  "downtime_hours": 0.25,  // 3 × 5 / 60
  "uptime_percentage": 75.0,  // 9/12 × 100
  "snapshot_count": 12,
  "ride_operated": true,
  "created_at": "2025-12-05T22:05:00Z"
}
```

---

### 3. AggregationLog (Extended)

**Purpose**: Audit trail for hourly aggregation job executions. **Extends existing table**, no schema changes needed.

**Table**: `aggregation_log` (existing)

**New Usage**:
```sql
-- Log hourly aggregation completion
INSERT INTO aggregation_log (
    aggregation_type,  -- 'hourly'
    target_period,     -- '2025-12-05 13:00:00'
    status,           -- 'success' | 'failure'
    parks_processed,
    rides_processed,
    error_message,
    processing_time_seconds
) VALUES (...);
```

**Example Log Entry**:
```json
{
  "id": 9876,
  "aggregation_type": "hourly",
  "target_period": "2025-12-05T13:00:00Z",
  "status": "success",
  "parks_processed": 80,
  "rides_processed": 4200,
  "error_message": null,
  "processing_time_seconds": 8.3,
  "created_at": "2025-12-05T14:05:12Z"
}
```

---

## Entity Relationships

```
parks (existing)
  │
  ├──< park_hourly_stats (NEW)
  │      └── Aggregates park_activity_snapshots by hour
  │
  └──< rides (existing)
         │
         └──< ride_hourly_stats (NEW)
                └── Aggregates ride_status_snapshots by hour

park_activity_snapshots (existing)
  └──> Aggregated into park_hourly_stats every hour

ride_status_snapshots (existing)
  └──> Aggregated into ride_hourly_stats every hour

aggregation_log (existing)
  └── Extended to track hourly job runs
```

---

## Data Flow

```
┌─────────────────────────────────────────────────────────────┐
│ COLLECTION (Every 5 minutes)                                 │
│ - collect_snapshots.py                                       │
│ - Creates: park_activity_snapshots, ride_status_snapshots   │
│ - Computes: shame_score at collection time                   │
└────────────────────┬────────────────────────────────────────┘
                     │
                     │ Raw snapshots (5-min intervals)
                     ▼
┌─────────────────────────────────────────────────────────────┐
│ HOURLY AGGREGATION (At :05 past each hour)                  │
│ - aggregate_hourly.py (NEW)                                  │
│ - Reads: Last hour of snapshots                              │
│ - Writes: park_hourly_stats, ride_hourly_stats              │
│ - Uses: MAX(effective_park_weight) for accuracy             │
│ - Logs: aggregation_log with processing time                │
└────────────────────┬────────────────────────────────────────┘
                     │
                     │ Hourly aggregates
                     ▼
┌─────────────────────────────────────────────────────────────┐
│ DAILY AGGREGATION (Once per day at 2 AM Pacific)            │
│ - aggregate_daily.py (NOT MODIFIED - Phase 2 follow-up)     │
│ - Reads: park_activity_snapshots (continues using raw data) │
│ - Writes: park_daily_stats, ride_daily_stats                │
│ - Cleanup: Deletes snapshots >90 days, hourly stats >3 years│
└────────────────────┬────────────────────────────────────────┘
                     │
                     │ Daily/weekly/monthly/yearly aggregates
                     ▼
┌─────────────────────────────────────────────────────────────┐
│ API QUERIES (On-demand)                                      │
│ - LIVE: park_live_rankings (10-min refresh)                  │
│ - TODAY: Hybrid (hourly stats + live rankings current hour)  │
│ - YESTERDAY: park_daily_stats (1 day ago)                    │
│ - CHARTS: park_hourly_stats (time range query)               │
└─────────────────────────────────────────────────────────────┘
```

**Note**: `aggregate_daily.py` is intentionally NOT modified in this phase to de-risk deployment. Refactoring it to read from hourly tables is a Phase 2 follow-up task after the hourly system is stable in production.

---

## Migration Strategy

### Migration 012: Add Hourly Stats Tables

**File**: `backend/src/database/migrations/012_add_hourly_stats.sql`

```sql
-- Migration: Add hourly aggregation tables
-- Date: 2025-12-05
-- Feature: 001-aggregation-tables

START TRANSACTION;

-- Park hourly aggregates
CREATE TABLE park_hourly_stats (
    -- [Full schema from ParkHourlyStat entity above]
) ENGINE=InnoDB COMMENT='Hourly park performance aggregates (3-year retention)';

-- Ride hourly aggregates
CREATE TABLE ride_hourly_stats (
    -- [Full schema from RideHourlyStat entity above]
) ENGINE=InnoDB COMMENT='Hourly ride downtime aggregates (3-year retention)';

-- Verify indexes created
SHOW INDEX FROM park_hourly_stats;
SHOW INDEX FROM ride_hourly_stats;

COMMIT;
```

**Rollback Plan**:
```sql
DROP TABLE IF EXISTS ride_hourly_stats;
DROP TABLE IF EXISTS park_hourly_stats;
```

---

## Index Strategy

### Query Patterns & Indexes

| Query Type | Example | Index Used | Rationale |
|------------|---------|------------|-----------|
| **Chart (single park)** | "Show park 196 shame over last 24h" | `idx_park_hour (park_id, hour_start_utc)` | Composite index covers both WHERE clauses |
| **Chart (time range)** | "Show all parks for 2025-12-05" | `idx_hour_start (hour_start_utc)` | Filter by time, scan parks |
| **TODAY rankings** | "Avg shame for all parks since midnight" | `idx_hour_start (hour_start_utc)` | Filter by time range, GROUP BY park_id |
| **Ride detail chart** | "Show ride 5823 downtime over last week" | `idx_ride_hour (ride_id, hour_start_utc)` | Composite index for ride + time |
| **Upsert** | Aggregation job re-run | `unique_park_hour (park_id, hour_start_utc)` | UNIQUE KEY enables ON DUPLICATE KEY UPDATE |

**Index Maintenance**:
- Indexes automatically updated on INSERT/UPDATE
- No manual ANALYZE needed (InnoDB auto-stats sufficient)
- Monitor with `SHOW INDEX FROM park_hourly_stats` (Cardinality should match row count)

---

## Data Retention

| Data Type | Table | Retention | Cleanup Method | Rationale |
|-----------|-------|-----------|----------------|-----------|
| **Raw snapshots** | park_activity_snapshots | 90 days | aggregate_daily.py | Source data, short-term debugging |
| **Hourly aggregates** | park_hourly_stats | 3 years | aggregate_daily.py (NEW) | Chart queries, mid-term trends |
| **Daily aggregates** | park_daily_stats | Indefinite | None | Historical analysis, awards |
| **Yearly aggregates** | park_yearly_stats | Indefinite | None | Long-term trends |

**Cleanup SQL** (added to aggregate_daily.py):
```sql
-- Delete hourly stats older than 3 years
DELETE FROM park_hourly_stats WHERE hour_start_utc < DATE_SUB(NOW(), INTERVAL 3 YEAR);
DELETE FROM ride_hourly_stats WHERE hour_start_utc < DATE_SUB(NOW(), INTERVAL 3 YEAR);
```

---

## Validation & Quality Checks

### Data Quality Invariants

1. **Snapshot count**: `snapshot_count ≈ 12` (5-min collection × 60 min / 5 = 12)
   - Alert if `< 6` (missing snapshots, collection failure)
   - Normal if `< 12` (partial hour at day boundaries)

2. **Uptime invariant**: `operating_snapshots + down_snapshots ≤ snapshot_count`
   - Should always hold (ride can't be both operating and down)

3. **Park status**: If `park_was_open = FALSE`, then `rides_operating = 0` and `rides_down = 0`
   - Validates park status precedence rule

4. **Shame score range**: `0 ≤ shame_score ≤ 10` when not NULL
   - NULL only if park was closed entire hour

### Integration Test Validation

**Test**: `test_hourly_aggregation_matches_group_by`
```python
def test_hourly_aggregation_matches_group_by():
    """Verify hourly table results match original GROUP BY query."""
    # Run original GROUP BY HOUR query on raw snapshots
    original_results = execute_group_by_query(park_id=196, start=yesterday, end=today)

    # Run new query on hourly stats table
    new_results = execute_hourly_stats_query(park_id=196, start=yesterday, end=today)

    # Assert results match within tolerance (rounding differences ok)
    assert_results_match(original_results, new_results, tolerance=0.1)
```

---

## Next Steps

Phase 1 data model complete. Continue with:
1. **contracts/api/parks.yaml**: Document existing API response format
2. **quickstart.md**: Local testing guide for hourly aggregation
3. **Update agent context**: Run `.specify/scripts/bash/update-agent-context.sh claude`

