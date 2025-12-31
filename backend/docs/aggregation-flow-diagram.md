# Hourly Aggregation Flow Diagram

## Part 1: Table Relationships (Entity Relationship)

```
┌─────────────────────────────────────────────────────────────────────┐
│                         RAW DATA LAYER                               │
│                     (5-minute snapshots)                             │
└─────────────────────────────────────────────────────────────────────┘

    parks (static metadata)
      ├── park_id (PK)
      ├── name
      ├── is_disney
      └── is_universal

            ↓ has many

    park_activity_snapshots               rides (static metadata)
      ├── park_id (FK)                      ├── ride_id (PK)
      ├── recorded_at                       ├── park_id (FK)
      ├── park_appears_open (bool)          ├── name
      ├── shame_score (computed)            └── tier (1-3)
      ├── rides_open (count)                      ↓ has many
      ├── rides_closed (count)
      ├── avg_wait_time                     ride_status_snapshots
      └── [collected every 5 min]             ├── ride_id (FK)
                                              ├── recorded_at
                                              ├── status ('OPERATING','DOWN','CLOSED')
                                              ├── computed_is_open (bool)
                                              ├── wait_time
                                              └── [collected every 5 min]

┌─────────────────────────────────────────────────────────────────────┐
│                    AGGREGATED DATA LAYER                             │
│                     (hourly summaries)                               │
└─────────────────────────────────────────────────────────────────────┘

    park_hourly_stats                     ride_hourly_stats
      ├── park_id (FK)                      ├── ride_id (FK)
      ├── hour_start_utc                    ├── park_id (FK)
      ├── shame_score (AVG)                 ├── hour_start_utc
      ├── avg_wait_time (AVG)               ├── avg_wait_time (AVG)
      ├── rides_operating (AVG)             ├── operating_snapshots (COUNT)
      ├── rides_down (AVG)                  ├── down_snapshots (COUNT)
      ├── weighted_downtime_hours (SUM)     ├── downtime_hours (calculated)
      ├── snapshot_count (~12)              ├── uptime_percentage (%)
      ├── park_was_open (bool)              ├── snapshot_count (~12)
      └── [computed hourly]                 ├── ride_operated (bool)
                                            └── [computed hourly]

    aggregation_log (audit trail)
      ├── aggregation_date (DATE)
      ├── aggregation_type ('hourly')
      ├── started_at
      ├── completed_at
      ├── status ('success'/'failed')
      ├── parks_processed
      └── rides_processed
```

---

## Part 2: Hourly Aggregation Sequence Diagram

```
┌──────────────┐  ┌──────────────────┐  ┌─────────────────┐  ┌──────────────────┐  ┌─────────────────┐
│   Cron Job   │  │ aggregate_hourly │  │  Database       │  │ Raw Snapshots    │  │ Aggregated      │
│  (every :05) │  │     .py          │  │  Connection     │  │ Tables           │  │ Tables          │
└──────┬───────┘  └────────┬─────────┘  └────────┬────────┘  └────────┬─────────┘  └────────┬────────┘
       │                   │                     │                    │                     │
       │  1. Trigger       │                     │                    │                     │
       ├──────────────────>│                     │                    │                     │
       │  (target_hour:    │                     │                    │                     │
       │   previous hour)  │                     │                    │                     │
       │                   │                     │                    │                     │
       │                   │  2. Open connection │                    │                     │
       │                   ├────────────────────>│                    │                     │
       │                   │                     │                    │                     │
       │                   │  3. Check if already aggregated          │                     │
       │                   ├─────────────────────┼───────────────────>│                     │
       │                   │                     │  SELECT COUNT(*)   │  park_hourly_stats  │
       │                   │                     │  WHERE hour_start  │  WHERE hour_start   │
       │                   │                     │   = target_hour    │   = target_hour     │
       │                   │<────────────────────┼────────────────────┤                     │
       │                   │  count > 0?         │                    │                     │
       │                   │  → Skip (idempotent)│                    │                     │
       │                   │                     │                    │                     │
       │                   │  4. Create aggregation_log entry (UPSERT)│                     │
       │                   ├─────────────────────┼───────────────────────────────────────>│
       │                   │                     │  INSERT INTO aggregation_log            │
       │                   │                     │  (aggregation_date, type='hourly')      │
       │                   │                     │  ON DUPLICATE KEY UPDATE                │
       │                   │<────────────────────┼─────────────────────────────────────────┤
       │                   │  log_id = 18        │                    │                     │
       │                   │                     │                    │                     │
       │                   │                     │                    │                     │
       │              ╔════╧════════════════════════════════════════════════════════════╗  │
       │              ║  PHASE 1: AGGREGATE RIDES                                       ║  │
       │              ╚════╤════════════════════════════════════════════════════════════╝  │
       │                   │                     │                    │                     │
       │                   │  5. For each ride...│                    │                     │
       │                   │  (4200+ rides)      │                    │                     │
       │                   │                     │                    │                     │
       │                   │  5a. Check if ride has snapshots         │                     │
       │                   ├─────────────────────┼───────────────────>│                     │
       │                   │                     │  SELECT COUNT(*)   │  ride_status_       │
       │                   │                     │  FROM ride_status_ │  snapshots          │
       │                   │                     │  snapshots         │  WHERE ride_id=X    │
       │                   │                     │  WHERE ride_id=X   │  AND recorded_at    │
       │                   │                     │  AND hour=[13:00-  │  BETWEEN 13:00      │
       │                   │                     │         14:00)     │  AND 14:00          │
       │                   │<────────────────────┼────────────────────┤                     │
       │                   │  count = 0?         │                    │                     │
       │                   │  → Skip this ride   │                    │                     │
       │                   │                     │                    │                     │
       │                   │  5b. Aggregate ride stats (INSERT...SELECT)                    │
       │                   ├─────────────────────┼────────────────────┼────────────────────>│
       │                   │                     │  INSERT INTO ride_hourly_stats          │
       │                   │                     │  SELECT                                 │
       │                   │                     │    ride_id,                             │
       │                   │                     │    park_id,                             │
       │                   │                     │    '2025-12-05 13:00:00',               │
       │                   │                     │    ROUND(AVG(wait_time), 2),            │
       │                   │                     │    SUM(CASE WHEN computed_is_open       │
       │                   │                     │        THEN 1 ELSE 0 END),              │
       │                   │                     │    SUM(CASE WHEN is_down               │
       │                   │                     │        THEN 1 ELSE 0 END),              │
       │                   │                     │    ROUND(downtime_hours, 2),            │
       │                   │                     │    ROUND(uptime_pct, 2),                │
       │                   │                     │    COUNT(*),                            │
       │                   │                     │    MAX(CASE WHEN park_appears_open      │
       │                   │                     │        AND computed_is_open             │
       │                   │                     │        THEN 1 ELSE 0 END)               │
       │                   │                     │  FROM ride_status_snapshots rss         │
       │                   │                     │  JOIN parks p ON ...                    │
       │                   │                     │  JOIN park_activity_snapshots pas ON .. │
       │                   │                     │  WHERE rss.ride_id = X                  │
       │                   │                     │    AND rss.recorded_at >= '13:00'       │
       │                   │                     │    AND rss.recorded_at < '14:00'        │
       │                   │                     │  ON DUPLICATE KEY UPDATE ...            │
       │                   │<────────────────────┼─────────────────────────────────────────┤
       │                   │  1572 rides         │                    │  ride_hourly_stats  │
       │                   │  aggregated         │                    │  (1572 new rows)    │
       │                   │                     │                    │                     │
       │                   │                     │                    │                     │
       │              ╔════╧════════════════════════════════════════════════════════════╗  │
       │              ║  PHASE 2: AGGREGATE PARKS                                       ║  │
       │              ╚════╤════════════════════════════════════════════════════════════╝  │
       │                   │                     │                    │                     │
       │                   │  6. For each park...│                    │                     │
       │                   │  (80 parks)         │                    │                     │
       │                   │                     │                    │                     │
       │                   │  6a. Check if park has snapshots         │                     │
       │                   ├─────────────────────┼───────────────────>│                     │
       │                   │                     │  SELECT COUNT(*)   │  park_activity_     │
       │                   │                     │  FROM park_        │  snapshots          │
       │                   │                     │  activity_snapshots│  WHERE park_id=196  │
       │                   │                     │  WHERE park_id=196 │  AND recorded_at    │
       │                   │                     │  AND hour=[13:00-  │  BETWEEN 13:00-14:00│
       │                   │                     │         14:00)     │                     │
       │                   │<────────────────────┼────────────────────┤                     │
       │                   │  count = 12         │                    │                     │
       │                   │  → Proceed          │                    │                     │
       │                   │                     │                    │                     │
       │                   │  6b. Aggregate park stats (INSERT...SELECT)                    │
       │                   ├─────────────────────┼────────────────────┼────────────────────>│
       │                   │                     │  INSERT INTO park_hourly_stats          │
       │                   │                     │  SELECT                                 │
       │                   │                     │    park_id,                             │
       │                   │                     │    '2025-12-05 13:00:00',               │
       │                   │                     │    ROUND(AVG(CASE WHEN                  │
       │                   │                     │      park_appears_open = 1              │
       │                   │                     │      THEN shame_score END), 1),         │
       │                   │                     │    ROUND(AVG(CASE WHEN                  │
       │                   │                     │      park_appears_open = 1              │
       │                   │                     │      THEN avg_wait_time END), 2),       │
       │                   │                     │    ROUND(AVG(rides_open), 0),           │
       │                   │                     │    ROUND(AVG(rides_closed), 0),         │
       │                   │                     │    COALESCE((                           │
       │                   │                     │      SELECT SUM(downtime_hours)         │
       │                   │                     │      FROM ride_hourly_stats             │
       │                   │                     │      WHERE park_id=196                  │
       │                   │                     │        AND hour_start='13:00'), 0),     │
       │                   │                     │    COALESCE((                           │
       │                   │                     │      SELECT SUM(downtime_hours * tier)  │
       │                   │                     │      FROM ride_hourly_stats rhs         │
       │                   │                     │      JOIN rides r ON ...                │
       │                   │                     │      WHERE park_id=196), 0),            │
       │                   │                     │    0,  -- effective_park_weight (TODO)  │
       │                   │                     │    COUNT(*),                            │
       │                   │                     │    MAX(park_appears_open)               │
       │                   │                     │  FROM park_activity_snapshots           │
       │                   │                     │  WHERE park_id = 196                    │
       │                   │                     │    AND recorded_at >= '13:00'           │
       │                   │                     │    AND recorded_at < '14:00'            │
       │                   │                     │  ON DUPLICATE KEY UPDATE ...            │
       │                   │<────────────────────┼─────────────────────────────────────────┤
       │                   │  33 parks           │                    │  park_hourly_stats  │
       │                   │  aggregated         │                    │  (33 new rows)      │
       │                   │                     │                    │                     │
       │                   │                     │                    │                     │
       │                   │  7. Mark aggregation complete            │                     │
       │                   ├─────────────────────┼───────────────────────────────────────>│
       │                   │                     │  UPDATE aggregation_log                 │
       │                   │                     │  SET status='success',                  │
       │                   │                     │      completed_at=NOW(),                │
       │                   │                     │      parks_processed=33,                │
       │                   │                     │      rides_processed=1572               │
       │                   │                     │  WHERE log_id=18                        │
       │                   │<────────────────────┼─────────────────────────────────────────┤
       │                   │                     │                    │                     │
       │                   │  8. Close connection│                    │                     │
       │                   ├────────────────────>│                    │                     │
       │                   │                     │                    │                     │
       │<──────────────────┤                     │                    │                     │
       │  Success!         │                     │                    │                     │
       │  - 33 parks       │                     │                    │                     │
       │  - 1572 rides     │                     │                    │                     │
       │  - 0 errors       │                     │                    │                     │
       │                   │                     │                    │                     │
```

---

## Part 2.5: Shame Score Formula (CRITICAL)

### The Authoritative Formula

**Shame score is a RATE (0-10 scale), NOT a cumulative value.**

```
                           Σ(weighted_downtime_hours)
DAILY shame_score = ──────────────────────────────────────── × 10
                    effective_park_weight × operating_hours
```

### Components

| Component | Definition | Example |
|-----------|------------|---------|
| `weighted_downtime_hours` | SUM(ride_downtime_hours × tier_weight) for all rides | 39.33 |
| `effective_park_weight` | SUM(tier_weight) for rides that operated (had uptime > 0) | 47 |
| `operating_hours` | AVG(ride operating_hours_minutes) / 60 | 14 |

### Tier Weights

| Tier | Description | Weight |
|------|-------------|--------|
| 1 | Flagship rides (Space Mountain, Incredicoaster, etc.) | 3 |
| 2 | Major attractions (standard) | 2 |
| 3 | Minor attractions | 1 |
| Default | Unclassified rides | 2 |

### Why Time Normalization Matters

**WRONG (old formula, missing operating_hours):**
```python
shame = (weighted_downtime / effective_park_weight) × 10
# Result: DCA = 8.4 (cumulative, grows with park hours)
```

**CORRECT (with time normalization):**
```python
shame = (weighted_downtime / (effective_park_weight × operating_hours)) × 10
# Result: DCA = 0.6 (rate, stays in 0-10 range)
```

### Key Invariant

> **If hourly shame = 1.0 for all hours, daily shame should ≈ 1.0**

The time-normalized formula ensures that:
- Rankings compare parks fairly regardless of operating hours
- AVG(hourly_shame) ≈ daily_shame (as expected by users)
- The 0-10 scale has consistent meaning across time periods

### Implementation Locations

- **Daily aggregation**: `scripts/aggregate_daily.py` (lines 580-593)
- **Hourly aggregation**: DEPRECATED (`scripts/aggregate_hourly.py`)
- **Calculator**: `database/calculators/shame_score.py`

---

## Part 3: Key Business Logic Decisions

### During Ride Aggregation (Step 5b)

**Rule 1: Park Status Takes Precedence**
```sql
-- Only count ride as "down" if park was open
CASE WHEN pas.park_appears_open = 1 AND ...
```

**Rule 2: Rides Must Have Operated**
```sql
-- ride_operated = 1 only if ride operated while park was open
MAX(CASE WHEN pas.park_appears_open = 1
         AND rss.computed_is_open THEN 1 ELSE 0 END)
```

**Rule 3: Park-Type Aware Downtime**
```sql
-- Disney/Universal: Only status='DOWN' counts
-- Other parks: status='DOWN' OR status='CLOSED' counts
CASE
  WHEN (p.is_disney = 1 OR p.is_universal = 1)
    AND rss.status = 'DOWN' THEN 1
  WHEN (p.is_disney = 0 AND p.is_universal = 0)
    AND rss.status IN ('DOWN', 'CLOSED') THEN 1
  ELSE 0
END
```

### During Park Aggregation (Step 6b)

**Only aggregate when park was open:**
```sql
AVG(CASE WHEN park_appears_open = 1 THEN shame_score END)
-- Returns NULL if park was closed all hour
```

**Pull ride downtime from already-aggregated ride_hourly_stats:**
```sql
COALESCE((
  SELECT SUM(downtime_hours)
  FROM ride_hourly_stats
  WHERE park_id = 196 AND hour_start_utc = '13:00'
), 0)
```

---

## Part 4: Data Flow Summary

### Input Data (Every 5 minutes)
- `collect_snapshots.py` → Creates 1 row per park in `park_activity_snapshots`
- `collect_snapshots.py` → Creates 1 row per ride in `ride_status_snapshots`
- **Frequency**: 12 snapshots per hour per park/ride

### Aggregation (Every hour at :05)
- `aggregate_hourly.py` → Processes previous complete hour
- **Output**:
  - 1 row per park in `park_hourly_stats` (33 parks)
  - 1 row per ride in `ride_hourly_stats` (~1500 rides)

### Backfill (On-demand)
- `backfill_hourly_stats.py` → Processes historical hours (newest first)
- **Uses**: Same logic as `aggregate_hourly.py`
- **Idempotent**: Skips already-aggregated hours

---

## Part 5: Performance Characteristics

| Operation | Input Rows | Output Rows | Time | Notes |
|-----------|-----------|-------------|------|-------|
| Hourly aggregation | ~10,000 snapshots | ~1,600 rows | ~2-3 sec | 33 parks × 12 snapshots + 1572 rides × 12 snapshots |
| Backfill (1 day) | ~240,000 snapshots | ~38,000 rows | ~60 sec | 24 hours × 1,600 rows/hour |
| Chart query (raw) | ~1,440 snapshots | 24 data points | 5-10 sec | GROUP BY HOUR on raw data |
| Chart query (aggregated) | 24 rows | 24 data points | <100ms | Simple SELECT from hourly tables |

**Performance Gain**: 50-100x faster queries for charts and TODAY rankings.
