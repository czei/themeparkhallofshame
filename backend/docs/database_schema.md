# Database Schema Documentation

## Overview
The Theme Park Downtime Tracker database consists of 16 tables organized into 4 main categories:
1. **Core Entities** (parks, rides)
2. **Raw Data Collection** (snapshots, activity tracking)
3. **Aggregate Statistics** (daily/weekly/monthly/yearly stats)
4. **System Metadata** (aggregation logs, classifications)

## Schema Diagram

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          CORE ENTITIES                                   │
└─────────────────────────────────────────────────────────────────────────┘

    ┌──────────────────────┐
    │       parks          │
    ├──────────────────────┤
    │ PK: park_id          │
    │     queue_times_id   │
    │     name             │
    │     city             │
    │     state_province   │
    │     country          │
    │     latitude         │
    │     longitude        │
    │     timezone         │
    │     operator         │
    │     is_disney        │
    │     is_universal     │
    │     is_active        │
    │     created_at       │
    │     updated_at       │
    └──────────────────────┘
            │
            │ 1:N
            ▼
    ┌──────────────────────┐
    │       rides          │
    ├──────────────────────┤
    │ PK: ride_id          │
    │ FK: park_id          │
    │     queue_times_id   │
    │     name             │
    │     land_area        │
    │     tier (1-4)       │
    │     is_active        │
    │     created_at       │
    │     updated_at       │
    └──────────────────────┘
            │
            │ 1:1
            ▼
    ┌──────────────────────┐
    │ ride_classifications │
    ├──────────────────────┤
    │ PK: classification_id│
    │ FK: ride_id (UNIQUE) │
    │     tier (1-4)       │
    │     tier_weight      │
    │     confidence       │
    │     classification   │
    │     method           │
    │     classified_at    │
    └──────────────────────┘

┌─────────────────────────────────────────────────────────────────────────┐
│                    RAW DATA COLLECTION (24-hour retention)               │
└─────────────────────────────────────────────────────────────────────────┘

    ┌──────────────────────────┐
    │ park_activity_snapshots  │
    ├──────────────────────────┤
    │ PK: snapshot_id          │
    │ FK: park_id              │
    │     recorded_at          │◄── Cleaned up after 24h
    │     park_appears_open    │    by MySQL Event
    │     active_rides_count   │
    │     total_rides_count    │
    │     created_at           │
    └──────────────────────────┘

    ┌──────────────────────────┐
    │ ride_status_snapshots    │
    ├──────────────────────────┤
    │ PK: snapshot_id          │
    │ FK: ride_id              │
    │     recorded_at          │◄── Cleaned up after 24h
    │     wait_time            │    by MySQL Event
    │     is_open (from API)   │
    │     computed_is_open     │
    │     created_at           │
    └──────────────────────────┘
            │
            │ Related
            ▼
    ┌──────────────────────────┐
    │  ride_status_changes     │
    ├──────────────────────────┤
    │ PK: change_id            │
    │ FK: ride_id              │
    │     changed_at           │◄── Cleaned up after 24h
    │     old_status           │    by MySQL Event
    │     new_status           │
    │     downtime_duration    │
    │     created_at           │
    └──────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────┐
│                    PERMANENT AGGREGATE STATISTICS                        │
└─────────────────────────────────────────────────────────────────────────┘

    ┌──────────────────────────┐
    │ park_operating_sessions  │
    ├──────────────────────────┤
    │ PK: session_id           │
    │ FK: park_id              │
    │     session_date         │◄── Used to calculate
    │     session_start_utc    │    operating hours for
    │     session_end_utc      │    downtime percentages
    │     operating_minutes    │
    │     created_at           │
    │     updated_at           │
    └──────────────────────────┘

    ┌─────────────────────────────────────────────────────────────────┐
    │                    PARK AGGREGATES (permanent)                   │
    ├─────────────────────────────────────────────────────────────────┤
    │                                                                  │
    │  park_daily_stats        park_weekly_stats                      │
    │  ├─ stat_date            ├─ year, week_number                   │
    │  ├─ total_rides          ├─ week_start_date                     │
    │  ├─ avg_uptime_%         ├─ total_rides                         │
    │  ├─ downtime_hours       ├─ avg_uptime_%                        │
    │  ├─ rides_with_downtime  ├─ downtime_hours                      │
    │  └─ operating_hours      ├─ trend_vs_previous_week              │
    │                          └─ ...                                  │
    │                                                                  │
    │  park_monthly_stats      park_yearly_stats                      │
    │  ├─ year, month          ├─ year                                │
    │  ├─ total_rides          ├─ total_rides                         │
    │  ├─ avg_uptime_%         ├─ avg_uptime_%                        │
    │  ├─ downtime_hours       ├─ downtime_hours                      │
    │  ├─ trend_vs_prev_month  ├─ trend_vs_previous_year              │
    │  └─ ...                  └─ ...                                  │
    └─────────────────────────────────────────────────────────────────┘

    ┌─────────────────────────────────────────────────────────────────┐
    │                    RIDE AGGREGATES (permanent)                   │
    ├─────────────────────────────────────────────────────────────────┤
    │                                                                  │
    │  ride_daily_stats        ride_weekly_stats                      │
    │  ├─ stat_date            ├─ year, week_number                   │
    │  ├─ uptime_minutes       ├─ week_start_date                     │
    │  ├─ downtime_minutes     ├─ uptime_minutes                      │
    │  ├─ uptime_%             ├─ downtime_minutes                    │
    │  ├─ operating_hours      ├─ uptime_%                            │
    │  ├─ avg_wait_time        ├─ trend_vs_previous_week              │
    │  ├─ status_changes       ├─ ...                                 │
    │  ├─ longest_downtime     │                                       │
    │  └─ ...                  │                                       │
    │                                                                  │
    │  ride_monthly_stats      ride_yearly_stats                      │
    │  ├─ year, month          ├─ year                                │
    │  ├─ uptime_minutes       ├─ uptime_minutes                      │
    │  ├─ downtime_minutes     ├─ downtime_minutes                    │
    │  ├─ uptime_%             ├─ uptime_%                            │
    │  ├─ trend_vs_prev_month  ├─ trend_vs_previous_year              │
    │  └─ ...                  └─ ...                                  │
    └─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────┐
│                         SYSTEM METADATA                                  │
└─────────────────────────────────────────────────────────────────────────┘

    ┌──────────────────────────┐
    │    aggregation_log       │
    ├──────────────────────────┤
    │ PK: log_id               │
    │     aggregation_date     │
    │     aggregation_type     │◄── Tracks which aggregations
    │     started_at           │    have been run
    │     completed_at         │    (daily/weekly/monthly)
    │     status               │
    │     aggregated_until_ts  │
    │     parks_processed      │
    │     rides_processed      │
    └──────────────────────────┘
```

## Key Relationships

### 1. Core Entity Hierarchy
- **parks** (1) → (N) **rides**: One park has many rides
- **rides** (1) → (1) **ride_classifications**: Each ride has one classification

### 2. Data Collection Flow
- **parks** → **park_activity_snapshots**: Park-level activity every 10 minutes
- **rides** → **ride_status_snapshots**: Ride-level status every 10 minutes
- **rides** → **ride_status_changes**: Status transitions (open ↔ closed)

### 3. Aggregation Dependencies
- **park_operating_sessions**: Calculated from park_activity_snapshots
- **park_*_stats**: Aggregated from park_operating_sessions + ride stats
- **ride_*_stats**: Aggregated from ride_status_snapshots + ride_status_changes

### 4. Data Retention
- **Raw snapshots**: 24-hour retention (cleaned by MySQL Events)
- **Aggregates**: Permanent retention
- **Operating sessions**: Permanent retention

## Foreign Key Constraints

All foreign keys use `ON DELETE SET NULL` to preserve historical data:

```sql
-- If a park is deleted, aggregates remain but park_id becomes NULL
FOREIGN KEY (park_id) REFERENCES parks(park_id) ON DELETE SET NULL

-- If a ride is deleted, aggregates remain but ride_id becomes NULL
FOREIGN KEY (ride_id) REFERENCES rides(ride_id) ON DELETE SET NULL
```

This design ensures:
- Historical statistics are never lost
- Deleted entities can be identified (NULL foreign keys)
- Data integrity is maintained

## MySQL Events (Automated Cleanup)

Three events run every hour to maintain 24-hour retention:

1. **cleanup_ride_snapshots**
   - Deletes ride_status_snapshots older than 24 hours
   - Runs at 5 minutes past each hour

2. **cleanup_ride_changes**
   - Deletes ride_status_changes older than 24 hours
   - Runs at 10 minutes past each hour

3. **cleanup_park_snapshots**
   - Deletes park_activity_snapshots older than 24 hours
   - Runs at 15 minutes past each hour

## Indexes

### Performance-Critical Indexes

**Current Status Queries (FR-017):**
```sql
idx_snapshots_current_status ON ride_status_snapshots
  (ride_id, recorded_at DESC, wait_time, computed_is_open)
```

**Downtime Rankings (FR-010, FR-014):**
```sql
idx_park_daily_ranking ON park_daily_stats
  (stat_date DESC, total_downtime_hours DESC)

idx_ride_daily_ranking ON ride_daily_stats
  (stat_date DESC, downtime_minutes DESC)
```

**Weighted Calculations (FR-024):**
```sql
idx_classifications_tier_weight ON ride_classifications
  (ride_id, tier, tier_weight)
```

**Operating Hours Detection:**
```sql
idx_park_activity_detection ON park_activity_snapshots
  (park_id, park_appears_open, recorded_at DESC)
```

## Table Statistics

| Category | Tables | Purpose |
|----------|--------|---------|
| Core Entities | 2 | parks, rides |
| Classification | 1 | ride_classifications |
| Raw Data (24h) | 3 | snapshots, changes, activity |
| Operating Sessions | 1 | park_operating_sessions |
| Park Aggregates | 4 | daily/weekly/monthly/yearly |
| Ride Aggregates | 4 | daily/weekly/monthly/yearly |
| System Metadata | 1 | aggregation_log |
| **Total** | **16** | |

## Data Flow

```
Queue-Times.com API
        ↓
  [Data Collection]
        ↓
Raw Snapshots (24h retention)
        ↓
  [Aggregation Jobs]
        ↓
Permanent Statistics
        ↓
    [Flask API]
        ↓
  Frontend Dashboard
```

## Storage Optimization

- **24-hour retention**: Raw snapshots deleted after aggregation
- **Permanent aggregates**: Pre-calculated statistics for fast queries
- **Indexed queries**: All ranking and filtering operations use indexes
- **Connection pooling**: 10 base + 20 overflow connections
- **Pool recycling**: Connections recycled every hour
