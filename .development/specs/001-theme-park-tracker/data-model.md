# Data Model: Theme Park Downtime Tracker

**Version**: 1.0
**Created**: 2025-11-22
**Database**: MySQL 8.0+

## Overview

This document defines the MySQL database schema for the Theme Park Downtime Tracker. The schema is optimized for:
- Fast current status queries (<50ms)
- Fast aggregate queries (<100ms)
- 24-hour retention for raw snapshots
- Permanent storage for daily/weekly/monthly/yearly aggregates
- Time-series query patterns

## Design Principles

1. **Time-Series Optimization**: Heavy indexing on timestamp fields with DESC ordering for recent-data queries
2. **Computed Status Logic**: `computed_is_open` field derived from: `wait_time > 0 OR (is_open = true AND wait_time = 0)`
3. **UTC Everywhere**: All timestamps stored in UTC; timezone conversion happens at application layer
4. **Cascade Deletion**: Raw snapshots auto-delete after 24 hours; aggregates retain foreign key references with SET NULL on park/ride deletion
5. **Denormalization**: Park operator info duplicated in parks table for fast filtering without joins

## Entity Relationship Diagram

```
parks (1) ----< (M) rides (1) ----< (1) ride_classifications
  |                   |
  |                   +----< (M) ride_status_snapshots (24-hour retention)
  |                   |
  |                   +----< (M) ride_status_changes (24-hour retention)
  |                   |
  |                   +----< (M) ride_daily_stats (permanent)
  |                   +----< (M) ride_weekly_stats (permanent)
  |                   +----< (M) ride_monthly_stats (permanent)
  |                   +----< (M) ride_yearly_stats (permanent)
  |
  +----< (M) park_activity_snapshots (24-hour retention)
  |
  +----< (M) park_operating_sessions (permanent)
  |
  +----< (M) park_daily_stats (permanent)
  +----< (M) park_weekly_stats (permanent)
  +----< (M) park_monthly_stats (permanent)
  +----< (M) park_yearly_stats (permanent)
```

## Schema Definitions

### Core Entities

#### `parks`

Stores theme park master data.

```sql
CREATE TABLE parks (
    park_id INT PRIMARY KEY AUTO_INCREMENT,
    queue_times_id INT UNIQUE NOT NULL COMMENT 'External ID from Queue-Times.com API',
    name VARCHAR(255) NOT NULL,
    city VARCHAR(100) NOT NULL,
    state_province VARCHAR(100) DEFAULT NULL COMMENT 'State/Province abbreviation (e.g., FL, CA, QC)',
    country VARCHAR(2) NOT NULL COMMENT 'ISO 3166-1 alpha-2 country code (US, CA)',
    latitude DECIMAL(10, 8) DEFAULT NULL,
    longitude DECIMAL(11, 8) DEFAULT NULL,
    timezone VARCHAR(50) NOT NULL DEFAULT 'America/New_York' COMMENT 'IANA timezone (e.g., America/Los_Angeles)',
    operator VARCHAR(100) DEFAULT NULL COMMENT 'Park operator (Disney, Universal, Six Flags, etc.)',
    is_disney BOOLEAN NOT NULL DEFAULT FALSE COMMENT 'Denormalized flag for Disney & Universal filter',
    is_universal BOOLEAN NOT NULL DEFAULT FALSE COMMENT 'Denormalized flag for Disney & Universal filter',
    is_active BOOLEAN NOT NULL DEFAULT TRUE COMMENT 'Whether park is currently tracked',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    INDEX idx_operator (operator),
    INDEX idx_disney_universal (is_disney, is_universal),
    INDEX idx_country (country),
    INDEX idx_active (is_active)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
```

**Sample Data**:
```sql
INSERT INTO parks (queue_times_id, name, city, state_province, country, latitude, longitude,
                   timezone, operator, is_disney, is_universal)
VALUES
(16, 'Magic Kingdom', 'Orlando', 'FL', 'US', 28.417663, -81.581213,
 'America/New_York', 'Disney', TRUE, FALSE),
(17, 'Universal Studios Florida', 'Orlando', 'FL', 'US', 28.479594, -81.467155,
 'America/New_York', 'Universal', FALSE, TRUE);
```

---

#### `rides`

Stores individual ride/attraction master data.

```sql
CREATE TABLE rides (
    ride_id INT PRIMARY KEY AUTO_INCREMENT,
    queue_times_id INT UNIQUE NOT NULL COMMENT 'External ID from Queue-Times.com API',
    park_id INT NOT NULL,
    name VARCHAR(255) NOT NULL,
    land_area VARCHAR(100) DEFAULT NULL COMMENT 'Themed land/area within park (e.g., Fantasyland)',
    tier TINYINT DEFAULT NULL COMMENT 'Ride tier classification: 1 (major, 3x weight), 2 (standard, 2x weight), 3 (minor, 1x weight)',
    is_active BOOLEAN NOT NULL DEFAULT TRUE COMMENT 'Whether ride is currently tracked',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    FOREIGN KEY (park_id) REFERENCES parks(park_id) ON DELETE CASCADE,
    INDEX idx_park_id (park_id),
    INDEX idx_active (is_active),
    INDEX idx_park_active (park_id, is_active),
    INDEX idx_tier (tier),
    CHECK (tier IN (1, 2, 3) OR tier IS NULL)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
```

**Sample Data**:
```sql
INSERT INTO rides (queue_times_id, park_id, name, land_area, tier)
VALUES
(1234, 1, 'Space Mountain', 'Tomorrowland', 1),  -- Tier 1: Major E-ticket attraction
(1235, 1, 'Big Thunder Mountain Railroad', 'Frontierland', 1),  -- Tier 1: Major coaster
(1236, 1, 'Haunted Mansion', 'Liberty Square', 2),  -- Tier 2: Classic dark ride
(1237, 1, 'Prince Charming Regal Carrousel', 'Fantasyland', 3);  -- Tier 3: Classic flat ride
```

---

#### `ride_classifications`

Stores tier classification metadata for each ride. Populated by separate classification script.

```sql
CREATE TABLE ride_classifications (
    classification_id INT PRIMARY KEY AUTO_INCREMENT,
    ride_id INT NOT NULL,
    tier TINYINT NOT NULL COMMENT 'Tier: 1 (3x weight), 2 (2x weight), 3 (1x weight)',
    tier_weight TINYINT NOT NULL COMMENT 'Multiplier for weighted downtime: 3, 2, or 1',
    classification_method ENUM('manual_override', 'cached_ai', 'pattern_match', 'ai_agent') NOT NULL COMMENT 'Method used: manual (Priority 1), cached_ai (Priority 2), pattern (Priority 3), ai_agent (Priority 4)',
    confidence_score DECIMAL(3, 2) DEFAULT NULL COMMENT 'Confidence: 0.00 to 1.00 (1.00 for manual overrides, NULL accepted)',
    reasoning_text TEXT DEFAULT NULL COMMENT 'Explanation for classification (e.g., "310 ft giga coaster, world-renowned")',
    override_reason VARCHAR(500) DEFAULT NULL COMMENT 'Manual override justification if classification_method = manual_override',
    research_sources JSON DEFAULT NULL COMMENT 'Array of URLs used by AI agent for classification (e.g., ["https://rcdb.com/11130.htm"])',
    cache_key VARCHAR(50) DEFAULT NULL COMMENT 'Cache key format: {park_id}:{ride_id} for exact_matches.json lookup',
    schema_version VARCHAR(10) DEFAULT '1.0' COMMENT 'Classification schema version for cache invalidation',
    classified_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    FOREIGN KEY (ride_id) REFERENCES rides(ride_id) ON DELETE CASCADE,
    UNIQUE KEY unique_ride (ride_id),
    INDEX idx_tier (tier),
    INDEX idx_method (classification_method),
    INDEX idx_confidence (confidence_score),
    INDEX idx_cache_key (cache_key),
    CHECK (tier IN (1, 2, 3)),
    CHECK (tier_weight IN (1, 2, 3)),
    CHECK (confidence_score IS NULL OR (confidence_score >= 0.00 AND confidence_score <= 1.00))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
```

**Sample Data**:
```sql
INSERT INTO ride_classifications (ride_id, tier, tier_weight, classification_method, confidence_score, reasoning_text, research_sources, cache_key, schema_version)
VALUES
(1, 1, 3, 'manual_override', 1.00, 'Known E-ticket: Space Mountain', NULL, '16:1234', '1.0'),
(2, 1, 3, 'cached_ai', 0.98, '310 ft giga coaster, world-renowned flagship attraction', '["https://rcdb.com/11130.htm", "https://wikipedia.org/wiki/Millennium_Force"]', '57:2341', '1.0'),
(3, 1, 3, 'ai_agent', 0.92, 'Advanced dark ride with trackless vehicles, major E-ticket at Disneyland', '["https://disney.com/rides/rise-of-resistance"]', '16:5678', '1.0'),
(4, 2, 2, 'pattern_match', 0.95, 'Contains "Haunted" - classic dark ride pattern', NULL, NULL, '1.0'),
(5, 3, 1, 'pattern_match', 0.98, 'Contains "Carousel" - Tier 3 pattern', NULL, NULL, '1.0');
```

**Classification Method Priority** (hierarchical 4-tier system):
1. **manual_override**: Human corrections from `data/manual_overrides.csv` (highest authority)
2. **cached_ai**: Reused AI decisions from `data/exact_matches.json` (confidence > 0.85)
3. **pattern_match**: Keyword-based rules for obvious cases (~76% of rides)
4. **ai_agent**: AI research with web search for ambiguous cases (~12% of rides)

---

### Raw Data (24-Hour Retention)

#### `ride_status_snapshots`

Point-in-time snapshots of ride operational status. **Retention: 24 hours**.

```sql
CREATE TABLE ride_status_snapshots (
    snapshot_id BIGINT PRIMARY KEY AUTO_INCREMENT,
    ride_id INT NOT NULL,
    recorded_at TIMESTAMP NOT NULL COMMENT 'UTC timestamp when data was collected',
    is_open BOOLEAN DEFAULT NULL COMMENT 'Raw is_open flag from Queue-Times.com API',
    wait_time INT DEFAULT NULL COMMENT 'Wait time in minutes from API',
    computed_is_open BOOLEAN NOT NULL COMMENT 'Computed status: wait_time > 0 OR (is_open = true AND wait_time = 0)',
    last_updated_api TIMESTAMP DEFAULT NULL COMMENT 'Last update timestamp from API metadata',

    FOREIGN KEY (ride_id) REFERENCES rides(ride_id) ON DELETE CASCADE,
    INDEX idx_ride_recorded (ride_id, recorded_at DESC) COMMENT 'Optimized for time-series queries',
    INDEX idx_recorded_at (recorded_at DESC) COMMENT 'For cleanup jobs',
    INDEX idx_computed_status (computed_is_open, recorded_at DESC) COMMENT 'For downtime queries'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
```

**Retention Policy**: Automated job deletes rows where `recorded_at < NOW() - INTERVAL 24 HOUR` runs hourly.

**Sample Data**:
```sql
INSERT INTO ride_status_snapshots (ride_id, recorded_at, is_open, wait_time, computed_is_open)
VALUES
(1, '2025-11-22 14:00:00', TRUE, 45, TRUE),
(1, '2025-11-22 14:10:00', FALSE, 0, FALSE),
(1, '2025-11-22 14:20:00', FALSE, 0, FALSE);
```

---

#### `ride_status_changes`

Records state transitions (open ↔ closed) with duration metrics. **Retention: 24 hours**.

```sql
CREATE TABLE ride_status_changes (
    change_id BIGINT PRIMARY KEY AUTO_INCREMENT,
    ride_id INT NOT NULL,
    changed_at TIMESTAMP NOT NULL COMMENT 'UTC timestamp of status change',
    previous_status BOOLEAN NOT NULL COMMENT 'Previous computed_is_open value',
    new_status BOOLEAN NOT NULL COMMENT 'New computed_is_open value',
    duration_in_previous_status INT NOT NULL COMMENT 'Minutes spent in previous status',
    wait_time_at_change INT DEFAULT NULL COMMENT 'Wait time when change occurred',

    FOREIGN KEY (ride_id) REFERENCES rides(ride_id) ON DELETE CASCADE,
    INDEX idx_ride_changed (ride_id, changed_at DESC),
    INDEX idx_changed_at (changed_at DESC),
    INDEX idx_downtime (ride_id, new_status, changed_at) COMMENT 'For downtime period queries'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
```

**Sample Data**:
```sql
INSERT INTO ride_status_changes (ride_id, changed_at, previous_status, new_status, duration_in_previous_status, wait_time_at_change)
VALUES
(1, '2025-11-22 14:10:00', TRUE, FALSE, 120, 0),  -- Went down after 2 hours up
(1, '2025-11-22 14:50:00', FALSE, TRUE, 40, 25);  -- Came back up after 40 min down
```

---

#### `park_activity_snapshots`

Park-wide operational metrics at collection interval. **Retention: 24 hours**.

```sql
CREATE TABLE park_activity_snapshots (
    snapshot_id BIGINT PRIMARY KEY AUTO_INCREMENT,
    park_id INT NOT NULL,
    recorded_at TIMESTAMP NOT NULL COMMENT 'UTC timestamp when data was collected',
    total_rides_tracked INT NOT NULL DEFAULT 0,
    rides_open INT NOT NULL DEFAULT 0,
    rides_closed INT NOT NULL DEFAULT 0,
    avg_wait_time DECIMAL(5,2) DEFAULT NULL COMMENT 'Average wait time across all open rides',
    max_wait_time INT DEFAULT NULL COMMENT 'Maximum wait time across all rides',
    park_appears_open BOOLEAN NOT NULL COMMENT 'TRUE if any ride has activity (computed_is_open = TRUE)',

    FOREIGN KEY (park_id) REFERENCES parks(park_id) ON DELETE CASCADE,
    INDEX idx_park_recorded (park_id, recorded_at DESC),
    INDEX idx_recorded_at (recorded_at DESC),
    INDEX idx_park_open (park_id, park_appears_open, recorded_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
```

**Sample Data**:
```sql
INSERT INTO park_activity_snapshots (park_id, recorded_at, total_rides_tracked, rides_open, rides_closed, avg_wait_time, max_wait_time, park_appears_open)
VALUES
(1, '2025-11-22 14:00:00', 45, 42, 3, 32.50, 90, TRUE);
```

---

#### `aggregation_log`

Tracks successful completion of daily aggregation jobs to prevent data loss. **Permanent storage**.

```sql
CREATE TABLE aggregation_log (
    log_id INT PRIMARY KEY AUTO_INCREMENT,
    aggregation_date DATE NOT NULL COMMENT 'Date for which aggregation was performed (local date)',
    aggregation_type ENUM('daily', 'weekly', 'monthly', 'yearly') NOT NULL,
    started_at TIMESTAMP NOT NULL COMMENT 'When aggregation job started',
    completed_at TIMESTAMP DEFAULT NULL COMMENT 'When aggregation job completed successfully',
    status ENUM('running', 'success', 'failed') NOT NULL DEFAULT 'running',
    aggregated_until_ts TIMESTAMP DEFAULT NULL COMMENT 'Maximum recorded_at timestamp that was aggregated',
    error_message TEXT DEFAULT NULL COMMENT 'Error details if status = failed',
    parks_processed INT DEFAULT 0 COMMENT 'Number of parks successfully aggregated',
    rides_processed INT DEFAULT 0 COMMENT 'Number of rides successfully aggregated',

    UNIQUE KEY unique_aggregation (aggregation_date, aggregation_type),
    INDEX idx_status (status, aggregation_date),
    INDEX idx_completed (completed_at DESC)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
```

**Purpose**:
- Prevents raw data deletion before successful aggregation
- Enables retry logic (run at 12:10 AM, 1:10 AM, 2:10 AM)
- Provides audit trail of aggregation jobs
- Cleanup job queries this table to determine safe deletion threshold

**Sample Data**:
```sql
INSERT INTO aggregation_log (aggregation_date, aggregation_type, started_at, completed_at, status, aggregated_until_ts, parks_processed, rides_processed)
VALUES
('2025-11-22', 'daily', '2025-11-23 00:10:00', '2025-11-23 00:14:32', 'success', '2025-11-23 00:00:00', 85, 5247),
('2025-11-21', 'daily', '2025-11-22 00:10:00', '2025-11-22 00:13:18', 'success', '2025-11-22 00:00:00', 85, 5247),
('2025-11-20', 'daily', '2025-11-21 00:10:00', NULL, 'failed', NULL, 42, 2100);
```

---

#### `park_operating_sessions`

Daily operating periods derived from ride activity. **Permanent storage**.

```sql
CREATE TABLE park_operating_sessions (
    session_id INT PRIMARY KEY AUTO_INCREMENT,
    park_id INT NOT NULL,
    session_date DATE NOT NULL COMMENT 'Local date of operating session',
    session_start_utc TIMESTAMP DEFAULT NULL COMMENT 'UTC timestamp of first detected ride activity',
    session_end_utc TIMESTAMP DEFAULT NULL COMMENT 'UTC timestamp of last detected ride activity',
    operating_minutes INT DEFAULT NULL COMMENT 'Total minutes between start and end',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    FOREIGN KEY (park_id) REFERENCES parks(park_id) ON DELETE SET NULL,
    UNIQUE KEY unique_park_session (park_id, session_date),
    INDEX idx_park_date (park_id, session_date DESC),
    INDEX idx_session_date (session_date DESC)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
```

**Timezone Handling**:
- `session_date` is stored as the park's **local calendar date** (e.g., "2025-11-22" in America/Los_Angeles timezone)
- `session_start_utc` and `session_end_utc` are stored in **UTC** for consistency
- Conversion logic: Use `parks.timezone` field to convert UTC timestamps to park local time when detecting operating hours
- Aggregation jobs iterate through distinct timezones, aggregating each timezone group separately to ensure 24-hour windows align with local midnight-to-midnight periods

**Sample Data**:
```sql
INSERT INTO park_operating_sessions (park_id, session_date, session_start_utc, session_end_utc, operating_minutes)
VALUES
(1, '2025-11-22', '2025-11-22 14:00:00', '2025-11-23 02:00:00', 720);  -- 9am EST-2am EST local (session_date is local date '2025-11-22')
```

---

### Permanent Aggregates

#### `ride_daily_stats`

Daily ride performance statistics. **Permanent storage**.

```sql
CREATE TABLE ride_daily_stats (
    stat_id INT PRIMARY KEY AUTO_INCREMENT,
    ride_id INT NOT NULL,
    stat_date DATE NOT NULL COMMENT 'Local date for this statistic',
    uptime_minutes INT NOT NULL DEFAULT 0 COMMENT 'Minutes ride was open during park operating hours',
    downtime_minutes INT NOT NULL DEFAULT 0 COMMENT 'Minutes ride was closed during park operating hours',
    uptime_percentage DECIMAL(5,2) NOT NULL DEFAULT 0.00 COMMENT 'Percentage of operating hours ride was up',
    operating_hours_minutes INT NOT NULL DEFAULT 0 COMMENT 'Total park operating minutes this day',
    avg_wait_time DECIMAL(5,2) DEFAULT NULL,
    min_wait_time INT DEFAULT NULL,
    max_wait_time INT DEFAULT NULL,
    peak_wait_time INT DEFAULT NULL COMMENT 'Single highest wait time observed',
    status_changes INT NOT NULL DEFAULT 0 COMMENT 'Number of open/closed transitions',
    longest_downtime_minutes INT DEFAULT NULL COMMENT 'Longest single downtime period',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (ride_id) REFERENCES rides(ride_id) ON DELETE SET NULL,
    UNIQUE KEY unique_ride_date (ride_id, stat_date),
    INDEX idx_ride_date (ride_id, stat_date DESC),
    INDEX idx_stat_date (stat_date DESC),
    INDEX idx_downtime (downtime_minutes DESC, stat_date DESC) COMMENT 'For downtime rankings'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
```

**Sample Data**:
```sql
INSERT INTO ride_daily_stats (ride_id, stat_date, uptime_minutes, downtime_minutes, uptime_percentage,
                               operating_hours_minutes, avg_wait_time, peak_wait_time, status_changes)
VALUES
(1, '2025-11-22', 660, 60, 91.67, 720, 35.50, 90, 4);
```

---

#### `ride_weekly_stats`, `ride_monthly_stats`, `ride_yearly_stats`

Longer-term aggregations with trend analysis. **Permanent storage**.

```sql
CREATE TABLE ride_weekly_stats (
    stat_id INT PRIMARY KEY AUTO_INCREMENT,
    ride_id INT NOT NULL,
    year INT NOT NULL,
    week_number INT NOT NULL COMMENT 'ISO week number (1-53)',
    week_start_date DATE NOT NULL COMMENT 'Monday of the ISO week',
    uptime_minutes INT NOT NULL DEFAULT 0,
    downtime_minutes INT NOT NULL DEFAULT 0,
    uptime_percentage DECIMAL(5,2) NOT NULL DEFAULT 0.00,
    operating_hours_minutes INT NOT NULL DEFAULT 0,
    avg_wait_time DECIMAL(5,2) DEFAULT NULL,
    peak_wait_time INT DEFAULT NULL,
    status_changes INT NOT NULL DEFAULT 0,
    trend_vs_previous_week DECIMAL(6,2) DEFAULT NULL COMMENT 'Percentage change in downtime vs previous week',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (ride_id) REFERENCES rides(ride_id) ON DELETE SET NULL,
    UNIQUE KEY unique_ride_week (ride_id, year, week_number),
    INDEX idx_ride_week (ride_id, year DESC, week_number DESC),
    INDEX idx_week (year DESC, week_number DESC)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE ride_monthly_stats (
    stat_id INT PRIMARY KEY AUTO_INCREMENT,
    ride_id INT NOT NULL,
    year INT NOT NULL,
    month INT NOT NULL COMMENT 'Month number (1-12)',
    uptime_minutes INT NOT NULL DEFAULT 0,
    downtime_minutes INT NOT NULL DEFAULT 0,
    uptime_percentage DECIMAL(5,2) NOT NULL DEFAULT 0.00,
    operating_hours_minutes INT NOT NULL DEFAULT 0,
    avg_wait_time DECIMAL(5,2) DEFAULT NULL,
    peak_wait_time INT DEFAULT NULL,
    status_changes INT NOT NULL DEFAULT 0,
    trend_vs_previous_month DECIMAL(6,2) DEFAULT NULL COMMENT 'Percentage change in downtime vs previous month',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (ride_id) REFERENCES rides(ride_id) ON DELETE SET NULL,
    UNIQUE KEY unique_ride_month (ride_id, year, month),
    INDEX idx_ride_month (ride_id, year DESC, month DESC),
    INDEX idx_month (year DESC, month DESC)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE ride_yearly_stats (
    stat_id INT PRIMARY KEY AUTO_INCREMENT,
    ride_id INT NOT NULL,
    year INT NOT NULL,
    uptime_minutes INT NOT NULL DEFAULT 0,
    downtime_minutes INT NOT NULL DEFAULT 0,
    uptime_percentage DECIMAL(5,2) NOT NULL DEFAULT 0.00,
    operating_hours_minutes INT NOT NULL DEFAULT 0,
    avg_wait_time DECIMAL(5,2) DEFAULT NULL,
    peak_wait_time INT DEFAULT NULL,
    status_changes INT NOT NULL DEFAULT 0,
    trend_vs_previous_year DECIMAL(6,2) DEFAULT NULL COMMENT 'Percentage change in downtime vs previous year',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (ride_id) REFERENCES rides(ride_id) ON DELETE SET NULL,
    UNIQUE KEY unique_ride_year (ride_id, year),
    INDEX idx_ride_year (ride_id, year DESC),
    INDEX idx_year (year DESC)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
```

---

#### `park_daily_stats`

Daily park-wide performance statistics. **Permanent storage**.

```sql
CREATE TABLE park_daily_stats (
    stat_id INT PRIMARY KEY AUTO_INCREMENT,
    park_id INT NOT NULL,
    stat_date DATE NOT NULL COMMENT 'Local date for this statistic',
    total_rides_tracked INT NOT NULL DEFAULT 0,
    avg_uptime_percentage DECIMAL(5,2) DEFAULT NULL COMMENT 'Average uptime % across all rides',
    total_downtime_hours DECIMAL(8,2) NOT NULL DEFAULT 0.00 COMMENT 'Sum of all ride downtime in hours',
    rides_with_downtime INT NOT NULL DEFAULT 0 COMMENT 'Count of rides that had any downtime',
    avg_wait_time DECIMAL(5,2) DEFAULT NULL,
    peak_wait_time INT DEFAULT NULL,
    operating_hours_minutes INT NOT NULL DEFAULT 0,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (park_id) REFERENCES parks(park_id) ON DELETE SET NULL,
    UNIQUE KEY unique_park_date (park_id, stat_date),
    INDEX idx_park_date (park_id, stat_date DESC),
    INDEX idx_stat_date (stat_date DESC),
    INDEX idx_downtime (total_downtime_hours DESC, stat_date DESC)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
```

---

#### `park_weekly_stats`, `park_monthly_stats`, `park_yearly_stats`

Longer-term park aggregations with trend analysis. **Permanent storage**.

```sql
CREATE TABLE park_weekly_stats (
    stat_id INT PRIMARY KEY AUTO_INCREMENT,
    park_id INT NOT NULL,
    year INT NOT NULL,
    week_number INT NOT NULL,
    week_start_date DATE NOT NULL,
    total_rides_tracked INT NOT NULL DEFAULT 0,
    avg_uptime_percentage DECIMAL(5,2) DEFAULT NULL,
    total_downtime_hours DECIMAL(8,2) NOT NULL DEFAULT 0.00,
    rides_with_downtime INT NOT NULL DEFAULT 0,
    avg_wait_time DECIMAL(5,2) DEFAULT NULL,
    peak_wait_time INT DEFAULT NULL,
    trend_vs_previous_week DECIMAL(6,2) DEFAULT NULL COMMENT 'Percentage change in downtime vs previous week',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (park_id) REFERENCES parks(park_id) ON DELETE SET NULL,
    UNIQUE KEY unique_park_week (park_id, year, week_number),
    INDEX idx_park_week (park_id, year DESC, week_number DESC),
    INDEX idx_week (year DESC, week_number DESC)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE park_monthly_stats (
    stat_id INT PRIMARY KEY AUTO_INCREMENT,
    park_id INT NOT NULL,
    year INT NOT NULL,
    month INT NOT NULL,
    total_rides_tracked INT NOT NULL DEFAULT 0,
    avg_uptime_percentage DECIMAL(5,2) DEFAULT NULL,
    total_downtime_hours DECIMAL(8,2) NOT NULL DEFAULT 0.00,
    rides_with_downtime INT NOT NULL DEFAULT 0,
    avg_wait_time DECIMAL(5,2) DEFAULT NULL,
    peak_wait_time INT DEFAULT NULL,
    trend_vs_previous_month DECIMAL(6,2) DEFAULT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (park_id) REFERENCES parks(park_id) ON DELETE SET NULL,
    UNIQUE KEY unique_park_month (park_id, year, month),
    INDEX idx_park_month (park_id, year DESC, month DESC),
    INDEX idx_month (year DESC, month DESC)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE park_yearly_stats (
    stat_id INT PRIMARY KEY AUTO_INCREMENT,
    park_id INT NOT NULL,
    year INT NOT NULL,
    total_rides_tracked INT NOT NULL DEFAULT 0,
    avg_uptime_percentage DECIMAL(5,2) DEFAULT NULL,
    total_downtime_hours DECIMAL(8,2) NOT NULL DEFAULT 0.00,
    rides_with_downtime INT NOT NULL DEFAULT 0,
    avg_wait_time DECIMAL(5,2) DEFAULT NULL,
    peak_wait_time INT DEFAULT NULL,
    trend_vs_previous_year DECIMAL(6,2) DEFAULT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (park_id) REFERENCES parks(park_id) ON DELETE SET NULL,
    UNIQUE KEY unique_park_year (park_id, year),
    INDEX idx_park_year (park_id, year DESC),
    INDEX idx_year (year DESC)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
```

---

## Data Retention Policies

### Automated Cleanup Job

Runs hourly via cron job or scheduled event:

```sql
-- SAFE CLEANUP: Only delete raw data that has been successfully aggregated
-- This prevents data loss if aggregation jobs fail

DELETE FROM ride_status_snapshots
WHERE recorded_at < (
    SELECT MAX(aggregated_until_ts)
    FROM aggregation_log
    WHERE aggregation_type = 'daily'
      AND status = 'success'
);

DELETE FROM ride_status_changes
WHERE changed_at < (
    SELECT MAX(aggregated_until_ts)
    FROM aggregation_log
    WHERE aggregation_type = 'daily'
      AND status = 'success'
);

DELETE FROM park_activity_snapshots
WHERE recorded_at < (
    SELECT MAX(aggregated_until_ts)
    FROM aggregation_log
    WHERE aggregation_type = 'daily'
      AND status = 'success'
);
```

**Safety Guarantee**: Raw data is only deleted **after** successful aggregation has been logged. If aggregation fails, raw data is preserved for retry attempts.

### Retention Summary

| Table Type | Retention Period | Cleanup Method |
|------------|-----------------|----------------|
| `*_snapshots` | 24 hours | Automated DELETE |
| `*_changes` | 24 hours | Automated DELETE |
| `*_daily_stats` | Permanent | Manual archive only |
| `*_weekly_stats` | Permanent | Manual archive only |
| `*_monthly_stats` | Permanent | Manual archive only |
| `*_yearly_stats` | Permanent | Manual archive only |
| `park_operating_sessions` | Permanent | Manual archive only |

---

## Sample Queries

### Query 1: Get Park Rankings by Downtime (FR-010)

**Use Case**: Display parks ranked by total downtime for user-selected period.

```sql
-- Today's rankings
SELECT
    p.park_id,
    p.name AS park_name,
    CONCAT(p.city, ', ', p.state_province) AS location,
    pds.total_downtime_hours,
    pds.rides_with_downtime AS affected_rides,
    pds.avg_uptime_percentage,
    prev.total_downtime_hours AS prev_day_downtime,
    ROUND(
        ((pds.total_downtime_hours - IFNULL(prev.total_downtime_hours, 0)) /
         NULLIF(prev.total_downtime_hours, 0)) * 100,
        2
    ) AS trend_percentage
FROM parks p
INNER JOIN park_daily_stats pds ON p.park_id = pds.park_id
LEFT JOIN park_daily_stats prev ON p.park_id = prev.park_id
    AND prev.stat_date = DATE_SUB(pds.stat_date, INTERVAL 1 DAY)
WHERE pds.stat_date = CURDATE()
    AND p.is_active = TRUE
ORDER BY pds.total_downtime_hours DESC
LIMIT 50;
```

```sql
-- 7-day rankings (weekly stats)
SELECT
    p.park_id,
    p.name AS park_name,
    CONCAT(p.city, ', ', p.state_province) AS location,
    pws.total_downtime_hours,
    pws.rides_with_downtime AS affected_rides,
    pws.avg_uptime_percentage,
    pws.trend_vs_previous_week AS trend_percentage
FROM parks p
INNER JOIN park_weekly_stats pws ON p.park_id = pws.park_id
WHERE pws.year = YEAR(CURDATE())
    AND pws.week_number = WEEK(CURDATE(), 3)  -- ISO week
    AND p.is_active = TRUE
ORDER BY pws.total_downtime_hours DESC
LIMIT 50;
```

**Performance**: Uses index `idx_downtime` on `park_daily_stats` for fast sorting. Expected: <50ms.

---

### Query 1b: Get Park Rankings by Weighted Downtime Score (FR-024)

**Use Case**: Display parks ranked by weighted downtime score accounting for ride tier importance.

```sql
-- Weighted downtime rankings for 7-day period
WITH park_weights AS (
    -- Calculate total weight per park (sum of all ride weights)
    SELECT
        p.park_id,
        SUM(IFNULL(rc.tier_weight, 2)) AS total_park_weight,  -- Default to Tier 2 (weight 2) if not classified
        COUNT(r.ride_id) AS total_rides,
        SUM(CASE WHEN r.tier = 1 THEN 1 ELSE 0 END) AS tier1_count,
        SUM(CASE WHEN r.tier = 2 THEN 1 ELSE 0 END) AS tier2_count,
        SUM(CASE WHEN r.tier = 3 THEN 1 ELSE 0 END) AS tier3_count
    FROM parks p
    INNER JOIN rides r ON p.park_id = r.park_id AND r.is_active = TRUE
    LEFT JOIN ride_classifications rc ON r.ride_id = rc.ride_id
    WHERE p.is_active = TRUE
    GROUP BY p.park_id
),
weighted_downtime AS (
    -- Calculate weighted downtime per park
    SELECT
        p.park_id,
        SUM(rws.downtime_minutes / 60.0 * IFNULL(rc.tier_weight, 2)) AS total_weighted_downtime_hours
    FROM parks p
    INNER JOIN rides r ON p.park_id = r.park_id AND r.is_active = TRUE
    LEFT JOIN ride_classifications rc ON r.ride_id = rc.ride_id
    INNER JOIN ride_weekly_stats rws ON r.ride_id = rws.ride_id
    WHERE rws.year = YEAR(CURDATE())
        AND rws.week_number = WEEK(CURDATE(), 3)
        AND p.is_active = TRUE
    GROUP BY p.park_id
)
SELECT
    p.park_id,
    p.name AS park_name,
    CONCAT(p.city, ', ', p.state_province) AS location,
    pw.total_park_weight,
    pw.tier1_count,
    pw.tier2_count,
    pw.tier3_count,
    wd.total_weighted_downtime_hours,
    ROUND(wd.total_weighted_downtime_hours / pw.total_park_weight, 4) AS weighted_downtime_score,
    ROUND((wd.total_weighted_downtime_hours / pw.total_park_weight) * 100, 2) AS score_percentage
FROM parks p
INNER JOIN park_weights pw ON p.park_id = pw.park_id
INNER JOIN weighted_downtime wd ON p.park_id = wd.park_id
WHERE p.is_active = TRUE
ORDER BY weighted_downtime_score DESC
LIMIT 50;
```

**Example Result**:
```
park_id | park_name       | location      | total_park_weight | tier1 | tier2 | tier3 | weighted_downtime_hours | weighted_score | score_percentage
--------|-----------------|---------------|-------------------|-------|-------|-------|-------------------------|----------------|------------------
1       | Magic Kingdom   | Orlando, FL   | 81                | 11    | 18    | 12    | 62.0                    | 0.7654         | 76.54%
2       | Cedar Point     | Sandusky, OH  | 95                | 15    | 22    | 8     | 48.5                    | 0.5105         | 51.05%
```

**Interpretation**: Magic Kingdom had 76.54% of its weighted operational capacity offline during the week - worse than Cedar Point at 51.05% despite Cedar Point having more total downtime hours.

**Performance**: Requires joins to rides, ride_classifications, and ride_weekly_stats. Expected: <100ms with proper indexes.

---

### Query 2: Get Ride Performance Rankings (FR-014)

**Use Case**: Display individual rides ranked by downtime.

```sql
-- 7-day ride downtime rankings
SELECT
    r.ride_id,
    r.name AS ride_name,
    p.name AS park_name,
    rws.downtime_minutes / 60.0 AS downtime_hours,
    rws.uptime_percentage,
    -- Current status from most recent snapshot (denormalized or separate query)
    (SELECT computed_is_open
     FROM ride_status_snapshots
     WHERE ride_id = r.ride_id
     ORDER BY recorded_at DESC
     LIMIT 1) AS current_status,
    rws.avg_wait_time AS seven_day_avg_wait,
    rws.trend_vs_previous_week AS trend_percentage
FROM rides r
INNER JOIN parks p ON r.park_id = p.park_id
INNER JOIN ride_weekly_stats rws ON r.ride_id = rws.ride_id
WHERE rws.year = YEAR(CURDATE())
    AND rws.week_number = WEEK(CURDATE(), 3)
    AND r.is_active = TRUE
    AND p.is_active = TRUE
ORDER BY rws.downtime_minutes DESC
LIMIT 100;
```

**Performance**: Uses composite index `idx_ride_week` + `idx_downtime`. Expected: <100ms.

---

### Query 3: Get Current Wait Times (FR-017)

**Use Case**: Display rides sorted by current wait time.

```sql
-- Current wait times with 7-day averages
SELECT
    r.ride_id,
    r.name AS ride_name,
    p.name AS park_name,
    rss.wait_time AS current_wait,
    rws.avg_wait_time AS seven_day_avg,
    rws.peak_wait_time,
    rss.computed_is_open AS is_currently_open,
    ROUND(
        ((rss.wait_time - rws.avg_wait_time) / NULLIF(rws.avg_wait_time, 0)) * 100,
        2
    ) AS trend_percentage
FROM rides r
INNER JOIN parks p ON r.park_id = p.park_id
INNER JOIN ride_status_snapshots rss ON r.ride_id = rss.ride_id
INNER JOIN ride_weekly_stats rws ON r.ride_id = rws.ride_id
WHERE rss.snapshot_id IN (
    -- Get most recent snapshot per ride (subquery optimized with index)
    SELECT MAX(snapshot_id)
    FROM ride_status_snapshots
    WHERE recorded_at >= NOW() - INTERVAL 30 MINUTE
    GROUP BY ride_id
)
AND rws.year = YEAR(CURDATE())
AND rws.week_number = WEEK(CURDATE(), 3)
AND r.is_active = TRUE
AND p.is_active = TRUE
AND rss.computed_is_open = TRUE  -- Only show operating rides
ORDER BY rss.wait_time DESC
LIMIT 100;
```

**Performance**: Uses `idx_ride_recorded` on snapshots + `idx_computed_status`. Expected: <50ms.

---

### Query 4: Get Aggregate Statistics (FR-013)

**Use Case**: Display top-of-page statistics (total parks tracked, peak downtime, currently down rides).

```sql
-- Today's aggregate statistics
SELECT
    COUNT(DISTINCT p.park_id) AS total_parks_tracked,
    ROUND(MAX(pds.total_downtime_hours), 2) AS peak_downtime_hours,
    SUM(pds.rides_with_downtime) AS total_rides_with_downtime,
    -- Currently down count from live snapshots
    (SELECT COUNT(DISTINCT ride_id)
     FROM ride_status_snapshots
     WHERE recorded_at >= NOW() - INTERVAL 15 MINUTE
         AND computed_is_open = FALSE
    ) AS currently_down_rides
FROM parks p
INNER JOIN park_daily_stats pds ON p.park_id = pds.park_id
WHERE pds.stat_date = CURDATE()
    AND p.is_active = TRUE;
```

**Performance**: Single aggregation query with subquery. Expected: <50ms.

---

### Query 5: Filter by Disney & Universal (FR-020)

**Use Case**: Apply park operator filter to any query.

```sql
-- Add WHERE clause to any park query:
WHERE (p.is_disney = TRUE OR p.is_universal = TRUE)
    AND p.is_active = TRUE
```

**Performance**: Uses index `idx_disney_universal`. No performance degradation.

---

### Query 6: Get Park Operating Hours

**Use Case**: Calculate uptime percentage during actual operating hours (FR-009).

```sql
-- Get operating session for a specific date
SELECT
    park_id,
    session_date,
    session_start_utc,
    session_end_utc,
    operating_minutes,
    TIME_FORMAT(SEC_TO_TIME(operating_minutes * 60), '%H:%i') AS operating_hours_formatted
FROM park_operating_sessions
WHERE park_id = 1
    AND session_date = '2025-11-22';
```

---

### Query 7: Detect Status Changes

**Use Case**: Generate `ride_status_changes` records during data collection.

```sql
-- Pseudocode for data collector logic
-- Compare current snapshot to previous snapshot:

SELECT
    curr.ride_id,
    curr.computed_is_open AS current_status,
    prev.computed_is_open AS previous_status,
    TIMESTAMPDIFF(MINUTE, prev.recorded_at, curr.recorded_at) AS duration_minutes
FROM ride_status_snapshots curr
INNER JOIN ride_status_snapshots prev ON curr.ride_id = prev.ride_id
WHERE curr.snapshot_id = (SELECT MAX(snapshot_id) FROM ride_status_snapshots WHERE ride_id = curr.ride_id)
    AND prev.snapshot_id = (SELECT MAX(snapshot_id) FROM ride_status_snapshots
                             WHERE ride_id = curr.ride_id
                             AND snapshot_id < curr.snapshot_id)
    AND curr.computed_is_open != prev.computed_is_open;

-- Insert change record if status differs
INSERT INTO ride_status_changes (ride_id, changed_at, previous_status, new_status, duration_in_previous_status, wait_time_at_change)
VALUES (?, NOW(), ?, ?, ?, ?);
```

---

### Query 8: Get Park Trends (Most Improved)

**Use Case**: Display parks with improving uptime percentages (FR-046, FR-047, FR-048, FR-049).

```sql
-- Parks showing reliability improvements (7-day example)
-- Current week vs previous week comparison
WITH current_period AS (
    SELECT
        p.park_id,
        p.name AS park_name,
        CONCAT(p.city, ', ', p.state_province) AS location,
        pws.uptime_percentage AS current_uptime_pct,
        pws.year,
        pws.week_number
    FROM parks p
    INNER JOIN park_weekly_stats pws ON p.park_id = pws.park_id
    WHERE pws.year = YEAR(CURDATE())
        AND pws.week_number = WEEK(CURDATE(), 3)
        AND p.is_active = TRUE
        -- Optional: Apply park filter
        -- AND (p.is_disney = TRUE OR p.is_universal = TRUE)
),
previous_period AS (
    SELECT
        p.park_id,
        pws.uptime_percentage AS previous_uptime_pct,
        pws.total_downtime_hours AS previous_downtime_hours
    FROM parks p
    INNER JOIN park_weekly_stats pws ON p.park_id = pws.park_id
    WHERE pws.year = YEAR(CURDATE())
        AND pws.week_number = WEEK(CURDATE(), 3) - 1
        AND p.is_active = TRUE
)
SELECT
    c.park_name,
    c.location,
    p.previous_uptime_pct AS previous_period_uptime,
    c.current_uptime_pct AS current_period_uptime,
    (c.current_uptime_pct - p.previous_uptime_pct) AS uptime_change_pct,
    p.previous_downtime_hours AS previous_period_downtime_hours,
    CONCAT('+', ROUND(c.current_uptime_pct - p.previous_uptime_pct, 1), '%') AS improvement
FROM current_period c
INNER JOIN previous_period p ON c.park_id = p.park_id
WHERE (c.current_uptime_pct - p.previous_uptime_pct) >= 5.0  -- FR-048: Only show ≥5% improvement
ORDER BY (c.current_uptime_pct - p.previous_uptime_pct) DESC  -- FR-049: Rank by improvement
LIMIT 50;
```

**For Today (daily comparison)**:
```sql
-- Replace park_weekly_stats with park_daily_stats
-- WHERE stat_date = CURDATE() (current)
-- WHERE stat_date = DATE_SUB(CURDATE(), INTERVAL 1 DAY) (previous)
```

**For 30 Days (monthly comparison)**:
```sql
-- Replace park_weekly_stats with park_monthly_stats
-- WHERE year = YEAR(CURDATE()) AND month = MONTH(CURDATE()) (current)
-- WHERE year = YEAR(DATE_SUB(CURDATE(), INTERVAL 1 MONTH)) AND month = MONTH(DATE_SUB(CURDATE(), INTERVAL 1 MONTH)) (previous)
```

**Performance**: Uses indexes `idx_park_week`, `idx_park_date`, `idx_park_month`. Expected: <100ms.

---

### Query 9: Get Park Trends (Declining Performance)

**Use Case**: Display parks with declining uptime percentages (FR-046, FR-047, FR-048, FR-050).

```sql
-- Parks showing reliability decline (7-day example)
-- Same structure as Query 8, but filter for decline
WITH current_period AS (
    SELECT
        p.park_id,
        p.name AS park_name,
        CONCAT(p.city, ', ', p.state_province) AS location,
        pws.uptime_percentage AS current_uptime_pct,
        pws.total_downtime_hours AS current_downtime_hours
    FROM parks p
    INNER JOIN park_weekly_stats pws ON p.park_id = pws.park_id
    WHERE pws.year = YEAR(CURDATE())
        AND pws.week_number = WEEK(CURDATE(), 3)
        AND p.is_active = TRUE
),
previous_period AS (
    SELECT
        p.park_id,
        pws.uptime_percentage AS previous_uptime_pct,
        pws.total_downtime_hours AS previous_downtime_hours
    FROM parks p
    INNER JOIN park_weekly_stats pws ON p.park_id = pws.park_id
    WHERE pws.year = YEAR(CURDATE())
        AND pws.week_number = WEEK(CURDATE(), 3) - 1
        AND p.is_active = TRUE
)
SELECT
    c.park_name,
    c.location,
    p.previous_uptime_pct AS previous_period_uptime,
    c.current_uptime_pct AS current_period_uptime,
    (p.previous_uptime_pct - c.current_uptime_pct) AS uptime_decline_pct,
    p.previous_downtime_hours AS previous_period_downtime_hours,
    c.current_downtime_hours AS current_period_downtime_hours,
    CONCAT('↓ ', ROUND(p.previous_uptime_pct - c.current_uptime_pct, 1), '%') AS decline
FROM current_period c
INNER JOIN previous_period p ON c.park_id = p.park_id
WHERE (p.previous_uptime_pct - c.current_uptime_pct) >= 5.0  -- FR-048: Only show ≥5% decline
ORDER BY (p.previous_uptime_pct - c.current_uptime_pct) DESC  -- FR-050: Rank by decline magnitude
LIMIT 50;
```

**Performance**: Uses indexes `idx_park_week`, `idx_park_date`, `idx_park_month`. Expected: <100ms.

---

### Query 10: Get Ride Trends (Most Improved)

**Use Case**: Display individual rides with improving uptime percentages (FR-046, FR-047, FR-048, FR-049).

```sql
-- Rides showing reliability improvements (7-day example)
WITH current_period AS (
    SELECT
        r.ride_id,
        r.name AS ride_name,
        p.name AS park_name,
        rws.uptime_percentage AS current_uptime_pct,
        rws.downtime_minutes / 60.0 AS current_downtime_hours
    FROM rides r
    INNER JOIN parks p ON r.park_id = p.park_id
    INNER JOIN ride_weekly_stats rws ON r.ride_id = rws.ride_id
    WHERE rws.year = YEAR(CURDATE())
        AND rws.week_number = WEEK(CURDATE(), 3)
        AND r.is_active = TRUE
        AND p.is_active = TRUE
        -- Optional: Apply park filter
        -- AND (p.is_disney = TRUE OR p.is_universal = TRUE)
),
previous_period AS (
    SELECT
        r.ride_id,
        rws.uptime_percentage AS previous_uptime_pct,
        rws.downtime_minutes / 60.0 AS previous_downtime_hours
    FROM rides r
    INNER JOIN ride_weekly_stats rws ON r.ride_id = rws.ride_id
    WHERE rws.year = YEAR(CURDATE())
        AND rws.week_number = WEEK(CURDATE(), 3) - 1
        AND r.is_active = TRUE
)
SELECT
    c.ride_name,
    c.park_name,
    p.previous_uptime_pct AS previous_period_uptime,
    c.current_uptime_pct AS current_period_uptime,
    (c.current_uptime_pct - p.previous_uptime_pct) AS uptime_change_pct,
    p.previous_downtime_hours AS previous_period_downtime_hours,
    c.current_downtime_hours AS current_period_downtime_hours,
    CONCAT('↑ ', ROUND(c.current_uptime_pct - p.previous_uptime_pct, 1), '%') AS improvement
FROM current_period c
INNER JOIN previous_period p ON c.ride_id = p.ride_id
WHERE (c.current_uptime_pct - p.previous_uptime_pct) >= 5.0  -- FR-048: Only show ≥5% improvement
ORDER BY (c.current_uptime_pct - p.previous_uptime_pct) DESC  -- FR-049: Rank by improvement
LIMIT 50;
```

**Performance**: Uses indexes `idx_ride_week`, `idx_ride_date`, `idx_ride_month`. Expected: <100ms.

---

### Query 11: Get Ride Trends (Declining Performance)

**Use Case**: Display individual rides with declining uptime percentages (FR-046, FR-047, FR-048, FR-050).

```sql
-- Rides showing reliability decline (7-day example)
WITH current_period AS (
    SELECT
        r.ride_id,
        r.name AS ride_name,
        p.name AS park_name,
        rws.uptime_percentage AS current_uptime_pct,
        rws.downtime_minutes / 60.0 AS current_downtime_hours
    FROM rides r
    INNER JOIN parks p ON r.park_id = p.park_id
    INNER JOIN ride_weekly_stats rws ON r.ride_id = rws.ride_id
    WHERE rws.year = YEAR(CURDATE())
        AND rws.week_number = WEEK(CURDATE(), 3)
        AND r.is_active = TRUE
        AND p.is_active = TRUE
),
previous_period AS (
    SELECT
        r.ride_id,
        rws.uptime_percentage AS previous_uptime_pct,
        rws.downtime_minutes / 60.0 AS previous_downtime_hours
    FROM rides r
    INNER JOIN ride_weekly_stats rws ON r.ride_id = rws.ride_id
    WHERE rws.year = YEAR(CURDATE())
        AND rws.week_number = WEEK(CURDATE(), 3) - 1
        AND r.is_active = TRUE
)
SELECT
    c.ride_name,
    c.park_name,
    p.previous_uptime_pct AS previous_period_uptime,
    c.current_uptime_pct AS current_period_uptime,
    (p.previous_uptime_pct - c.current_uptime_pct) AS uptime_decline_pct,
    p.previous_downtime_hours AS previous_period_downtime_hours,
    c.current_downtime_hours AS current_period_downtime_hours,
    CONCAT('↓ ', ROUND(p.previous_uptime_pct - c.current_uptime_pct, 1), '%') AS decline
FROM current_period c
INNER JOIN previous_period p ON c.ride_id = p.ride_id
WHERE (p.previous_uptime_pct - c.current_uptime_pct) >= 5.0  -- FR-048: Only show ≥5% decline
ORDER BY (p.previous_uptime_pct - c.current_uptime_pct) DESC  -- FR-050: Rank by decline magnitude
LIMIT 50;
```

**Performance**: Uses indexes `idx_ride_week`, `idx_ride_date`, `idx_ride_month`. Expected: <100ms.

---

## Performance Optimization

### Index Strategy

1. **Time-Series Indexes**: All timestamp columns indexed DESC for recent-first queries
2. **Composite Indexes**: `(entity_id, date_column DESC)` for filtered time-series
3. **Foreign Key Indexes**: Automatically created on all foreign key columns
4. **Filter Indexes**: Dedicated indexes on `is_disney`, `is_universal`, `is_active`

### Query Optimization Checklist

- [ ] Use `LIMIT` on all user-facing queries
- [ ] Avoid `SELECT *`; specify needed columns
- [ ] Use `EXPLAIN` to verify index usage
- [ ] Denormalize frequently joined data (e.g., park operator flags)
- [ ] Use covering indexes where possible
- [ ] Partition large tables by date if growth exceeds expectations

### Expected Performance

| Query Type | Target | Index Dependencies |
|------------|--------|-------------------|
| Current status | <50ms | `idx_ride_recorded`, `idx_recorded_at` |
| Daily aggregates | <50ms | `idx_park_date`, `idx_stat_date` |
| Weekly aggregates | <100ms | `idx_park_week`, `idx_ride_week` |
| Wait time rankings | <50ms | `idx_computed_status`, `idx_ride_recorded` |
| Park rankings | <50ms | `idx_downtime` |

---

## Data Migration Notes

### Initial Schema Setup

```bash
# Create database
mysql -u root -p -e "CREATE DATABASE theme_park_tracker CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"

# Run schema
mysql -u root -p theme_park_tracker < schema.sql

# Verify tables
mysql -u root -p theme_park_tracker -e "SHOW TABLES;"
```

### Backfill Historical Data

If backfilling data after system has been running:

```sql
-- Regenerate daily stats from snapshots (before cleanup)
-- This would be part of the aggregation job
INSERT INTO ride_daily_stats (ride_id, stat_date, uptime_minutes, downtime_minutes, ...)
SELECT
    ride_id,
    DATE(recorded_at) AS stat_date,
    SUM(CASE WHEN computed_is_open = TRUE THEN 10 ELSE 0 END) AS uptime_minutes,
    SUM(CASE WHEN computed_is_open = FALSE THEN 10 ELSE 0 END) AS downtime_minutes,
    ...
FROM ride_status_snapshots
WHERE DATE(recorded_at) = '2025-11-22'
GROUP BY ride_id, DATE(recorded_at);
```

---

## Appendix: Computed Status Logic

### `computed_is_open` Calculation

```sql
-- Applied during data collection insert/update
computed_is_open = (wait_time > 0) OR (is_open = TRUE AND wait_time = 0)
```

**Rationale**: Queue-Times.com API sometimes reports `is_open = false` while `wait_time > 0`. Per API documentation, rides with wait times should be treated as open.

**Examples**:
- `wait_time = 45, is_open = true` → `computed_is_open = TRUE` ✓
- `wait_time = 45, is_open = false` → `computed_is_open = TRUE` ✓ (quirk handling)
- `wait_time = 0, is_open = true` → `computed_is_open = TRUE` ✓ (open but no wait)
- `wait_time = 0, is_open = false` → `computed_is_open = FALSE` ✓ (clearly closed)

---

## Appendix: Time Zone Handling

All timestamps stored in UTC. Application layer converts to park local time for:
- Determining `session_date` (local calendar date)
- Displaying user-friendly times
- Calculating "during operating hours" windows

Example conversion:
```python
from datetime import datetime
import pytz

# Store in database as UTC
utc_now = datetime.utcnow()

# Convert to park timezone for display
park_tz = pytz.timezone('America/Los_Angeles')
local_time = utc_now.replace(tzinfo=pytz.utc).astimezone(park_tz)
```

---

## Appendix: Storage Estimates

### 24-Hour Raw Data

Assumptions:
- 100 parks × 50 rides/park = 5,000 rides
- 6 snapshots/hour × 24 hours = 144 snapshots/ride/day
- 5,000 rides × 144 snapshots = 720,000 rows/day

**Storage per snapshot row**: ~100 bytes
**Total raw snapshot storage**: 720,000 × 100 bytes = ~70 MB/day (rolling)

### Permanent Aggregates

**Daily stats**: 5,000 rides × 365 days × 200 bytes = ~365 MB/year
**Weekly stats**: 5,000 rides × 52 weeks × 220 bytes = ~56 MB/year
**Monthly stats**: 5,000 rides × 12 months × 220 bytes = ~13 MB/year
**Yearly stats**: 5,000 rides × 1 year × 220 bytes = ~1 MB/year

**Total permanent storage (first year)**: ~435 MB
**Meets requirement**: SC-009 (under 500 MB) ✓

---

## Database Configuration Recommendations

```ini
# /etc/mysql/my.cnf or /etc/my.cnf

[mysqld]
# Character set
character-set-server = utf8mb4
collation-server = utf8mb4_unicode_ci

# InnoDB settings for time-series workload
innodb_buffer_pool_size = 2G  # Adjust based on available RAM
innodb_log_file_size = 256M
innodb_flush_log_at_trx_commit = 2  # Better performance for non-critical writes
innodb_flush_method = O_DIRECT

# Query cache (disabled in MySQL 8.0+)
# query_cache_type = 0

# Connection limits
max_connections = 200
wait_timeout = 600

# Timezone
default_time_zone = '+00:00'  # Force UTC
```

---

**End of Data Model Document**
