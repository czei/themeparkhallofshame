-- Migration: Add hourly aggregation tables
-- Date: 2025-12-05
-- Feature: 001-aggregation-tables
-- Purpose: Pre-computed hourly aggregates to replace slow GROUP BY HOUR queries

START TRANSACTION;

-- ============================================================================
-- Park Hourly Stats: Aggregated park performance metrics by hour
-- ============================================================================
CREATE TABLE IF NOT EXISTS park_hourly_stats (
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
    FOREIGN KEY (park_id) REFERENCES parks(park_id) ON DELETE CASCADE
) ENGINE=InnoDB COMMENT='Hourly park performance aggregates (3-year retention)';

-- ============================================================================
-- Ride Hourly Stats: Aggregated ride downtime metrics by hour
-- ============================================================================
CREATE TABLE IF NOT EXISTS ride_hourly_stats (
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
    FOREIGN KEY (ride_id) REFERENCES rides(ride_id) ON DELETE CASCADE,
    FOREIGN KEY (park_id) REFERENCES parks(park_id) ON DELETE CASCADE
) ENGINE=InnoDB COMMENT='Hourly ride downtime aggregates (3-year retention)';

COMMIT;

-- Note: To verify indexes after migration, run manually:
--   SHOW INDEX FROM park_hourly_stats;
--   SHOW INDEX FROM ride_hourly_stats;

-- ============================================================================
-- Rollback Plan
-- ============================================================================
-- To rollback this migration:
--   DROP TABLE IF EXISTS ride_hourly_stats;
--   DROP TABLE IF EXISTS park_hourly_stats;
