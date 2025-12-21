-- Theme Park Downtime Tracker - Raw Data Tables Migration
-- Migration: 002_raw_data_tables.sql
-- Purpose: Create raw data tables with 24-hour retention
-- Date: 2025-11-23

-- Raw Data: Ride Status Snapshots (24-hour retention)
CREATE TABLE IF NOT EXISTS ride_status_snapshots (
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

-- Raw Data: Ride Status Changes (24-hour retention)
CREATE TABLE IF NOT EXISTS ride_status_changes (
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

-- Raw Data: Park Activity Snapshots (24-hour retention)
CREATE TABLE IF NOT EXISTS park_activity_snapshots (
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
