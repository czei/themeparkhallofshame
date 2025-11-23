-- Theme Park Downtime Tracker - Aggregate Tables Migration
-- Migration: 003_aggregates_tables.sql
-- Purpose: Create permanent aggregate and session tracking tables
-- Date: 2025-11-23

-- Aggregation tracking table (permanent)
CREATE TABLE IF NOT EXISTS aggregation_log (
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

-- Park operating sessions (permanent)
CREATE TABLE IF NOT EXISTS park_operating_sessions (
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

-- Ride daily statistics (permanent)
CREATE TABLE IF NOT EXISTS ride_daily_stats (
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

-- Ride weekly statistics (permanent)
CREATE TABLE IF NOT EXISTS ride_weekly_stats (
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

-- Ride monthly statistics (permanent)
CREATE TABLE IF NOT EXISTS ride_monthly_stats (
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

-- Ride yearly statistics (permanent)
CREATE TABLE IF NOT EXISTS ride_yearly_stats (
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

-- Park daily statistics (permanent)
CREATE TABLE IF NOT EXISTS park_daily_stats (
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

-- Park weekly statistics (permanent)
CREATE TABLE IF NOT EXISTS park_weekly_stats (
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

-- Park monthly statistics (permanent)
CREATE TABLE IF NOT EXISTS park_monthly_stats (
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

-- Park yearly statistics (permanent)
CREATE TABLE IF NOT EXISTS park_yearly_stats (
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
