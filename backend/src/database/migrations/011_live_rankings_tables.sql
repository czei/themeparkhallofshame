-- Theme Park Downtime Tracker - Live Rankings Pre-Aggregation Tables
-- Migration: 011_live_rankings_tables.sql
-- Purpose: Pre-aggregated rankings for instant API responses
-- Date: 2025-12-01
--
-- These tables are populated every 10 minutes by the aggregation job.
-- API reads from these tables instead of running expensive CTE queries.
-- Uses atomic table swap (staging + RENAME) for zero-downtime updates.

-- ============================================
-- PARK LIVE RANKINGS TABLE
-- ============================================
-- Pre-computed park rankings for period=live and period=today
-- Updated every 10 minutes after snapshot collection

CREATE TABLE IF NOT EXISTS park_live_rankings (
    park_id INT PRIMARY KEY,
    queue_times_id INT NULL,
    park_name VARCHAR(255) NOT NULL,
    location VARCHAR(255) NULL,
    timezone VARCHAR(50) NULL,

    -- Park classification (for filtering)
    is_disney BOOLEAN DEFAULT FALSE,
    is_universal BOOLEAN DEFAULT FALSE,

    -- Live metrics (current snapshot - rides down RIGHT NOW)
    rides_down INT DEFAULT 0,
    total_rides INT DEFAULT 0,
    shame_score DECIMAL(4,1) DEFAULT 0,
    park_is_open BOOLEAN DEFAULT FALSE,

    -- Today cumulative metrics (midnight Pacific to now)
    total_downtime_hours DECIMAL(6,2) DEFAULT 0,
    weighted_downtime_hours DECIMAL(6,2) DEFAULT 0,

    -- For shame score calculation
    total_park_weight DECIMAL(8,2) DEFAULT 0,

    -- Metadata
    calculated_at DATETIME NOT NULL,

    -- Indexes for common query patterns
    INDEX idx_shame_score (shame_score DESC),
    INDEX idx_rides_down (rides_down DESC),
    INDEX idx_disney_universal (is_disney, is_universal),
    INDEX idx_calculated_at (calculated_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================
-- RIDE LIVE RANKINGS TABLE
-- ============================================
-- Pre-computed ride rankings for period=live and period=today
-- Updated every 10 minutes after snapshot collection

CREATE TABLE IF NOT EXISTS ride_live_rankings (
    ride_id INT PRIMARY KEY,
    park_id INT NOT NULL,
    queue_times_id INT NULL,
    ride_name VARCHAR(255) NOT NULL,
    park_name VARCHAR(255) NOT NULL,

    -- Ride classification
    tier INT DEFAULT 3,
    tier_weight DECIMAL(3,1) DEFAULT 2.0,
    category VARCHAR(50) DEFAULT 'ATTRACTION',

    -- Park classification (denormalized for filtering)
    is_disney BOOLEAN DEFAULT FALSE,
    is_universal BOOLEAN DEFAULT FALSE,

    -- Live status (current snapshot)
    is_down BOOLEAN DEFAULT FALSE,
    current_status VARCHAR(50) NULL,
    current_wait_time INT NULL,
    last_status_change DATETIME NULL,

    -- Today cumulative metrics
    downtime_hours DECIMAL(6,2) DEFAULT 0,
    downtime_incidents INT DEFAULT 0,
    avg_wait_time DECIMAL(5,1) NULL,
    max_wait_time INT NULL,

    -- Metadata
    calculated_at DATETIME NOT NULL,

    -- Indexes
    INDEX idx_park (park_id),
    INDEX idx_downtime (downtime_hours DESC),
    INDEX idx_is_down (is_down, downtime_hours DESC),
    INDEX idx_disney_universal (is_disney, is_universal),
    INDEX idx_calculated_at (calculated_at),

    -- Foreign key
    CONSTRAINT fk_ride_live_park FOREIGN KEY (park_id)
        REFERENCES parks(park_id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================
-- STAGING TABLES (for atomic swap)
-- ============================================
-- These are used during the aggregation job for zero-downtime updates

CREATE TABLE IF NOT EXISTS park_live_rankings_staging LIKE park_live_rankings;
CREATE TABLE IF NOT EXISTS ride_live_rankings_staging LIKE ride_live_rankings;

-- ============================================
-- USAGE NOTES
-- ============================================
--
-- Aggregation job workflow:
-- 1. TRUNCATE park_live_rankings_staging
-- 2. INSERT INTO park_live_rankings_staging SELECT ... (slow CTE query)
-- 3. RENAME TABLE park_live_rankings TO park_live_rankings_old,
--              park_live_rankings_staging TO park_live_rankings
-- 4. RENAME TABLE park_live_rankings_old TO park_live_rankings_staging
--    (reuse old table as next staging table)
--
-- This ensures:
-- - Zero downtime (RENAME is atomic)
-- - API always sees complete data
-- - If INSERT fails, old data remains intact
