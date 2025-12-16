-- Theme Park Downtime Tracker - Park Schedules
-- Migration: 009_park_schedules.sql
-- Purpose: Store official park operating schedules from ThemeParks.wiki API
-- Date: 2025-12-01
-- Updated: 2025-12-16 - Made index creation idempotent

-- ============================================
-- PARK SCHEDULES TABLE
-- ============================================
-- Stores official operating hours from ThemeParks.wiki /entity/{id}/schedule endpoint
-- This replaces the hacky "park_appears_open" heuristic with actual schedule data

CREATE TABLE IF NOT EXISTS park_schedules (
    schedule_id INT AUTO_INCREMENT PRIMARY KEY,

    -- Park reference
    park_id INT NOT NULL,

    -- Schedule date (in park's local timezone)
    schedule_date DATE NOT NULL,

    -- Operating times (stored as UTC for consistency)
    opening_time DATETIME NULL COMMENT 'UTC opening time',
    closing_time DATETIME NULL COMMENT 'UTC closing time',

    -- Schedule type from API: OPERATING, TICKETED_EVENT, PRIVATE_EVENT, EXTRA_HOURS, INFO
    schedule_type ENUM('OPERATING', 'TICKETED_EVENT', 'PRIVATE_EVENT', 'EXTRA_HOURS', 'INFO')
        DEFAULT 'OPERATING' COMMENT 'Type of schedule entry from ThemeParks.wiki',

    -- When this schedule was last fetched from API
    fetched_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,

    -- For tracking updates
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    -- Foreign key
    CONSTRAINT fk_park_schedules_park FOREIGN KEY (park_id)
        REFERENCES parks(park_id) ON DELETE CASCADE,

    -- Unique constraint: one schedule entry per park per date per type
    UNIQUE KEY uk_park_date_type (park_id, schedule_date, schedule_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Indexes for common queries (idempotent with DROP IF EXISTS)
DROP INDEX IF EXISTS idx_ps_park_date ON park_schedules;
CREATE INDEX idx_ps_park_date ON park_schedules (park_id, schedule_date);

DROP INDEX IF EXISTS idx_ps_date ON park_schedules;
CREATE INDEX idx_ps_date ON park_schedules (schedule_date);

DROP INDEX IF EXISTS idx_ps_fetched ON park_schedules;
CREATE INDEX idx_ps_fetched ON park_schedules (fetched_at);

DROP INDEX IF EXISTS idx_ps_type ON park_schedules;
CREATE INDEX idx_ps_type ON park_schedules (schedule_type);

-- ============================================
-- VERIFICATION QUERIES (run manually)
-- ============================================

-- Verify table structure:
-- DESCRIBE park_schedules;

-- Check if park is currently open (example):
-- SELECT ps.*
-- FROM park_schedules ps
-- INNER JOIN parks p ON ps.park_id = p.park_id
-- WHERE ps.schedule_date = CURDATE()
--   AND ps.schedule_type = 'OPERATING'
--   AND NOW() BETWEEN ps.opening_time AND ps.closing_time;
