-- Theme Park Downtime Tracker - ThemeParks.wiki Integration
-- Migration: 008_themeparks_wiki.sql
-- Purpose: Add ThemeParks.wiki entity IDs and rich status enum
-- Date: 2025-11-29

-- ============================================
-- PHASE 1: Add ThemeParks.wiki IDs
-- ============================================

-- Add themeparks_wiki_id to parks table
ALTER TABLE parks
ADD COLUMN themeparks_wiki_id CHAR(36) DEFAULT NULL
    COMMENT 'ThemeParks.wiki entity UUID for this park'
AFTER queue_times_id;

-- Add index for lookups
ALTER TABLE parks
ADD INDEX idx_themeparks_wiki_id (themeparks_wiki_id);

-- Add themeparks_wiki_id to rides table
ALTER TABLE rides
ADD COLUMN themeparks_wiki_id CHAR(36) DEFAULT NULL
    COMMENT 'ThemeParks.wiki entity UUID for this ride'
AFTER queue_times_id;

-- Add index for lookups
ALTER TABLE rides
ADD INDEX idx_themeparks_wiki_id (themeparks_wiki_id);

-- ============================================
-- PHASE 2: Add entity_type to rides
-- ============================================

-- Add entity type (from ThemeParks.wiki: ATTRACTION, SHOW, RESTAURANT)
ALTER TABLE rides
ADD COLUMN entity_type ENUM('ATTRACTION', 'SHOW', 'RESTAURANT') DEFAULT 'ATTRACTION'
    COMMENT 'Entity type from ThemeParks.wiki API'
AFTER name;

-- Add index for filtering by type
ALTER TABLE rides
ADD INDEX idx_entity_type (entity_type);

-- ============================================
-- PHASE 3: Add rich status to snapshots
-- ============================================

-- Add status enum to ride_status_snapshots
-- Values: OPERATING, DOWN, CLOSED, REFURBISHMENT (from ThemeParks.wiki)
ALTER TABLE ride_status_snapshots
ADD COLUMN status ENUM('OPERATING', 'DOWN', 'CLOSED', 'REFURBISHMENT') DEFAULT NULL
    COMMENT 'Rich status from ThemeParks.wiki: OPERATING=running, DOWN=breakdown, CLOSED=scheduled closure, REFURBISHMENT=extended maintenance'
AFTER is_open;

-- Add index for status queries
ALTER TABLE ride_status_snapshots
ADD INDEX idx_status (status, recorded_at DESC);

-- ============================================
-- PHASE 4: Add status to changes table
-- ============================================

-- Add status columns to ride_status_changes
ALTER TABLE ride_status_changes
ADD COLUMN previous_status_enum ENUM('OPERATING', 'DOWN', 'CLOSED', 'REFURBISHMENT') DEFAULT NULL
    COMMENT 'Previous status (ThemeParks.wiki enum)'
AFTER previous_status;

ALTER TABLE ride_status_changes
ADD COLUMN new_status_enum ENUM('OPERATING', 'DOWN', 'CLOSED', 'REFURBISHMENT') DEFAULT NULL
    COMMENT 'New status (ThemeParks.wiki enum)'
AFTER new_status;

-- ============================================
-- PHASE 5: Cleanup abandoned screen-scraping
-- ============================================

-- Drop ride_operating_schedules table (no longer needed - ThemeParks.wiki provides operating hours in live data)
DROP TABLE IF EXISTS ride_operating_schedules;

-- Drop disney_slug column from rides (no longer needed for screen scraping)
SET @col_exists := (
    SELECT COUNT(*)
    FROM information_schema.columns
    WHERE table_schema = DATABASE()
      AND table_name = 'rides'
      AND column_name = 'disney_slug'
);
SET @ddl := IF(
    @col_exists = 1,
    'ALTER TABLE rides DROP COLUMN disney_slug;',
    'SELECT "disney_slug column already removed" AS message;'
);
PREPARE stmt FROM @ddl;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

-- ============================================
-- VERIFICATION QUERIES (run manually)
-- ============================================

-- Verify parks table has new columns:
-- DESCRIBE parks;

-- Verify rides table has new columns:
-- DESCRIBE rides;

-- Verify snapshots table has status column:
-- DESCRIBE ride_status_snapshots;
