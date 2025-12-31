-- Theme Park Downtime Tracker - ThemeParks.wiki Integration
-- Migration: 008_themeparks_wiki.sql
-- Purpose: Add ThemeParks.wiki entity IDs and rich status enum
-- Date: 2025-11-29
-- Updated: 2025-12-16 - Made idempotent with conditional checks

-- ============================================
-- PHASE 1: Add ThemeParks.wiki IDs
-- ============================================

-- Add themeparks_wiki_id to parks table (if not exists)
SET @col_exists := (
    SELECT COUNT(*)
    FROM information_schema.columns
    WHERE table_schema = DATABASE()
      AND table_name = 'parks'
      AND column_name = 'themeparks_wiki_id'
);
SET @ddl := IF(
    @col_exists = 0,
    'ALTER TABLE parks ADD COLUMN themeparks_wiki_id CHAR(36) DEFAULT NULL COMMENT ''ThemeParks.wiki entity UUID for this park'' AFTER queue_times_id;',
    'SELECT "parks.themeparks_wiki_id already exists" AS message;'
);
PREPARE stmt FROM @ddl;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

-- Add index for lookups (if not exists)
SET @idx_exists := (
    SELECT COUNT(*)
    FROM information_schema.statistics
    WHERE table_schema = DATABASE()
      AND table_name = 'parks'
      AND index_name = 'idx_themeparks_wiki_id'
);
SET @ddl := IF(
    @idx_exists = 0,
    'ALTER TABLE parks ADD INDEX idx_themeparks_wiki_id (themeparks_wiki_id);',
    'SELECT "parks.idx_themeparks_wiki_id already exists" AS message;'
);
PREPARE stmt FROM @ddl;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

-- Add themeparks_wiki_id to rides table (if not exists)
SET @col_exists := (
    SELECT COUNT(*)
    FROM information_schema.columns
    WHERE table_schema = DATABASE()
      AND table_name = 'rides'
      AND column_name = 'themeparks_wiki_id'
);
SET @ddl := IF(
    @col_exists = 0,
    'ALTER TABLE rides ADD COLUMN themeparks_wiki_id CHAR(36) DEFAULT NULL COMMENT ''ThemeParks.wiki entity UUID for this ride'' AFTER queue_times_id;',
    'SELECT "rides.themeparks_wiki_id already exists" AS message;'
);
PREPARE stmt FROM @ddl;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

-- Add index for lookups (if not exists)
SET @idx_exists := (
    SELECT COUNT(*)
    FROM information_schema.statistics
    WHERE table_schema = DATABASE()
      AND table_name = 'rides'
      AND index_name = 'idx_themeparks_wiki_id'
);
SET @ddl := IF(
    @idx_exists = 0,
    'ALTER TABLE rides ADD INDEX idx_themeparks_wiki_id (themeparks_wiki_id);',
    'SELECT "rides.idx_themeparks_wiki_id already exists" AS message;'
);
PREPARE stmt FROM @ddl;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

-- ============================================
-- PHASE 2: Add entity_type to rides
-- ============================================

-- Add entity type (if not exists)
SET @col_exists := (
    SELECT COUNT(*)
    FROM information_schema.columns
    WHERE table_schema = DATABASE()
      AND table_name = 'rides'
      AND column_name = 'entity_type'
);
SET @ddl := IF(
    @col_exists = 0,
    'ALTER TABLE rides ADD COLUMN entity_type ENUM(''ATTRACTION'', ''SHOW'', ''RESTAURANT'') DEFAULT ''ATTRACTION'' COMMENT ''Entity type from ThemeParks.wiki API'' AFTER name;',
    'SELECT "rides.entity_type already exists" AS message;'
);
PREPARE stmt FROM @ddl;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

-- Add index for filtering by type (if not exists)
SET @idx_exists := (
    SELECT COUNT(*)
    FROM information_schema.statistics
    WHERE table_schema = DATABASE()
      AND table_name = 'rides'
      AND index_name = 'idx_entity_type'
);
SET @ddl := IF(
    @idx_exists = 0,
    'ALTER TABLE rides ADD INDEX idx_entity_type (entity_type);',
    'SELECT "rides.idx_entity_type already exists" AS message;'
);
PREPARE stmt FROM @ddl;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

-- ============================================
-- PHASE 3: Add rich status to snapshots
-- ============================================

-- Add status enum to ride_status_snapshots (if not exists)
SET @col_exists := (
    SELECT COUNT(*)
    FROM information_schema.columns
    WHERE table_schema = DATABASE()
      AND table_name = 'ride_status_snapshots'
      AND column_name = 'status'
);
SET @ddl := IF(
    @col_exists = 0,
    'ALTER TABLE ride_status_snapshots ADD COLUMN status ENUM(''OPERATING'', ''DOWN'', ''CLOSED'', ''REFURBISHMENT'') DEFAULT NULL COMMENT ''Rich status from ThemeParks.wiki: OPERATING=running, DOWN=breakdown, CLOSED=scheduled closure, REFURBISHMENT=extended maintenance'' AFTER is_open;',
    'SELECT "ride_status_snapshots.status already exists" AS message;'
);
PREPARE stmt FROM @ddl;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

-- Add index for status queries (if not exists)
SET @idx_exists := (
    SELECT COUNT(*)
    FROM information_schema.statistics
    WHERE table_schema = DATABASE()
      AND table_name = 'ride_status_snapshots'
      AND index_name = 'idx_status'
);
SET @ddl := IF(
    @idx_exists = 0,
    'ALTER TABLE ride_status_snapshots ADD INDEX idx_status (status, recorded_at DESC);',
    'SELECT "ride_status_snapshots.idx_status already exists" AS message;'
);
PREPARE stmt FROM @ddl;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

-- ============================================
-- PHASE 4: Add status to changes table
-- ============================================

-- Add previous_status_enum to ride_status_changes (if not exists)
SET @col_exists := (
    SELECT COUNT(*)
    FROM information_schema.columns
    WHERE table_schema = DATABASE()
      AND table_name = 'ride_status_changes'
      AND column_name = 'previous_status_enum'
);
SET @ddl := IF(
    @col_exists = 0,
    'ALTER TABLE ride_status_changes ADD COLUMN previous_status_enum ENUM(''OPERATING'', ''DOWN'', ''CLOSED'', ''REFURBISHMENT'') DEFAULT NULL COMMENT ''Previous status (ThemeParks.wiki enum)'' AFTER previous_status;',
    'SELECT "ride_status_changes.previous_status_enum already exists" AS message;'
);
PREPARE stmt FROM @ddl;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

-- Add new_status_enum to ride_status_changes (if not exists)
SET @col_exists := (
    SELECT COUNT(*)
    FROM information_schema.columns
    WHERE table_schema = DATABASE()
      AND table_name = 'ride_status_changes'
      AND column_name = 'new_status_enum'
);
SET @ddl := IF(
    @col_exists = 0,
    'ALTER TABLE ride_status_changes ADD COLUMN new_status_enum ENUM(''OPERATING'', ''DOWN'', ''CLOSED'', ''REFURBISHMENT'') DEFAULT NULL COMMENT ''New status (ThemeParks.wiki enum)'' AFTER new_status;',
    'SELECT "ride_status_changes.new_status_enum already exists" AS message;'
);
PREPARE stmt FROM @ddl;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

-- ============================================
-- PHASE 5: Cleanup abandoned screen-scraping
-- ============================================

-- Drop ride_operating_schedules table (no longer needed)
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
