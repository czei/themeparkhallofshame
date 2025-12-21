-- Migration: Add category column to rides and ride_classifications tables
-- Purpose: Enable filtering of non-mechanical attractions (Meet & Greets, Shows, Experiences)
--          from downtime and wait time statistics
-- Date: 2025-11-27
-- Version: Idempotent - safe to re-run

-- Add category column to rides table (if not exists)
SET @column_exists = (SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'rides' AND COLUMN_NAME = 'category');
SET @sql = IF(@column_exists = 0,
    "ALTER TABLE rides ADD COLUMN category ENUM('ATTRACTION', 'MEET_AND_GREET', 'SHOW', 'EXPERIENCE') DEFAULT 'ATTRACTION' AFTER tier",
    "SELECT 'Column rides.category already exists'");
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

-- Add category column to ride_classifications table (if not exists)
SET @column_exists = (SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'ride_classifications' AND COLUMN_NAME = 'category');
SET @sql = IF(@column_exists = 0,
    "ALTER TABLE ride_classifications ADD COLUMN category ENUM('ATTRACTION', 'MEET_AND_GREET', 'SHOW', 'EXPERIENCE') DEFAULT 'ATTRACTION' AFTER tier_weight",
    "SELECT 'Column ride_classifications.category already exists'");
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

-- Add index for filtering by category (if not exists)
SET @index_exists = (SELECT COUNT(*) FROM INFORMATION_SCHEMA.STATISTICS
    WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'rides' AND INDEX_NAME = 'idx_rides_category');
SET @sql = IF(@index_exists = 0,
    "CREATE INDEX idx_rides_category ON rides(category)",
    "SELECT 'Index idx_rides_category already exists'");
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
