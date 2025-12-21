-- Migration: Add 'hourly' to aggregation_log.aggregation_type enum
-- Date: 2025-12-08
-- Feature: 001-aggregation-tables
-- Purpose: Allow hourly aggregations to be logged in aggregation_log table

START TRANSACTION;

-- ============================================================================
-- Add 'hourly' to aggregation_type enum
-- ============================================================================
ALTER TABLE aggregation_log
MODIFY COLUMN aggregation_type ENUM('daily','weekly','monthly','yearly','hourly') NOT NULL;

-- Verify the change
SELECT COLUMN_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = DATABASE()
    AND TABLE_NAME = 'aggregation_log'
    AND COLUMN_NAME = 'aggregation_type';

COMMIT;

-- ============================================================================
-- Rollback Plan
-- ============================================================================
-- To rollback this migration:
--   ALTER TABLE aggregation_log
--   MODIFY COLUMN aggregation_type ENUM('daily','weekly','monthly','yearly') NOT NULL;
-- Note: This will fail if there are existing rows with aggregation_type='hourly'
--       Those rows must be deleted or updated before rollback.