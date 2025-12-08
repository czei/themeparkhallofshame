-- Migration: Relax aggregation_log unique constraint for hourly aggregations
-- Date: 2025-12-08
-- Feature: 001-aggregation-tables
-- Purpose: Allow multiple hourly aggregation log entries per day

START TRANSACTION;

-- ============================================================================
-- Drop unique constraint on (aggregation_date, aggregation_type)
-- ============================================================================
-- The existing constraint prevents multiple hourly entries per day.
-- Daily/weekly/monthly/yearly still have unique entries per date, but hourly
-- needs multiple entries (one per hour).

ALTER TABLE aggregation_log
DROP INDEX unique_aggregation;

-- Add regular index for query performance (non-unique)
CREATE INDEX idx_aggregation_date_type
ON aggregation_log(aggregation_date, aggregation_type);

-- Add index on aggregated_until_ts for efficient per-hour lookups
CREATE INDEX idx_aggregated_until_ts
ON aggregation_log(aggregated_until_ts);

COMMIT;

-- ============================================================================
-- Rollback Plan
-- ============================================================================
-- To rollback this migration (ONLY if no hourly entries exist):
--   DROP INDEX IF EXISTS idx_aggregation_date_type ON aggregation_log;
--   DROP INDEX IF EXISTS idx_aggregated_until_ts ON aggregation_log;
--   ALTER TABLE aggregation_log
--   ADD UNIQUE KEY unique_aggregation (aggregation_date, aggregation_type);
--
-- WARNING: Rollback will fail if there are multiple hourly entries for the same date.
--          Delete duplicate hourly entries before attempting rollback:
--   DELETE FROM aggregation_log
--   WHERE aggregation_type = 'hourly'
--     AND log_id NOT IN (
--       SELECT MIN(log_id)
--       FROM aggregation_log
--       WHERE aggregation_type = 'hourly'
--       GROUP BY aggregation_date
--     );
