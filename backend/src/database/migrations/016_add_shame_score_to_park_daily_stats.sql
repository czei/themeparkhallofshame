-- Theme Park Downtime Tracker - Add shame_score to park_daily_stats
-- Migration: 016_add_shame_score_to_park_daily_stats.sql
-- Updated: 2025-12-16 - Made idempotent
-- Purpose:
--   Aligns park_daily_stats with AggregationService which stores per-day shame scores.
--   Without this column, inserts fail with "Unknown column 'shame_score'".
--   Adds nullable DECIMAL(6,3) field since shame scores are normalized to a 0-10+ range.

SET @col_exists := (
    SELECT COUNT(*)
    FROM information_schema.columns
    WHERE table_schema = DATABASE()
      AND table_name = 'park_daily_stats'
      AND column_name = 'shame_score'
);
SET @ddl := IF(
    @col_exists = 0,
    'ALTER TABLE park_daily_stats ADD COLUMN shame_score DECIMAL(6,3) NULL AFTER avg_uptime_percentage;',
    'SELECT "park_daily_stats.shame_score already exists" AS message;'
);
PREPARE stmt FROM @ddl;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
