-- Theme Park Downtime Tracker - Add shame_score to park_activity_snapshots
-- Migration: 012_add_shame_score_column.sql
-- Purpose: Store pre-calculated shame score at snapshot time (THE single source of truth)
-- Date: 2025-12-03
-- Updated: 2025-12-16 - Made idempotent with conditional checks
--
-- PROBLEM: Shame score was calculated in 20+ files with 8+ different formulas,
-- causing inconsistencies between Rankings table, Details modal, and Charts.
--
-- SOLUTION: Calculate shame score ONCE during data collection, store in
-- park_activity_snapshots table, all queries just READ the stored value.

-- Add shame_score column to park_activity_snapshots (if not exists)
SET @col_exists := (
    SELECT COUNT(*)
    FROM information_schema.columns
    WHERE table_schema = DATABASE()
      AND table_name = 'park_activity_snapshots'
      AND column_name = 'shame_score'
);
SET @ddl := IF(
    @col_exists = 0,
    'ALTER TABLE park_activity_snapshots ADD COLUMN shame_score DECIMAL(4,1) NULL COMMENT ''Pre-calculated shame score at snapshot time. THE authoritative value. Formula: (down_weight / effective_park_weight) * 10'';',
    'SELECT "park_activity_snapshots.shame_score already exists" AS message;'
);
PREPARE stmt FROM @ddl;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

-- Index for efficient lookups on shame_score with park and time
DROP INDEX IF EXISTS idx_pas_shame_score ON park_activity_snapshots;
CREATE INDEX idx_pas_shame_score ON park_activity_snapshots(park_id, recorded_at, shame_score);

-- Note: Historical data backfill will be handled by collect_snapshots.py
-- During the transition period, NULL shame_score values indicate
-- snapshots taken before this feature was implemented.
