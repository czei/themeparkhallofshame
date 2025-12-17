-- Theme Park Downtime Tracker - Add last_operated_at to rides
-- Updated: 2025-12-16 - Made idempotent
-- Purpose:
--   Track when each ride last operated so we can exclude stale rides from rankings.
--   Required by stats_repository.get_excluded_rides and related endpoints.

SET @col_exists := (
    SELECT COUNT(*)
    FROM information_schema.columns
    WHERE table_schema = DATABASE()
      AND table_name = 'rides'
      AND column_name = 'last_operated_at'
);
SET @ddl := IF(
    @col_exists = 0,
    'ALTER TABLE rides ADD COLUMN last_operated_at DATETIME NULL AFTER is_active;',
    'SELECT "rides.last_operated_at already exists" AS message;'
);
PREPARE stmt FROM @ddl;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

DROP INDEX IF EXISTS idx_rides_last_operated_at ON rides;
CREATE INDEX idx_rides_last_operated_at ON rides(last_operated_at);
