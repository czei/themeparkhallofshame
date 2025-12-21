-- Theme Park Downtime Tracker - Automated Cleanup Events
-- Migration: 005_cleanup_events.sql
-- Purpose: Create MySQL Events for automated 24-hour data retention cleanup
-- Date: 2025-11-23
-- IMPORTANT: This requires the MySQL Event Scheduler to be enabled:
--   SET GLOBAL event_scheduler = ON;
-- NOTE: Event scheduler must be enabled by database administrator with SUPER privilege.
--       Application user does not have permission to enable it.
--       To enable: mysql -u root -p -e "SET GLOBAL event_scheduler = ON;"

-- Drop existing events if they exist (for idempotency)
DROP EVENT IF EXISTS cleanup_ride_snapshots;
DROP EVENT IF EXISTS cleanup_ride_changes;
DROP EVENT IF EXISTS cleanup_park_snapshots;

-- Event 1: Clean up ride_status_snapshots
-- Runs hourly, only deletes data that has been successfully aggregated
CREATE EVENT IF NOT EXISTS cleanup_ride_snapshots
ON SCHEDULE EVERY 1 HOUR
STARTS CURRENT_TIMESTAMP
DO
DELETE FROM ride_status_snapshots
WHERE recorded_at < (
    SELECT COALESCE(MAX(aggregated_until_ts), CURRENT_TIMESTAMP - INTERVAL 48 HOUR)
    FROM aggregation_log
    WHERE aggregation_type = 'daily'
      AND status = 'success'
);

-- Event 2: Clean up ride_status_changes
CREATE EVENT IF NOT EXISTS cleanup_ride_changes
ON SCHEDULE EVERY 1 HOUR
STARTS CURRENT_TIMESTAMP + INTERVAL 5 MINUTE
DO
DELETE FROM ride_status_changes
WHERE changed_at < (
    SELECT COALESCE(MAX(aggregated_until_ts), CURRENT_TIMESTAMP - INTERVAL 48 HOUR)
    FROM aggregation_log
    WHERE aggregation_type = 'daily'
      AND status = 'success'
);

-- Event 3: Clean up park_activity_snapshots
CREATE EVENT IF NOT EXISTS cleanup_park_snapshots
ON SCHEDULE EVERY 1 HOUR
STARTS CURRENT_TIMESTAMP + INTERVAL 10 MINUTE
DO
DELETE FROM park_activity_snapshots
WHERE recorded_at < (
    SELECT COALESCE(MAX(aggregated_until_ts), CURRENT_TIMESTAMP - INTERVAL 48 HOUR)
    FROM aggregation_log
    WHERE aggregation_type = 'daily'
      AND status = 'success'
);

-- Verify events are created
SELECT
    event_name,
    event_definition,
    interval_value,
    interval_field,
    status
FROM information_schema.events
WHERE event_schema = DATABASE()
  AND event_name LIKE 'cleanup_%';
