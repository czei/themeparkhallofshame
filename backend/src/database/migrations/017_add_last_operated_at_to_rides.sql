-- Theme Park Downtime Tracker - Add last_operated_at to rides
-- Purpose:
--   Track when each ride last operated so we can exclude stale rides from rankings.
--   Required by stats_repository.get_excluded_rides and related endpoints.

ALTER TABLE rides
ADD COLUMN last_operated_at DATETIME NULL AFTER is_active;

CREATE INDEX idx_rides_last_operated_at ON rides(last_operated_at);
