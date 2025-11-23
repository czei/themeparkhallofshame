-- Theme Park Downtime Tracker - Additional Indexes Migration
-- Migration: 004_indexes.sql
-- Purpose: Add performance-critical composite and covering indexes
-- Date: 2025-11-23
-- Note: Most indexes are already defined in table CREATE statements.
--        This migration adds additional performance optimization indexes.

-- Additional indexes for park filtering (Disney & Universal filter)
CREATE INDEX idx_parks_filter_active
    ON parks(is_disney, is_universal, is_active);

-- Additional indexes for ride performance queries
CREATE INDEX idx_rides_park_tier_active
    ON rides(park_id, tier, is_active);

-- Covering index for current wait times query (FR-017)
CREATE INDEX idx_snapshots_current_status
    ON ride_status_snapshots(ride_id, recorded_at DESC, wait_time, computed_is_open);

-- Covering index for park-wide downtime rankings (FR-010)
CREATE INDEX idx_park_daily_ranking
    ON park_daily_stats(stat_date DESC, total_downtime_hours DESC);

CREATE INDEX idx_park_weekly_ranking
    ON park_weekly_stats(year DESC, week_number DESC, total_downtime_hours DESC);

CREATE INDEX idx_park_monthly_ranking
    ON park_monthly_stats(year DESC, month DESC, total_downtime_hours DESC);

-- Covering index for ride downtime rankings (FR-014)
CREATE INDEX idx_ride_daily_ranking
    ON ride_daily_stats(stat_date DESC, downtime_minutes DESC);

CREATE INDEX idx_ride_weekly_ranking
    ON ride_weekly_stats(year DESC, week_number DESC, downtime_minutes DESC);

-- Index for weighted downtime calculations (FR-024)
CREATE INDEX idx_classifications_tier_weight
    ON ride_classifications(ride_id, tier, tier_weight);

-- Index for cleanup job queries
CREATE INDEX idx_aggregation_cleanup
    ON aggregation_log(aggregation_type, status, aggregated_until_ts DESC);

-- Index for operating hours detection
CREATE INDEX idx_park_activity_detection
    ON park_activity_snapshots(park_id, park_appears_open, recorded_at DESC);
