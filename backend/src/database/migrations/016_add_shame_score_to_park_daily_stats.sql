-- Theme Park Downtime Tracker - Add shame_score to park_daily_stats
-- Migration: 016_add_shame_score_to_park_daily_stats.sql
-- Purpose:
--   Aligns park_daily_stats with AggregationService which stores per-day shame scores.
--   Without this column, inserts fail with "Unknown column 'shame_score'".
--   Adds nullable DECIMAL(6,3) field since shame scores are normalized to a 0-10+ range.

ALTER TABLE park_daily_stats
ADD COLUMN shame_score DECIMAL(6,3) NULL
AFTER avg_uptime_percentage;
