-- Performance Optimization Indexes
-- Migration: 005_performance_indexes.sql
-- Created: 2025-12-01
-- Purpose: Add covering indexes for time-range aggregation queries
--
-- These indexes address performance bottlenecks identified in TODAY queries
-- that aggregate ride_status_snapshots and park_activity_snapshots.
--
-- Expected Impact:
-- - TODAY ride wait times: 20-30s → < 2s
-- - TODAY park downtime: 20-30s → < 2s
-- - Awards queries: 90-120s → < 10s
--
-- Run this migration during low-traffic period (early morning Pacific)
-- as CREATE INDEX may lock the table briefly.

-- ===========================================================================
-- ride_status_snapshots: Covering index for TODAY queries
-- ===========================================================================
-- This index optimizes queries that:
-- 1. Filter by recorded_at range (WHERE recorded_at >= :start AND recorded_at < :end)
-- 2. Group by ride_id
-- 3. Aggregate wait_time and computed_is_open
--
-- Column order rationale:
-- - recorded_at first: Most selective for time-range queries
-- - ride_id: GROUP BY column
-- - computed_is_open, wait_time: Covering columns to avoid table access
--
-- Note: This is a covering index - the query can be satisfied entirely from
-- the index without accessing the main table data (Using index in EXPLAIN).

CREATE INDEX IF NOT EXISTS idx_rss_time_range_covering
ON ride_status_snapshots (recorded_at, ride_id, computed_is_open, wait_time);

-- ===========================================================================
-- park_activity_snapshots: Covering index for JOIN optimization
-- ===========================================================================
-- This index optimizes the JOIN between ride_status_snapshots and
-- park_activity_snapshots:
--   INNER JOIN park_activity_snapshots pas ON p.park_id = pas.park_id
--       AND pas.recorded_at = rss.recorded_at
--   WHERE pas.park_appears_open = TRUE
--
-- Column order rationale:
-- - recorded_at first: JOIN condition with ride_status_snapshots
-- - park_id: JOIN condition with parks table
-- - park_appears_open: Filter condition (covering)

CREATE INDEX IF NOT EXISTS idx_pas_time_range_covering
ON park_activity_snapshots (recorded_at, park_id, park_appears_open);

-- ===========================================================================
-- Verify indexes were created
-- ===========================================================================
-- After running this migration, verify indexes exist:
--
-- SELECT index_name, column_name, seq_in_index
-- FROM information_schema.statistics
-- WHERE table_schema = DATABASE()
--   AND table_name IN ('ride_status_snapshots', 'park_activity_snapshots')
--   AND index_name LIKE 'idx_%_time_range_covering'
-- ORDER BY table_name, index_name, seq_in_index;
--
-- Expected output:
-- | index_name                | column_name      | seq_in_index |
-- |---------------------------|------------------|--------------|
-- | idx_pas_time_range_covering | recorded_at    | 1            |
-- | idx_pas_time_range_covering | park_id        | 2            |
-- | idx_pas_time_range_covering | park_appears_open | 3         |
-- | idx_rss_time_range_covering | recorded_at    | 1            |
-- | idx_rss_time_range_covering | ride_id        | 2            |
-- | idx_rss_time_range_covering | computed_is_open | 3          |
-- | idx_rss_time_range_covering | wait_time      | 4            |

-- ===========================================================================
-- Rollback instructions
-- ===========================================================================
-- To remove these indexes if needed:
--
-- DROP INDEX idx_rss_time_range_covering ON ride_status_snapshots;
-- DROP INDEX idx_pas_time_range_covering ON park_activity_snapshots;
