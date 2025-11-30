-- =============================================================================
-- AUDIT VIEWS FOR DATA VALIDATION
-- =============================================================================
-- These views provide an auditable calculation path from raw snapshots
-- to final shame scores. Every number can be traced back to source data.
--
-- View Hierarchy:
--   v_audit_ride_daily   <- Ride-level daily stats from raw snapshots
--   v_audit_park_daily   <- Park-level shame scores from ride stats
--
-- Usage:
--   1. Validate API results by comparing with view calculations
--   2. Debug discrepancies by tracing calculation path
--   3. Generate computation traces for user-triggered audits
--
-- Created: 2024-11 (Data Accuracy Audit Framework)
-- =============================================================================

-- =============================================================================
-- v_audit_ride_daily: Ride-Level Daily Statistics
-- =============================================================================
-- Calculates downtime for each ride each day from raw snapshots.
--
-- Key Logic (matches expressions.py):
--   - is_down = status='DOWN' OR (status IS NULL AND computed_is_open=FALSE)
--   - Only counts downtime when park_appears_open = TRUE
--   - Each snapshot represents 5 minutes
--   - downtime_hours = (down_snapshots * 5) / 60
--
-- Columns:
--   ride_id, ride_name, park_id, park_name, stat_date
--   total_snapshots         - Total snapshots recorded
--   park_open_snapshots     - Snapshots when park was open
--   operating_snapshots     - Snapshots with OPERATING status
--   down_snapshots          - Snapshots with DOWN status (during park open hours)
--   closed_snapshots        - Snapshots with CLOSED status
--   refurbishment_snapshots - Snapshots with REFURBISHMENT status
--   downtime_minutes        - Minutes of downtime (down_snapshots * 5)
--   downtime_hours          - Hours of downtime (downtime_minutes / 60)
--   uptime_percentage       - % of park_open_snapshots that were operating
--   tier                    - Ride tier (1, 2, or 3)
--   tier_weight             - Weight for shame score (3, 2, or 1)
-- =============================================================================

DROP VIEW IF EXISTS v_audit_ride_daily;

CREATE VIEW v_audit_ride_daily AS
SELECT
    r.ride_id,
    r.name AS ride_name,
    p.park_id,
    p.name AS park_name,
    DATE(rss.recorded_at) AS stat_date,

    -- Snapshot counts
    COUNT(*) AS total_snapshots,

    -- Park open snapshots (denominator for uptime %)
    SUM(CASE WHEN pas.park_appears_open = 1 THEN 1 ELSE 0 END) AS park_open_snapshots,

    -- Status breakdown (during park open hours only)
    SUM(CASE
        WHEN pas.park_appears_open = 1 AND (
            rss.status = 'OPERATING' OR
            (rss.status IS NULL AND rss.computed_is_open = 1)
        ) THEN 1 ELSE 0
    END) AS operating_snapshots,

    SUM(CASE
        WHEN pas.park_appears_open = 1 AND (
            rss.status = 'DOWN' OR
            (rss.status IS NULL AND rss.computed_is_open = 0)
        ) THEN 1 ELSE 0
    END) AS down_snapshots,

    SUM(CASE
        WHEN pas.park_appears_open = 1 AND rss.status = 'CLOSED'
        THEN 1 ELSE 0
    END) AS closed_snapshots,

    SUM(CASE
        WHEN pas.park_appears_open = 1 AND rss.status = 'REFURBISHMENT'
        THEN 1 ELSE 0
    END) AS refurbishment_snapshots,

    -- Downtime calculations (5 minutes per snapshot)
    SUM(CASE
        WHEN pas.park_appears_open = 1 AND (
            rss.status = 'DOWN' OR
            (rss.status IS NULL AND rss.computed_is_open = 0)
        ) THEN 5 ELSE 0
    END) AS downtime_minutes,

    ROUND(
        SUM(CASE
            WHEN pas.park_appears_open = 1 AND (
                rss.status = 'DOWN' OR
                (rss.status IS NULL AND rss.computed_is_open = 0)
            ) THEN 5 ELSE 0
        END) / 60.0,
        2
    ) AS downtime_hours,

    -- Uptime percentage (operating / park_open * 100)
    ROUND(
        100.0 * SUM(CASE
            WHEN pas.park_appears_open = 1 AND (
                rss.status = 'OPERATING' OR
                (rss.status IS NULL AND rss.computed_is_open = 1)
            ) THEN 1 ELSE 0
        END) / NULLIF(SUM(CASE WHEN pas.park_appears_open = 1 THEN 1 ELSE 0 END), 0),
        1
    ) AS uptime_percentage,

    -- Tier classification
    COALESCE(rc.tier, r.tier, 2) AS tier,
    COALESCE(rc.tier_weight,
        CASE COALESCE(r.tier, 2)
            WHEN 1 THEN 3
            WHEN 2 THEN 2
            WHEN 3 THEN 1
            ELSE 2
        END
    ) AS tier_weight

FROM ride_status_snapshots rss
JOIN rides r ON rss.ride_id = r.ride_id
JOIN parks p ON r.park_id = p.park_id
-- Join to park activity to know when park was open
LEFT JOIN park_activity_snapshots pas
    ON p.park_id = pas.park_id
    AND rss.recorded_at = pas.recorded_at
-- Join to classifications for tier weight
LEFT JOIN ride_classifications rc ON r.ride_id = rc.ride_id

WHERE r.is_active = 1
  AND r.category = 'ATTRACTION'

GROUP BY r.ride_id, r.name, p.park_id, p.name, DATE(rss.recorded_at),
         COALESCE(rc.tier, r.tier, 2),
         COALESCE(rc.tier_weight,
             CASE COALESCE(r.tier, 2)
                 WHEN 1 THEN 3
                 WHEN 2 THEN 2
                 WHEN 3 THEN 1
                 ELSE 2
             END
         );


-- =============================================================================
-- v_audit_park_daily: Park-Level Daily Statistics with Shame Score
-- =============================================================================
-- Aggregates ride stats into park-level shame score.
--
-- Shame Score Formula:
--   shame_score = total_weighted_downtime_hours / total_park_weight
--
-- Where:
--   weighted_downtime = SUM(ride_downtime_hours * tier_weight)
--   total_park_weight = SUM(tier_weight for all active rides)
--
-- Example:
--   Space Mountain: 2.0 hours down * weight 3 = 6.0
--   Pirates:        1.0 hours down * weight 2 = 2.0
--   Total weighted: 8.0 hours
--   Total weight:   5 (3 + 2)
--   Shame Score:    8.0 / 5 = 1.6
-- =============================================================================

DROP VIEW IF EXISTS v_audit_park_daily;

CREATE VIEW v_audit_park_daily AS
SELECT
    park_id,
    park_name,
    stat_date,

    -- Ride counts
    COUNT(DISTINCT ride_id) AS total_rides,
    SUM(CASE WHEN downtime_minutes > 0 THEN 1 ELSE 0 END) AS rides_with_downtime,

    -- Raw downtime (sum across all rides)
    SUM(downtime_minutes) AS total_downtime_minutes,
    ROUND(SUM(downtime_hours), 2) AS total_downtime_hours,

    -- Weighted downtime (for shame score)
    ROUND(SUM(downtime_hours * tier_weight), 2) AS weighted_downtime_hours,

    -- Total park weight (sum of all ride tier weights)
    SUM(tier_weight) AS total_park_weight,

    -- SHAME SCORE = weighted_downtime / total_weight
    ROUND(
        SUM(downtime_hours * tier_weight) / NULLIF(SUM(tier_weight), 0),
        2
    ) AS shame_score,

    -- Average uptime percentage across rides
    ROUND(AVG(uptime_percentage), 1) AS avg_uptime_percentage,

    -- Data quality metrics
    SUM(total_snapshots) AS total_ride_snapshots,
    SUM(park_open_snapshots) AS total_park_open_snapshots

FROM v_audit_ride_daily

GROUP BY park_id, park_name, stat_date;


-- =============================================================================
-- v_audit_trail: Full Lineage for Any Score
-- =============================================================================
-- Joins ride-level and park-level for complete audit trail.
-- Useful for debugging specific scores.
-- =============================================================================

DROP VIEW IF EXISTS v_audit_trail;

CREATE VIEW v_audit_trail AS
SELECT
    vrd.park_id,
    vrd.park_name,
    vrd.stat_date,
    vrd.ride_id,
    vrd.ride_name,
    vrd.tier,
    vrd.tier_weight,
    vrd.total_snapshots,
    vrd.park_open_snapshots,
    vrd.operating_snapshots,
    vrd.down_snapshots,
    vrd.downtime_hours,
    vrd.uptime_percentage,
    -- Ride's contribution to park shame score
    ROUND(vrd.downtime_hours * vrd.tier_weight, 2) AS weighted_downtime_contribution,
    -- Park-level totals for context
    vpd.total_park_weight,
    vpd.shame_score AS park_shame_score
FROM v_audit_ride_daily vrd
JOIN v_audit_park_daily vpd
    ON vrd.park_id = vpd.park_id
    AND vrd.stat_date = vpd.stat_date;


-- =============================================================================
-- VALIDATION HELPER VIEWS
-- =============================================================================

-- View to find rides with impossible values
DROP VIEW IF EXISTS v_validation_impossible_values;

CREATE VIEW v_validation_impossible_values AS
SELECT
    'downtime_exceeds_24h' AS violation_type,
    ride_id,
    ride_name,
    park_name,
    stat_date,
    downtime_hours AS value,
    '24.0' AS max_allowed
FROM v_audit_ride_daily
WHERE downtime_hours > 24

UNION ALL

SELECT
    'uptime_out_of_bounds' AS violation_type,
    ride_id,
    ride_name,
    park_name,
    stat_date,
    uptime_percentage AS value,
    '100.0' AS max_allowed
FROM v_audit_ride_daily
WHERE uptime_percentage > 100 OR uptime_percentage < 0;


-- View to find parks with insufficient data
DROP VIEW IF EXISTS v_validation_data_coverage;

CREATE VIEW v_validation_data_coverage AS
SELECT
    park_id,
    park_name,
    stat_date,
    total_rides,
    total_park_open_snapshots,
    -- Expected: ~288 snapshots per day per ride (24h * 60min / 5min)
    ROUND(100.0 * total_park_open_snapshots / NULLIF(total_rides * 288, 0), 1) AS coverage_percentage,
    CASE
        WHEN total_park_open_snapshots < total_rides * 144 THEN 'LOW'  -- <50%
        WHEN total_park_open_snapshots < total_rides * 230 THEN 'MEDIUM'  -- <80%
        ELSE 'HIGH'
    END AS data_quality
FROM v_audit_park_daily;
