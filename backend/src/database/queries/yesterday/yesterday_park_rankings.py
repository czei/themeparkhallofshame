"""
Yesterday Park Rankings Query (Average Shame Score)
===================================================

Endpoint: GET /api/parks/downtime?period=yesterday
UI Location: Parks tab → Downtime Rankings (yesterday)

Returns parks ranked by AVERAGE shame score for the FULL previous day.

KEY DIFFERENCES FROM TODAY:
- TODAY: midnight Pacific to NOW (partial, live-updating)
- YESTERDAY: full previous Pacific day (complete, immutable)

Because YESTERDAY is immutable, responses can be cached for 24 hours.

SHAME SCORE CALCULATION:
- Same as TODAY: Average of instantaneous shame scores across all snapshots
- Shame = (sum of weights of down rides) / total_park_weight × 10

Database Tables:
- parks (park metadata)
- rides (ride metadata)
- ride_classifications (tier weights)
- ride_status_snapshots (real-time status)
- park_activity_snapshots (park open status)

Single Source of Truth:
- Formulas: utils/metrics.py
- SQL Helpers: utils/sql_helpers.py
"""

from datetime import datetime
from typing import List, Dict, Any

from sqlalchemy import text
from sqlalchemy.engine import Connection

from utils.timezone import get_yesterday_range_utc
from utils.sql_helpers import (
    RideStatusSQL,
    ParkStatusSQL,
    DowntimeSQL,
    RideFilterSQL,
)


class YesterdayParkRankingsQuery:
    """
    Query handler for yesterday's park rankings using AVERAGE shame score.

    Shame score for YESTERDAY = average of instantaneous shame scores across
    all snapshots from the full previous Pacific day.

    This makes the score comparable to LIVE and TODAY (same 0-100 scale).
    Unlike TODAY, YESTERDAY data is immutable and highly cacheable.
    """

    # Snapshot interval in minutes (for converting snapshot counts to time)
    SNAPSHOT_INTERVAL_MINUTES = 5

    def __init__(self, connection: Connection):
        self.conn = connection

    def get_rankings(
        self,
        filter_disney_universal: bool = False,
        limit: int = 50,
        sort_by: str = "shame_score",
    ) -> List[Dict[str, Any]]:
        """
        Get park rankings for the full previous Pacific day using AVERAGE shame score.

        Args:
            filter_disney_universal: Only Disney/Universal parks
            limit: Maximum results
            sort_by: Sort field (shame_score or downtime_hours)

        Returns:
            List of parks ranked by average shame_score (descending)
        """
        # Get time range for yesterday (full previous day)
        start_utc, end_utc, label = get_yesterday_range_utc()

        # Use centralized SQL helpers for consistent logic
        filter_clause = f"AND {RideFilterSQL.disney_universal_filter('p')}" if filter_disney_universal else ""
        filter_clause_inner = f"AND {RideFilterSQL.disney_universal_filter('p_inner')}" if filter_disney_universal else ""
        # PARK-TYPE AWARE: Disney/Universal only counts DOWN (not CLOSED)
        is_down = RideStatusSQL.is_down("rss", parks_alias="p")
        is_down_inner = RideStatusSQL.is_down("rss_inner", parks_alias="p_inner")
        park_open = ParkStatusSQL.park_appears_open_filter("pas")
        park_open_inner = ParkStatusSQL.park_appears_open_filter("pas_inner")
        park_is_open_sq = ParkStatusSQL.park_is_open_subquery("p.park_id")

        # Use centralized CTE for rides that operated (includes park-open check)
        rides_operated_cte = RideStatusSQL.rides_that_operated_cte(
            start_param=":start_utc",
            end_param=":end_utc",
            filter_clause=filter_clause
        )

        # Determine sort column
        sort_column = "shame_score" if sort_by == "shame_score" else "total_downtime_hours"

        query = text(f"""
            WITH
            {rides_operated_cte},
            park_weights AS (
                -- Total tier weight for each park (for shame score normalization)
                -- Only count rides that have operated
                SELECT
                    p.park_id,
                    SUM(COALESCE(rc.tier_weight, 2)) AS total_park_weight
                FROM parks p
                INNER JOIN rides r ON p.park_id = r.park_id
                    AND r.is_active = TRUE AND r.category = 'ATTRACTION'
                LEFT JOIN ride_classifications rc ON r.ride_id = rc.ride_id
                WHERE p.is_active = TRUE
                    AND r.ride_id IN (SELECT ride_id FROM rides_that_operated)
                    {filter_clause}
                GROUP BY p.park_id
            ),
            park_operating_snapshots AS (
                -- Count total snapshots when park was open (for averaging)
                SELECT
                    p.park_id,
                    COUNT(DISTINCT rss.recorded_at) AS total_snapshots
                FROM parks p
                INNER JOIN rides r ON p.park_id = r.park_id
                    AND r.is_active = TRUE AND r.category = 'ATTRACTION'
                INNER JOIN ride_status_snapshots rss ON r.ride_id = rss.ride_id
                INNER JOIN park_activity_snapshots pas ON p.park_id = pas.park_id
                    AND pas.recorded_at = rss.recorded_at
                WHERE rss.recorded_at >= :start_utc AND rss.recorded_at < :end_utc
                    AND p.is_active = TRUE
                    AND {park_open}
                    AND r.ride_id IN (SELECT ride_id FROM rides_that_operated)
                    {filter_clause}
                GROUP BY p.park_id
            ),
            per_snapshot_shame AS (
                -- Calculate instantaneous shame score for each park at each snapshot
                -- shame = (sum of weights of down rides that have operated) / total_park_weight * 10
                SELECT
                    p_inner.park_id,
                    rss_inner.recorded_at,
                    COALESCE(
                        SUM(CASE
                            WHEN {is_down_inner} AND {park_open_inner} AND rto_inner.ride_id IS NOT NULL
                            THEN COALESCE(rc_inner.tier_weight, 2)
                            ELSE 0
                        END) / NULLIF(pw_inner.total_park_weight, 0) * 10,
                        0
                    ) AS snapshot_shame_score
                FROM parks p_inner
                INNER JOIN rides r_inner ON p_inner.park_id = r_inner.park_id
                    AND r_inner.is_active = TRUE AND r_inner.category = 'ATTRACTION'
                LEFT JOIN ride_classifications rc_inner ON r_inner.ride_id = rc_inner.ride_id
                INNER JOIN ride_status_snapshots rss_inner ON r_inner.ride_id = rss_inner.ride_id
                INNER JOIN park_activity_snapshots pas_inner ON p_inner.park_id = pas_inner.park_id
                    AND pas_inner.recorded_at = rss_inner.recorded_at
                INNER JOIN park_weights pw_inner ON p_inner.park_id = pw_inner.park_id
                LEFT JOIN rides_that_operated rto_inner ON r_inner.ride_id = rto_inner.ride_id
                WHERE rss_inner.recorded_at >= :start_utc AND rss_inner.recorded_at < :end_utc
                    AND p_inner.is_active = TRUE
                    AND {park_open_inner}
                    AND r_inner.ride_id IN (SELECT ride_id FROM rides_that_operated)
                    {filter_clause_inner}
                GROUP BY p_inner.park_id, rss_inner.recorded_at, pw_inner.total_park_weight
            )
            SELECT
                p.park_id,
                p.queue_times_id,
                p.name AS park_name,
                CONCAT(p.city, ', ', p.state_province) AS location,

                -- Total downtime hours (for reference)
                ROUND(
                    SUM(CASE
                        WHEN {is_down} AND {park_open} AND rto.ride_id IS NOT NULL
                        THEN {self.SNAPSHOT_INTERVAL_MINUTES} / 60.0
                        ELSE 0
                    END),
                    2
                ) AS total_downtime_hours,

                -- Weighted downtime hours (for reference)
                ROUND(
                    SUM(CASE
                        WHEN {is_down} AND {park_open} AND rto.ride_id IS NOT NULL
                        THEN ({self.SNAPSHOT_INTERVAL_MINUTES} / 60.0) * COALESCE(rc.tier_weight, 2)
                        ELSE 0
                    END),
                    2
                ) AS weighted_downtime_hours,

                -- AVERAGE Shame Score = average of per-snapshot instantaneous shame scores
                -- This makes YESTERDAY comparable to LIVE and TODAY (same 0-100 scale)
                ROUND(
                    (SELECT AVG(pss.snapshot_shame_score) FROM per_snapshot_shame pss WHERE pss.park_id = p.park_id),
                    1
                ) AS shame_score,

                -- Uptime percentage = (operating snapshots) / (total ride-snapshots) * 100
                -- Must divide by total (rides × snapshots), not just snapshots
                ROUND(
                    100.0 * SUM(CASE
                        WHEN {park_open} AND rto.ride_id IS NOT NULL AND NOT ({is_down})
                        THEN 1
                        ELSE 0
                    END) / NULLIF(
                        SUM(CASE WHEN {park_open} AND rto.ride_id IS NOT NULL THEN 1 ELSE 0 END),
                        0
                    ),
                    1
                ) AS uptime_percentage,

                -- Count of DISTINCT rides that were down at some point yesterday
                COUNT(DISTINCT CASE
                    WHEN {is_down} AND {park_open} AND rto.ride_id IS NOT NULL
                    THEN r.ride_id
                END) AS rides_affected,

                -- Park operating status (current - may differ from yesterday's status)
                {park_is_open_sq}

            FROM parks p
            INNER JOIN rides r ON p.park_id = r.park_id
                AND r.is_active = TRUE AND r.category = 'ATTRACTION'
            LEFT JOIN ride_classifications rc ON r.ride_id = rc.ride_id
            INNER JOIN ride_status_snapshots rss ON r.ride_id = rss.ride_id
            INNER JOIN park_activity_snapshots pas ON p.park_id = pas.park_id
                AND pas.recorded_at = rss.recorded_at
            INNER JOIN park_weights pw ON p.park_id = pw.park_id
            LEFT JOIN park_operating_snapshots pos ON p.park_id = pos.park_id
            LEFT JOIN rides_that_operated rto ON r.ride_id = rto.ride_id
            WHERE rss.recorded_at >= :start_utc AND rss.recorded_at < :end_utc
                AND p.is_active = TRUE
                {filter_clause}
            GROUP BY p.park_id, p.name, p.city, p.state_province, pw.total_park_weight, pos.total_snapshots
            HAVING total_downtime_hours > 0
            ORDER BY {sort_column} DESC
            LIMIT :limit
        """)

        result = self.conn.execute(query, {
            "start_utc": start_utc,
            "end_utc": end_utc,
            "limit": limit
        })
        return [dict(row._mapping) for row in result]
