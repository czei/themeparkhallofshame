"""
Today Park Rankings Query (Average Shame Score)
===============================================

Endpoint: GET /api/parks/downtime?period=today
UI Location: Parks tab → Downtime Rankings (today)

Returns parks ranked by AVERAGE shame score from midnight Pacific to now.

SHAME SCORE CALCULATION:
- LIVE: Instantaneous shame = (sum of weights of down rides) / total_park_weight × 10
- TODAY: Average of instantaneous shame scores across all snapshots

This makes TODAY comparable to LIVE - both on the same 0-100 scale representing
"percentage of weighted capacity that was down".

Example: If a park had shame scores of [0, 0, 2, 2, 0, 0] across 6 snapshots:
- Average shame score = (0+0+2+2+0+0) / 6 = 0.67

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

from typing import List, Dict, Any

from sqlalchemy import text
from sqlalchemy.engine import Connection

from utils.timezone import get_today_range_to_now_utc
from utils.sql_helpers import (
    RideStatusSQL,
    ParkStatusSQL,
    RideFilterSQL,
)


class TodayParkRankingsQuery:
    """
    Query handler for today's park rankings using AVERAGE shame score.

    Shame score for TODAY = average of instantaneous shame scores across all
    snapshots from midnight Pacific to now.

    This makes the score comparable to LIVE (both on the same 0-100 scale).
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
        Get park rankings from midnight Pacific to now using AVERAGE shame score.

        Args:
            filter_disney_universal: Only Disney/Universal parks
            limit: Maximum results
            sort_by: Sort field (shame_score or downtime_hours)

        Returns:
            List of parks ranked by average shame_score (descending)
        """
        # Get time range from midnight Pacific to now
        start_utc, now_utc = get_today_range_to_now_utc()

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
            end_param=":now_utc",
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
                WHERE rss.recorded_at >= :start_utc AND rss.recorded_at < :now_utc
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
                WHERE rss_inner.recorded_at >= :start_utc AND rss_inner.recorded_at < :now_utc
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
                -- This makes TODAY comparable to LIVE (same 0-100 scale)
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

                -- Count of DISTINCT rides that were down at some point today
                COUNT(DISTINCT CASE
                    WHEN {is_down} AND {park_open} AND rto.ride_id IS NOT NULL
                    THEN r.ride_id
                END) AS rides_down,

                -- Park operating status (current)
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
            WHERE rss.recorded_at >= :start_utc AND rss.recorded_at < :now_utc
                AND p.is_active = TRUE
                {filter_clause}
            GROUP BY p.park_id, p.name, p.city, p.state_province, pw.total_park_weight, pos.total_snapshots
            HAVING total_downtime_hours > 0
            ORDER BY {sort_column} DESC
            LIMIT :limit
        """)

        result = self.conn.execute(query, {
            "start_utc": start_utc,
            "now_utc": now_utc,
            "limit": limit
        })
        return [dict(row._mapping) for row in result]
