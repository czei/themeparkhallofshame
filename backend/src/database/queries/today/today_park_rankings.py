"""
Today Park Rankings Query (Cumulative)
======================================

Endpoint: GET /api/parks/downtime?period=today
UI Location: Parks tab → Downtime Rankings (today - cumulative)

Returns parks ranked by CUMULATIVE downtime from midnight Pacific to now.

CRITICAL DIFFERENCE FROM LIVE:
- LIVE: Shame score only counts rides CURRENTLY down (latest snapshot)
- TODAY: Shame score is CUMULATIVE weighted downtime since midnight

Example: If a ride was down for 2 hours this morning but is now operating:
- LIVE: Ride doesn't contribute to shame score
- TODAY: Ride contributes 2 hours * tier_weight to shame score

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

from utils.timezone import get_today_range_to_now_utc
from utils.sql_helpers import (
    RideStatusSQL,
    ParkStatusSQL,
    DowntimeSQL,
    RideFilterSQL,
)


class TodayParkRankingsQuery:
    """
    Query handler for today's CUMULATIVE park rankings.

    Unlike LiveParkRankingsQuery which only counts rides currently down,
    this aggregates ALL downtime from midnight Pacific to now.
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
        Get cumulative park rankings from midnight Pacific to now.

        Args:
            filter_disney_universal: Only Disney/Universal parks
            limit: Maximum results
            sort_by: Sort field (shame_score or downtime_hours)

        Returns:
            List of parks ranked by cumulative shame_score (descending)
        """
        # Get time range from midnight Pacific to now
        start_utc, now_utc = get_today_range_to_now_utc()

        # Use centralized SQL helpers for consistent logic
        filter_clause = f"AND {RideFilterSQL.disney_universal_filter('p')}" if filter_disney_universal else ""
        is_down = RideStatusSQL.is_down("rss")
        park_open = ParkStatusSQL.park_appears_open_filter("pas")
        park_is_open_sq = ParkStatusSQL.park_is_open_subquery("p.park_id")

        # Determine sort column
        sort_column = "shame_score" if sort_by == "shame_score" else "total_downtime_hours"

        query = text(f"""
            WITH
            park_weights AS (
                -- Total tier weight for each park (for shame score normalization)
                SELECT
                    p.park_id,
                    SUM(COALESCE(rc.tier_weight, 2)) AS total_park_weight
                FROM parks p
                INNER JOIN rides r ON p.park_id = r.park_id
                    AND r.is_active = TRUE AND r.category = 'ATTRACTION'
                LEFT JOIN ride_classifications rc ON r.ride_id = rc.ride_id
                WHERE p.is_active = TRUE
                    {filter_clause}
                GROUP BY p.park_id
            ),
            rides_that_operated AS (
                -- Rides that operated at least once today (to exclude scheduled closures)
                SELECT DISTINCT r.ride_id
                FROM rides r
                INNER JOIN ride_status_snapshots rss ON r.ride_id = rss.ride_id
                WHERE rss.recorded_at >= :start_utc AND rss.recorded_at < :now_utc
                    AND rss.computed_is_open = TRUE
            )
            SELECT
                p.park_id,
                p.queue_times_id,
                p.name AS park_name,
                CONCAT(p.city, ', ', p.state_province) AS location,

                -- CUMULATIVE downtime hours (all downtime since midnight)
                ROUND(
                    SUM(CASE
                        WHEN {is_down} AND {park_open} AND rto.ride_id IS NOT NULL
                        THEN {self.SNAPSHOT_INTERVAL_MINUTES} / 60.0
                        ELSE 0
                    END),
                    2
                ) AS total_downtime_hours,

                -- CUMULATIVE weighted downtime hours
                ROUND(
                    SUM(CASE
                        WHEN {is_down} AND {park_open} AND rto.ride_id IS NOT NULL
                        THEN ({self.SNAPSHOT_INTERVAL_MINUTES} / 60.0) * COALESCE(rc.tier_weight, 2)
                        ELSE 0
                    END),
                    2
                ) AS weighted_downtime_hours,

                -- CUMULATIVE Shame Score = weighted downtime / total park weight * 10
                -- This counts ALL downtime throughout the day, not just current
                ROUND(
                    SUM(CASE
                        WHEN {is_down} AND {park_open} AND rto.ride_id IS NOT NULL
                        THEN ({self.SNAPSHOT_INTERVAL_MINUTES} / 60.0) * COALESCE(rc.tier_weight, 2)
                        ELSE 0
                    END) / NULLIF(pw.total_park_weight, 0) * 10,
                    1
                ) AS shame_score,

                -- Count of DISTINCT rides that were down at some point today
                COUNT(DISTINCT CASE
                    WHEN {is_down} AND {park_open} AND rto.ride_id IS NOT NULL
                    THEN r.ride_id
                END) AS rides_affected,

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
            LEFT JOIN rides_that_operated rto ON r.ride_id = rto.ride_id
            WHERE rss.recorded_at >= :start_utc AND rss.recorded_at < :now_utc
                AND p.is_active = TRUE
                {filter_clause}
            GROUP BY p.park_id, p.name, p.city, p.state_province, pw.total_park_weight
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
