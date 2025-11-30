"""
Live Park Rankings Query
========================

Endpoint: GET /api/parks/downtime?period=today
UI Location: Parks tab → Downtime Rankings (today)

Returns parks ranked by current-day downtime from real-time snapshots.

NOTE: This class is currently bypassed for performance. The routes use
StatsRepository.get_park_live_downtime_rankings() instead, which uses
the same centralized SQL helpers but with optimized CTEs.

Database Tables:
- parks (park metadata)
- rides (ride metadata)
- ride_classifications (tier weights)
- ride_status_snapshots (real-time status)
- park_activity_snapshots (park open status)

Single Source of Truth:
- Formulas: utils/metrics.py
- SQL Helpers: utils/sql_helpers.py (used here)
- Python calculations: utils/metrics.py

Performance: Uses raw SQL with centralized helpers for consistency.
"""

from datetime import date, datetime
from typing import List, Dict, Any

from sqlalchemy import text
from sqlalchemy.engine import Connection

from utils.timezone import get_today_pacific, get_pacific_day_range_utc
from utils.sql_helpers import (
    RideStatusSQL,
    ParkStatusSQL,
    DowntimeSQL,
    RideFilterSQL,
    AffectedRidesSQL,
)


class LiveParkRankingsQuery:
    """
    Query handler for live (today) park rankings.

    Uses centralized SQL helpers from utils/sql_helpers.py to ensure
    consistent calculations across all queries.

    NOTE: For production use, prefer StatsRepository.get_park_live_downtime_rankings()
    which has additional optimizations.
    """

    def __init__(self, connection: Connection):
        self.conn = connection

    def get_rankings(
        self,
        filter_disney_universal: bool = False,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """
        Get live park rankings for today from real-time snapshots.

        Uses centralized SQL helpers for consistent status logic.

        Args:
            filter_disney_universal: Only Disney/Universal parks
            limit: Maximum results

        Returns:
            List of parks ranked by shame_score (descending)
        """
        # Get Pacific day bounds in UTC
        today = get_today_pacific()
        start_utc, end_utc = get_pacific_day_range_utc(today)

        # Use centralized SQL helpers for consistent logic
        filter_clause = f"AND {RideFilterSQL.disney_universal_filter('p')}" if filter_disney_universal else ""
        is_down = RideStatusSQL.is_down("rss")
        park_open = ParkStatusSQL.park_appears_open_filter("pas")
        downtime_hours = DowntimeSQL.downtime_hours_rounded("rss", "pas")
        weighted_downtime = DowntimeSQL.weighted_downtime_hours("rss", "pas", "COALESCE(rc.tier_weight, 2)")
        affected_rides = AffectedRidesSQL.count_distinct_down_rides("r.ride_id", "rss", "pas")
        park_is_open_sq = ParkStatusSQL.park_is_open_subquery("p.park_id")

        query = text(f"""
            WITH park_weights AS (
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
            )
            SELECT
                p.park_id,
                p.queue_times_id,
                p.name AS park_name,
                CONCAT(p.city, ', ', p.state_province) AS location,

                -- Total downtime hours (using centralized helper)
                {downtime_hours} AS total_downtime_hours,

                -- Weighted downtime hours (using centralized helper)
                {weighted_downtime} AS weighted_downtime_hours,

                -- Shame Score = weighted downtime / total park weight
                -- Formula from utils/metrics.py: calculate_shame_score()
                ROUND(
                    SUM(CASE
                        WHEN {park_open} AND {is_down}
                        THEN 5 * COALESCE(rc.tier_weight, 2)
                        ELSE 0
                    END) / 60.0
                    / NULLIF(pw.total_park_weight, 0),
                    2
                ) AS shame_score,

                -- Count of rides with downtime (using centralized helper)
                {affected_rides} AS affected_rides_count,

                -- Park operating status
                {park_is_open_sq}

            FROM parks p
            INNER JOIN rides r ON p.park_id = r.park_id
                AND r.is_active = TRUE AND r.category = 'ATTRACTION'
            LEFT JOIN ride_classifications rc ON r.ride_id = rc.ride_id
            INNER JOIN ride_status_snapshots rss ON r.ride_id = rss.ride_id
            INNER JOIN park_activity_snapshots pas ON p.park_id = pas.park_id
                AND pas.recorded_at = rss.recorded_at
            INNER JOIN park_weights pw ON p.park_id = pw.park_id
            WHERE rss.recorded_at >= :start_utc AND rss.recorded_at < :end_utc
                AND p.is_active = TRUE
                {filter_clause}
            GROUP BY p.park_id, p.name, p.city, p.state_province, pw.total_park_weight
            HAVING total_downtime_hours > 0
            ORDER BY shame_score DESC
            LIMIT :limit
        """)

        result = self.conn.execute(query, {
            "start_utc": start_utc,
            "end_utc": end_utc,
            "limit": limit
        })
        return [dict(row._mapping) for row in result]
