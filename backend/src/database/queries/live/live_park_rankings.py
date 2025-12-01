"""
Live Park Rankings Query
========================

Endpoint: GET /api/parks/downtime?period=today
UI Location: Parks tab → Downtime Rankings (today)

Returns parks ranked by current-day downtime from real-time snapshots.

NOTE: This class is currently bypassed for performance. The routes use
StatsRepository.get_park_live_downtime_rankings() instead, which uses
the same centralized SQL helpers but with optimized CTEs.

CRITICAL: Shame score only counts rides that are CURRENTLY down.
Rides that were down earlier but are now operating do NOT contribute
to the shame score. "Rides Down" shows count of currently down rides.

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
)


class LiveParkRankingsQuery:
    """
    Query handler for live (today) park rankings.

    Uses centralized SQL helpers from utils/sql_helpers.py to ensure
    consistent calculations across all queries.

    CRITICAL: Shame score only counts rides CURRENTLY down (latest snapshot),
    not cumulative downtime throughout the day.

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
        park_is_open_sq = ParkStatusSQL.park_is_open_subquery("p.park_id")

        # For latest snapshot check - used in rides_currently_down CTE
        is_down_latest = RideStatusSQL.is_down("rss_latest")
        park_open_latest = ParkStatusSQL.park_appears_open_filter("pas_latest")

        query = text(f"""
            WITH
            latest_snapshot AS (
                -- Find the most recent snapshot timestamp for each ride today
                SELECT ride_id, MAX(recorded_at) as latest_recorded_at
                FROM ride_status_snapshots
                WHERE recorded_at >= :start_utc AND recorded_at < :end_utc
                GROUP BY ride_id
            ),
            rides_currently_down AS (
                -- Identify rides that are DOWN in their latest snapshot
                -- This is used to filter shame score to only currently down rides
                SELECT DISTINCT r_inner.ride_id, r_inner.park_id
                FROM rides r_inner
                INNER JOIN ride_status_snapshots rss_latest ON r_inner.ride_id = rss_latest.ride_id
                INNER JOIN latest_snapshot ls ON rss_latest.ride_id = ls.ride_id
                    AND rss_latest.recorded_at = ls.latest_recorded_at
                INNER JOIN park_activity_snapshots pas_latest ON r_inner.park_id = pas_latest.park_id
                    AND pas_latest.recorded_at = rss_latest.recorded_at
                WHERE r_inner.is_active = TRUE
                    AND r_inner.category = 'ATTRACTION'
                    AND {is_down_latest}
                    AND {park_open_latest}
            ),
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

                -- Shame Score = weighted downtime ONLY for rides CURRENTLY down
                -- Rides that were down earlier but are now operating don't count
                ROUND(
                    SUM(CASE
                        WHEN rcd.ride_id IS NOT NULL AND {park_open} AND {is_down}
                        THEN 5 * COALESCE(rc.tier_weight, 2)
                        ELSE 0
                    END) / 60.0
                    / NULLIF(pw.total_park_weight, 0),
                    2
                ) AS shame_score,

                -- Count of rides CURRENTLY down (not cumulative)
                COUNT(DISTINCT rcd.ride_id) AS rides_down,

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
            LEFT JOIN rides_currently_down rcd ON r.ride_id = rcd.ride_id
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
