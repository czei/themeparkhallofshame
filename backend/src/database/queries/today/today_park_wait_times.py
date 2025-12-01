"""
Today Park Wait Time Rankings Query (Cumulative)
=================================================

Endpoint: GET /api/parks/waittimes?period=today
UI Location: Parks tab → Wait Times Rankings (today - cumulative)

Returns parks ranked by CUMULATIVE wait times from midnight Pacific to now.

CRITICAL DIFFERENCE FROM 7-DAY/30-DAY:
- 7-DAY/30-DAY: Uses pre-aggregated park_daily_stats table
- TODAY: Queries ride_status_snapshots directly for real-time accuracy

Database Tables:
- parks (park metadata)
- rides (ride metadata)
- ride_status_snapshots (real-time wait time data)
- park_activity_snapshots (park open status)

Single Source of Truth:
- Formulas: utils/metrics.py
- SQL Helpers: utils/sql_helpers.py
"""

from typing import List, Dict, Any

from sqlalchemy import text
from sqlalchemy.engine import Connection

from utils.timezone import get_today_range_to_now_utc
from utils.sql_helpers import ParkStatusSQL, RideFilterSQL


class TodayParkWaitTimesQuery:
    """
    Query handler for today's CUMULATIVE park wait time rankings.

    Unlike weekly/monthly queries which use park_daily_stats,
    this aggregates ALL wait times from ride_status_snapshots
    since midnight Pacific to now.
    """

    def __init__(self, connection: Connection):
        self.conn = connection

    def get_rankings(
        self,
        filter_disney_universal: bool = False,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """
        Get cumulative park wait time rankings from midnight Pacific to now.

        Args:
            filter_disney_universal: Only Disney/Universal parks
            limit: Maximum results

        Returns:
            List of parks ranked by average wait time (descending)
        """
        # Get time range from midnight Pacific to now
        start_utc, now_utc = get_today_range_to_now_utc()

        # Use centralized SQL helpers for consistent logic
        filter_clause = f"AND {RideFilterSQL.disney_universal_filter('p')}" if filter_disney_universal else ""
        park_open = ParkStatusSQL.park_appears_open_filter("pas")
        park_is_open_sq = ParkStatusSQL.park_is_open_subquery("p.park_id")

        query = text(f"""
            SELECT
                p.park_id,
                p.queue_times_id,
                p.name AS park_name,
                CONCAT(p.city, ', ', p.state_province) AS location,

                -- Average wait time across all rides (only when park is open and wait > 0)
                ROUND(
                    AVG(CASE
                        WHEN {park_open} AND rss.wait_time > 0
                        THEN rss.wait_time
                    END),
                    1
                ) AS avg_wait_time,

                -- Peak wait time today
                MAX(CASE
                    WHEN {park_open}
                    THEN rss.wait_time
                END) AS peak_wait_time,

                -- Count of rides with wait time data
                COUNT(DISTINCT CASE
                    WHEN rss.wait_time > 0
                    THEN r.ride_id
                END) AS rides_with_waits,

                -- Park operating status (current)
                {park_is_open_sq}

            FROM parks p
            INNER JOIN rides r ON p.park_id = r.park_id
                AND r.is_active = TRUE AND r.category = 'ATTRACTION'
            INNER JOIN ride_status_snapshots rss ON r.ride_id = rss.ride_id
            INNER JOIN park_activity_snapshots pas ON p.park_id = pas.park_id
                AND pas.recorded_at = rss.recorded_at
            WHERE rss.recorded_at >= :start_utc AND rss.recorded_at < :now_utc
                AND p.is_active = TRUE
                {filter_clause}
            GROUP BY p.park_id, p.name, p.city, p.state_province
            HAVING avg_wait_time IS NOT NULL
            ORDER BY avg_wait_time DESC
            LIMIT :limit
        """)

        result = self.conn.execute(query, {
            "start_utc": start_utc,
            "now_utc": now_utc,
            "limit": limit
        })
        return [dict(row._mapping) for row in result]
