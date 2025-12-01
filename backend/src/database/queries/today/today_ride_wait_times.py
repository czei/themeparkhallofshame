"""
Today Ride Wait Time Rankings Query (Cumulative)
=================================================

Endpoint: GET /api/rides/waittimes?period=today
UI Location: Rides tab → Wait Times Rankings (today - cumulative)

Returns rides ranked by CUMULATIVE wait times from midnight Pacific to now.

CRITICAL DIFFERENCE FROM 7-DAY/30-DAY:
- 7-DAY/30-DAY: Uses pre-aggregated ride_daily_stats table
- TODAY: Queries ride_status_snapshots directly for real-time accuracy

Database Tables:
- rides (ride metadata)
- parks (park metadata)
- ride_classifications (tier info)
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


class TodayRideWaitTimesQuery:
    """
    Query handler for today's CUMULATIVE ride wait time rankings.

    Unlike weekly/monthly queries which use ride_daily_stats,
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
        Get cumulative ride wait time rankings from midnight Pacific to now.

        Args:
            filter_disney_universal: Only Disney/Universal parks
            limit: Maximum results

        Returns:
            List of rides ranked by average wait time (descending)
        """
        # Get time range from midnight Pacific to now
        start_utc, now_utc = get_today_range_to_now_utc()

        # Use centralized SQL helpers for consistent logic
        filter_clause = f"AND {RideFilterSQL.disney_universal_filter('p')}" if filter_disney_universal else ""
        park_open = ParkStatusSQL.park_appears_open_filter("pas")
        park_is_open_sq = ParkStatusSQL.park_is_open_subquery("p.park_id")

        query = text(f"""
            SELECT
                r.ride_id,
                r.queue_times_id,
                p.queue_times_id AS park_queue_times_id,
                r.name AS ride_name,
                p.name AS park_name,
                p.park_id,
                CONCAT(p.city, ', ', p.state_province) AS location,
                rc.tier,

                -- Average wait time (only when park is open and wait > 0)
                -- IMPORTANT: Use avg_wait_minutes (not avg_wait_time) for frontend compatibility
                ROUND(
                    AVG(CASE
                        WHEN {park_open} AND rss.wait_time > 0
                        THEN rss.wait_time
                    END),
                    1
                ) AS avg_wait_minutes,

                -- Peak wait time today
                -- IMPORTANT: Use peak_wait_minutes (not peak_wait_time) for frontend compatibility
                MAX(CASE
                    WHEN {park_open}
                    THEN rss.wait_time
                END) AS peak_wait_minutes,

                -- Current wait time (most recent snapshot)
                (SELECT rss2.wait_time
                 FROM ride_status_snapshots rss2
                 WHERE rss2.ride_id = r.ride_id
                 ORDER BY rss2.recorded_at DESC
                 LIMIT 1) AS current_wait_time,

                -- Current ride status (most recent snapshot) - for status badge
                (SELECT rss3.status
                 FROM ride_status_snapshots rss3
                 WHERE rss3.ride_id = r.ride_id
                 ORDER BY rss3.recorded_at DESC
                 LIMIT 1) AS current_status,

                -- Current ride open status (most recent snapshot) - for status badge
                (SELECT rss4.computed_is_open
                 FROM ride_status_snapshots rss4
                 WHERE rss4.ride_id = r.ride_id
                 ORDER BY rss4.recorded_at DESC
                 LIMIT 1) AS current_is_open,

                -- Park operating status (current)
                {park_is_open_sq}

            FROM rides r
            INNER JOIN parks p ON r.park_id = p.park_id
            LEFT JOIN ride_classifications rc ON r.ride_id = rc.ride_id
            INNER JOIN ride_status_snapshots rss ON r.ride_id = rss.ride_id
            INNER JOIN park_activity_snapshots pas ON p.park_id = pas.park_id
                AND pas.recorded_at = rss.recorded_at
            WHERE rss.recorded_at >= :start_utc AND rss.recorded_at < :now_utc
                AND r.is_active = TRUE
                AND r.category = 'ATTRACTION'
                AND p.is_active = TRUE
                {filter_clause}
            GROUP BY r.ride_id, r.name, p.name, p.park_id, p.city, p.state_province, rc.tier
            HAVING avg_wait_minutes IS NOT NULL
            ORDER BY avg_wait_minutes DESC
            LIMIT :limit
        """)

        result = self.conn.execute(query, {
            "start_utc": start_utc,
            "now_utc": now_utc,
            "limit": limit
        })
        return [dict(row._mapping) for row in result]
