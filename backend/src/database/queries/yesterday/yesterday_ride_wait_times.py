"""
Yesterday Ride Wait Time Rankings Query
=======================================

Endpoint: GET /api/rides/waittimes?period=yesterday
UI Location: Rides tab -> Wait Times Rankings (yesterday)

Returns rides ranked by average wait times for the previous full Pacific day.

Uses same snapshot-based approach as TODAY query, but for yesterday's
full day range instead of partial day.

Database Tables:
- rides (ride metadata)
- parks (park metadata)
- ride_classifications (tier info)
- ride_status_snapshots (wait time data)
- park_activity_snapshots (park open status)

Single Source of Truth:
- Formulas: utils/metrics.py
- SQL Helpers: utils/sql_helpers.py
"""

from typing import List, Dict, Any

from sqlalchemy import text
from sqlalchemy.engine import Connection

from utils.timezone import get_yesterday_range_utc
from utils.sql_helpers import ParkStatusSQL, RideFilterSQL


class YesterdayRideWaitTimesQuery:
    """
    Query handler for yesterday's ride wait time rankings.

    Uses snapshot data from yesterday's full Pacific day
    (midnight to midnight Pacific, converted to UTC).
    """

    def __init__(self, connection: Connection):
        self.conn = connection

    def get_rankings(
        self,
        filter_disney_universal: bool = False,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """
        Get ride wait time rankings from yesterday's full day.

        Args:
            filter_disney_universal: Only Disney/Universal parks
            limit: Maximum results

        Returns:
            List of rides ranked by average wait time (descending)
        """
        # Get yesterday's full day range in UTC
        start_utc, end_utc, _ = get_yesterday_range_utc()

        # Use centralized SQL helpers for consistent logic
        filter_clause = f"AND {RideFilterSQL.disney_universal_filter('p')}" if filter_disney_universal else ""
        park_open = ParkStatusSQL.park_appears_open_filter("pas")

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

                -- Peak wait time yesterday
                -- IMPORTANT: Use peak_wait_minutes (not peak_wait_time) for frontend compatibility
                MAX(CASE
                    WHEN {park_open}
                    THEN rss.wait_time
                END) AS peak_wait_minutes,

                -- Count of snapshots with wait time data
                COUNT(CASE WHEN rss.wait_time > 0 THEN 1 END) AS snapshots_with_waits

            FROM rides r
            INNER JOIN parks p ON r.park_id = p.park_id
            LEFT JOIN ride_classifications rc ON r.ride_id = rc.ride_id
            INNER JOIN ride_status_snapshots rss ON r.ride_id = rss.ride_id
            INNER JOIN park_activity_snapshots pas ON p.park_id = pas.park_id
                AND pas.recorded_at = rss.recorded_at
            WHERE rss.recorded_at >= :start_utc AND rss.recorded_at < :end_utc
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
            "end_utc": end_utc,
            "limit": limit
        })
        return [dict(row._mapping) for row in result]
