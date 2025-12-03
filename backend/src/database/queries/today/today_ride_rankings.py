"""
Today Ride Rankings Query (Cumulative)
======================================

Endpoint: GET /api/rides/downtime?period=today
UI Location: Rides tab → Downtime Rankings (today - cumulative)

Returns rides ranked by CUMULATIVE downtime from midnight Pacific to now.

CRITICAL DIFFERENCE FROM LIVE:
- LIVE: Shows rides CURRENTLY down (latest snapshot status)
- TODAY: Shows CUMULATIVE downtime since midnight

Database Tables:
- rides (ride metadata)
- parks (park metadata)
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
    UptimeSQL,
    RideFilterSQL,
)


class TodayRideRankingsQuery:
    """
    Query handler for today's CUMULATIVE ride rankings.

    Unlike LiveRideRankingsQuery which shows current status,
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
    ) -> List[Dict[str, Any]]:
        """
        Get cumulative ride rankings from midnight Pacific to now.

        Args:
            filter_disney_universal: Only Disney/Universal parks
            limit: Maximum results

        Returns:
            List of rides ranked by cumulative downtime hours (descending)
        """
        # Get time range from midnight Pacific to now
        start_utc, now_utc = get_today_range_to_now_utc()

        # Use centralized SQL helpers for consistent logic
        filter_clause = f"AND {RideFilterSQL.disney_universal_filter('p')}" if filter_disney_universal else ""
        # PARK-TYPE AWARE: Disney/Universal only counts DOWN (not CLOSED)
        is_down = RideStatusSQL.is_down("rss", parks_alias="p")
        park_open = ParkStatusSQL.park_appears_open_filter("pas")
        current_status_sq = RideStatusSQL.current_status_subquery("r.ride_id", include_time_window=True, park_id_expr="r.park_id")
        current_is_open_sq = RideStatusSQL.current_is_open_subquery("r.ride_id", include_time_window=True, park_id_expr="r.park_id")
        park_is_open_sq = ParkStatusSQL.park_is_open_subquery("p.park_id")

        # Use centralized CTE for rides that operated (includes park-open check)
        rides_operated_cte = RideStatusSQL.rides_that_operated_cte(
            start_param=":start_utc",
            end_param=":now_utc",
            filter_clause=filter_clause
        )

        query = text(f"""
            WITH
            {rides_operated_cte},
            operating_snapshots AS (
                -- Count total snapshots when park was open (for uptime calculation)
                SELECT
                    r.ride_id,
                    COUNT(*) AS total_operating_snapshots
                FROM rides r
                INNER JOIN ride_status_snapshots rss ON r.ride_id = rss.ride_id
                INNER JOIN park_activity_snapshots pas ON r.park_id = pas.park_id
                    AND pas.recorded_at = rss.recorded_at
                WHERE rss.recorded_at >= :start_utc AND rss.recorded_at < :now_utc
                    AND {park_open}
                GROUP BY r.ride_id
            )
            SELECT
                r.ride_id,
                r.queue_times_id,
                p.queue_times_id AS park_queue_times_id,
                r.name AS ride_name,
                p.name AS park_name,
                p.park_id,
                CONCAT(p.city, ', ', p.state_province) AS location,
                rc.tier,

                -- CUMULATIVE downtime hours (all downtime since midnight)
                ROUND(
                    SUM(CASE
                        WHEN {is_down} AND {park_open} AND rto.ride_id IS NOT NULL
                        THEN {self.SNAPSHOT_INTERVAL_MINUTES} / 60.0
                        ELSE 0
                    END),
                    2
                ) AS downtime_hours,

                -- Uptime percentage for today
                ROUND(
                    100 - (
                        SUM(CASE
                            WHEN {is_down} AND {park_open} AND rto.ride_id IS NOT NULL
                            THEN 1
                            ELSE 0
                        END) * 100.0 / NULLIF(os.total_operating_snapshots, 0)
                    ),
                    1
                ) AS uptime_percentage,

                -- Current status (for display)
                {current_status_sq},
                {current_is_open_sq},
                {park_is_open_sq},

                -- Wait time stats for today
                MAX(rss.wait_time) AS peak_wait_time,
                ROUND(AVG(CASE WHEN rss.wait_time > 0 THEN rss.wait_time END), 0) AS avg_wait_time

            FROM rides r
            INNER JOIN parks p ON r.park_id = p.park_id
            LEFT JOIN ride_classifications rc ON r.ride_id = rc.ride_id
            INNER JOIN ride_status_snapshots rss ON r.ride_id = rss.ride_id
            INNER JOIN park_activity_snapshots pas ON p.park_id = pas.park_id
                AND pas.recorded_at = rss.recorded_at
            LEFT JOIN rides_that_operated rto ON r.ride_id = rto.ride_id
            LEFT JOIN operating_snapshots os ON r.ride_id = os.ride_id
            WHERE rss.recorded_at >= :start_utc AND rss.recorded_at < :now_utc
                AND r.is_active = TRUE
                AND r.category = 'ATTRACTION'
                AND p.is_active = TRUE
                {filter_clause}
            GROUP BY r.ride_id, r.name, p.name, p.park_id, p.city, p.state_province,
                     rc.tier, os.total_operating_snapshots
            HAVING downtime_hours > 0
            ORDER BY downtime_hours DESC
            LIMIT :limit
        """)

        result = self.conn.execute(query, {
            "start_utc": start_utc,
            "now_utc": now_utc,
            "limit": limit
        })
        return [dict(row._mapping) for row in result]
