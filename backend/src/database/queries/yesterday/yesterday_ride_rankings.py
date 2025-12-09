"""
Yesterday Ride Rankings Query (Cumulative)
==========================================

Endpoint: GET /api/rides/downtime?period=yesterday
UI Location: Rides tab → Downtime Rankings (yesterday)

Returns rides ranked by CUMULATIVE downtime for the full previous day.

KEY DIFFERENCES FROM TODAY:
- TODAY: midnight Pacific to NOW (partial, live-updating)
- YESTERDAY: full previous Pacific day (complete, immutable)

Because YESTERDAY is immutable, responses can be cached for 24 hours.

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

from typing import List, Dict, Any

from sqlalchemy import text
from sqlalchemy.engine import Connection

from utils.timezone import get_yesterday_range_utc
from utils.sql_helpers import (
    RideStatusSQL,
    ParkStatusSQL,
    RideFilterSQL,
)


class YesterdayRideRankingsQuery:
    """
    Query handler for yesterday's CUMULATIVE ride rankings.

    Aggregates ALL downtime for the full previous Pacific day.
    Unlike TODAY, this data is immutable and highly cacheable.
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
        Get cumulative ride rankings for the full previous Pacific day.

        Args:
            filter_disney_universal: Only Disney/Universal parks
            limit: Maximum results

        Returns:
            List of rides ranked by cumulative downtime hours (descending)
        """
        # Get time range for yesterday (full previous day)
        start_utc, end_utc, label = get_yesterday_range_utc()

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
            end_param=":end_utc",
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
                    AND DATE_FORMAT(pas.recorded_at, '%Y-%m-%d %H:%i') = DATE_FORMAT(rss.recorded_at, '%Y-%m-%d %H:%i')
                WHERE rss.recorded_at >= :start_utc AND rss.recorded_at < :end_utc
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

                -- CUMULATIVE downtime hours (all downtime yesterday)
                ROUND(
                    SUM(CASE
                        WHEN {is_down} AND {park_open} AND rto.ride_id IS NOT NULL
                        THEN {self.SNAPSHOT_INTERVAL_MINUTES} / 60.0
                        ELSE 0
                    END),
                    2
                ) AS downtime_hours,

                -- Uptime percentage for yesterday
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

                -- Current status (for display - may differ from yesterday's status)
                {current_status_sq},
                {current_is_open_sq},
                {park_is_open_sq},

                -- Wait time stats for yesterday
                MAX(rss.wait_time) AS peak_wait_time,
                ROUND(AVG(CASE WHEN rss.wait_time > 0 THEN rss.wait_time END), 0) AS avg_wait_time

            FROM rides r
            INNER JOIN parks p ON r.park_id = p.park_id
            LEFT JOIN ride_classifications rc ON r.ride_id = rc.ride_id
            INNER JOIN ride_status_snapshots rss ON r.ride_id = rss.ride_id
            INNER JOIN park_activity_snapshots pas ON p.park_id = pas.park_id
                AND DATE_FORMAT(pas.recorded_at, '%Y-%m-%d %H:%i') = DATE_FORMAT(rss.recorded_at, '%Y-%m-%d %H:%i')
            LEFT JOIN rides_that_operated rto ON r.ride_id = rto.ride_id
            LEFT JOIN operating_snapshots os ON r.ride_id = os.ride_id
            WHERE rss.recorded_at >= :start_utc AND rss.recorded_at < :end_utc
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
            "end_utc": end_utc,
            "limit": limit
        })
        return [dict(row._mapping) for row in result]
