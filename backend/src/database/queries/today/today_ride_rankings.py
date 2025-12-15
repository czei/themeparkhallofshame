"""
Today Ride Rankings Query (Cumulative)
======================================

Endpoint: GET /api/rides/downtime?period=today
UI Location: Rides tab → Downtime Rankings (today - cumulative)

Returns rides ranked by CUMULATIVE downtime from midnight Pacific to now.

PERFORMANCE UPDATE (Dec 2025):
- Switched to pre-aggregated ride_hourly_stats (fast, indexed) to avoid
  scanning raw ride_status_snapshots for the entire day.
- Only a tiny "latest status" subquery hits ride_status_snapshots to show the
  current badge; the heavy aggregation now stays on ride_hourly_stats.

Database Tables:
- ride_hourly_stats (pre-aggregated downtime/uptime per hour)
- rides (ride metadata)
- parks (park metadata)
- ride_classifications (tier weights)
- ride_status_snapshots (latest-only subquery for current badge)

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


class TodayRideRankingsQuery:
    """
    Query handler for today's CUMULATIVE ride rankings using pre-aggregated
    ride_hourly_stats (fast path).
    """

    def __init__(self, connection: Connection):
        self.conn = connection

    def get_rankings(
        self,
        filter_disney_universal: bool = False,
        limit: int = 50,
        sort_by: str = "downtime_hours",
    ) -> List[Dict[str, Any]]:
        """
        Get cumulative ride rankings from midnight Pacific to now.

        Args:
            filter_disney_universal: Only Disney/Universal parks
            limit: Maximum results
            sort_by: Sort column (downtime_hours, uptime_percentage,
                     current_is_open, trend_percentage)

        Returns:
            List of rides ranked by cumulative downtime hours (descending)
        """
        # Get time range from midnight Pacific to now (UTC)
        start_utc, now_utc = get_today_range_to_now_utc()

        filter_clause = f"AND {RideFilterSQL.disney_universal_filter('p')}" if filter_disney_universal else ""

        # Latest status subqueries (tiny, bounded by live window)
        current_status_sq = RideStatusSQL.current_status_subquery(
            "r.ride_id", include_time_window=True, park_id_expr="r.park_id"
        )
        current_is_open_sq = RideStatusSQL.current_is_open_subquery(
            "r.ride_id", include_time_window=True, park_id_expr="r.park_id"
        )
        park_is_open_sq = ParkStatusSQL.park_is_open_subquery("p.park_id")

        # Determine sort column/direction
        sort_map = {
            "downtime_hours": "downtime_hours DESC",
            "uptime_percentage": "uptime_percentage ASC",
            "current_is_open": "current_is_open ASC",  # down/closed first
            "trend_percentage": "trend_percentage DESC",
        }
        sort_clause = sort_map.get(sort_by, "downtime_hours DESC")

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

                -- Cumulative downtime from pre-aggregated hourly stats
                ROUND(SUM(rhs.downtime_hours), 2) AS downtime_hours,

                -- Uptime percentage based on aggregated snapshots
                ROUND(
                    100 - (SUM(rhs.down_snapshots) * 100.0 / NULLIF(SUM(rhs.snapshot_count), 0)),
                    1
                ) AS uptime_percentage,

                -- Current status (latest-only subquery for badge/sorting)
                {current_status_sq},
                {current_is_open_sq},
                {park_is_open_sq},

                -- Trend placeholder (not available for partial day)
                NULL AS trend_percentage

            FROM ride_hourly_stats rhs
            INNER JOIN rides r ON rhs.ride_id = r.ride_id
            INNER JOIN parks p ON rhs.park_id = p.park_id
            LEFT JOIN ride_classifications rc ON r.ride_id = rc.ride_id
            WHERE rhs.hour_start_utc >= :start_utc AND rhs.hour_start_utc < :now_utc
                AND r.is_active = TRUE
                AND r.category = 'ATTRACTION'
                AND p.is_active = TRUE
                {filter_clause}
            GROUP BY r.ride_id, r.name, p.name, p.park_id, p.city, p.state_province, rc.tier
            HAVING SUM(CASE WHEN rhs.ride_operated THEN 1 ELSE 0 END) > 0
                AND downtime_hours > 0
            ORDER BY {sort_clause}
            LIMIT :limit
        """)

        result = self.conn.execute(query, {
            "start_utc": start_utc,
            "now_utc": now_utc,
            "limit": limit
        })
        return [dict(row._mapping) for row in result]
