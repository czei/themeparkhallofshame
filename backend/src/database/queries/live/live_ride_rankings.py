"""
Live Ride Rankings Query
========================

Endpoint: GET /api/rides/downtime?period=today
UI Location: Rides tab → Downtime Rankings (today)

Returns rides ranked by current-day downtime from real-time snapshots.

NOTE: This class is currently bypassed for performance. The routes use
StatsRepository.get_ride_live_downtime_rankings() instead, which uses
the same centralized SQL helpers but with optimized CTEs.

Database Tables:
- rides (ride metadata)
- parks (park metadata)
- ride_classifications (tier weights)
- ride_status_snapshots (real-time status)
- park_activity_snapshots (park open status)

Single Source of Truth:
- Formulas: utils/metrics.py
- SQL Helpers: utils/sql_helpers.py (used here)
- Python calculations: utils/metrics.py

Performance: Uses raw SQL with centralized helpers for consistency.
"""

from typing import List, Dict, Any

from sqlalchemy import text
from sqlalchemy.engine import Connection

from utils.timezone import get_today_pacific, get_pacific_day_range_utc
from utils.sql_helpers import (
    RideStatusSQL,
    ParkStatusSQL,
    DowntimeSQL,
    UptimeSQL,
    RideFilterSQL,
)


class LiveRideRankingsQuery:
    """
    Query handler for live (today) ride rankings.

    Uses centralized SQL helpers from utils/sql_helpers.py to ensure
    consistent calculations across all queries.

    NOTE: For production use, prefer StatsRepository.get_ride_live_downtime_rankings()
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
        Get live ride rankings for today from real-time snapshots.

        Uses centralized SQL helpers for consistent status logic.

        Args:
            filter_disney_universal: Only Disney/Universal parks
            limit: Maximum results

        Returns:
            List of rides ranked by downtime hours (descending)
        """
        # Get Pacific day bounds in UTC
        today = get_today_pacific()
        start_utc, end_utc = get_pacific_day_range_utc(today)

        # Use centralized SQL helpers for consistent logic
        filter_clause = f"AND {RideFilterSQL.disney_universal_filter('p')}" if filter_disney_universal else ""
        is_down = RideStatusSQL.is_down("rss", parks_alias="p")
        park_open = ParkStatusSQL.park_appears_open_filter("pas")
        # Use schedule-based filtering (not heuristic) for more accurate downtime
        # This fixes the bug where park_appears_open heuristic marked parks as open
        # before official opening time due to test rides operating
        # PARK-TYPE AWARE: Disney/Universal only count DOWN status (not CLOSED)
        downtime_hours = DowntimeSQL.downtime_hours_rounded("rss", "pas", park_id_expr="p.park_id", parks_alias="p")
        uptime_pct = UptimeSQL.uptime_percentage("rss", "pas")
        current_status_sq = RideStatusSQL.current_status_subquery("r.ride_id", include_time_window=True, park_id_expr="r.park_id")
        current_is_open_sq = RideStatusSQL.current_is_open_subquery("r.ride_id", include_time_window=True, park_id_expr="r.park_id")
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

                -- Total downtime hours (using centralized helper)
                -- Formula from utils/metrics.py: calculate_downtime_hours()
                {downtime_hours} AS downtime_hours,

                -- Uptime percentage (using centralized helper)
                -- Formula from utils/metrics.py: calculate_uptime_percentage()
                {uptime_pct} AS uptime_percentage,

                -- Current status using centralized helper with park awareness
                {current_status_sq},
                {current_is_open_sq},
                {park_is_open_sq},

                -- Wait time data (live)
                MAX(rss.wait_time) AS peak_wait_time,
                ROUND(AVG(CASE WHEN rss.wait_time > 0 THEN rss.wait_time END), 0) AS avg_wait_time

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
