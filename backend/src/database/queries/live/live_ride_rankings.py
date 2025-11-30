"""
Live Ride Rankings Query
========================

Endpoint: GET /api/rides/downtime?period=today
UI Location: Rides tab → Downtime Rankings (today)

Returns rides ranked by current-day downtime from pre-aggregated daily stats.

Database Tables:
- rides (ride metadata)
- parks (park metadata)
- ride_classifications (tier weights)
- ride_daily_stats (pre-aggregated daily statistics)

Performance: Uses pre-aggregated tables for <100ms response time.

Note: Requires hourly aggregation job to keep today's data fresh.
The aggregation script is: scripts/aggregate_daily.py --date YYYY-MM-DD
"""

from datetime import date, datetime
from typing import List, Dict, Any

from sqlalchemy import text
from sqlalchemy.engine import Connection

from utils.timezone import get_today_pacific


class LiveRideRankingsQuery:
    """
    Query handler for live (today) ride rankings.

    Uses pre-aggregated ride_daily_stats for fast queries.
    """

    def __init__(self, connection: Connection):
        self.conn = connection

    def get_rankings(
        self,
        filter_disney_universal: bool = False,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """
        Get live ride rankings for today from pre-aggregated stats.

        Args:
            filter_disney_universal: Only Disney/Universal parks
            limit: Maximum results

        Returns:
            List of rides ranked by downtime (descending)
        """
        today = get_today_pacific()

        # Use raw SQL for optimal performance with the pre-aggregated tables
        query = text("""
            SELECT
                r.ride_id,
                r.name AS ride_name,
                p.name AS park_name,
                p.park_id,
                rc.tier,
                ROUND(rds.downtime_minutes / 60.0, 2) AS total_downtime_hours,
                COALESCE(rds.uptime_percentage, 0) AS uptime_percentage
            FROM ride_daily_stats rds
            INNER JOIN rides r ON rds.ride_id = r.ride_id
            INNER JOIN parks p ON r.park_id = p.park_id
            LEFT JOIN ride_classifications rc ON r.ride_id = rc.ride_id
            WHERE rds.stat_date = :today
            AND r.is_active = 1
            AND r.category = 'ATTRACTION'
            AND p.is_active = 1
            AND rds.downtime_minutes > 0
            {filter_clause}
            ORDER BY rds.downtime_minutes DESC
            LIMIT :limit
        """.format(
            filter_clause="AND (p.is_disney = 1 OR p.is_universal = 1)" if filter_disney_universal else ""
        ))

        result = self.conn.execute(query, {"today": today, "limit": limit})
        return [dict(row._mapping) for row in result]
