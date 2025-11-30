"""
Live Park Rankings Query
========================

Endpoint: GET /api/parks/downtime?period=today
UI Location: Parks tab → Downtime Rankings (today)

Returns parks ranked by current-day downtime from pre-aggregated daily stats.

Database Tables:
- parks (park metadata)
- park_daily_stats (pre-aggregated daily statistics)
- ride_daily_stats (for calculating shame score with tier weights)
- ride_classifications (tier weights)

Performance: Uses pre-aggregated tables for <100ms response time.

Note: Requires hourly aggregation job to keep today's data fresh.
The aggregation script is: scripts/aggregate_daily.py --date YYYY-MM-DD
"""

from datetime import date, datetime
from typing import List, Dict, Any

from sqlalchemy import select, func, and_, or_, text
from sqlalchemy.engine import Connection

from database.schema import parks, rides, ride_classifications
from database.queries.builders import Filters
from utils.timezone import get_today_pacific


class LiveParkRankingsQuery:
    """
    Query handler for live (today) park rankings.

    Uses pre-aggregated park_daily_stats for fast queries.
    """

    def __init__(self, connection: Connection):
        self.conn = connection

    def get_rankings(
        self,
        filter_disney_universal: bool = False,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """
        Get live park rankings for today from pre-aggregated stats.

        Args:
            filter_disney_universal: Only Disney/Universal parks
            limit: Maximum results

        Returns:
            List of parks ranked by shame_score (descending)
        """
        today = get_today_pacific()

        # Use raw SQL for optimal performance with the pre-aggregated tables
        # Calculate shame_score from ride-level data with tier weights
        query = text("""
            SELECT
                p.park_id,
                p.name AS park_name,
                CONCAT(p.city, ', ', p.state_province) AS location,
                COALESCE(pds.total_downtime_hours, 0) AS total_downtime_hours,
                COALESCE(
                    ROUND(
                        SUM((rds.downtime_minutes / 60.0) * COALESCE(rc.weight, 2)) /
                        NULLIF(SUM(COALESCE(rc.weight, 2)), 0),
                        2
                    ),
                    0
                ) AS shame_score,
                COALESCE(pds.rides_with_downtime, 0) AS affected_rides_count
            FROM parks p
            INNER JOIN park_daily_stats pds ON p.park_id = pds.park_id
            LEFT JOIN rides r ON p.park_id = r.park_id AND r.is_active = 1 AND r.category = 'ATTRACTION'
            LEFT JOIN ride_daily_stats rds ON r.ride_id = rds.ride_id AND rds.stat_date = :today
            LEFT JOIN ride_classifications rc ON r.ride_id = rc.ride_id
            WHERE pds.stat_date = :today
            AND p.is_active = 1
            AND pds.total_downtime_hours > 0
            {filter_clause}
            GROUP BY p.park_id, p.name, p.city, p.state_province,
                     pds.total_downtime_hours, pds.rides_with_downtime
            ORDER BY shame_score DESC
            LIMIT :limit
        """.format(
            filter_clause="AND (p.is_disney = 1 OR p.is_universal = 1)" if filter_disney_universal else ""
        ))

        result = self.conn.execute(query, {"today": today, "limit": limit})
        return [dict(row._mapping) for row in result]
