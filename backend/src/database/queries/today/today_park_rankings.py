"""
Today Park Rankings Query (Average Shame Score)
===============================================

Endpoint: GET /api/parks/downtime?period=today
UI Location: Parks tab → Downtime Rankings (today)

Returns parks ranked by AVERAGE shame score from midnight Pacific to now.

SHAME SCORE CALCULATION:
- LIVE: Instantaneous shame = (sum of weights of down rides) / total_park_weight × 10
- TODAY: Average of instantaneous shame scores across today's hourly stats

This makes TODAY comparable to LIVE - both on the same 0-100 scale representing
"percentage of weighted capacity that was down".

Performance Optimization (2025-12):
====================================
This query uses ONLY pre-aggregated tables (park_hourly_stats, park_live_rankings).
It does NOT query raw snapshot tables, which eliminates the slow DATE_FORMAT joins
that were causing >2 minute query times.

Database Tables:
- park_hourly_stats (pre-aggregated hourly data with shame_score, downtime)
- park_live_rankings (current live status for rides_down, park_is_open)
- parks (park metadata)

Single Source of Truth:
- Hourly aggregation: scripts/aggregate_hourly.py
- Live aggregation: scripts/aggregate_live_rankings.py
"""

from typing import List, Dict, Any
from datetime import datetime

from sqlalchemy import text
from sqlalchemy.engine import Connection

from utils.timezone import get_today_pacific, get_pacific_day_range_utc
from utils.sql_helpers import RideFilterSQL


class TodayParkRankingsQuery:
    """
    Query handler for today's park rankings using AVERAGE shame score.

    Uses ONLY pre-aggregated tables for instant performance (<50ms).
    No raw snapshot queries, no DATE_FORMAT joins.
    """

    def __init__(self, connection: Connection):
        self.conn = connection

    def get_rankings(
        self,
        filter_disney_universal: bool = False,
        limit: int = 50,
        sort_by: str = "shame_score",
    ) -> List[Dict[str, Any]]:
        """
        Get park rankings from midnight Pacific to now using AVERAGE shame score.

        Uses pre-aggregated park_hourly_stats table for complete hours,
        combined with park_live_rankings for current status.

        Args:
            filter_disney_universal: Only Disney/Universal parks
            limit: Maximum results
            sort_by: Sort field (shame_score or total_downtime_hours)

        Returns:
            List of parks ranked by average shame_score (descending)
        """
        # Get Pacific day boundaries in UTC
        today = get_today_pacific()
        start_utc, end_utc = get_pacific_day_range_utc(today)

        # Build filter clause
        filter_clause = f"AND {RideFilterSQL.disney_universal_filter('p')}" if filter_disney_universal else ""

        # Determine sort column
        sort_column = "shame_score" if sort_by == "shame_score" else "total_downtime_hours"

        # Simple, fast query using only pre-aggregated tables
        # NO raw snapshot queries, NO DATE_FORMAT joins
        query = text(f"""
            SELECT
                p.park_id,
                p.queue_times_id,
                p.name AS park_name,
                CONCAT(p.city, ', ', p.state_province) AS location,

                -- Shame score: average across today's hourly stats
                ROUND(AVG(phs.shame_score), 1) AS shame_score,

                -- Total downtime hours: sum across today
                ROUND(SUM(phs.total_downtime_hours), 2) AS total_downtime_hours,

                -- Weighted downtime hours: sum across today
                ROUND(SUM(phs.weighted_downtime_hours), 2) AS weighted_downtime_hours,

                -- Rides currently down: from live rankings (current status)
                COALESCE(plr.rides_down, 0) AS rides_down,

                -- Park is open: from live rankings (current status)
                COALESCE(plr.park_is_open, 0) AS park_is_open,

                -- Uptime percentage: calculated from hourly aggregates
                ROUND(
                    100.0 * SUM(phs.rides_operating) /
                    NULLIF(SUM(phs.rides_operating) + SUM(phs.rides_down), 0),
                    1
                ) AS uptime_percentage

            FROM park_hourly_stats phs
            INNER JOIN parks p ON phs.park_id = p.park_id
            LEFT JOIN park_live_rankings plr ON p.park_id = plr.park_id
            WHERE phs.hour_start_utc >= :start_utc
              AND phs.hour_start_utc < :end_utc
              AND p.is_active = TRUE
              {filter_clause}
            GROUP BY p.park_id, p.queue_times_id, p.name, p.city, p.state_province,
                     plr.rides_down, plr.park_is_open
            HAVING AVG(phs.shame_score) > 0
            ORDER BY {sort_column} DESC
            LIMIT :limit
        """)

        result = self.conn.execute(query, {
            "start_utc": start_utc,
            "end_utc": end_utc,
            "limit": limit
        })
        return [dict(row._mapping) for row in result]
