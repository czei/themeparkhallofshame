"""
Yesterday Park Rankings Query
==============================

Endpoint: GET /api/parks/rankings?period=yesterday
UI Location: Parks tab → Yesterday Rankings

Returns parks ranked by AVERAGE shame score from the full previous Pacific day
(midnight to midnight).

Performance Optimization (2025-12):
====================================
This query uses pre-aggregated park_hourly_stats for fast performance.
It does NOT query raw snapshot tables.

Database Tables:
- park_hourly_stats (pre-aggregated hourly data)
- parks (park metadata)

Single Source of Truth:
- Hourly aggregation: scripts/aggregate_hourly.py
"""

from typing import List, Dict, Any
from datetime import timedelta

from sqlalchemy import text
from sqlalchemy.engine import Connection

from utils.timezone import get_today_pacific, get_pacific_day_range_utc
from utils.sql_helpers import RideFilterSQL, ParkStatusSQL


class YesterdayParkRankingsQuery:
    """
    Query handler for YESTERDAY park rankings using pre-aggregated hourly stats.

    Uses ONLY pre-aggregated tables for instant performance (<50ms).
    YESTERDAY data is immutable and highly cacheable.
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
        Get park rankings for the full previous Pacific day using AVERAGE shame score.

        Uses pre-aggregated park_hourly_stats table for fast performance.

        Args:
            filter_disney_universal: Only Disney/Universal parks
            limit: Maximum results
            sort_by: Sort field (shame_score, total_downtime_hours, uptime_percentage, rides_down)

        Returns:
            List of parks ranked by the specified sort field
        """
        # Get time range for yesterday (full previous Pacific day)
        today = get_today_pacific()
        yesterday = today - timedelta(days=1)
        start_utc, end_utc = get_pacific_day_range_utc(yesterday)

        # Determine sort column and direction based on parameter
        sort_column_map = {
            "total_downtime_hours": "total_downtime_hours",
            "uptime_percentage": "uptime_percentage",
            "rides_down": "rides_down",
            "shame_score": "shame_score"
        }
        sort_column = sort_column_map.get(sort_by, "shame_score")
        # Uptime sorts ascending (higher is better), others sort descending (higher is worse)
        sort_direction = "ASC" if sort_column == "uptime_percentage" else "DESC"

        # Use centralized SQL helpers for consistent logic
        filter_clause = f"AND {RideFilterSQL.disney_universal_filter('p')}" if filter_disney_universal else ""
        park_is_open_sq = ParkStatusSQL.park_is_open_subquery("p.park_id")

        # Fast query using pre-aggregated park_hourly_stats
        query = text(f"""
            SELECT
                p.park_id,
                p.queue_times_id,
                p.name AS park_name,
                CONCAT(p.city, ', ', p.state_province) AS location,

                -- Shame score: average across yesterday's hourly stats
                ROUND(AVG(phs.shame_score), 1) AS shame_score,

                -- Total downtime hours: sum across yesterday
                ROUND(SUM(phs.total_downtime_hours), 2) AS total_downtime_hours,

                -- Weighted downtime hours: sum across yesterday
                ROUND(SUM(phs.weighted_downtime_hours), 2) AS weighted_downtime_hours,

                -- Uptime percentage: calculated from hourly aggregates
                ROUND(
                    100.0 * SUM(phs.rides_operating) /
                    NULLIF(SUM(phs.rides_operating) + SUM(phs.rides_down), 0),
                    1
                ) AS uptime_percentage,

                -- Rides down: max concurrent across yesterday
                MAX(phs.rides_down) AS rides_down,

                -- Park operating status (current - may differ from yesterday's status)
                {park_is_open_sq}

            FROM park_hourly_stats phs
            INNER JOIN parks p ON phs.park_id = p.park_id
            WHERE phs.hour_start_utc >= :start_utc
              AND phs.hour_start_utc < :end_utc
              AND p.is_active = TRUE
              {filter_clause}
            GROUP BY p.park_id, p.queue_times_id, p.name, p.city, p.state_province
            HAVING AVG(phs.shame_score) > 0
            ORDER BY {sort_column} {sort_direction}
            LIMIT :limit
        """)

        result = self.conn.execute(query, {
            "start_utc": start_utc,
            "end_utc": end_utc,
            "limit": limit
        })
        return [dict(row._mapping) for row in result]
