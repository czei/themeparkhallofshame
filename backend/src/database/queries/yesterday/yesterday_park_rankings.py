"""
Yesterday Park Rankings Query
==============================

Endpoint: GET /api/parks/rankings?period=yesterday
UI Location: Parks tab → Yesterday Rankings

Returns parks ranked by AVERAGE shame score from the full previous Pacific day
(midnight to midnight).

CRITICAL: For YESTERDAY, we use stored shame_scores from park_activity_snapshots
rather than recalculating from ride-level data. This avoids timestamp mismatches
between ride_status_snapshots and park_activity_snapshots which can occur when
snapshots are collected at slightly different times.

Database Tables:
- parks (park metadata)
- park_activity_snapshots (stored shame_scores)

Single Source of Truth:
- Shame scores: Pre-calculated and stored in park_activity_snapshots.shame_score
"""

from typing import List, Dict, Any
from datetime import datetime

from sqlalchemy import text
from sqlalchemy.engine import Connection

from utils.timezone import get_yesterday_range_utc
from utils.sql_helpers import RideFilterSQL, ParkStatusSQL


class YesterdayParkRankingsQuery:
    """
    Query handler for YESTERDAY park rankings using stored shame scores.

    YESTERDAY aggregates PRE-CALCULATED shame scores from park_activity_snapshots.
    Unlike TODAY/LIVE which calculate shame scores in real-time, YESTERDAY uses
    all snapshots from the full previous Pacific day.

    This makes the score comparable to LIVE and TODAY (same 0-100 scale).
    Unlike TODAY, YESTERDAY data is immutable and highly cacheable.
    """

    # Snapshot interval in minutes (for converting snapshot counts to time)
    SNAPSHOT_INTERVAL_MINUTES = 5

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

        For YESTERDAY, we use stored shame_scores from park_activity_snapshots
        rather than recalculating from ride-level data. This avoids issues with
        timestamp mismatches between ride and park snapshot collections.

        Args:
            filter_disney_universal: Only Disney/Universal parks
            limit: Maximum results
            sort_by: Sort field (shame_score, total_downtime_hours, uptime_percentage, rides_down)

        Returns:
            List of parks ranked by the specified sort field
        """
        # Get time range for yesterday (full previous day)
        start_utc, end_utc, label = get_yesterday_range_utc()

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

        # SIMPLIFIED QUERY: Use stored shame_scores from park_activity_snapshots
        # This avoids timestamp mismatch issues with ride-level joins
        query = text(f"""
            SELECT
                p.park_id,
                p.queue_times_id,
                p.name AS park_name,
                CONCAT(p.city, ', ', p.state_province) AS location,

                -- AVERAGE Shame Score: Use stored values from park_activity_snapshots
                -- THE SINGLE SOURCE OF TRUTH - calculated during data collection
                ROUND(
                    AVG(CASE
                        WHEN pas.park_appears_open = TRUE AND pas.shame_score IS NOT NULL
                        THEN pas.shame_score
                    END),
                    1
                ) AS shame_score,

                -- Total downtime hours: Calculate from snapshot counts
                -- {self.SNAPSHOT_INTERVAL_MINUTES} minutes per snapshot
                ROUND(
                    SUM(CASE
                        WHEN pas.park_appears_open = TRUE
                        THEN pas.rides_closed * {self.SNAPSHOT_INTERVAL_MINUTES}.0 / 60.0
                        ELSE 0
                    END),
                    2
                ) AS total_downtime_hours,

                -- Weighted downtime: Not available (would need ride tier weights)
                NULL AS weighted_downtime_hours,

                -- Uptime percentage: Calculate from rides_open vs total_rides_tracked
                ROUND(
                    AVG(CASE
                        WHEN pas.park_appears_open = TRUE AND pas.total_rides_tracked > 0
                        THEN (pas.rides_open * 100.0 / pas.total_rides_tracked)
                    END),
                    1
                ) AS uptime_percentage,

                -- Rides down: Peak number of concurrent closed rides
                -- This is the maximum number of rides closed at any single snapshot,
                -- NOT the total unique rides that experienced downtime during the day
                MAX(CASE
                    WHEN pas.park_appears_open = TRUE
                    THEN pas.rides_closed
                END) AS rides_down,

                -- Park operating status (current - may differ from yesterday's status)
                {park_is_open_sq}

            FROM parks p
            INNER JOIN park_activity_snapshots pas ON p.park_id = pas.park_id
            WHERE pas.recorded_at >= :start_utc AND pas.recorded_at < :end_utc
                AND p.is_active = TRUE
                {filter_clause}
            GROUP BY p.park_id, p.name, p.city, p.state_province
            HAVING shame_score IS NOT NULL AND shame_score > 0
            ORDER BY {sort_column} {sort_direction}
            LIMIT :limit
        """)

        result = self.conn.execute(query, {
            "start_utc": start_utc,
            "end_utc": end_utc,
            "limit": limit
        })
        return [dict(row._mapping) for row in result]
