"""
Fast Live Park Rankings Query
=============================

Endpoint: GET /api/parks/downtime?period=live
UI Location: Parks tab → Downtime Rankings (Live)

Returns parks ranked by INSTANTANEOUS current status from the pre-aggregated
`park_live_rankings` table. This provides true "live" data - what is down RIGHT NOW.

Performance: Uses ONLY the pre-aggregated park_live_rankings table for instant
performance (<10ms). No raw snapshot queries, no DATE_FORMAT joins.

Database Tables:
- park_live_rankings (pre-aggregated current state, updated every 5 minutes)
- parks (park metadata for queue_times_id)

Single Source of Truth:
- Live aggregation: scripts/aggregate_live_rankings.py
"""

from typing import List, Dict, Any

from sqlalchemy import text
from sqlalchemy.engine import Connection

from utils.sql_helpers import RideFilterSQL


class FastLiveParkRankingsQuery:
    """
    Query handler for TRUE live park rankings using pre-aggregated data.

    Uses ONLY the park_live_rankings cache table for instant performance (<10ms).
    This provides INSTANTANEOUS current state - what is down RIGHT NOW.
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
        Get live park rankings from pre-aggregated cache table.

        Args:
            filter_disney_universal: Only Disney/Universal parks
            limit: Maximum results
            sort_by: Sort field (shame_score or total_downtime_hours)

        Returns:
            List of parks ranked by shame_score (descending)
        """
        # Build filter clause
        filter_clause = f"AND {RideFilterSQL.disney_universal_filter('p')}" if filter_disney_universal else ""

        # Determine sort column
        sort_column = "plr.shame_score" if sort_by == "shame_score" else "plr.total_downtime_hours"

        # Simple, fast query using only pre-aggregated table
        query = text(f"""
            SELECT
                plr.park_id,
                p.queue_times_id,
                plr.park_name,
                CONCAT(p.city, ', ', p.state_province) AS location,

                -- Instantaneous shame score (current state)
                plr.shame_score,

                -- Total downtime hours for today so far
                COALESCE(plr.total_downtime_hours, 0) AS total_downtime_hours,

                -- Weighted downtime hours for today so far
                COALESCE(plr.weighted_downtime_hours, 0) AS weighted_downtime_hours,

                -- Rides currently down RIGHT NOW
                plr.rides_down,

                -- Park is open (current state)
                plr.park_is_open,

                -- Total rides and uptime percentage (calculated)
                plr.total_rides,
                CASE
                    WHEN plr.total_rides > 0 THEN
                        ROUND(100.0 * (plr.total_rides - plr.rides_down) / plr.total_rides, 1)
                    ELSE 100.0
                END AS uptime_percentage

            FROM park_live_rankings plr
            INNER JOIN parks p ON plr.park_id = p.park_id
            WHERE p.is_active = TRUE
              AND plr.shame_score > 0
              {filter_clause}
            ORDER BY {sort_column} DESC
            LIMIT :limit
        """)

        result = self.conn.execute(query, {"limit": limit})
        return [dict(row._mapping) for row in result]
