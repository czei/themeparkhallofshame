"""
Park Downtime Rankings Query (Average Shame Score)
=================================================

Endpoint: GET /api/parks/downtime?period=last_week|last_month
UI Location: Parks tab → Downtime Rankings table

Returns parks ranked by AVERAGE shame_score across the period.
Higher shame_score = more downtime relative to park's ride portfolio.

SHAME SCORE CALCULATION:
- Average of per-day shame scores across the period
- Per-day shame = (daily_weighted_downtime / total_park_weight) × 10
- This makes LAST_WEEK/LAST_MONTH comparable to LIVE/TODAY (same 0-100 scale)

CALENDAR-BASED PERIODS:
- last_week: Previous complete week (Sunday-Saturday, Pacific Time)
- last_month: Previous complete calendar month (Pacific Time)

These are fixed calendar periods for social media reporting, e.g.:
- "November's least reliable park was X"
- "Last week's worst performer was Y"

Database Tables:
- parks (park metadata)
- park_daily_stats (aggregated daily downtime data)
- rides (ride metadata for tier calculations)
- ride_classifications (tier weights)
- ride_daily_stats (per-ride downtime for weighted calculations)

How to Modify This Query:
1. To add a new column: Add to the select() in _build_rankings_query()
2. To change the ranking formula: Modify the shame_score calculation
3. To add a new filter: Add parameter and extend the where() clause

Example Response:
{
    "park_id": 1,
    "park_name": "Magic Kingdom",
    "location": "Orlando, FL",
    "total_downtime_hours": 12.5,
    "shame_score": 2.45,
    "affected_rides_count": 8,
    "uptime_percentage": 94.5,
    "trend_percentage": null,
    "period_label": "Nov 24-30, 2024"
}
"""

from datetime import date
from typing import List, Dict, Any

from sqlalchemy import text
from sqlalchemy.engine import Connection

from utils.timezone import get_last_week_date_range, get_last_month_date_range


# =============================================================================
# SHAME SCORE CALCULATION
# =============================================================================
# Formula: shame_score = AVG(daily_shame_score)
# Where: daily_shame_score = (daily_weighted_downtime / total_park_weight) × 10
#
# This calculates the AVERAGE daily shame score across the period, making it
# comparable to LIVE/TODAY scores (all on the same 0-100 scale).
#
# Example:
#   Day 1: Tier-1 ride down 3h → weighted = 9h, shame = 9/45 × 10 = 2.0
#   Day 2: Nothing down → weighted = 0h, shame = 0
#   Day 3: Tier-3 ride down 1h → weighted = 1h, shame = 1/45 × 10 = 0.22
#   Average shame = (2.0 + 0 + 0.22) / 3 = 0.74
#
# Higher score = worse average daily performance
# =============================================================================


class ParkDowntimeRankingsQuery:
    """
    Query handler for park downtime rankings.

    Methods:
        get_weekly(): 7-day period from daily stats
        get_monthly(): 30-day period from daily stats

    For live (today) rankings, use live/live_park_rankings.py instead.
    """

    def __init__(self, connection: Connection):
        """
        Initialize with database connection.

        Args:
            connection: SQLAlchemy connection (from get_db_connection())
        """
        self.conn = connection

    def get_weekly(
        self,
        filter_disney_universal: bool = False,
        limit: int = 50,
        sort_by: str = "shame_score",
    ) -> List[Dict[str, Any]]:
        """
        Get park rankings for the previous complete week (Sunday-Saturday).

        Called by: parks.get_park_downtime_rankings() when period='last_week'

        Uses calendar-based periods for social media reporting, e.g.:
        "Last week's worst performing park was Magic Kingdom"

        Args:
            filter_disney_universal: Only Disney/Universal parks
            limit: Maximum results
            sort_by: Column to sort by

        Returns:
            List of parks ranked by specified column, includes period_label
        """
        start_date, end_date, period_label = get_last_week_date_range()

        return self._get_rankings(
            start_date=start_date,
            end_date=end_date,
            period_label=period_label,
            filter_disney_universal=filter_disney_universal,
            limit=limit,
            sort_by=sort_by,
        )

    def get_monthly(
        self,
        filter_disney_universal: bool = False,
        limit: int = 50,
        sort_by: str = "shame_score",
    ) -> List[Dict[str, Any]]:
        """
        Get park rankings for the previous complete calendar month.

        Called by: parks.get_park_downtime_rankings() when period='last_month'

        Uses calendar-based periods for social media reporting, e.g.:
        "November's worst performing park was Magic Kingdom"

        Args:
            filter_disney_universal: Only Disney/Universal parks
            limit: Maximum results
            sort_by: Column to sort by

        Returns:
            List of parks ranked by specified column, includes period_label
        """
        start_date, end_date, period_label = get_last_month_date_range()

        return self._get_rankings(
            start_date=start_date,
            end_date=end_date,
            period_label=period_label,
            filter_disney_universal=filter_disney_universal,
            limit=limit,
            sort_by=sort_by,
        )

    def _get_rankings(
        self,
        start_date: date,
        end_date: date,
        period_label: str = "",
        filter_disney_universal: bool = False,
        limit: int = 50,
        sort_by: str = "shame_score",
    ) -> List[Dict[str, Any]]:
        """
        Internal method to build and execute rankings query.

        Uses AVERAGE daily shame scores to be comparable with LIVE/TODAY.
        For each day, calculates: (daily_weighted_downtime / total_park_weight) × 10
        Then averages these daily scores across the period.

        Args:
            start_date: Start of date range
            end_date: End of date range
            period_label: Human-readable label (e.g., "Nov 24-30, 2024")
            filter_disney_universal: Only Disney/Universal parks
            limit: Maximum results
            sort_by: Column to sort by

        Returns:
            List of park ranking dictionaries with period_label included
        """
        # Build filter clause for Disney/Universal if needed
        filter_clause = "AND (p.is_disney = TRUE OR p.is_universal = TRUE)" if filter_disney_universal else ""

        # Determine sort column
        sort_column = {
            "total_downtime_hours": "total_downtime_hours",
            "uptime_percentage": "uptime_percentage",  # Will use ASC for this
            "rides_down": "rides_down",
        }.get(sort_by, "shame_score")

        # Sort direction - lower uptime is worse, so ASC for that column
        sort_direction = "ASC" if sort_by == "uptime_percentage" else "DESC"

        query = text(f"""
            WITH
            park_weights AS (
                -- Total tier weight for each park (for shame score normalization)
                SELECT
                    p.park_id,
                    SUM(COALESCE(rc.tier_weight, 2)) AS total_park_weight,
                    COUNT(DISTINCT r.ride_id) AS total_rides
                FROM parks p
                INNER JOIN rides r ON p.park_id = r.park_id
                    AND r.is_active = TRUE AND r.category = 'ATTRACTION'
                LEFT JOIN ride_classifications rc ON r.ride_id = rc.ride_id
                WHERE p.is_active = TRUE
                    {filter_clause}
                GROUP BY p.park_id
            ),
            daily_weighted_downtime AS (
                -- Weighted downtime per park per day
                -- weighted_downtime = SUM(downtime_minutes * tier_weight) / 60
                SELECT
                    r.park_id,
                    rds.stat_date,
                    SUM(rds.downtime_minutes / 60.0 * COALESCE(rc.tier_weight, 2)) AS weighted_downtime_hours
                FROM ride_daily_stats rds
                INNER JOIN rides r ON rds.ride_id = r.ride_id
                    AND r.is_active = TRUE AND r.category = 'ATTRACTION'
                INNER JOIN parks p ON r.park_id = p.park_id
                    AND p.is_active = TRUE
                LEFT JOIN ride_classifications rc ON r.ride_id = rc.ride_id
                WHERE rds.stat_date >= :start_date AND rds.stat_date <= :end_date
                    {filter_clause}
                GROUP BY r.park_id, rds.stat_date
            ),
            daily_shame_scores AS (
                -- Per-day shame score = (daily_weighted_downtime / total_park_weight) × 10
                SELECT
                    dwd.park_id,
                    dwd.stat_date,
                    COALESCE(
                        (dwd.weighted_downtime_hours / NULLIF(pw.total_park_weight, 0)) * 10,
                        0
                    ) AS daily_shame_score
                FROM daily_weighted_downtime dwd
                INNER JOIN park_weights pw ON dwd.park_id = pw.park_id
            )
            SELECT
                p.park_id,
                p.name AS park_name,
                CONCAT(p.city, ', ', p.state_province) AS location,

                -- Total downtime hours (sum across period)
                ROUND(SUM(pds.total_downtime_hours), 2) AS total_downtime_hours,

                -- AVERAGE Shame Score = average of per-day shame scores
                -- This makes LAST_WEEK/LAST_MONTH comparable to LIVE/TODAY
                ROUND(
                    (SELECT AVG(dss.daily_shame_score) FROM daily_shame_scores dss WHERE dss.park_id = p.park_id),
                    1
                ) AS shame_score,

                -- Max rides affected on any day (named rides_down for frontend compatibility)
                MAX(pds.rides_with_downtime) AS rides_down,

                -- Average uptime percentage across days
                ROUND(AVG(pds.avg_uptime_percentage), 2) AS uptime_percentage,

                -- Trend not available for aggregated queries
                NULL AS trend_percentage

            FROM parks p
            INNER JOIN park_daily_stats pds ON p.park_id = pds.park_id
            INNER JOIN park_weights pw ON p.park_id = pw.park_id
            WHERE pds.stat_date >= :start_date AND pds.stat_date <= :end_date
                AND p.is_active = TRUE
                AND pds.operating_hours_minutes > 0
                {filter_clause}
            GROUP BY p.park_id, p.name, p.city, p.state_province
            HAVING SUM(pds.total_downtime_hours) > 0
            ORDER BY {sort_column} {sort_direction}
            LIMIT :limit
        """)

        result = self.conn.execute(query, {
            "start_date": start_date,
            "end_date": end_date,
            "limit": limit
        })

        # Add period_label to each result
        rankings = []
        for row in result:
            row_dict = dict(row._mapping)
            if period_label:
                row_dict['period_label'] = period_label
            rankings.append(row_dict)
        return rankings
