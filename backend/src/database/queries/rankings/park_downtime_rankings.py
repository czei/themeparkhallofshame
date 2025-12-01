"""
Park Downtime Rankings Query
============================

Endpoint: GET /api/parks/downtime?period=7days|30days
UI Location: Parks tab → Downtime Rankings table

Returns parks ranked by shame_score (weighted downtime per unit weight).
Higher shame_score = more downtime relative to park's ride portfolio.

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
    "trend_percentage": null
}
"""

from datetime import date, timedelta
from typing import List, Dict, Any, Optional

from sqlalchemy import select, func, and_, case
from sqlalchemy.engine import Connection

from database.schema import (
    parks,
    rides,
    ride_classifications,
    park_daily_stats,
    ride_daily_stats,
)
from database.queries.builders import Filters, ParkWeightsCTE, WeightedDowntimeCTE


# =============================================================================
# SHAME SCORE CALCULATION
# =============================================================================
# Formula: shame_score = weighted_downtime_hours / total_park_weight
#
# Example:
#   Park with Tier-1 ride (3 hours down) + Tier-3 ride (1 hour down):
#   Weighted = (3 × 3) + (1 × 1) = 10 hours
#   Weight = 3 + 1 = 4
#   Shame = 10 / 4 = 2.5
#
# Higher score = worse performance relative to park size
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
    ) -> List[Dict[str, Any]]:
        """
        Get park rankings for the last 7 days.

        Called by: parks.get_park_downtime_rankings() when period='7days'

        Args:
            filter_disney_universal: Only Disney/Universal parks
            limit: Maximum results

        Returns:
            List of parks ranked by shame_score (descending)
        """
        end_date = date.today()
        start_date = end_date - timedelta(days=6)

        return self._get_rankings(
            start_date=start_date,
            end_date=end_date,
            filter_disney_universal=filter_disney_universal,
            limit=limit,
        )

    def get_monthly(
        self,
        filter_disney_universal: bool = False,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """
        Get park rankings for the last 30 days.

        Called by: parks.get_park_downtime_rankings() when period='30days'

        Args:
            filter_disney_universal: Only Disney/Universal parks
            limit: Maximum results

        Returns:
            List of parks ranked by shame_score (descending)
        """
        end_date = date.today()
        start_date = end_date - timedelta(days=29)

        return self._get_rankings(
            start_date=start_date,
            end_date=end_date,
            filter_disney_universal=filter_disney_universal,
            limit=limit,
        )

    def _get_rankings(
        self,
        start_date: date,
        end_date: date,
        filter_disney_universal: bool = False,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """
        Internal method to build and execute rankings query.

        Args:
            start_date: Start of date range
            end_date: End of date range
            filter_disney_universal: Only Disney/Universal parks
            limit: Maximum results

        Returns:
            List of park ranking dictionaries
        """
        # Build CTEs for weight and downtime calculations
        pw = ParkWeightsCTE.build(filter_disney_universal=filter_disney_universal)
        wd = WeightedDowntimeCTE.from_daily_stats(
            start_date=start_date,
            end_date=end_date,
            filter_disney_universal=filter_disney_universal,
        )

        # Build filter conditions
        conditions = [
            parks.c.is_active == True,
            park_daily_stats.c.stat_date >= start_date,
            park_daily_stats.c.stat_date <= end_date,
            park_daily_stats.c.operating_hours_minutes > 0,
        ]

        if filter_disney_universal:
            conditions.append(Filters.disney_universal(parks))

        # Main query
        stmt = (
            select(
                parks.c.park_id,
                parks.c.name.label("park_name"),
                func.concat(parks.c.city, ", ", parks.c.state_province).label("location"),
                func.round(func.sum(park_daily_stats.c.total_downtime_hours), 2).label(
                    "total_downtime_hours"
                ),
                func.round(
                    (wd.c.total_weighted_downtime_hours / func.nullif(pw.c.total_park_weight, 0)) * 10,
                    1,
                ).label("shame_score"),
                func.max(park_daily_stats.c.rides_with_downtime).label("affected_rides_count"),
                func.round(func.avg(park_daily_stats.c.avg_uptime_percentage), 2).label(
                    "uptime_percentage"
                ),
                # Trend not available for aggregated queries
                func.cast(None, parks.c.park_id.type).label("trend_percentage"),
            )
            .select_from(
                parks.join(park_daily_stats, parks.c.park_id == park_daily_stats.c.park_id)
                .outerjoin(pw, parks.c.park_id == pw.c.park_id)
                .outerjoin(wd, parks.c.park_id == wd.c.park_id)
            )
            .where(and_(*conditions))
            .group_by(
                parks.c.park_id,
                parks.c.name,
                parks.c.city,
                parks.c.state_province,
                pw.c.total_park_weight,
                wd.c.total_weighted_downtime_hours,
            )
            .having(func.sum(park_daily_stats.c.total_downtime_hours) > 0)
            .order_by(
                func.round(
                    (wd.c.total_weighted_downtime_hours / func.nullif(pw.c.total_park_weight, 0)) * 10,
                    1,
                ).desc()
            )
            .limit(limit)
        )

        result = self.conn.execute(stmt)
        return [dict(row._mapping) for row in result]
