"""
Ride Downtime Rankings Query
============================

Endpoint: GET /api/rides/downtime?period=7days|30days
UI Location: Rides tab → Downtime Rankings table

Returns rides ranked by downtime percentage during operating hours.
Includes tier classification for context.

Database Tables:
- rides (ride metadata)
- parks (park metadata for location/filter)
- ride_classifications (tier weights)
- ride_daily_stats (aggregated daily downtime data)

How to Modify This Query:
1. To add a new column: Add to the select() in _get_rankings()
2. To change the ranking order: Modify the order_by() expression
3. To add a new filter: Add parameter and extend the where() clause

Example Response:
{
    "ride_id": 42,
    "ride_name": "Space Mountain",
    "park_name": "Magic Kingdom",
    "park_id": 1,
    "tier": 1,
    "total_downtime_hours": 8.5,
    "uptime_percentage": 89.2,
    "status_changes": 12,
    "trend_percentage": -5.2
}
"""

from datetime import date, timedelta
from typing import List, Dict, Any

from sqlalchemy import select, func, and_, case
from sqlalchemy.engine import Connection

from database.schema import (
    parks,
    rides,
    ride_classifications,
    ride_daily_stats,
)
from database.queries.builders import Filters


class RideDowntimeRankingsQuery:
    """
    Query handler for ride downtime rankings.

    Methods:
        get_weekly(): 7-day period from daily stats
        get_monthly(): 30-day period from daily stats

    For live (today) rankings, use live/live_ride_rankings.py instead.
    """

    def __init__(self, connection: Connection):
        self.conn = connection

    def get_weekly(
        self,
        filter_disney_universal: bool = False,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """
        Get ride rankings for the last 7 days.

        Args:
            filter_disney_universal: Only Disney/Universal parks
            limit: Maximum results

        Returns:
            List of rides ranked by downtime (descending)
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
        Get ride rankings for the last 30 days.

        Args:
            filter_disney_universal: Only Disney/Universal parks
            limit: Maximum results

        Returns:
            List of rides ranked by downtime (descending)
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
        """
        conditions = [
            rides.c.is_active == True,
            rides.c.category == "ATTRACTION",
            parks.c.is_active == True,
            ride_daily_stats.c.stat_date >= start_date,
            ride_daily_stats.c.stat_date <= end_date,
        ]

        if filter_disney_universal:
            conditions.append(Filters.disney_universal(parks))

        stmt = (
            select(
                rides.c.ride_id,
                rides.c.name.label("ride_name"),
                parks.c.name.label("park_name"),
                parks.c.park_id,
                ride_classifications.c.tier,
                func.round(
                    func.sum(ride_daily_stats.c.downtime_minutes) / 60.0, 2
                ).label("total_downtime_hours"),
                func.round(func.avg(ride_daily_stats.c.uptime_percentage), 2).label(
                    "uptime_percentage"
                ),
                func.sum(ride_daily_stats.c.status_changes).label("status_changes"),
                # Trend not available for aggregated queries
                func.cast(None, rides.c.ride_id.type).label("trend_percentage"),
            )
            .select_from(
                rides.join(parks, rides.c.park_id == parks.c.park_id)
                .join(ride_daily_stats, rides.c.ride_id == ride_daily_stats.c.ride_id)
                .outerjoin(
                    ride_classifications,
                    rides.c.ride_id == ride_classifications.c.ride_id,
                )
            )
            .where(and_(*conditions))
            .group_by(
                rides.c.ride_id,
                rides.c.name,
                parks.c.name,
                parks.c.park_id,
                ride_classifications.c.tier,
            )
            .having(func.sum(ride_daily_stats.c.downtime_minutes) > 0)
            .order_by(func.sum(ride_daily_stats.c.downtime_minutes).desc())
            .limit(limit)
        )

        result = self.conn.execute(stmt)
        return [dict(row._mapping) for row in result]
