"""
Ride Wait Time Rankings Query
=============================

Endpoint: GET /api/rides/waittimes?period=7days|30days
UI Location: Rides tab → Wait Times Rankings table

Returns rides ranked by average wait time.

Database Tables:
- rides (ride metadata)
- parks (park metadata)
- ride_daily_stats (aggregated wait time data)

Example Response:
{
    "ride_id": 42,
    "ride_name": "Flight of Passage",
    "park_name": "Animal Kingdom",
    "park_id": 3,
    "avg_wait_time": 85.5,
    "peak_wait_time": 180
}
"""

from datetime import date, timedelta
from typing import List, Dict, Any

from sqlalchemy import select, func, and_
from sqlalchemy.engine import Connection

from database.schema import parks, rides, ride_daily_stats
from database.queries.builders import Filters


class RideWaitTimeRankingsQuery:
    """
    Query handler for ride wait time rankings.
    """

    def __init__(self, connection: Connection):
        self.conn = connection

    def get_weekly(
        self,
        filter_disney_universal: bool = False,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """Get ride wait time rankings for the last 7 days."""
        end_date = date.today()
        start_date = end_date - timedelta(days=6)
        return self._get_rankings(start_date, end_date, filter_disney_universal, limit)

    def get_monthly(
        self,
        filter_disney_universal: bool = False,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """Get ride wait time rankings for the last 30 days."""
        end_date = date.today()
        start_date = end_date - timedelta(days=29)
        return self._get_rankings(start_date, end_date, filter_disney_universal, limit)

    def _get_rankings(
        self,
        start_date: date,
        end_date: date,
        filter_disney_universal: bool = False,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        conditions = [
            rides.c.is_active == True,
            rides.c.category == "ATTRACTION",
            parks.c.is_active == True,
            ride_daily_stats.c.stat_date >= start_date,
            ride_daily_stats.c.stat_date <= end_date,
            ride_daily_stats.c.avg_wait_time.isnot(None),
        ]

        if filter_disney_universal:
            conditions.append(Filters.disney_universal(parks))

        stmt = (
            select(
                rides.c.ride_id,
                rides.c.name.label("ride_name"),
                parks.c.name.label("park_name"),
                parks.c.park_id,
                func.round(func.avg(ride_daily_stats.c.avg_wait_time), 1).label(
                    "avg_wait_time"
                ),
                func.max(ride_daily_stats.c.peak_wait_time).label("peak_wait_time"),
            )
            .select_from(
                rides.join(parks, rides.c.park_id == parks.c.park_id).join(
                    ride_daily_stats, rides.c.ride_id == ride_daily_stats.c.ride_id
                )
            )
            .where(and_(*conditions))
            .group_by(rides.c.ride_id, rides.c.name, parks.c.name, parks.c.park_id)
            .order_by(func.avg(ride_daily_stats.c.avg_wait_time).desc())
            .limit(limit)
        )

        result = self.conn.execute(stmt)
        return [dict(row._mapping) for row in result]
