"""
Improving Rides Query
=====================

Endpoint: GET /api/trends?category=rides-improving
UI Location: Trends tab → Rides Improving section

Returns rides with uptime improvement >= 5% vs previous period.

Database Tables:
- rides (ride metadata)
- parks (park metadata)
- ride_weekly_stats (trend_vs_previous_week)

Example Response:
{
    "ride_id": 42,
    "ride_name": "Space Mountain",
    "park_name": "Magic Kingdom",
    "current_uptime": 98.5,
    "previous_uptime": 92.1,
    "improvement_percentage": 6.4
}
"""

from datetime import date
from typing import List, Dict, Any

from sqlalchemy import select, func, and_
from sqlalchemy.engine import Connection

from database.schema import parks, rides, ride_weekly_stats
from database.queries.builders import Filters


IMPROVEMENT_THRESHOLD = 5.0


class ImprovingRidesQuery:
    """
    Query for rides showing uptime improvement.
    """

    def __init__(self, connection: Connection):
        self.conn = connection

    def get_weekly(
        self,
        filter_disney_universal: bool = False,
        limit: int = 10,
    ) -> List[Dict[str, Any]]:
        """
        Get rides with improving uptime for current week.
        """
        today = date.today()
        year = today.year
        week_number = today.isocalendar()[1]

        conditions = [
            rides.c.is_active == True,
            rides.c.category == "ATTRACTION",
            parks.c.is_active == True,
            ride_weekly_stats.c.year == year,
            ride_weekly_stats.c.week_number == week_number,
            # Negative trend = improving (less downtime)
            ride_weekly_stats.c.trend_vs_previous_week < -IMPROVEMENT_THRESHOLD,
        ]

        if filter_disney_universal:
            conditions.append(Filters.disney_universal(parks))

        stmt = (
            select(
                rides.c.ride_id,
                rides.c.name.label("ride_name"),
                parks.c.name.label("park_name"),
                ride_weekly_stats.c.uptime_percentage.label("current_uptime"),
                func.round(
                    ride_weekly_stats.c.uptime_percentage
                    / (1 + ride_weekly_stats.c.trend_vs_previous_week / 100),
                    2,
                ).label("previous_uptime"),
                func.abs(ride_weekly_stats.c.trend_vs_previous_week).label(
                    "improvement_percentage"
                ),
            )
            .select_from(
                rides.join(parks, rides.c.park_id == parks.c.park_id).join(
                    ride_weekly_stats, rides.c.ride_id == ride_weekly_stats.c.ride_id
                )
            )
            .where(and_(*conditions))
            .order_by(ride_weekly_stats.c.trend_vs_previous_week.asc())
            .limit(limit)
        )

        result = self.conn.execute(stmt)
        return [dict(row._mapping) for row in result]
