"""
Declining Rides Query
=====================

Endpoint: GET /api/trends?category=rides-declining
UI Location: Trends tab → Rides Declining section

Returns rides with uptime decline >= 5% vs previous period.

Database Tables:
- rides (ride metadata)
- parks (park metadata)
- ride_weekly_stats (trend_vs_previous_week)

Example Response:
{
    "ride_id": 55,
    "ride_name": "Test Track",
    "park_name": "EPCOT",
    "current_uptime": 85.2,
    "previous_uptime": 92.8,
    "decline_percentage": 7.6
}
"""

from datetime import date
from typing import List, Dict, Any

from sqlalchemy import select, func, and_
from sqlalchemy.engine import Connection

from database.schema import parks, rides, ride_weekly_stats
from database.queries.builders import Filters


DECLINE_THRESHOLD = 5.0


class DecliningRidesQuery:
    """
    Query for rides showing uptime decline.
    """

    def __init__(self, connection: Connection):
        self.conn = connection

    def get_declining(
        self,
        period: str = '7days',
        filter_disney_universal: bool = False,
        limit: int = 10,
    ) -> List[Dict[str, Any]]:
        """Get declining rides for the specified period."""
        # Currently all periods use weekly data
        return self.get_weekly(filter_disney_universal, limit)

    def get_weekly(
        self,
        filter_disney_universal: bool = False,
        limit: int = 10,
    ) -> List[Dict[str, Any]]:
        """
        Get rides with declining uptime for current week.
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
            # Positive trend = declining (more downtime)
            ride_weekly_stats.c.trend_vs_previous_week > DECLINE_THRESHOLD,
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
                ride_weekly_stats.c.trend_vs_previous_week.label("decline_percentage"),
            )
            .select_from(
                rides.join(parks, rides.c.park_id == parks.c.park_id).join(
                    ride_weekly_stats, rides.c.ride_id == ride_weekly_stats.c.ride_id
                )
            )
            .where(and_(*conditions))
            .order_by(ride_weekly_stats.c.trend_vs_previous_week.desc())
            .limit(limit)
        )

        result = self.conn.execute(stmt)
        return [dict(row._mapping) for row in result]
