"""
Declining Parks Query
=====================

Endpoint: GET /api/trends?category=parks-declining
UI Location: Trends tab → Parks Declining section

Returns parks with uptime decline >= 5% vs previous period.

Database Tables:
- parks (park metadata)
- park_weekly_stats (trend_vs_previous_week)

Example Response:
{
    "park_id": 2,
    "park_name": "EPCOT",
    "location": "Orlando, FL",
    "current_uptime": 88.5,
    "previous_uptime": 94.2,
    "decline_percentage": 5.7
}
"""

from datetime import date, timedelta
from typing import List, Dict, Any

from sqlalchemy import select, func, and_
from sqlalchemy.engine import Connection

from database.schema import parks, park_weekly_stats
from database.queries.builders import Filters


DECLINE_THRESHOLD = 5.0


class DecliningParksQuery:
    """
    Query for parks showing uptime decline.
    """

    def __init__(self, connection: Connection):
        self.conn = connection

    def get_declining(
        self,
        period: str = '7days',
        filter_disney_universal: bool = False,
        limit: int = 10,
    ) -> List[Dict[str, Any]]:
        """Get declining parks for the specified period."""
        # Currently all periods use weekly data
        return self.get_weekly(filter_disney_universal, limit)

    def get_weekly(
        self,
        filter_disney_universal: bool = False,
        limit: int = 10,
    ) -> List[Dict[str, Any]]:
        """
        Get parks with declining uptime for current week.
        """
        today = date.today()
        year = today.year
        week_number = today.isocalendar()[1]
        prev_week_date = today - timedelta(weeks=1)
        prev_year = prev_week_date.year
        prev_week_number = prev_week_date.isocalendar()[1]
        prev_week = park_weekly_stats.alias("prev_week")

        conditions = [
            parks.c.is_active == True,
            park_weekly_stats.c.year == year,
            park_weekly_stats.c.week_number == week_number,
            # Positive trend = declining (more downtime)
            park_weekly_stats.c.trend_vs_previous_week > DECLINE_THRESHOLD,
        ]

        if filter_disney_universal:
            conditions.append(Filters.disney_universal(parks))

        stmt = (
            select(
                parks.c.park_id,
                parks.c.queue_times_id,
                parks.c.name.label("park_name"),
                func.concat(parks.c.city, ", ", parks.c.state_province).label("location"),
                park_weekly_stats.c.avg_uptime_percentage.label("current_uptime"),
                func.round(
                    park_weekly_stats.c.avg_uptime_percentage
                    / (1 + park_weekly_stats.c.trend_vs_previous_week / 100),
                    2,
                ).label("previous_uptime"),
                park_weekly_stats.c.trend_vs_previous_week.label("decline_percentage"),
                park_weekly_stats.c.total_downtime_hours.label("current_downtime_hours"),
                func.coalesce(
                    prev_week.c.total_downtime_hours,
                    0,
                ).label("previous_downtime_hours"),
            )
            .select_from(
                parks.join(
                    park_weekly_stats, parks.c.park_id == park_weekly_stats.c.park_id
                ).outerjoin(
                    prev_week,
                    and_(
                        prev_week.c.park_id == park_weekly_stats.c.park_id,
                        prev_week.c.year == prev_year,
                        prev_week.c.week_number == prev_week_number,
                    ),
                )
            )
            .where(and_(*conditions))
            .order_by(park_weekly_stats.c.trend_vs_previous_week.desc())  # Most declined first
            .limit(limit)
        )

        result = self.conn.execute(stmt)
        return [dict(row._mapping) for row in result]
