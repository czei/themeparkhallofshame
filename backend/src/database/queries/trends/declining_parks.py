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

from sqlalchemy import select, func, and_, or_
from sqlalchemy.orm import Session, aliased

from models import Park, ParkWeeklyStats
from utils.query_helpers import QueryClassBase


DECLINE_THRESHOLD = 5.0


class DecliningParksQuery(QueryClassBase):
    """
    Query for parks showing uptime decline.
    """

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
        prev_week = aliased(ParkWeeklyStats, name="prev_week")

        conditions = [
            Park.is_active == True,
            ParkWeeklyStats.year == year,
            ParkWeeklyStats.week_number == week_number,
            # Positive trend = declining (more downtime)
            ParkWeeklyStats.trend_vs_previous_week > DECLINE_THRESHOLD,
        ]

        if filter_disney_universal:
            conditions.append(or_(Park.is_disney == True, Park.is_universal == True))

        stmt = (
            select(
                Park.park_id,
                Park.queue_times_id,
                Park.name.label("park_name"),
                func.concat(Park.city, ", ", Park.state_province).label("location"),
                ParkWeeklyStats.avg_uptime_percentage.label("current_uptime"),
                func.round(
                    ParkWeeklyStats.avg_uptime_percentage
                    / (1 + ParkWeeklyStats.trend_vs_previous_week / 100),
                    2,
                ).label("previous_uptime"),
                ParkWeeklyStats.trend_vs_previous_week.label("decline_percentage"),
                ParkWeeklyStats.total_downtime_hours.label("current_downtime_hours"),
                func.coalesce(
                    prev_week.total_downtime_hours,
                    0,
                ).label("previous_downtime_hours"),
            )
            .select_from(
                Park.__table__.join(
                    ParkWeeklyStats, Park.park_id == ParkWeeklyStats.park_id
                ).outerjoin(
                    prev_week,
                    and_(
                        prev_week.park_id == ParkWeeklyStats.park_id,
                        prev_week.year == prev_year,
                        prev_week.week_number == prev_week_number,
                    ),
                )
            )
            .where(and_(*conditions))
            .order_by(ParkWeeklyStats.trend_vs_previous_week.desc())  # Most declined first
            .limit(limit)
        )

        return self.execute_and_fetchall(stmt)
