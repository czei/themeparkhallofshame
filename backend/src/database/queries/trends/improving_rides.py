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

from datetime import date, timedelta
from typing import List, Dict, Any

from sqlalchemy import select, func, and_, or_
from sqlalchemy.orm import Session, aliased

from src.models import Ride, Park, RideWeeklyStats
from src.utils.query_helpers import QueryClassBase


IMPROVEMENT_THRESHOLD = 5.0


class ImprovingRidesQuery(QueryClassBase):
    """
    Query for rides showing uptime improvement.
    """

    def get_improving(
        self,
        period: str = '7days',
        filter_disney_universal: bool = False,
        limit: int = 10,
    ) -> List[Dict[str, Any]]:
        """Get improving rides for the specified period."""
        # Currently all periods use weekly data
        return self.get_weekly(filter_disney_universal, limit)

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
        prev_week_date = today - timedelta(weeks=1)
        prev_year = prev_week_date.year
        prev_week_number = prev_week_date.isocalendar()[1]

        # Create alias for previous week stats
        prev_week = aliased(RideWeeklyStats)

        conditions = [
            Ride.is_active == True,
            Ride.category == "ATTRACTION",
            Park.is_active == True,
            RideWeeklyStats.year == year,
            RideWeeklyStats.week_number == week_number,
            # Negative trend = improving (less downtime)
            RideWeeklyStats.trend_vs_previous_week < -IMPROVEMENT_THRESHOLD,
        ]

        if filter_disney_universal:
            conditions.append(or_(Park.is_disney == True, Park.is_universal == True))

        stmt = (
            select(
                Ride.ride_id,
                Ride.queue_times_id,
                Park.queue_times_id.label("park_queue_times_id"),
                Ride.name.label("ride_name"),
                Park.name.label("park_name"),
                RideWeeklyStats.uptime_percentage.label("current_uptime"),
                func.round(
                    RideWeeklyStats.uptime_percentage
                    / (1 + RideWeeklyStats.trend_vs_previous_week / 100),
                    2,
                ).label("previous_uptime"),
                func.abs(RideWeeklyStats.trend_vs_previous_week).label(
                    "improvement_percentage"
                ),
                (RideWeeklyStats.downtime_minutes / 60.0).label("current_downtime_hours"),
                (prev_week.downtime_minutes / 60.0).label("previous_downtime_hours"),
            )
            .select_from(Ride)
            .join(Park, Ride.park_id == Park.park_id)
            .join(RideWeeklyStats, Ride.ride_id == RideWeeklyStats.ride_id)
            .outerjoin(
                prev_week,
                and_(
                    prev_week.ride_id == RideWeeklyStats.ride_id,
                    prev_week.year == prev_year,
                    prev_week.week_number == prev_week_number,
                ),
            )
            .where(and_(*conditions))
            .order_by(RideWeeklyStats.trend_vs_previous_week.asc())
            .limit(limit)
        )

        return self.execute_and_fetchall(stmt)
