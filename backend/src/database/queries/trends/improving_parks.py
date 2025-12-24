"""
Improving Parks Query
=====================

Endpoint: GET /api/trends?category=parks-improving
UI Location: Trends tab → Parks Improving section

Returns parks with uptime improvement >= 5% vs previous period.

Database Tables:
- parks (park metadata)
- park_weekly_stats (trend_vs_previous_week)

How to Modify:
1. To change threshold: Modify IMPROVEMENT_THRESHOLD constant
2. To add columns: Extend select()

Example Response:
{
    "park_id": 1,
    "park_name": "Magic Kingdom",
    "location": "Orlando, FL",
    "current_uptime": 96.5,
    "previous_uptime": 91.2,
    "improvement_percentage": 5.3
}
"""

from datetime import date, timedelta
from typing import List, Dict, Any

from sqlalchemy import select, func, and_, or_
from sqlalchemy.orm import Session

from src.models import Park, ParkWeeklyStats
from src.utils.query_helpers import QueryClassBase


# =============================================================================
# IMPROVEMENT THRESHOLD
# =============================================================================
# Parks must show >= 5% improvement in uptime to appear in this list
IMPROVEMENT_THRESHOLD = 5.0


class ImprovingParksQuery(QueryClassBase):
    """
    Query for parks showing uptime improvement.
    """

    def get_improving(
        self,
        period: str = '7days',
        filter_disney_universal: bool = False,
        limit: int = 10,
    ) -> List[Dict[str, Any]]:
        """Get improving parks for the specified period."""
        # Currently all periods use weekly data
        return self.get_weekly(filter_disney_universal, limit)

    def get_weekly(
        self,
        filter_disney_universal: bool = False,
        limit: int = 10,
    ) -> List[Dict[str, Any]]:
        """
        Get parks with improving uptime for current week.

        Args:
            filter_disney_universal: Only Disney/Universal parks
            limit: Maximum results

        Returns:
            List of improving parks sorted by improvement % (descending)
        """
        today = date.today()
        year = today.year
        week_number = today.isocalendar()[1]
        prev_week_date = today - timedelta(weeks=1)
        prev_year = prev_week_date.year
        prev_week_number = prev_week_date.isocalendar()[1]
        prev_week = ParkWeeklyStats.__table__.alias("prev_week")

        conditions = [
            Park.is_active == True,
            ParkWeeklyStats.year == year,
            ParkWeeklyStats.week_number == week_number,
            # Positive trend = improving (less downtime)
            ParkWeeklyStats.trend_vs_previous_week < -IMPROVEMENT_THRESHOLD,
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
                # Calculate previous uptime from trend
                func.round(
                    ParkWeeklyStats.avg_uptime_percentage
                    / (1 + ParkWeeklyStats.trend_vs_previous_week / 100),
                    2,
                ).label("previous_uptime"),
                # Improvement is negative trend (less downtime = better)
                func.abs(ParkWeeklyStats.trend_vs_previous_week).label(
                    "improvement_percentage"
                ),
                ParkWeeklyStats.total_downtime_hours.label("current_downtime_hours"),
                func.coalesce(
                    prev_week.c.total_downtime_hours,
                    0,
                ).label("previous_downtime_hours"),
            )
            .select_from(
                Park.__table__.join(
                    ParkWeeklyStats, Park.park_id == ParkWeeklyStats.park_id
                ).outerjoin(
                    prev_week,
                    and_(
                        prev_week.c.park_id == ParkWeeklyStats.park_id,
                        prev_week.c.year == prev_year,
                        prev_week.c.week_number == prev_week_number,
                    ),
                )
            )
            .where(and_(*conditions))
            .order_by(ParkWeeklyStats.trend_vs_previous_week.asc())  # Most improved first
            .limit(limit)
        )

        return self.execute_and_fetchall(stmt)
