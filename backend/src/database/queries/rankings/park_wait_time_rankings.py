"""
Park Wait Time Rankings Query
=============================

Endpoint: GET /api/parks/waittimes?period=last_week|last_month
UI Location: Parks tab → Wait Times Rankings table

Returns parks ranked by average wait time.

CALENDAR-BASED PERIODS:
- last_week: Previous complete week (Sunday-Saturday, Pacific Time)
- last_month: Previous complete calendar month (Pacific Time)

Database Tables:
- parks (park metadata)
- park_daily_stats (aggregated wait time data)

Example Response:
{
    "park_id": 1,
    "park_name": "Magic Kingdom",
    "location": "Orlando, FL",
    "avg_wait_time": 45.5,
    "peak_wait_time": 120,
    "period_label": "Nov 24-30, 2024"
}
"""

from datetime import date
from typing import List, Dict, Any

from sqlalchemy import select, func, and_
from sqlalchemy.orm import Session

from models.orm_park import Park
from models.orm_stats import ParkDailyStats
from utils.query_helpers import QueryClassBase
from utils.timezone import get_last_week_date_range, get_last_month_date_range


class ParkWaitTimeRankingsQuery(QueryClassBase):
    """
    Query handler for park wait time rankings.
    """

    def __init__(self, session: Session):
        super().__init__(session)

    def get_by_period(
        self,
        period: str,
        filter_disney_universal: bool = False,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """Get park wait time rankings for the specified period."""
        if period == 'last_week':
            return self.get_weekly(filter_disney_universal, limit)
        else:  # last_month
            return self.get_monthly(filter_disney_universal, limit)

    def get_weekly(
        self,
        filter_disney_universal: bool = False,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """Get park wait time rankings for the previous complete week."""
        start_date, end_date, period_label = get_last_week_date_range()
        return self._get_rankings(start_date, end_date, period_label, filter_disney_universal, limit)

    def get_monthly(
        self,
        filter_disney_universal: bool = False,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """Get park wait time rankings for the previous complete month."""
        start_date, end_date, period_label = get_last_month_date_range()
        return self._get_rankings(start_date, end_date, period_label, filter_disney_universal, limit)

    def _get_rankings(
        self,
        start_date: date,
        end_date: date,
        period_label: str = "",
        filter_disney_universal: bool = False,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        conditions = [
            Park.is_active == True,
            ParkDailyStats.stat_date >= start_date,
            ParkDailyStats.stat_date <= end_date,
            ParkDailyStats.avg_wait_time.isnot(None),
        ]

        if filter_disney_universal:
            conditions.append(
                (Park.is_disney == True) | (Park.is_universal == True)
            )

        stmt = (
            select(
                Park.park_id,
                Park.name.label("park_name"),
                func.concat(Park.city, ", ", Park.state_province).label("location"),
                func.round(func.avg(ParkDailyStats.avg_wait_time), 1).label(
                    "avg_wait_time"
                ),
                func.max(ParkDailyStats.peak_wait_time).label("peak_wait_time"),
            )
            .select_from(
                Park.__table__.join(
                    ParkDailyStats.__table__,
                    Park.park_id == ParkDailyStats.park_id
                )
            )
            .where(and_(*conditions))
            .group_by(Park.park_id, Park.name, Park.city, Park.state_province)
            .order_by(func.avg(ParkDailyStats.avg_wait_time).desc())
            .limit(limit)
        )

        # Add period_label to each result
        rankings = []
        for row_dict in self.execute_and_fetchall(stmt):
            if period_label:
                row_dict['period_label'] = period_label
            rankings.append(row_dict)
        return rankings
