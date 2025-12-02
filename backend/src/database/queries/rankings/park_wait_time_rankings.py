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

from datetime import date, timedelta
from typing import List, Dict, Any

from sqlalchemy import select, func, and_
from sqlalchemy.engine import Connection

from database.schema import parks, park_daily_stats
from database.queries.builders import Filters
from utils.timezone import get_last_week_date_range, get_last_month_date_range


class ParkWaitTimeRankingsQuery:
    """
    Query handler for park wait time rankings.
    """

    def __init__(self, connection: Connection):
        self.conn = connection

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
            parks.c.is_active == True,
            park_daily_stats.c.stat_date >= start_date,
            park_daily_stats.c.stat_date <= end_date,
            park_daily_stats.c.avg_wait_time.isnot(None),
        ]

        if filter_disney_universal:
            conditions.append(Filters.disney_universal(parks))

        stmt = (
            select(
                parks.c.park_id,
                parks.c.name.label("park_name"),
                func.concat(parks.c.city, ", ", parks.c.state_province).label("location"),
                func.round(func.avg(park_daily_stats.c.avg_wait_time), 1).label(
                    "avg_wait_time"
                ),
                func.max(park_daily_stats.c.peak_wait_time).label("peak_wait_time"),
            )
            .select_from(
                parks.join(park_daily_stats, parks.c.park_id == park_daily_stats.c.park_id)
            )
            .where(and_(*conditions))
            .group_by(parks.c.park_id, parks.c.name, parks.c.city, parks.c.state_province)
            .order_by(func.avg(park_daily_stats.c.avg_wait_time).desc())
            .limit(limit)
        )

        result = self.conn.execute(stmt)
        # Add period_label to each result
        rankings = []
        for row in result:
            row_dict = dict(row._mapping)
            if period_label:
                row_dict['period_label'] = period_label
            rankings.append(row_dict)
        return rankings
