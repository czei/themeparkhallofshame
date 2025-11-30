"""
Park Wait Time Rankings Query
=============================

Endpoint: GET /api/parks/waittimes?period=7days|30days
UI Location: Parks tab → Wait Times Rankings table

Returns parks ranked by average wait time.

Database Tables:
- parks (park metadata)
- park_daily_stats (aggregated wait time data)

How to Modify This Query:
1. To add a new column: Add to the select()
2. To change ranking order: Modify order_by()
3. To add filter: Extend where() clause

Example Response:
{
    "park_id": 1,
    "park_name": "Magic Kingdom",
    "location": "Orlando, FL",
    "avg_wait_time": 45.5,
    "peak_wait_time": 120
}
"""

from datetime import date, timedelta
from typing import List, Dict, Any

from sqlalchemy import select, func, and_
from sqlalchemy.engine import Connection

from database.schema import parks, park_daily_stats
from database.queries.builders import Filters


class ParkWaitTimeRankingsQuery:
    """
    Query handler for park wait time rankings.
    """

    def __init__(self, connection: Connection):
        self.conn = connection

    def get_weekly(
        self,
        filter_disney_universal: bool = False,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """Get park wait time rankings for the last 7 days."""
        end_date = date.today()
        start_date = end_date - timedelta(days=6)
        return self._get_rankings(start_date, end_date, filter_disney_universal, limit)

    def get_monthly(
        self,
        filter_disney_universal: bool = False,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """Get park wait time rankings for the last 30 days."""
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
        return [dict(row._mapping) for row in result]
