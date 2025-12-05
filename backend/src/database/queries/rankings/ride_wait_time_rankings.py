"""
Ride Wait Time Rankings Query
=============================

Endpoint: GET /api/rides/waittimes?period=last_week|last_month
UI Location: Rides tab → Wait Times Rankings table

Returns rides ranked by average wait time.

CALENDAR-BASED PERIODS:
- last_week: Previous complete week (Sunday-Saturday, Pacific Time)
- last_month: Previous complete calendar month (Pacific Time)

Database Tables:
- rides (ride metadata)
- parks (park metadata)
- ride_daily_stats (aggregated wait time data)
- ride_classifications (tier data)

Example Response:
{
    "ride_id": 42,
    "ride_name": "Flight of Passage",
    "park_name": "Animal Kingdom",
    "park_id": 3,
    "avg_wait_minutes": 85.5,
    "peak_wait_minutes": 180,
    "tier": 1,
    "location": "Orlando, Florida",
    "period_label": "Nov 24-30, 2024"
}
"""

from datetime import date
from typing import List, Dict, Any

from sqlalchemy import select, func, and_, literal
from sqlalchemy.engine import Connection

from database.schema import parks, rides, ride_daily_stats, ride_classifications
from database.queries.builders import Filters
from utils.timezone import get_last_week_date_range, get_last_month_date_range


class RideWaitTimeRankingsQuery:
    """
    Query handler for ride wait time rankings.
    """

    def __init__(self, connection: Connection):
        self.conn = connection

    def get_by_period(
        self,
        period: str,
        filter_disney_universal: bool = False,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """Get ride wait time rankings for the specified period."""
        if period == 'last_week':
            return self.get_weekly(filter_disney_universal, limit)
        else:  # last_month
            return self.get_monthly(filter_disney_universal, limit)

    def get_weekly(
        self,
        filter_disney_universal: bool = False,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """Get ride wait time rankings for the previous complete week."""
        start_date, end_date, period_label = get_last_week_date_range()
        return self._get_rankings(start_date, end_date, period_label, filter_disney_universal, limit)

    def get_monthly(
        self,
        filter_disney_universal: bool = False,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """Get ride wait time rankings for the previous complete month."""
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
            rides.c.is_active == True,
            rides.c.category == "ATTRACTION",
            parks.c.is_active == True,
            ride_daily_stats.c.stat_date >= start_date,
            ride_daily_stats.c.stat_date <= end_date,
            ride_daily_stats.c.avg_wait_time.isnot(None),
        ]

        if filter_disney_universal:
            conditions.append(Filters.disney_universal(parks))

        # Build location string: "city, state_province" or just "city" if no state
        location_expr = func.concat(
            parks.c.city,
            func.if_(parks.c.state_province.isnot(None), func.concat(", ", parks.c.state_province), "")
        )

        stmt = (
            select(
                rides.c.ride_id,
                rides.c.name.label("ride_name"),
                rides.c.queue_times_id,
                parks.c.name.label("park_name"),
                parks.c.park_id,
                parks.c.queue_times_id.label("park_queue_times_id"),
                location_expr.label("location"),
                # Use consistent field names with LIVE endpoint
                func.round(func.avg(ride_daily_stats.c.avg_wait_time), 1).label(
                    "avg_wait_minutes"
                ),
                func.max(ride_daily_stats.c.peak_wait_time).label("peak_wait_minutes"),
                # Include tier from ride_classifications (default to 3 if unclassified)
                func.coalesce(ride_classifications.c.tier, literal(3)).label("tier"),
            )
            .select_from(
                rides
                .join(parks, rides.c.park_id == parks.c.park_id)
                .join(ride_daily_stats, rides.c.ride_id == ride_daily_stats.c.ride_id)
                .outerjoin(ride_classifications, rides.c.ride_id == ride_classifications.c.ride_id)
            )
            .where(and_(*conditions))
            .group_by(
                rides.c.ride_id, rides.c.name, rides.c.queue_times_id,
                parks.c.name, parks.c.park_id, parks.c.queue_times_id,
                parks.c.city, parks.c.state_province,
                ride_classifications.c.tier
            )
            .order_by(func.avg(ride_daily_stats.c.avg_wait_time).desc())
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
