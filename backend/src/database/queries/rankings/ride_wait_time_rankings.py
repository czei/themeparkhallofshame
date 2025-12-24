"""
Ride Wait Time Rankings Query
=============================

Endpoint: GET /api/rides/waittimes?period=last_week|last_month
UI Location: Rides tab → Wait Times Rankings table

Returns rides ranked by average wait time.

CALENDAR-BASED PERIODS:
- last_week: Previous complete week (Sunday-Saturday, Pacific Time)
- last_month: Previous complete calendar month (Pacific Time)

Database Tables (ORM Models):
- Ride (ride metadata)
- Park (park metadata)
- RideDailyStats (aggregated wait time data)

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

from sqlalchemy import select, func, and_, or_, case
from sqlalchemy.orm import Session

from src.models.orm_ride import Ride
from src.models.orm_park import Park
from src.models.orm_stats import RideDailyStats
from src.utils.query_helpers import QueryClassBase
from utils.timezone import get_last_week_date_range, get_last_month_date_range


class RideWaitTimeRankingsQuery(QueryClassBase):
    """
    Query handler for ride wait time rankings.
    """

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
        """
        Get ride wait time rankings for a date range.

        Args:
            start_date: Start of period (inclusive)
            end_date: End of period (inclusive)
            period_label: Human-readable period label
            filter_disney_universal: If True, only include Disney/Universal parks
            limit: Maximum number of results

        Returns:
            List of ride wait time ranking dicts
        """
        # Build location string: "city, state_province" or just "city" if no state
        location_expr = case(
            (Park.state_province.isnot(None), func.concat(Park.city, ", ", Park.state_province)),
            else_=Park.city
        ).label("location")

        # Build base query
        stmt = (
            select(
                Ride.ride_id,
                Ride.name.label("ride_name"),
                Ride.queue_times_id,
                Park.name.label("park_name"),
                Park.park_id,
                Park.queue_times_id.label("park_queue_times_id"),
                location_expr,
                # Use consistent field names with LIVE endpoint
                func.round(func.avg(RideDailyStats.avg_wait_time), 1).label("avg_wait_minutes"),
                func.max(RideDailyStats.peak_wait_time).label("peak_wait_minutes"),
                # Include tier from rides table (default to 3 if NULL)
                func.coalesce(Ride.tier, 3).label("tier"),
            )
            .select_from(Ride)
            .join(Park, Ride.park_id == Park.park_id)
            .join(RideDailyStats, Ride.ride_id == RideDailyStats.ride_id)
            .where(
                and_(
                    Ride.is_active == True,
                    Ride.category == "ATTRACTION",
                    Park.is_active == True,
                    RideDailyStats.stat_date >= start_date,
                    RideDailyStats.stat_date <= end_date,
                    RideDailyStats.avg_wait_time.isnot(None),
                )
            )
        )

        # Apply Disney/Universal filter if requested
        if filter_disney_universal:
            stmt = stmt.where(
                or_(Park.is_disney == True, Park.is_universal == True)
            )

        # Group by and order
        stmt = (
            stmt
            .group_by(
                Ride.ride_id,
                Ride.name,
                Ride.queue_times_id,
                Ride.tier,
                Park.name,
                Park.park_id,
                Park.queue_times_id,
                Park.city,
                Park.state_province,
            )
            .order_by(func.avg(RideDailyStats.avg_wait_time).desc())
            .limit(limit)
        )

        # Execute query
        result = self.session.execute(stmt)

        # Add period_label to each result
        rankings = []
        for row in result:
            row_dict = dict(row._mapping)
            if period_label:
                row_dict['period_label'] = period_label
            rankings.append(row_dict)

        return rankings
