"""
Ride Downtime Rankings Query
============================

Endpoint: GET /api/rides/downtime?period=last_week|last_month
UI Location: Rides tab → Downtime Rankings table

Returns rides ranked by downtime percentage during operating hours.
Includes tier classification for context.

CALENDAR-BASED PERIODS:
- last_week: Previous complete week (Sunday-Saturday, Pacific Time)
- last_month: Previous complete calendar month (Pacific Time)

Database Tables:
- rides (ride metadata)
- parks (park metadata for location/filter)
- ride_classifications (tier weights)
- ride_daily_stats (aggregated daily downtime data)

Example Response:
{
    "ride_id": 42,
    "ride_name": "Space Mountain",
    "park_name": "Magic Kingdom",
    "park_id": 1,
    "tier": 1,
    "total_downtime_hours": 8.5,
    "uptime_percentage": 89.2,
    "status_changes": 12,
    "trend_percentage": -5.2,
    "period_label": "Nov 24-30, 2024"
}
"""

from datetime import date
from typing import List, Dict, Any

from sqlalchemy import select, func, and_, or_
from sqlalchemy.orm import Session

from models.orm_park import Park
from models.orm_ride import Ride
from models.orm_stats import RideDailyStats
from database.schema import ride_classifications
from utils.timezone import get_last_week_date_range, get_last_month_date_range
from utils.query_helpers import QueryClassBase


class RideDowntimeRankingsQuery(QueryClassBase):
    """
    Query handler for ride downtime rankings.

    Methods:
        get_weekly(): Previous complete week from daily stats
        get_monthly(): Previous complete month from daily stats

    For live (today) rankings, use live/live_ride_rankings.py instead.
    """

    def get_weekly(
        self,
        filter_disney_universal: bool = False,
        limit: int = 50,
        sort_by: str = "downtime_hours",
    ) -> List[Dict[str, Any]]:
        """
        Get ride rankings for the previous complete week (Sunday-Saturday).

        Args:
            filter_disney_universal: Only Disney/Universal parks
            limit: Maximum results
            sort_by: Column to sort by (downtime_hours, uptime_percentage, trend_percentage)

        Returns:
            List of rides ranked by specified column
        """
        start_date, end_date, period_label = get_last_week_date_range()

        return self._get_rankings(
            start_date=start_date,
            end_date=end_date,
            period_label=period_label,
            filter_disney_universal=filter_disney_universal,
            limit=limit,
            sort_by=sort_by,
        )

    def get_monthly(
        self,
        filter_disney_universal: bool = False,
        limit: int = 50,
        sort_by: str = "downtime_hours",
    ) -> List[Dict[str, Any]]:
        """
        Get ride rankings for the previous complete calendar month.

        Args:
            filter_disney_universal: Only Disney/Universal parks
            limit: Maximum results
            sort_by: Column to sort by (downtime_hours, uptime_percentage, trend_percentage)

        Returns:
            List of rides ranked by specified column
        """
        start_date, end_date, period_label = get_last_month_date_range()

        return self._get_rankings(
            start_date=start_date,
            end_date=end_date,
            period_label=period_label,
            filter_disney_universal=filter_disney_universal,
            limit=limit,
            sort_by=sort_by,
        )

    def _get_order_by_clause(self, sort_by: str):
        """
        Get the ORDER BY expression for ride downtime rankings.

        Args:
            sort_by: Column to sort by

        Returns:
            SQLAlchemy order expression
        """
        # Map sort options to SQLAlchemy expressions
        # Note: current_is_open not available for historical data, falls back to downtime
        downtime_expr = func.sum(RideDailyStats.downtime_minutes)
        uptime_expr = func.avg(RideDailyStats.uptime_percentage)

        sort_mapping = {
            "downtime_hours": downtime_expr.desc(),
            "uptime_percentage": uptime_expr.asc(),  # Lower uptime = worse
            "trend_percentage": downtime_expr.desc(),  # Trend not available, fall back
            "current_is_open": downtime_expr.desc(),  # Status not available, fall back
        }
        return sort_mapping.get(sort_by, downtime_expr.desc())

    def _get_rankings(
        self,
        start_date: date,
        end_date: date,
        period_label: str = "",
        filter_disney_universal: bool = False,
        limit: int = 50,
        sort_by: str = "downtime_hours",
    ) -> List[Dict[str, Any]]:
        """
        Internal method to build and execute rankings query.

        Args:
            start_date: Start of date range
            end_date: End of date range
            period_label: Human-readable label (e.g., "Nov 24-30, 2024")
            filter_disney_universal: Only Disney/Universal parks
            limit: Maximum results
            sort_by: Column to sort by
        """
        conditions = [
            Ride.is_active == True,
            Ride.category == "ATTRACTION",
            Park.is_active == True,
            RideDailyStats.stat_date >= start_date,
            RideDailyStats.stat_date <= end_date,
        ]

        if filter_disney_universal:
            # Use ORM Park model directly (not Filters class which uses Core tables)
            conditions.append(or_(Park.is_disney == True, Park.is_universal == True))

        stmt = (
            select(
                Ride.ride_id,
                Ride.name.label("ride_name"),
                Park.name.label("park_name"),
                Park.park_id,
                ride_classifications.c.tier,
                func.round(
                    func.sum(RideDailyStats.downtime_minutes) / 60.0, 2
                ).label("downtime_hours"),
                func.round(func.avg(RideDailyStats.uptime_percentage), 2).label(
                    "uptime_percentage"
                ),
                func.sum(RideDailyStats.status_changes).label("status_changes"),
                # Trend not available for aggregated queries
                func.cast(None, Ride.ride_id.type).label("trend_percentage"),
            )
            .select_from(
                Ride.__table__.join(Park, Ride.park_id == Park.park_id)
                .join(RideDailyStats, Ride.ride_id == RideDailyStats.ride_id)
                .outerjoin(
                    ride_classifications,
                    Ride.ride_id == ride_classifications.c.ride_id,
                )
            )
            .where(and_(*conditions))
            .group_by(
                Ride.ride_id,
                Ride.name,
                Park.name,
                Park.park_id,
                ride_classifications.c.tier,
            )
            .having(func.sum(RideDailyStats.downtime_minutes) > 0)
            .order_by(self._get_order_by_clause(sort_by))
            .limit(limit)
        )

        rankings = self.execute_and_fetchall(stmt)

        # Add period_label to each result
        if period_label:
            for row_dict in rankings:
                row_dict['period_label'] = period_label

        return rankings
