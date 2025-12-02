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

from datetime import date, timedelta
from typing import List, Dict, Any

from sqlalchemy import select, func, and_, case
from sqlalchemy.engine import Connection

from database.schema import (
    parks,
    rides,
    ride_classifications,
    ride_daily_stats,
)
from database.queries.builders import Filters
from utils.timezone import get_last_week_date_range, get_last_month_date_range


class RideDowntimeRankingsQuery:
    """
    Query handler for ride downtime rankings.

    Methods:
        get_weekly(): Previous complete week from daily stats
        get_monthly(): Previous complete month from daily stats

    For live (today) rankings, use live/live_ride_rankings.py instead.
    """

    def __init__(self, connection: Connection):
        self.conn = connection

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
        downtime_expr = func.sum(ride_daily_stats.c.downtime_minutes)
        uptime_expr = func.avg(ride_daily_stats.c.uptime_percentage)

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
            rides.c.is_active == True,
            rides.c.category == "ATTRACTION",
            parks.c.is_active == True,
            ride_daily_stats.c.stat_date >= start_date,
            ride_daily_stats.c.stat_date <= end_date,
        ]

        if filter_disney_universal:
            conditions.append(Filters.disney_universal(parks))

        stmt = (
            select(
                rides.c.ride_id,
                rides.c.name.label("ride_name"),
                parks.c.name.label("park_name"),
                parks.c.park_id,
                ride_classifications.c.tier,
                func.round(
                    func.sum(ride_daily_stats.c.downtime_minutes) / 60.0, 2
                ).label("total_downtime_hours"),
                func.round(func.avg(ride_daily_stats.c.uptime_percentage), 2).label(
                    "uptime_percentage"
                ),
                func.sum(ride_daily_stats.c.status_changes).label("status_changes"),
                # Trend not available for aggregated queries
                func.cast(None, rides.c.ride_id.type).label("trend_percentage"),
            )
            .select_from(
                rides.join(parks, rides.c.park_id == parks.c.park_id)
                .join(ride_daily_stats, rides.c.ride_id == ride_daily_stats.c.ride_id)
                .outerjoin(
                    ride_classifications,
                    rides.c.ride_id == ride_classifications.c.ride_id,
                )
            )
            .where(and_(*conditions))
            .group_by(
                rides.c.ride_id,
                rides.c.name,
                parks.c.name,
                parks.c.park_id,
                ride_classifications.c.tier,
            )
            .having(func.sum(ride_daily_stats.c.downtime_minutes) > 0)
            .order_by(self._get_order_by_clause(sort_by))
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
