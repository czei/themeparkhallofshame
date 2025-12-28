"""
Yesterday Ride Wait Time Rankings Query
=======================================

Endpoint: GET /api/rides/waittimes?period=yesterday
UI Location: Rides tab -> Wait Times Rankings (yesterday)

Returns rides ranked by average wait times for the previous full Pacific day.

PERFORMANCE FIX (2025-12-27):
- Previously: Joined ride_status_snapshots with park_activity_snapshots using
  date_format() for minute-level matching - caused 20+ second queries
- Now: Uses pre-aggregated ride_daily_stats table like last_week/last_month
- Result: Sub-second queries

Database Tables:
- rides (ride metadata)
- parks (park metadata)
- ride_daily_stats (pre-aggregated wait time data)

Single Source of Truth:
- Formulas: utils/metrics.py
- ORM Helpers: utils/query_helpers.py
"""

from typing import List, Dict, Any

from sqlalchemy import select, func, and_, or_, case
from sqlalchemy.orm import Session

from src.models.orm_ride import Ride
from src.models.orm_park import Park
from src.models.orm_stats import RideDailyStats
from src.utils.query_helpers import QueryClassBase
from src.utils.timezone import get_yesterday_date_range


class YesterdayRideWaitTimesQuery(QueryClassBase):
    """
    Query handler for yesterday's ride wait time rankings.

    Uses pre-aggregated ride_daily_stats for fast queries.
    """

    def get_rankings(
        self,
        filter_disney_universal: bool = False,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """
        Get ride wait time rankings from yesterday's daily stats.

        Args:
            filter_disney_universal: Only Disney/Universal parks
            limit: Maximum results

        Returns:
            List of rides ranked by average wait time (descending)
        """
        # Get yesterday's date (Pacific timezone)
        yesterday_date, _, period_label = get_yesterday_date_range()

        # Build location string: "city, state_province" or just "city" if no state
        location_expr = case(
            (Park.state_province.isnot(None), func.concat(Park.city, ", ", Park.state_province)),
            else_=Park.city
        ).label("location")

        # Build query using pre-aggregated ride_daily_stats
        stmt = (
            select(
                Ride.ride_id,
                Ride.name.label("ride_name"),
                Ride.queue_times_id,
                Park.name.label("park_name"),
                Park.park_id,
                Park.queue_times_id.label("park_queue_times_id"),
                location_expr,
                # Wait time metrics from daily stats
                func.round(RideDailyStats.avg_wait_time, 1).label("avg_wait_minutes"),
                RideDailyStats.peak_wait_time.label("peak_wait_minutes"),
                # Tier from rides table (default to 3 if NULL)
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
                    RideDailyStats.stat_date == yesterday_date,
                    RideDailyStats.avg_wait_time.isnot(None),
                    RideDailyStats.avg_wait_time > 0,
                )
            )
        )

        # Apply Disney/Universal filter if requested
        if filter_disney_universal:
            stmt = stmt.where(
                or_(Park.is_disney == True, Park.is_universal == True)
            )

        # Order by average wait time descending
        stmt = (
            stmt
            .order_by(RideDailyStats.avg_wait_time.desc())
            .limit(limit)
        )

        # Execute and add period label
        result = self.session.execute(stmt)
        rankings = []
        for row in result:
            row_dict = dict(row._mapping)
            row_dict['period_label'] = period_label
            rankings.append(row_dict)

        return rankings
