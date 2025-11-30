"""
Live Ride Rankings Query
========================

Endpoint: GET /api/rides/downtime?period=today
UI Location: Rides tab → Downtime Rankings (today)

Returns rides ranked by current-day downtime, calculated from live snapshots.

Database Tables:
- rides (ride metadata)
- parks (park metadata)
- ride_classifications (tier weights)
- ride_status_snapshots (live status data)
- park_activity_snapshots (park open status)

Time Window: Today's operating hours (Pacific time)
"""

from datetime import date, datetime
from typing import List, Dict, Any

from sqlalchemy import select, func, and_, case
from sqlalchemy.engine import Connection

from database.schema import (
    parks,
    rides,
    ride_classifications,
    ride_status_snapshots,
    park_activity_snapshots,
)
from database.queries.builders import Filters, StatusExpressions


SNAPSHOT_INTERVAL_MINUTES = 5


class LiveRideRankingsQuery:
    """
    Query handler for live (today) ride rankings.
    """

    def __init__(self, connection: Connection):
        self.conn = connection

    def get_rankings(
        self,
        filter_disney_universal: bool = False,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """
        Get live ride rankings for today.

        Args:
            filter_disney_universal: Only Disney/Universal parks
            limit: Maximum results

        Returns:
            List of rides ranked by downtime (descending)
        """
        start_utc, end_utc = self._get_today_range_utc()

        conditions = [
            rides.c.is_active == True,
            rides.c.category == "ATTRACTION",
            parks.c.is_active == True,
            ride_status_snapshots.c.recorded_at >= start_utc,
            ride_status_snapshots.c.recorded_at <= end_utc,
        ]

        if filter_disney_universal:
            conditions.append(Filters.disney_universal(parks))

        # Downtime case: count snapshots where ride is down and park is open
        downtime_case = case(
            (
                and_(
                    park_activity_snapshots.c.park_appears_open == True,
                    StatusExpressions.is_down(ride_status_snapshots),
                ),
                SNAPSHOT_INTERVAL_MINUTES,
            ),
            else_=0,
        )

        # Uptime case for percentage calculation
        uptime_case = case(
            (
                and_(
                    park_activity_snapshots.c.park_appears_open == True,
                    StatusExpressions.is_operating(ride_status_snapshots),
                ),
                1,
            ),
            else_=0,
        )

        # Total operating snapshots (when park is open)
        operating_case = case(
            (park_activity_snapshots.c.park_appears_open == True, 1),
            else_=0,
        )

        stmt = (
            select(
                rides.c.ride_id,
                rides.c.name.label("ride_name"),
                parks.c.name.label("park_name"),
                parks.c.park_id,
                ride_classifications.c.tier,
                func.round(func.sum(downtime_case) / 60.0, 2).label(
                    "total_downtime_hours"
                ),
                func.round(
                    100.0 * func.sum(uptime_case) / func.nullif(func.sum(operating_case), 0),
                    2,
                ).label("uptime_percentage"),
            )
            .select_from(
                rides.join(parks, rides.c.park_id == parks.c.park_id)
                .join(
                    ride_status_snapshots,
                    rides.c.ride_id == ride_status_snapshots.c.ride_id,
                )
                .outerjoin(
                    ride_classifications,
                    rides.c.ride_id == ride_classifications.c.ride_id,
                )
                .outerjoin(
                    park_activity_snapshots,
                    and_(
                        parks.c.park_id == park_activity_snapshots.c.park_id,
                        park_activity_snapshots.c.recorded_at
                        == ride_status_snapshots.c.recorded_at,
                    ),
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
            .having(func.sum(downtime_case) > 0)
            .order_by(func.sum(downtime_case).desc())
            .limit(limit)
        )

        result = self.conn.execute(stmt)
        return [dict(row._mapping) for row in result]

    def _get_today_range_utc(self):
        """Get today's date range in UTC."""
        try:
            from utils.timezone import get_pacific_day_range_utc
            return get_pacific_day_range_utc()
        except ImportError:
            today = date.today()
            start = datetime.combine(today, datetime.min.time())
            end = datetime.combine(today, datetime.max.time())
            return start, end
