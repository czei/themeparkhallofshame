"""
Yesterday Park Wait Time Rankings Query
=======================================

Endpoint: GET /api/parks/waittimes?period=yesterday
UI Location: Parks tab -> Wait Times Rankings (yesterday)

Returns parks ranked by average wait times for the previous full Pacific day.

Uses same snapshot-based approach as TODAY query, but for yesterday's
full day range instead of partial day.

Database Tables:
- parks (park metadata)
- rides (ride metadata)
- ride_status_snapshots (wait time data)
- park_activity_snapshots (park open status)

Single Source of Truth:
- Formulas: utils/metrics.py
- SQL Helpers: utils/sql_helpers.py
"""

from typing import List, Dict, Any

from sqlalchemy import select, func, and_, or_, case, literal_column
from sqlalchemy.orm import Session

from models import Park, Ride, RideStatusSnapshot, ParkActivitySnapshot
from utils.query_helpers import QueryClassBase
from utils.timezone import get_yesterday_range_utc


class YesterdayParkWaitTimesQuery(QueryClassBase):
    """
    Query handler for yesterday's park wait time rankings.

    Uses snapshot data from yesterday's full Pacific day
    (midnight to midnight Pacific, converted to UTC).
    """

    def get_rankings(
        self,
        filter_disney_universal: bool = False,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """
        Get park wait time rankings from yesterday's full day.

        Args:
            filter_disney_universal: Only Disney/Universal parks
            limit: Maximum results

        Returns:
            List of parks ranked by average wait time (descending)
        """
        # Get yesterday's full day range in UTC
        start_utc, end_utc, _ = get_yesterday_range_utc()

        # Park open filter (Business Rule 1: Park status takes precedence)
        park_open_filter = ParkActivitySnapshot.park_appears_open == True

        # Base query with joins
        stmt = (
            select(
                Park.park_id,
                Park.queue_times_id,
                Park.name.label('park_name'),
                (Park.city + ', ' + Park.state_province).label('location'),

                # Average wait time across all rides (only when park is open and wait > 0)
                # IMPORTANT: Use avg_wait_minutes (not avg_wait_time) for frontend compatibility
                func.round(
                    func.avg(
                        case(
                            (and_(park_open_filter, RideStatusSnapshot.wait_time > 0), RideStatusSnapshot.wait_time)
                        )
                    ),
                    1
                ).label('avg_wait_minutes'),

                # Peak wait time yesterday
                # IMPORTANT: Use peak_wait_minutes (not peak_wait_time) for frontend compatibility
                func.max(
                    case(
                        (park_open_filter, RideStatusSnapshot.wait_time)
                    )
                ).label('peak_wait_minutes'),

                # Count of rides with wait time data
                # IMPORTANT: Use rides_reporting (not rides_with_waits) for frontend compatibility
                func.count(
                    func.distinct(
                        case(
                            (RideStatusSnapshot.wait_time > 0, Ride.ride_id)
                        )
                    )
                ).label('rides_reporting')
            )
            .select_from(Park)
            .join(
                Ride,
                and_(
                    Park.park_id == Ride.park_id,
                    Ride.is_active == True,
                    Ride.category == 'ATTRACTION'
                )
            )
            .join(RideStatusSnapshot, Ride.ride_id == RideStatusSnapshot.ride_id)
            .join(
                ParkActivitySnapshot,
                and_(
                    Park.park_id == ParkActivitySnapshot.park_id,
                    ParkActivitySnapshot.recorded_at == RideStatusSnapshot.recorded_at
                )
            )
            .where(
                and_(
                    RideStatusSnapshot.recorded_at >= start_utc,
                    RideStatusSnapshot.recorded_at < end_utc,
                    Park.is_active == True
                )
            )
            .group_by(Park.park_id, Park.name, Park.city, Park.state_province)
            .having(literal_column('avg_wait_minutes') != None)
            .order_by(literal_column('avg_wait_minutes').desc())
            .limit(limit)
        )

        # Apply Disney/Universal filter if requested
        if filter_disney_universal:
            stmt = stmt.where(or_(Park.is_disney == True, Park.is_universal == True))

        return self.execute_and_fetchall(stmt)
