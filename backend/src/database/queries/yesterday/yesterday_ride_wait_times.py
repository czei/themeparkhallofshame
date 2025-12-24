"""
Yesterday Ride Wait Time Rankings Query
=======================================

Endpoint: GET /api/rides/waittimes?period=yesterday
UI Location: Rides tab -> Wait Times Rankings (yesterday)

Returns rides ranked by average wait times for the previous full Pacific day.

Uses same snapshot-based approach as TODAY query, but for yesterday's
full day range instead of partial day.

Database Tables:
- rides (ride metadata)
- parks (park metadata)
- ride_classifications (tier info)
- ride_status_snapshots (wait time data)
- park_activity_snapshots (park open status)

Single Source of Truth:
- Formulas: utils/metrics.py
- ORM Helpers: utils/query_helpers.py
"""

from typing import List, Dict, Any

from sqlalchemy import select, func, case, and_, or_, literal_column
from sqlalchemy.orm import Session, aliased

from src.models.orm_ride import Ride
from src.models.orm_park import Park
from src.models.orm_classification import RideClassification
from src.models.orm_snapshots import RideStatusSnapshot, ParkActivitySnapshot
from src.utils.timezone import get_yesterday_range_utc
from src.utils.query_helpers import QueryClassBase


class YesterdayRideWaitTimesQuery(QueryClassBase):
    """
    Query handler for yesterday's ride wait time rankings.

    Uses snapshot data from yesterday's full Pacific day
    (midnight to midnight Pacific, converted to UTC).
    """

    def get_rankings(
        self,
        filter_disney_universal: bool = False,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """
        Get ride wait time rankings from yesterday's full day.

        Args:
            filter_disney_universal: Only Disney/Universal parks
            limit: Maximum results

        Returns:
            List of rides ranked by average wait time (descending)
        """
        # Get yesterday's full day range in UTC
        start_utc, end_utc, _ = get_yesterday_range_utc()

        # Aliases for clarity
        RSS = aliased(RideStatusSnapshot)
        PAS = aliased(ParkActivitySnapshot)

        # Park open filter condition
        park_open_filter = PAS.park_appears_open == True

        # Main query
        query = (
            select(
                Ride.ride_id,
                Ride.queue_times_id,
                Park.queue_times_id.label('park_queue_times_id'),
                Ride.name.label('ride_name'),
                Park.name.label('park_name'),
                Park.park_id,
                (Park.city + ', ' + Park.state_province).label('location'),
                RideClassification.tier,

                # Average wait time (only when park is open and wait > 0)
                # IMPORTANT: Use avg_wait_minutes (not avg_wait_time) for frontend compatibility
                func.round(
                    func.avg(
                        case(
                            (and_(park_open_filter, RSS.wait_time > 0), RSS.wait_time),
                            else_=None
                        )
                    ),
                    1
                ).label('avg_wait_minutes'),

                # Peak wait time yesterday
                # IMPORTANT: Use peak_wait_minutes (not peak_wait_time) for frontend compatibility
                func.max(
                    case(
                        (park_open_filter, RSS.wait_time),
                        else_=None
                    )
                ).label('peak_wait_minutes'),

                # Count of snapshots with wait time data
                func.count(
                    case(
                        (RSS.wait_time > 0, 1),
                        else_=None
                    )
                ).label('snapshots_with_waits')
            )
            .select_from(Ride)
            .join(Park, Ride.park_id == Park.park_id)
            .outerjoin(RideClassification, Ride.ride_id == RideClassification.ride_id)
            .join(RSS, Ride.ride_id == RSS.ride_id)
            .join(
                PAS,
                and_(
                    Park.park_id == PAS.park_id,
                    # Minute-level timestamp matching (1-2 second drift between tables)
                    func.date_format(PAS.recorded_at, '%Y-%m-%d %H:%i') ==
                    func.date_format(RSS.recorded_at, '%Y-%m-%d %H:%i')
                )
            )
            .where(RSS.recorded_at >= start_utc)
            .where(RSS.recorded_at < end_utc)
            .where(Ride.is_active == True)
            .where(Ride.category == 'ATTRACTION')
            .where(Park.is_active == True)
        )

        # Apply Disney/Universal filter if requested
        if filter_disney_universal:
            query = query.where(
                or_(Park.is_disney == True, Park.is_universal == True)
            )

        # Group and filter
        query = (
            query
            .group_by(
                Ride.ride_id,
                Ride.name,
                Park.name,
                Park.park_id,
                Park.city,
                Park.state_province,
                RideClassification.tier
            )
            .having(literal_column('avg_wait_minutes').isnot(None))
            .order_by(literal_column('avg_wait_minutes').desc())
            .limit(limit)
        )

        return self.execute_and_fetchall(query)
