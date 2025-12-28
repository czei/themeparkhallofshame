"""
Live Park Wait Time Rankings Query
===================================

Endpoint: GET /api/parks/waittimes?period=live
UI Location: Parks tab → Wait Times Rankings (live)

Returns parks ranked by CURRENT wait times from the latest snapshots.

CRITICAL: This shows CURRENT wait times only - the latest snapshot for each ride.
Unlike TODAY which shows cumulative/average wait times since midnight.

Database Tables:
- parks (park metadata)
- rides (ride metadata)
- ride_status_snapshots (real-time wait time data)
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
from utils.timezone import get_today_pacific, get_pacific_day_range_utc


class LiveParkWaitTimesQuery(QueryClassBase):
    """
    Query handler for live park wait time rankings.

    Uses SQLAlchemy ORM for consistent calculations.

    Returns parks ranked by CURRENT average wait time from their
    latest ride snapshots.
    """

    def get_rankings(
        self,
        filter_disney_universal: bool = False,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """
        Get live park wait time rankings from latest snapshots.

        Args:
            filter_disney_universal: Only Disney/Universal parks
            limit: Maximum results

        Returns:
            List of parks ranked by avg_wait_time (descending)
        """
        # Get Pacific day bounds in UTC
        today = get_today_pacific()
        start_utc, end_utc = get_pacific_day_range_utc(today)

        # CTE 1: latest_snapshot - Find the most recent snapshot timestamp for each ride today
        latest_snapshot = (
            select(
                RideStatusSnapshot.ride_id,
                func.max(RideStatusSnapshot.recorded_at).label('latest_recorded_at')
            )
            .where(RideStatusSnapshot.recorded_at >= start_utc)
            .where(RideStatusSnapshot.recorded_at < end_utc)
            .group_by(RideStatusSnapshot.ride_id)
        ).cte('latest_snapshot')

        # CTE 2: rides_with_current_wait - Get wait times from latest snapshots for operating rides
        rides_with_current_wait = (
            select(
                Ride.park_id,
                RideStatusSnapshot.ride_id,
                RideStatusSnapshot.wait_time,
                RideStatusSnapshot.computed_is_open
            )
            .select_from(RideStatusSnapshot)
            .join(latest_snapshot, and_(
                RideStatusSnapshot.ride_id == latest_snapshot.c.ride_id,
                RideStatusSnapshot.recorded_at == latest_snapshot.c.latest_recorded_at
            ))
            .join(Ride, RideStatusSnapshot.ride_id == Ride.ride_id)
            .where(Ride.is_active == True)
            # Only include rides that are operating and have a valid wait time
            .where(RideStatusSnapshot.computed_is_open == True)
            .where(RideStatusSnapshot.wait_time.isnot(None))
            .where(RideStatusSnapshot.wait_time > 0)
        ).cte('rides_with_current_wait')

        # Build the park filter condition
        park_filter = and_(Park.is_active == True)
        if filter_disney_universal:
            park_filter = and_(
                park_filter,
                or_(Park.is_disney == True, Park.is_universal == True)
            )

        # Main query: Aggregate wait times by park
        query = (
            select(
                Park.park_id,
                Park.name.label('park_name'),
                (Park.city + literal_column("', '") + Park.country).label('location'),
                Park.queue_times_id,
                func.round(func.avg(rides_with_current_wait.c.wait_time), 1).label('avg_wait_time'),
                func.max(rides_with_current_wait.c.wait_time).label('max_wait_time'),
                func.count(rides_with_current_wait.c.ride_id).label('rides_reporting'),
            )
            .select_from(Park)
            .outerjoin(rides_with_current_wait, Park.park_id == rides_with_current_wait.c.park_id)
            .where(park_filter)
            .group_by(Park.park_id)
            .having(func.count(rides_with_current_wait.c.ride_id) > 0)  # Only parks with wait data
            .order_by(func.avg(rides_with_current_wait.c.wait_time).desc())
            .limit(limit)
        )

        result = self.session.execute(query)
        rows = result.fetchall()

        # Format results
        parks = []
        for idx, row in enumerate(rows, 1):
            parks.append({
                'rank': idx,
                'park_id': row.park_id,
                'park_name': row.park_name,
                'location': row.location,
                'avg_wait_time': float(row.avg_wait_time) if row.avg_wait_time else 0,
                'max_wait_time': int(row.max_wait_time) if row.max_wait_time else 0,
                'rides_reporting': row.rides_reporting or 0,
                'queue_times_url': f"https://queue-times.com/parks/{row.queue_times_id}"
            })

        return parks
