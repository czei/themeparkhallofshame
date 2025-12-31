"""
Today Ride Wait Time Rankings Query (Cumulative)
=================================================

Endpoint: GET /api/rides/waittimes?period=today
UI Location: Rides tab → Wait Times Rankings (today - cumulative)

Returns rides ranked by CUMULATIVE wait times from midnight Pacific to now.

CRITICAL DIFFERENCE FROM 7-DAY/30-DAY:
- 7-DAY/30-DAY: Uses pre-aggregated ride_daily_stats table
- TODAY: Queries ride_status_snapshots directly for real-time accuracy

Database Tables:
- rides (ride metadata)
- parks (park metadata)
- ride_classifications (tier info)
- ride_status_snapshots (real-time wait time data)
- park_activity_snapshots (park open status)

Single Source of Truth:
- Formulas: utils/metrics.py
- SQL Helpers: utils/sql_helpers.py
"""

from typing import List, Dict, Any

from sqlalchemy import select, func, and_, case, literal_column
from sqlalchemy.orm import Session

from models import Park, Ride, RideClassification, RideStatusSnapshot, ParkActivitySnapshot
from utils.query_helpers import QueryClassBase
from utils.timezone import get_today_range_to_now_utc


class TodayRideWaitTimesQuery(QueryClassBase):
    """
    Query handler for today's CUMULATIVE ride wait time rankings.

    Unlike weekly/monthly queries which use ride_daily_stats,
    this aggregates ALL wait times from ride_status_snapshots
    since midnight Pacific to now.
    """

    def get_rankings(
        self,
        filter_disney_universal: bool = False,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """
        Get cumulative ride wait time rankings from midnight Pacific to now.

        Args:
            filter_disney_universal: Only Disney/Universal parks
            limit: Maximum results

        Returns:
            List of rides ranked by average wait time (descending)
        """
        # Get time range from midnight Pacific to now
        start_utc, now_utc = get_today_range_to_now_utc()

        # PERFORMANCE: Use CTE to get latest snapshot per ride once,
        # avoiding correlated subqueries that run per-row

        # CTE 1: Latest snapshot per ride
        latest_snapshot_subq = (
            select(
                RideStatusSnapshot.ride_id,
                func.max(RideStatusSnapshot.recorded_at).label('max_recorded_at')
            )
            .group_by(RideStatusSnapshot.ride_id)
            .subquery()
        )

        latest_snapshots = (
            select(
                RideStatusSnapshot.ride_id,
                RideStatusSnapshot.wait_time.label('current_wait_time'),
                RideStatusSnapshot.status.label('current_status'),
                RideStatusSnapshot.computed_is_open.label('current_is_open')
            )
            .select_from(RideStatusSnapshot)
            .join(
                latest_snapshot_subq,
                and_(
                    RideStatusSnapshot.ride_id == latest_snapshot_subq.c.ride_id,
                    RideStatusSnapshot.recorded_at == latest_snapshot_subq.c.max_recorded_at
                )
            )
        ).cte('latest_snapshots')

        # CTE 2: Latest park status per park
        latest_park_subq = (
            select(
                ParkActivitySnapshot.park_id,
                func.max(ParkActivitySnapshot.recorded_at).label('max_recorded_at')
            )
            .group_by(ParkActivitySnapshot.park_id)
            .subquery()
        )

        latest_park_status = (
            select(
                ParkActivitySnapshot.park_id,
                ParkActivitySnapshot.park_appears_open.label('park_is_open')
            )
            .select_from(ParkActivitySnapshot)
            .join(
                latest_park_subq,
                and_(
                    ParkActivitySnapshot.park_id == latest_park_subq.c.park_id,
                    ParkActivitySnapshot.recorded_at == latest_park_subq.c.max_recorded_at
                )
            )
        ).cte('latest_park_status')

        # Main query
        # Average wait time (only when park is open and wait > 0)
        avg_wait_minutes = func.round(
            func.avg(
                case(
                    (
                        and_(
                            ParkActivitySnapshot.park_appears_open == True,
                            RideStatusSnapshot.wait_time > 0
                        ),
                        RideStatusSnapshot.wait_time
                    )
                )
            ),
            1
        ).label('avg_wait_minutes')

        # Peak wait time today (only when park is open)
        peak_wait_minutes = func.max(
            case(
                (
                    ParkActivitySnapshot.park_appears_open == True,
                    RideStatusSnapshot.wait_time
                )
            )
        ).label('peak_wait_minutes')

        # Location concatenation
        location = func.concat(Park.city, literal_column("', '"), Park.state_province).label('location')

        stmt = (
            select(
                Ride.ride_id,
                Ride.queue_times_id,
                Park.queue_times_id.label('park_queue_times_id'),
                Ride.name.label('ride_name'),
                Park.name.label('park_name'),
                Park.park_id,
                location,
                RideClassification.tier,
                avg_wait_minutes,
                peak_wait_minutes,
                latest_snapshots.c.current_wait_time,
                latest_snapshots.c.current_status,
                latest_snapshots.c.current_is_open,
                latest_park_status.c.park_is_open
            )
            .select_from(Ride)
            .join(Park, Ride.park_id == Park.park_id)
            .outerjoin(RideClassification, Ride.ride_id == RideClassification.ride_id)
            .join(RideStatusSnapshot, Ride.ride_id == RideStatusSnapshot.ride_id)
            .join(
                ParkActivitySnapshot,
                and_(
                    Park.park_id == ParkActivitySnapshot.park_id,
                    ParkActivitySnapshot.recorded_at == RideStatusSnapshot.recorded_at
                )
            )
            .outerjoin(latest_snapshots, Ride.ride_id == latest_snapshots.c.ride_id)
            .outerjoin(latest_park_status, Park.park_id == latest_park_status.c.park_id)
            .where(
                and_(
                    RideStatusSnapshot.recorded_at >= start_utc,
                    RideStatusSnapshot.recorded_at < now_utc,
                    Ride.is_active == True,
                    Ride.category == 'ATTRACTION',
                    Park.is_active == True
                )
            )
            .group_by(
                Ride.ride_id,
                Ride.name,
                Park.name,
                Park.park_id,
                Park.city,
                Park.state_province,
                RideClassification.tier,
                latest_snapshots.c.current_wait_time,
                latest_snapshots.c.current_status,
                latest_snapshots.c.current_is_open,
                latest_park_status.c.park_is_open
            )
            .having(avg_wait_minutes.isnot(None))
            .order_by(avg_wait_minutes.desc())
            .limit(limit)
        )

        # Apply Disney/Universal filter if requested
        if filter_disney_universal:
            stmt = stmt.where(
                Park.name.like('%Disney%') | Park.name.like('%Universal%')
            )

        return self.execute_and_fetchall(stmt)
