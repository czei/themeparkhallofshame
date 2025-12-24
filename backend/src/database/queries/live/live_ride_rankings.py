"""
Live Ride Rankings Query
========================

Endpoint: GET /api/rides/downtime?period=today
UI Location: Rides tab → Downtime Rankings (today)

Returns rides ranked by current-day downtime from real-time snapshots.

NOTE: This class is currently bypassed for performance. The routes use
StatsRepository.get_ride_live_downtime_rankings() instead, which uses
the same centralized SQL helpers but with optimized CTEs.

Database Tables:
- rides (ride metadata)
- parks (park metadata)
- ride_classifications (tier weights)
- ride_status_snapshots (real-time status)
- park_activity_snapshots (park open status)

Single Source of Truth:
- Formulas: utils/metrics.py
- ORM Helpers: utils/query_helpers.py

Performance: Uses SQLAlchemy ORM with status expressions.
"""

from typing import List, Dict, Any

from sqlalchemy import select, func, case, literal, and_, or_
from sqlalchemy.orm import Session, aliased

from src.models import Park, Ride, RideClassification, RideStatusSnapshot, ParkActivitySnapshot
from src.utils.query_helpers import QueryClassBase, TimeIntervalHelper
from utils.timezone import get_today_pacific, get_pacific_day_range_utc
from utils.metrics import SNAPSHOT_INTERVAL_MINUTES, LIVE_WINDOW_HOURS


class LiveRideRankingsQuery(QueryClassBase):
    """
    Query handler for live (today) ride rankings.

    Uses SQLAlchemy ORM with centralized status expressions for
    consistent calculations across all queries.

    NOTE: For production use, prefer StatsRepository.get_ride_live_downtime_rankings()
    which has additional optimizations.
    """

    def get_rankings(
        self,
        filter_disney_universal: bool = False,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """
        Get live ride rankings for today from real-time snapshots.

        Uses ORM status expressions for consistent status logic.

        Args:
            filter_disney_universal: Only Disney/Universal parks
            limit: Maximum results

        Returns:
            List of rides ranked by downtime hours (descending)
        """
        # Get Pacific day bounds in UTC
        today = get_today_pacific()
        start_utc, end_utc = get_pacific_day_range_utc(today)

        # Live window cutoff for current status (last 2 hours)
        live_cutoff = TimeIntervalHelper.hours_ago(LIVE_WINDOW_HOURS)

        # =====================================================================
        # PARK-TYPE AWARE DOWNTIME LOGIC
        # =====================================================================
        # Disney/Universal: Only DOWN status counts
        # Other parks: DOWN, CLOSED, or NULL+computed_is_open=False counts
        parks_with_down_status = or_(
            Park.is_disney == True,
            Park.is_universal == True,
            Park.name == 'Dollywood'
        )

        is_down_expr = case(
            (parks_with_down_status, RideStatusSnapshot.status == 'DOWN'),
            else_=or_(
                RideStatusSnapshot.status.in_(['DOWN', 'CLOSED']),
                and_(
                    RideStatusSnapshot.status.is_(None),
                    RideStatusSnapshot.computed_is_open == False
                )
            )
        )

        # Park open filter
        park_open = ParkActivitySnapshot.park_appears_open == True

        # Downtime hours calculation: count minutes where park open AND ride down
        downtime_case = case(
            (and_(park_open, is_down_expr), SNAPSHOT_INTERVAL_MINUTES / 60.0),
            else_=0
        )

        # Operating snapshots: park open AND ride operating
        is_operating_expr = or_(
            RideStatusSnapshot.status == 'OPERATING',
            and_(
                RideStatusSnapshot.status.is_(None),
                RideStatusSnapshot.computed_is_open == True
            )
        )

        operating_case = case(
            (and_(park_open, is_operating_expr), 1),
            else_=0
        )

        park_open_case = case(
            (park_open, 1),
            else_=0
        )

        # =====================================================================
        # SUBQUERY: Current status from latest snapshot
        # =====================================================================
        rss_current = aliased(RideStatusSnapshot)
        pas_current = aliased(ParkActivitySnapshot)

        # Latest snapshot subquery
        latest_snapshot_subq = (
            select(
                rss_current.ride_id,
                rss_current.status,
                rss_current.computed_is_open,
                func.row_number().over(
                    partition_by=rss_current.ride_id,
                    order_by=rss_current.recorded_at.desc()
                ).label('rn')
            )
            .where(rss_current.recorded_at >= live_cutoff)
            .subquery()
        )

        # Current park status subquery
        park_status_subq = (
            select(
                pas_current.park_id,
                pas_current.park_appears_open,
                func.row_number().over(
                    partition_by=pas_current.park_id,
                    order_by=pas_current.recorded_at.desc()
                ).label('rn')
            )
            .where(pas_current.recorded_at >= live_cutoff)
            .subquery()
        )

        # Current status expression
        current_status_expr = case(
            (park_status_subq.c.park_appears_open == False, literal('PARK_CLOSED')),
            else_=func.coalesce(
                latest_snapshot_subq.c.status,
                case(
                    (latest_snapshot_subq.c.computed_is_open == True, literal('OPERATING')),
                    else_=literal('DOWN')
                )
            )
        ).label('current_status')

        # Current is_open boolean
        current_is_open_expr = case(
            (park_status_subq.c.park_appears_open == False, literal(False)),
            else_=or_(
                latest_snapshot_subq.c.status == 'OPERATING',
                and_(
                    latest_snapshot_subq.c.status.is_(None),
                    latest_snapshot_subq.c.computed_is_open == True
                )
            )
        ).label('current_is_open')

        # Park is_open boolean
        park_is_open_expr = func.coalesce(
            park_status_subq.c.park_appears_open,
            literal(False)
        ).label('park_is_open')

        # =====================================================================
        # MAIN QUERY: Aggregate from ride_status_snapshots
        # =====================================================================
        stmt = (
            select(
                Ride.ride_id,
                Ride.queue_times_id,
                Park.queue_times_id.label('park_queue_times_id'),
                Ride.name.label('ride_name'),
                Park.name.label('park_name'),
                Park.park_id,
                func.concat(Park.city, ', ', Park.state_province).label('location'),
                RideClassification.tier,

                # Total downtime hours (using park-type aware logic)
                # Formula from utils/metrics.py: calculate_downtime_hours()
                func.round(func.sum(downtime_case), 2).label('downtime_hours'),

                # Uptime percentage (operating snapshots / park-open snapshots)
                # Formula from utils/metrics.py: calculate_uptime_percentage()
                func.round(
                    100.0 - (
                        func.sum(operating_case) * 100.0 /
                        func.nullif(func.sum(park_open_case), 0)
                    ),
                    1
                ).label('uptime_percentage'),

                # Current status columns (from subqueries)
                current_status_expr,
                current_is_open_expr,
                park_is_open_expr,

                # Wait time data (live)
                func.max(RideStatusSnapshot.wait_time).label('peak_wait_time'),
                func.round(
                    func.avg(
                        case(
                            (RideStatusSnapshot.wait_time > 0, RideStatusSnapshot.wait_time),
                            else_=None
                        )
                    ),
                    0
                ).label('avg_wait_time')
            )
            .select_from(Ride)
            .join(Park, Ride.park_id == Park.park_id)
            .outerjoin(RideClassification, Ride.ride_id == RideClassification.ride_id)
            .join(RideStatusSnapshot, Ride.ride_id == RideStatusSnapshot.ride_id)
            .join(ParkActivitySnapshot, and_(
                Park.park_id == ParkActivitySnapshot.park_id,
                ParkActivitySnapshot.recorded_at == RideStatusSnapshot.recorded_at
            ))
            .outerjoin(
                latest_snapshot_subq,
                and_(
                    Ride.ride_id == latest_snapshot_subq.c.ride_id,
                    latest_snapshot_subq.c.rn == 1
                )
            )
            .outerjoin(
                park_status_subq,
                and_(
                    Park.park_id == park_status_subq.c.park_id,
                    park_status_subq.c.rn == 1
                )
            )
            .where(RideStatusSnapshot.recorded_at >= start_utc)
            .where(RideStatusSnapshot.recorded_at < end_utc)
            .where(Ride.is_active == True)
            .where(Ride.category == 'ATTRACTION')
            .where(Park.is_active == True)
        )

        # Apply Disney/Universal filter if requested
        if filter_disney_universal:
            stmt = stmt.where(
                or_(
                    Park.is_disney == True,
                    Park.is_universal == True
                )
            )

        # Group by all non-aggregated columns
        stmt = stmt.group_by(
            Ride.ride_id,
            Ride.queue_times_id,
            Park.queue_times_id,
            Ride.name,
            Park.name,
            Park.park_id,
            Park.city,
            Park.state_province,
            RideClassification.tier
        )

        # Having clause: only rides with downtime
        stmt = stmt.having(func.sum(downtime_case) > 0)

        # Order by downtime hours descending
        stmt = stmt.order_by(literal('downtime_hours').desc())

        # Apply limit
        stmt = stmt.limit(limit)

        # Execute and return results
        return self.execute_and_fetchall(stmt)
