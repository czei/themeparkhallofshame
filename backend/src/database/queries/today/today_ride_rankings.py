"""
Today Ride Rankings Query (Cumulative)
======================================

Endpoint: GET /api/rides/downtime?period=today
UI Location: Rides tab → Downtime Rankings (today - cumulative)

Returns rides ranked by CUMULATIVE downtime from midnight Pacific to now.

PERFORMANCE UPDATE (Dec 2025):
- Switched to pre-aggregated ride_hourly_stats (fast, indexed) to avoid
  scanning raw ride_status_snapshots for the entire day.
- Only a tiny "latest status" subquery hits ride_status_snapshots to show the
  current badge; the heavy aggregation now stays on ride_hourly_stats.

Database Tables:
- ride_hourly_stats (pre-aggregated downtime/uptime per hour)
- rides (ride metadata)
- parks (park metadata)
- ride_classifications (tier weights)
- ride_status_snapshots (latest-only subquery for current badge)

Single Source of Truth:
- Formulas: utils/metrics.py
- ORM Helpers: utils/query_helpers.py
"""

from typing import List, Dict, Any
from decimal import Decimal

from sqlalchemy import select, func, case, literal, and_, or_
from sqlalchemy.orm import Session, aliased

from src.models.orm_ride import Ride
from src.models.orm_classification import RideClassification
from src.models.orm_park import Park
from src.models.orm_stats import RideHourlyStats
from src.models.orm_snapshots import RideStatusSnapshot, ParkActivitySnapshot
from src.utils.query_helpers import QueryClassBase, TimeIntervalHelper
from utils.timezone import get_today_range_to_now_utc
from utils.metrics import LIVE_WINDOW_HOURS


class TodayRideRankingsQuery(QueryClassBase):
    """
    Query handler for today's CUMULATIVE ride rankings using pre-aggregated
    ride_hourly_stats (fast path).
    """

    def get_rankings(
        self,
        filter_disney_universal: bool = False,
        limit: int = 50,
        sort_by: str = "downtime_hours",
    ) -> List[Dict[str, Any]]:
        """
        Get cumulative ride rankings from midnight Pacific to now.

        Args:
            filter_disney_universal: Only Disney/Universal parks
            limit: Maximum results
            sort_by: Sort column (downtime_hours, uptime_percentage,
                     current_is_open, trend_percentage)

        Returns:
            List of rides ranked by cumulative downtime hours (descending)
        """
        # Get time range from midnight Pacific to now (UTC)
        start_utc, now_utc = get_today_range_to_now_utc()

        # Live window cutoff for current status (last 2 hours)
        live_cutoff = TimeIntervalHelper.hours_ago(LIVE_WINDOW_HOURS)

        # =====================================================================
        # SUBQUERY: Current status from latest snapshot
        # =====================================================================
        # Aliased table for subquery
        rss_current = aliased(RideStatusSnapshot)
        pas_current = aliased(ParkActivitySnapshot)

        # Subquery to get the latest snapshot for each ride
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

        # Subquery to get current park status
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

        # Current status expression (handles NULL status)
        # Use MAX() around subquery columns for MySQL GROUP BY compatibility
        current_status_expr = case(
            (func.max(park_status_subq.c.park_appears_open) == False, literal('PARK_CLOSED')),
            else_=func.coalesce(
                func.max(latest_snapshot_subq.c.status),
                case(
                    (func.max(latest_snapshot_subq.c.computed_is_open) == True, literal('OPERATING')),
                    else_=literal('DOWN')
                )
            )
        ).label('current_status')

        # Current is_open boolean
        # Use MAX() around subquery columns for MySQL GROUP BY compatibility
        current_is_open_expr = case(
            (func.max(park_status_subq.c.park_appears_open) == False, literal(False)),
            else_=or_(
                func.max(latest_snapshot_subq.c.status) == 'OPERATING',
                and_(
                    func.max(latest_snapshot_subq.c.status).is_(None),
                    func.max(latest_snapshot_subq.c.computed_is_open) == True
                )
            )
        ).label('current_is_open')

        # Park is_open boolean
        # Use MAX() around subquery column for MySQL GROUP BY compatibility
        park_is_open_expr = func.coalesce(
            func.max(park_status_subq.c.park_appears_open),
            literal(False)
        ).label('park_is_open')

        # =====================================================================
        # MAIN QUERY: Aggregate from ride_hourly_stats
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

                # Cumulative downtime from pre-aggregated hourly stats
                func.round(func.sum(RideHourlyStats.downtime_hours), 2).label('downtime_hours'),

                # Uptime percentage based on aggregated snapshots
                func.round(
                    100 - (
                        func.sum(RideHourlyStats.down_snapshots) * 100.0 /
                        func.nullif(func.sum(RideHourlyStats.snapshot_count), 0)
                    ),
                    1
                ).label('uptime_percentage'),

                # Current status columns (from subqueries)
                current_status_expr,
                current_is_open_expr,
                park_is_open_expr,

                # Trend placeholder (not available for partial day)
                literal(None).label('trend_percentage')
            )
            .select_from(RideHourlyStats)
            .join(Ride, RideHourlyStats.ride_id == Ride.ride_id)
            .join(Park, RideHourlyStats.park_id == Park.park_id)
            .outerjoin(RideClassification, Ride.ride_id == RideClassification.ride_id)
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
            .where(RideHourlyStats.hour_start_utc >= start_utc)
            .where(RideHourlyStats.hour_start_utc < now_utc)
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

        # Having clause: only rides that operated and have downtime
        stmt = stmt.having(
            func.sum(case((RideHourlyStats.ride_operated == True, 1), else_=0)) > 0
        ).having(
            func.sum(RideHourlyStats.downtime_hours) > 0
        )

        # Apply sorting - use actual aggregate expressions, not literal strings
        downtime_expr = func.sum(RideHourlyStats.downtime_hours)
        uptime_expr = 100 - (
            func.sum(RideHourlyStats.down_snapshots) * 100.0 /
            func.nullif(func.sum(RideHourlyStats.snapshot_count), 0)
        )

        if sort_by == "uptime_percentage":
            stmt = stmt.order_by(uptime_expr.asc())
        elif sort_by == "current_is_open":
            # Fall back to downtime since current_is_open comes from subquery
            stmt = stmt.order_by(downtime_expr.desc())
        elif sort_by == "trend_percentage":
            # Trend not available for today, fall back to downtime
            stmt = stmt.order_by(downtime_expr.desc())
        else:  # Default: downtime_hours
            stmt = stmt.order_by(downtime_expr.desc())

        # Apply limit
        stmt = stmt.limit(limit)

        # Execute and return results
        return self.execute_and_fetchall(stmt)
