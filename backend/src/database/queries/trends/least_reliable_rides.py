"""
Least Reliable Rides Query (Awards)
===================================

Endpoint: GET /api/trends/least-reliable
UI Location: Trends tab → Awards section

Returns top 10 rides ranked by total downtime hours.

Formula: COUNT(down_snapshots) * SNAPSHOT_INTERVAL_MINUTES / 60 = downtime hours
Only counts downtime when park is open.

CRITICAL: Only counts rides that have OPERATED during the period.
Rides that are DOWN all day (never operated) are excluded.
See `rides_that_operated_subq` implementation.

Periods:
- today: Aggregates from ride_status_snapshots (midnight Pacific to now)
- 7days/30days: Aggregates from ride_daily_stats

Database Tables:
- rides (ride metadata)
- parks (park metadata)
- ride_classifications (tier weights)
- ride_status_snapshots + park_activity_snapshots (TODAY period)
- ride_daily_stats + park_daily_stats (7days/30days periods)
"""

from datetime import timedelta, datetime
from typing import List, Dict, Any

from sqlalchemy import select, func, case, and_, or_, literal

from src.models import (
    Park, Ride, RideClassification, RideStatusSnapshot,
    ParkActivitySnapshot, RideDailyStats, ParkDailyStats
)
from src.utils.query_helpers import QueryClassBase
from utils.timezone import get_today_range_to_now_utc, get_today_pacific, get_yesterday_range_utc
from utils.metrics import SNAPSHOT_INTERVAL_MINUTES


class LeastReliableRidesQuery(QueryClassBase):
    """
    Query for rides with highest downtime hours, converted to SQLAlchemy ORM.
    """

    def get_rankings(
        self,
        period: str = 'today',
        filter_disney_universal: bool = False,
        limit: int = 10,
    ) -> List[Dict[str, Any]]:
        """
        Get rides ranked by total downtime hours.

        Args:
            period: 'today', 'yesterday', 'last_week', or 'last_month'
            filter_disney_universal: Only Disney/Universal parks
            limit: Maximum results (default 10)

        Returns:
            List of rides with downtime hours and uptime percentage
        """
        if period == 'today':
            start_utc, end_utc = get_today_range_to_now_utc()
            return self._get_snapshot_ride_rankings(start_utc, end_utc, filter_disney_universal, limit)
        elif period == 'yesterday':
            start_utc, end_utc, _ = get_yesterday_range_utc()
            return self._get_snapshot_ride_rankings(start_utc, end_utc, filter_disney_universal, limit)
        elif period in ['last_week', '7days']:
            return self._get_daily_aggregate_rides(7, filter_disney_universal, limit)
        elif period in ['last_month', '30days']:
            return self._get_daily_aggregate_rides(30, filter_disney_universal, limit)
        else:
            raise ValueError(f"Invalid period: {period}. Must be 'today', 'yesterday', 'last_week', or 'last_month'")

    def _get_snapshot_ride_rankings(
        self,
        start_utc: datetime,
        end_utc: datetime,
        filter_disney_universal: bool,
        limit: int,
    ) -> List[Dict[str, Any]]:
        """
        Helper to get ride downtime from snapshots for a given time range.
        Handles logic for both 'today' and 'yesterday'.
        """
        # Subquery: Get rides that operated during the period
        rides_operated_stmt = (
            select(RideStatusSnapshot.ride_id)
            .where(RideStatusSnapshot.recorded_at >= start_utc)
            .where(RideStatusSnapshot.recorded_at < end_utc)
            .join(Ride, RideStatusSnapshot.ride_id == Ride.ride_id)
            .join(Park, Ride.park_id == Park.park_id)
            .join(
                ParkActivitySnapshot,
                and_(
                    Park.park_id == ParkActivitySnapshot.park_id,
                    RideStatusSnapshot.recorded_at == ParkActivitySnapshot.recorded_at
                )
            )
            .where(ParkActivitySnapshot.park_appears_open == True)
            .where(or_(
                RideStatusSnapshot.status == 'OPERATING',
                RideStatusSnapshot.computed_is_open == True
            ))
            .distinct()
        )
        if filter_disney_universal:
            rides_operated_stmt = rides_operated_stmt.where(or_(Park.is_disney == True, Park.is_universal == True))
        rides_operated_subq = rides_operated_stmt.subquery()

        # Park-type aware status expressions
        parks_with_down_status = or_(Park.is_disney == True, Park.is_universal == True, Park.name == 'Dollywood')
        is_down_expr = case(
            (parks_with_down_status, RideStatusSnapshot.status == 'DOWN'),
            else_=or_(
                RideStatusSnapshot.status.in_(['DOWN', 'CLOSED']),
                and_(RideStatusSnapshot.status.is_(None), RideStatusSnapshot.computed_is_open == False)
            )
        )
        is_operating_expr = or_(
            RideStatusSnapshot.status == 'OPERATING',
            and_(RideStatusSnapshot.status.is_(None), RideStatusSnapshot.computed_is_open == True)
        )
        park_open = ParkActivitySnapshot.park_appears_open == True

        # Case expressions for aggregation
        downtime_case = case((and_(park_open, is_down_expr), SNAPSHOT_INTERVAL_MINUTES / 60.0), else_=0)
        downtime_incidents_case = case((and_(park_open, is_down_expr), 1), else_=0)
        operating_case = case((and_(park_open, is_operating_expr), 1), else_=0)
        park_open_case = case((park_open, 1), else_=0)

        # Main Query
        stmt = (
            select(
                Ride.ride_id,
                Ride.name.label('ride_name'),
                Park.park_id,
                Park.name.label('park_name'),
                func.round(func.sum(downtime_case), 2).label('downtime_hours'),
                func.sum(downtime_incidents_case).label('downtime_incidents'),
                func.round(
                    100.0 * func.sum(operating_case) / func.nullif(func.sum(park_open_case), 0),
                    1
                ).label('uptime_percentage')
            )
            .select_from(RideStatusSnapshot)
            .join(Ride, RideStatusSnapshot.ride_id == Ride.ride_id)
            .join(Park, Ride.park_id == Park.park_id)
            .outerjoin(
                ParkActivitySnapshot,
                and_(
                    Park.park_id == ParkActivitySnapshot.park_id,
                    RideStatusSnapshot.recorded_at == ParkActivitySnapshot.recorded_at
                )
            )
            .where(RideStatusSnapshot.recorded_at >= start_utc)
            .where(RideStatusSnapshot.recorded_at < end_utc)
            .where(Ride.ride_id.in_(select(rides_operated_subq.c.ride_id)))
            .group_by(Ride.ride_id, Ride.name, Park.park_id, Park.name)
            .having(func.sum(downtime_case) > 0)
            .order_by(literal('downtime_hours').desc())
            .limit(limit)
        )

        return self.execute_and_fetchall(stmt)

    def _get_daily_aggregate_rides(
        self,
        days: int,
        filter_disney_universal: bool,
        limit: int,
    ) -> List[Dict[str, Any]]:
        """
        Get downtime hours from daily stats (7days/30days).
        """
        today = get_today_pacific()
        start_date = today - timedelta(days=days - 1)

        stmt = (
            select(
                Ride.ride_id,
                Ride.name.label('ride_name'),
                Park.park_id,
                Park.name.label('park_name'),
                func.round(func.sum(RideDailyStats.downtime_minutes) / 60.0, 2).label('downtime_hours'),
                func.sum(RideDailyStats.status_changes).label('downtime_incidents'),
                func.round(func.avg(RideDailyStats.uptime_percentage), 1).label('uptime_percentage')
            )
            .select_from(Ride)
            .join(Park, Ride.park_id == Park.park_id)
            .join(RideDailyStats, Ride.ride_id == RideDailyStats.ride_id)
            .where(RideDailyStats.stat_date >= start_date)
            .where(RideDailyStats.stat_date <= today)
            .where(RideDailyStats.downtime_minutes > 0)
            .where(Ride.is_active == True)
            .where(Ride.category == 'ATTRACTION')
            .where(Park.is_active == True)
        )

        if filter_disney_universal:
            stmt = stmt.where(or_(Park.is_disney == True, Park.is_universal == True))

        stmt = (
            stmt.group_by(Ride.ride_id, Ride.name, Park.park_id, Park.name)
            .order_by(literal('downtime_hours').desc())
            .limit(limit)
        )

        return self.execute_and_fetchall(stmt)

    # ========================================
    # PARK-LEVEL RANKINGS
    # ========================================

    def get_park_rankings(
        self,
        period: str = 'today',
        filter_disney_universal: bool = False,
        limit: int = 10,
        min_avg_shame: float = 1.0,
    ) -> List[Dict[str, Any]]:
        """
        Get parks ranked by average shame score.

        Args:
            period: 'today', 'yesterday', 'last_week', or 'last_month'
            filter_disney_universal: Only Disney/Universal parks
            limit: Maximum results (default 10)
            min_avg_shame: Minimum average shame score to be included

        Returns:
            List of parks with shame score and uptime percentage
        """
        if period == 'today':
            start_utc, end_utc = get_today_range_to_now_utc()
            return self._get_snapshot_park_rankings(start_utc, end_utc, filter_disney_universal, limit, min_avg_shame)
        elif period == 'yesterday':
            start_utc, end_utc, _ = get_yesterday_range_utc()
            return self._get_snapshot_park_rankings(start_utc, end_utc, filter_disney_universal, limit, min_avg_shame)
        elif period in ['last_week', '7days']:
            return self._get_daily_aggregate_parks(7, filter_disney_universal, limit, min_avg_shame)
        elif period in ['last_month', '30days']:
            return self._get_daily_aggregate_parks(30, filter_disney_universal, limit, min_avg_shame)
        else:
            raise ValueError(f"Invalid period: {period}. Must be 'today', 'yesterday', 'last_week', or 'last_month'")

    def _get_snapshot_park_rankings(
        self,
        start_utc: datetime,
        end_utc: datetime,
        filter_disney_universal: bool,
        limit: int,
        min_avg_shame: float,
    ) -> List[Dict[str, Any]]:
        """
        Helper to get park reliability from snapshots for a time range.
        Handles logic for both 'today' and 'yesterday'.
        """
        # Subquery: Get rides that operated during the period
        rides_operated_stmt = (
            select(RideStatusSnapshot.ride_id)
            .where(RideStatusSnapshot.recorded_at >= start_utc)
            .where(RideStatusSnapshot.recorded_at < end_utc)
            .join(Ride, RideStatusSnapshot.ride_id == Ride.ride_id)
            .join(Park, Ride.park_id == Park.park_id)
            .join(
                ParkActivitySnapshot,
                and_(
                    Park.park_id == ParkActivitySnapshot.park_id,
                    RideStatusSnapshot.recorded_at == ParkActivitySnapshot.recorded_at
                )
            )
            .where(ParkActivitySnapshot.park_appears_open == True)
            .where(or_(
                RideStatusSnapshot.status == 'OPERATING',
                RideStatusSnapshot.computed_is_open == True
            ))
            .distinct()
        )
        if filter_disney_universal:
            rides_operated_stmt = rides_operated_stmt.where(or_(Park.is_disney == True, Park.is_universal == True))
        rides_operated_subq = rides_operated_stmt.subquery()

        # Subquery: Calculate total tier weight for each park
        park_weights_subq = (
            select(
                Ride.park_id,
                func.sum(func.coalesce(RideClassification.tier_weight, 2)).label('total_park_weight')
            )
            .join(Park, Ride.park_id == Park.park_id)
            .outerjoin(RideClassification, Ride.ride_id == RideClassification.ride_id)
            .where(Ride.is_active == True)
            .where(Ride.category == 'ATTRACTION')
            .where(Park.is_active == True)
            .where(Ride.ride_id.in_(select(rides_operated_subq.c.ride_id)))
            .group_by(Ride.park_id)
            .subquery()
        )

        # Park-type aware status expressions
        parks_with_down_status = or_(Park.is_disney == True, Park.is_universal == True, Park.name == 'Dollywood')
        is_down_expr = case(
            (parks_with_down_status, RideStatusSnapshot.status == 'DOWN'),
            else_=or_(
                RideStatusSnapshot.status.in_(['DOWN', 'CLOSED']),
                and_(RideStatusSnapshot.status.is_(None), RideStatusSnapshot.computed_is_open == False)
            )
        )
        is_operating_expr = or_(
            RideStatusSnapshot.status == 'OPERATING',
            and_(RideStatusSnapshot.status.is_(None), RideStatusSnapshot.computed_is_open == True)
        )
        park_open = ParkActivitySnapshot.park_appears_open == True

        # Case expressions for weighted downtime and operating counts
        weighted_down_case = case(
            (and_(park_open, is_down_expr), func.coalesce(RideClassification.tier_weight, 2)),
            else_=0
        )
        operating_case = case((and_(park_open, is_operating_expr), 1), else_=0)

        # Subquery to aggregate shame scores per snapshot
        snapshot_shame_subq = (
            select(
                Park.park_id,
                Park.name.label('park_name'),
                Park.city,
                Park.state_province,
                park_weights_subq.c.total_park_weight,
                func.sum(weighted_down_case).label('weighted_down'),
                func.sum(operating_case).label('operating_count'),
                func.sum(case((park_open, 1), else_=0)).label('total_park_open_snapshots')
            )
            .select_from(RideStatusSnapshot)
            .join(Ride, RideStatusSnapshot.ride_id == Ride.ride_id)
            .join(Park, Ride.park_id == Park.park_id)
            .outerjoin(RideClassification, Ride.ride_id == RideClassification.ride_id)
            .join(park_weights_subq, Park.park_id == park_weights_subq.c.park_id)
            .outerjoin(
                ParkActivitySnapshot,
                and_(
                    Park.park_id == ParkActivitySnapshot.park_id,
                    RideStatusSnapshot.recorded_at == ParkActivitySnapshot.recorded_at
                )
            )
            .where(RideStatusSnapshot.recorded_at >= start_utc)
            .where(RideStatusSnapshot.recorded_at < end_utc)
            .where(Ride.ride_id.in_(select(rides_operated_subq.c.ride_id)))
            .group_by(
                RideStatusSnapshot.recorded_at,
                Park.park_id,
                Park.name,
                Park.city,
                Park.state_province,
                park_weights_subq.c.total_park_weight
            )
        ).subquery()

        # Final query to average the per-snapshot shame scores
        stmt = (
            select(
                snapshot_shame_subq.c.park_id,
                snapshot_shame_subq.c.park_name,
                func.concat(snapshot_shame_subq.c.city, ', ', func.coalesce(snapshot_shame_subq.c.state_province, '')).label('location'),
                func.round(
                    func.avg(snapshot_shame_subq.c.weighted_down / func.nullif(snapshot_shame_subq.c.total_park_weight, 0) * 10),
                    1
                ).label('avg_shame_score'),
                func.round(
                    100.0 * func.sum(snapshot_shame_subq.c.operating_count) / func.nullif(func.sum(snapshot_shame_subq.c.total_park_open_snapshots), 0),
                    1
                ).label('uptime_percentage')
            )
            .group_by(
                snapshot_shame_subq.c.park_id,
                snapshot_shame_subq.c.park_name,
                snapshot_shame_subq.c.city,
                snapshot_shame_subq.c.state_province
            )
            .having(
                func.avg(snapshot_shame_subq.c.weighted_down / func.nullif(snapshot_shame_subq.c.total_park_weight, 0) * 10) >= min_avg_shame
            )
            .order_by(literal('avg_shame_score').desc())
            .limit(limit)
        )

        return self.execute_and_fetchall(stmt)

    def _get_daily_aggregate_parks(
        self,
        days: int,
        filter_disney_universal: bool,
        limit: int,
        min_avg_shame: float,
    ) -> List[Dict[str, Any]]:
        """
        Get park-level reliability rankings from daily stats (7days/30days).
        """
        today = get_today_pacific()
        start_date = today - timedelta(days=days - 1)

        stmt = (
            select(
                Park.park_id,
                Park.name.label('park_name'),
                func.concat(Park.city, ', ', func.coalesce(Park.state_province, '')).label('location'),
                func.round(func.avg(ParkDailyStats.shame_score), 1).label('avg_shame_score'),
                func.round(func.avg(ParkDailyStats.avg_uptime_percentage), 1).label('uptime_percentage')
            )
            .select_from(Park)
            .join(ParkDailyStats, Park.park_id == ParkDailyStats.park_id)
            .where(ParkDailyStats.stat_date >= start_date)
            .where(ParkDailyStats.stat_date <= today)
            .where(ParkDailyStats.shame_score >= min_avg_shame)
            .where(Park.is_active == True)
        )

        if filter_disney_universal:
            stmt = stmt.where(or_(Park.is_disney == True, Park.is_universal == True))

        stmt = (
            stmt.group_by(Park.park_id, Park.name, Park.city, Park.state_province)
            .order_by(literal('avg_shame_score').desc())
            .limit(limit)
        )

        return self.execute_and_fetchall(stmt)
