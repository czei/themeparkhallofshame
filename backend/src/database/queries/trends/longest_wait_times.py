"""
Longest Wait Times Query (Awards)
=================================

Endpoint: GET /api/trends/longest-wait-times
UI Location: Trends tab → Awards section

Returns top 10 rides ranked by average wait time (in minutes).

IMPORTANT: Rankings use avg_wait_time to match the Wait Times table.
This ensures consistency between Awards and the main rankings.

Uses CTE-based queries for performance on large snapshot tables.

Periods:
- today: Aggregates from ride_status_snapshots (midnight Pacific to now)
- last_week/last_month: Aggregates from ride_daily_stats

Database Tables:
- rides (ride metadata)
- parks (park metadata)
- ride_status_snapshots + park_activity_snapshots (TODAY period)
- ride_daily_stats (last_week/last_month periods)
"""

from datetime import date, timedelta
from typing import List, Dict, Any

from sqlalchemy import select, func, and_, or_, literal_column
from sqlalchemy.orm import Session

from models import Park, Ride, RideStatusSnapshot, ParkActivitySnapshot, RideDailyStats
from utils.query_helpers import QueryClassBase
from utils.timezone import (
    get_today_range_to_now_utc,
    get_today_pacific,
    get_yesterday_range_utc,
    get_last_week_date_range,
    get_last_month_date_range,
)
from utils.metrics import SNAPSHOT_INTERVAL_MINUTES


class LongestWaitTimesQuery(QueryClassBase):
    """
    Query for rides with highest average wait times.

    IMPORTANT: Rankings use avg_wait_time (not cumulative_wait_hours)
    to match the Wait Times table for consistency.

    Uses SNAPSHOT_INTERVAL_MINUTES from utils.metrics (10 minutes).
    """

    def get_rankings(
        self,
        period: str = 'today',
        filter_disney_universal: bool = False,
        limit: int = 10,
    ) -> List[Dict[str, Any]]:
        """
        Get rides ranked by average wait time.

        Args:
            period: 'today', 'yesterday', 'last_week', or 'last_month'
            filter_disney_universal: Only Disney/Universal parks
            limit: Maximum results (default 10)

        Returns:
            List of rides with average wait time (ranked highest first)
        """
        if period == 'today':
            return self._get_today(filter_disney_universal, limit)
        elif period == 'yesterday':
            # Use snapshot data for yesterday (daily stats not yet aggregated)
            return self._get_yesterday(filter_disney_universal, limit)
        elif period == 'last_week':
            # Use previous calendar week (Sunday-Saturday)
            start_date, end_date, _ = get_last_week_date_range()
            return self._get_date_range_aggregate(start_date, end_date, filter_disney_universal, limit)
        elif period == 'last_month':
            # Use previous calendar month
            start_date, end_date, _ = get_last_month_date_range()
            return self._get_date_range_aggregate(start_date, end_date, filter_disney_universal, limit)
        elif period == '7days':
            # Rolling 7-day window (for backwards compatibility)
            return self._get_daily_aggregate(7, filter_disney_universal, limit)
        elif period == '30days':
            # Rolling 30-day window (for backwards compatibility)
            return self._get_daily_aggregate(30, filter_disney_universal, limit)
        else:
            raise ValueError(f"Invalid period: {period}. Must be 'today', 'yesterday', 'last_week', or 'last_month'")

    def _get_today(
        self,
        filter_disney_universal: bool = False,
        limit: int = 10,
    ) -> List[Dict[str, Any]]:
        """
        Get average wait times from TODAY (snapshot data).

        Ranked by avg_wait_time to match the Wait Times table.
        Uses park_appears_open for consistency with other queries.

        Uses CTE-based query for performance on large snapshot tables.
        """
        start_utc, now_utc = get_today_range_to_now_utc()

        # CTE 1: active_rides - Pre-filter active attraction rides
        active_rides_cte = (
            select(
                Ride.ride_id,
                Ride.name.label('ride_name'),
                Park.park_id,
                Park.name.label('park_name')
            )
            .select_from(Ride)
            .join(Park, Ride.park_id == Park.park_id)
            .where(Ride.is_active == True)
            .where(Ride.category == 'ATTRACTION')
            .where(Park.is_active == True)
        )

        # Apply Disney/Universal filter if requested
        if filter_disney_universal:
            active_rides_cte = active_rides_cte.where(
                or_(Park.is_disney == True, Park.is_universal == True)
            )

        active_rides_cte = active_rides_cte.cte('active_rides')

        # CTE 2: wait_time_snapshots - Only select snapshots with wait times for active rides
        wait_time_snapshots_cte = (
            select(
                active_rides_cte.c.ride_id,
                active_rides_cte.c.ride_name,
                active_rides_cte.c.park_id,
                active_rides_cte.c.park_name,
                RideStatusSnapshot.wait_time
            )
            .select_from(RideStatusSnapshot)
            .join(
                active_rides_cte,
                RideStatusSnapshot.ride_id == active_rides_cte.c.ride_id
            )
            .join(
                ParkActivitySnapshot,
                and_(
                    active_rides_cte.c.park_id == ParkActivitySnapshot.park_id,
                    ParkActivitySnapshot.recorded_at == RideStatusSnapshot.recorded_at
                )
            )
            .where(RideStatusSnapshot.recorded_at >= start_utc)
            .where(RideStatusSnapshot.recorded_at <= now_utc)
            .where(RideStatusSnapshot.wait_time > 0)
            .where(ParkActivitySnapshot.park_appears_open == True)
        ).cte('wait_time_snapshots')

        # Main query - Aggregate by ride
        stmt = (
            select(
                wait_time_snapshots_cte.c.ride_id,
                wait_time_snapshots_cte.c.ride_name,
                wait_time_snapshots_cte.c.park_id,
                wait_time_snapshots_cte.c.park_name,
                func.round(func.avg(wait_time_snapshots_cte.c.wait_time), 0).label('avg_wait_time'),
                func.max(wait_time_snapshots_cte.c.wait_time).label('peak_wait_time'),
                func.count().label('snapshot_count')
            )
            .group_by(
                wait_time_snapshots_cte.c.ride_id,
                wait_time_snapshots_cte.c.ride_name,
                wait_time_snapshots_cte.c.park_id,
                wait_time_snapshots_cte.c.park_name
            )
            .order_by(literal_column('avg_wait_time').desc())
            .limit(limit)
        )

        return self.execute_and_fetchall(stmt)

    def _get_yesterday(
        self,
        filter_disney_universal: bool = False,
        limit: int = 10,
    ) -> List[Dict[str, Any]]:
        """
        Get average wait times from YESTERDAY (snapshot data).

        Same logic as _get_today() but for yesterday's full day UTC range.
        """
        start_utc, end_utc, _ = get_yesterday_range_utc()

        # CTE 1: active_rides - Pre-filter active attraction rides
        active_rides_cte = (
            select(
                Ride.ride_id,
                Ride.name.label('ride_name'),
                Park.park_id,
                Park.name.label('park_name')
            )
            .select_from(Ride)
            .join(Park, Ride.park_id == Park.park_id)
            .where(Ride.is_active == True)
            .where(Ride.category == 'ATTRACTION')
            .where(Park.is_active == True)
        )

        # Apply Disney/Universal filter if requested
        if filter_disney_universal:
            active_rides_cte = active_rides_cte.where(
                or_(Park.is_disney == True, Park.is_universal == True)
            )

        active_rides_cte = active_rides_cte.cte('active_rides')

        # CTE 2: wait_time_snapshots - Only select snapshots with wait times for active rides
        wait_time_snapshots_cte = (
            select(
                active_rides_cte.c.ride_id,
                active_rides_cte.c.ride_name,
                active_rides_cte.c.park_id,
                active_rides_cte.c.park_name,
                RideStatusSnapshot.wait_time
            )
            .select_from(RideStatusSnapshot)
            .join(
                active_rides_cte,
                RideStatusSnapshot.ride_id == active_rides_cte.c.ride_id
            )
            .join(
                ParkActivitySnapshot,
                and_(
                    active_rides_cte.c.park_id == ParkActivitySnapshot.park_id,
                    ParkActivitySnapshot.recorded_at == RideStatusSnapshot.recorded_at
                )
            )
            .where(RideStatusSnapshot.recorded_at >= start_utc)
            .where(RideStatusSnapshot.recorded_at < end_utc)
            .where(RideStatusSnapshot.wait_time > 0)
            .where(ParkActivitySnapshot.park_appears_open == True)
        ).cte('wait_time_snapshots')

        # Main query - Aggregate by ride
        stmt = (
            select(
                wait_time_snapshots_cte.c.ride_id,
                wait_time_snapshots_cte.c.ride_name,
                wait_time_snapshots_cte.c.park_id,
                wait_time_snapshots_cte.c.park_name,
                func.round(func.avg(wait_time_snapshots_cte.c.wait_time), 0).label('avg_wait_time'),
                func.max(wait_time_snapshots_cte.c.wait_time).label('peak_wait_time'),
                func.count().label('snapshot_count')
            )
            .group_by(
                wait_time_snapshots_cte.c.ride_id,
                wait_time_snapshots_cte.c.ride_name,
                wait_time_snapshots_cte.c.park_id,
                wait_time_snapshots_cte.c.park_name
            )
            .order_by(literal_column('avg_wait_time').desc())
            .limit(limit)
        )

        return self.execute_and_fetchall(stmt)

    def _get_date_range_aggregate(
        self,
        start_date: date,
        end_date: date,
        filter_disney_universal: bool = False,
        limit: int = 10,
    ) -> List[Dict[str, Any]]:
        """
        Get average wait times from daily stats for a specific date range.

        Ranked by avg_wait_time to match the Wait Times table.

        Args:
            start_date: First date to include (inclusive)
            end_date: Last date to include (inclusive)
            filter_disney_universal: Only Disney/Universal parks
            limit: Maximum results
        """
        stmt = (
            select(
                Ride.ride_id,
                Ride.name.label('ride_name'),
                Park.park_id,
                Park.name.label('park_name'),
                func.round(func.avg(RideDailyStats.avg_wait_time), 0).label('avg_wait_time'),
                func.max(RideDailyStats.peak_wait_time).label('peak_wait_time'),
                func.count(RideDailyStats.stat_id).label('days_with_data')
            )
            .select_from(Ride)
            .join(Park, Ride.park_id == Park.park_id)
            .join(RideDailyStats, Ride.ride_id == RideDailyStats.ride_id)
            .where(RideDailyStats.stat_date >= start_date)
            .where(RideDailyStats.stat_date <= end_date)
            .where(RideDailyStats.avg_wait_time > 0)
            .where(Ride.is_active == True)
            .where(Ride.category == 'ATTRACTION')
            .where(Park.is_active == True)
        )

        # Apply Disney/Universal filter if requested
        if filter_disney_universal:
            stmt = stmt.where(
                or_(Park.is_disney == True, Park.is_universal == True)
            )

        stmt = stmt.group_by(
            Ride.ride_id,
            Ride.name,
            Park.park_id,
            Park.name
        ).order_by(
            literal_column('avg_wait_time').desc()
        ).limit(limit)

        return self.execute_and_fetchall(stmt)

    def _get_daily_aggregate(
        self,
        days: int,
        filter_disney_universal: bool = False,
        limit: int = 10,
    ) -> List[Dict[str, Any]]:
        """
        Get average wait times using a rolling N-day window.

        Used for '7days' and '30days' periods (backwards compatibility).
        For calendar periods, use _get_date_range_aggregate() instead.

        Args:
            days: Number of days to look back (inclusive of today)
            filter_disney_universal: Only Disney/Universal parks
            limit: Maximum results
        """
        today = get_today_pacific()
        start_date = today - timedelta(days=days - 1)
        return self._get_date_range_aggregate(start_date, today, filter_disney_universal, limit)

    # ========================================
    # PARK-LEVEL RANKINGS
    # ========================================

    def get_park_rankings(
        self,
        period: str = 'today',
        filter_disney_universal: bool = False,
        limit: int = 10,
    ) -> List[Dict[str, Any]]:
        """
        Get parks ranked by average wait time.

        Args:
            period: 'today', 'yesterday', 'last_week', or 'last_month'
            filter_disney_universal: Only Disney/Universal parks
            limit: Maximum results (default 10)

        Returns:
            List of parks with average wait time (ranked highest first)
        """
        if period == 'today':
            return self._get_parks_today(filter_disney_universal, limit)
        elif period == 'yesterday':
            return self._get_parks_yesterday(filter_disney_universal, limit)
        elif period == 'last_week':
            # Use previous calendar week (Sunday-Saturday)
            start_date, end_date, _ = get_last_week_date_range()
            return self._get_parks_date_range_aggregate(start_date, end_date, filter_disney_universal, limit)
        elif period == 'last_month':
            # Use previous calendar month
            start_date, end_date, _ = get_last_month_date_range()
            return self._get_parks_date_range_aggregate(start_date, end_date, filter_disney_universal, limit)
        elif period == '7days':
            # Rolling 7-day window (for backwards compatibility)
            return self._get_parks_daily_aggregate(7, filter_disney_universal, limit)
        elif period == '30days':
            # Rolling 30-day window (for backwards compatibility)
            return self._get_parks_daily_aggregate(30, filter_disney_universal, limit)
        else:
            raise ValueError(f"Invalid period: {period}. Must be 'today', 'yesterday', 'last_week', or 'last_month'")

    def _get_parks_today(
        self,
        filter_disney_universal: bool = False,
        limit: int = 10,
    ) -> List[Dict[str, Any]]:
        """
        Get park-level wait time rankings from TODAY (snapshot data).

        Sorted by avg_wait_time to match the Wait Times table.
        Uses park_appears_open for consistency with other queries.
        Uses CTE-based query for performance on large snapshot tables.
        """
        start_utc, now_utc = get_today_range_to_now_utc()

        # CTE 1: active_rides - Pre-filter active attraction rides
        active_rides_cte = (
            select(
                Ride.ride_id,
                Park.park_id,
                Park.name.label('park_name'),
                Park.city,
                Park.state_province
            )
            .select_from(Ride)
            .join(Park, Ride.park_id == Park.park_id)
            .where(Ride.is_active == True)
            .where(Ride.category == 'ATTRACTION')
            .where(Park.is_active == True)
        )

        # Apply Disney/Universal filter if requested
        if filter_disney_universal:
            active_rides_cte = active_rides_cte.where(
                or_(Park.is_disney == True, Park.is_universal == True)
            )

        active_rides_cte = active_rides_cte.cte('active_rides')

        # CTE 2: wait_time_snapshots - Only select snapshots with wait times for active rides
        wait_time_snapshots_cte = (
            select(
                active_rides_cte.c.park_id,
                active_rides_cte.c.park_name,
                active_rides_cte.c.city,
                active_rides_cte.c.state_province,
                active_rides_cte.c.ride_id,
                RideStatusSnapshot.wait_time
            )
            .select_from(RideStatusSnapshot)
            .join(
                active_rides_cte,
                RideStatusSnapshot.ride_id == active_rides_cte.c.ride_id
            )
            .join(
                ParkActivitySnapshot,
                and_(
                    active_rides_cte.c.park_id == ParkActivitySnapshot.park_id,
                    ParkActivitySnapshot.recorded_at == RideStatusSnapshot.recorded_at
                )
            )
            .where(RideStatusSnapshot.recorded_at >= start_utc)
            .where(RideStatusSnapshot.recorded_at <= now_utc)
            .where(RideStatusSnapshot.wait_time > 0)
            .where(ParkActivitySnapshot.park_appears_open == True)
        ).cte('wait_time_snapshots')

        # Main query - Aggregate by park
        # Location concatenation
        location = func.concat(
            wait_time_snapshots_cte.c.city,
            literal_column("', '"),
            func.coalesce(wait_time_snapshots_cte.c.state_province, literal_column("''"))
        ).label('location')

        stmt = (
            select(
                wait_time_snapshots_cte.c.park_id,
                wait_time_snapshots_cte.c.park_name,
                location,
                func.round(func.avg(wait_time_snapshots_cte.c.wait_time), 0).label('avg_wait_time'),
                func.count(func.distinct(wait_time_snapshots_cte.c.ride_id)).label('rides_with_waits')
            )
            .group_by(
                wait_time_snapshots_cte.c.park_id,
                wait_time_snapshots_cte.c.park_name,
                wait_time_snapshots_cte.c.city,
                wait_time_snapshots_cte.c.state_province
            )
            .order_by(literal_column('avg_wait_time').desc())
            .limit(limit)
        )

        return self.execute_and_fetchall(stmt)

    def _get_parks_yesterday(
        self,
        filter_disney_universal: bool = False,
        limit: int = 10,
    ) -> List[Dict[str, Any]]:
        """
        Get park-level wait time rankings from YESTERDAY (snapshot data).

        Same logic as _get_parks_today() but for yesterday's full day UTC range.
        """
        start_utc, end_utc, _ = get_yesterday_range_utc()

        # CTE 1: active_rides - Pre-filter active attraction rides
        active_rides_cte = (
            select(
                Ride.ride_id,
                Park.park_id,
                Park.name.label('park_name'),
                Park.city,
                Park.state_province
            )
            .select_from(Ride)
            .join(Park, Ride.park_id == Park.park_id)
            .where(Ride.is_active == True)
            .where(Ride.category == 'ATTRACTION')
            .where(Park.is_active == True)
        )

        # Apply Disney/Universal filter if requested
        if filter_disney_universal:
            active_rides_cte = active_rides_cte.where(
                or_(Park.is_disney == True, Park.is_universal == True)
            )

        active_rides_cte = active_rides_cte.cte('active_rides')

        # CTE 2: wait_time_snapshots - Only select snapshots with wait times for active rides
        wait_time_snapshots_cte = (
            select(
                active_rides_cte.c.park_id,
                active_rides_cte.c.park_name,
                active_rides_cte.c.city,
                active_rides_cte.c.state_province,
                active_rides_cte.c.ride_id,
                RideStatusSnapshot.wait_time
            )
            .select_from(RideStatusSnapshot)
            .join(
                active_rides_cte,
                RideStatusSnapshot.ride_id == active_rides_cte.c.ride_id
            )
            .join(
                ParkActivitySnapshot,
                and_(
                    active_rides_cte.c.park_id == ParkActivitySnapshot.park_id,
                    ParkActivitySnapshot.recorded_at == RideStatusSnapshot.recorded_at
                )
            )
            .where(RideStatusSnapshot.recorded_at >= start_utc)
            .where(RideStatusSnapshot.recorded_at < end_utc)
            .where(RideStatusSnapshot.wait_time > 0)
            .where(ParkActivitySnapshot.park_appears_open == True)
        ).cte('wait_time_snapshots')

        # Main query - Aggregate by park
        # Location concatenation
        location = func.concat(
            wait_time_snapshots_cte.c.city,
            literal_column("', '"),
            func.coalesce(wait_time_snapshots_cte.c.state_province, literal_column("''"))
        ).label('location')

        stmt = (
            select(
                wait_time_snapshots_cte.c.park_id,
                wait_time_snapshots_cte.c.park_name,
                location,
                func.round(func.avg(wait_time_snapshots_cte.c.wait_time), 0).label('avg_wait_time'),
                func.count(func.distinct(wait_time_snapshots_cte.c.ride_id)).label('rides_with_waits')
            )
            .group_by(
                wait_time_snapshots_cte.c.park_id,
                wait_time_snapshots_cte.c.park_name,
                wait_time_snapshots_cte.c.city,
                wait_time_snapshots_cte.c.state_province
            )
            .order_by(literal_column('avg_wait_time').desc())
            .limit(limit)
        )

        return self.execute_and_fetchall(stmt)

    def _get_parks_date_range_aggregate(
        self,
        start_date: date,
        end_date: date,
        filter_disney_universal: bool = False,
        limit: int = 10,
    ) -> List[Dict[str, Any]]:
        """
        Get park-level wait time rankings from daily stats for a specific date range.

        Sorted by avg_wait_time (not cumulative hours - that just shows parks with most rides).

        Args:
            start_date: First date to include (inclusive)
            end_date: Last date to include (inclusive)
            filter_disney_universal: Only Disney/Universal parks
            limit: Maximum results
        """
        # Location concatenation
        location = func.concat(
            Park.city,
            literal_column("', '"),
            func.coalesce(Park.state_province, literal_column("''"))
        ).label('location')

        stmt = (
            select(
                Park.park_id,
                Park.name.label('park_name'),
                location,
                func.round(func.avg(RideDailyStats.avg_wait_time), 0).label('avg_wait_time'),
                func.count(func.distinct(Ride.ride_id)).label('rides_with_waits')
            )
            .select_from(Ride)
            .join(Park, Ride.park_id == Park.park_id)
            .join(RideDailyStats, Ride.ride_id == RideDailyStats.ride_id)
            .where(RideDailyStats.stat_date >= start_date)
            .where(RideDailyStats.stat_date <= end_date)
            .where(RideDailyStats.avg_wait_time > 0)
            .where(Ride.is_active == True)
            .where(Ride.category == 'ATTRACTION')
            .where(Park.is_active == True)
        )

        # Apply Disney/Universal filter if requested
        if filter_disney_universal:
            stmt = stmt.where(
                or_(Park.is_disney == True, Park.is_universal == True)
            )

        stmt = stmt.group_by(
            Park.park_id,
            Park.name,
            Park.city,
            Park.state_province
        ).order_by(
            literal_column('avg_wait_time').desc()
        ).limit(limit)

        return self.execute_and_fetchall(stmt)

    def _get_parks_daily_aggregate(
        self,
        days: int,
        filter_disney_universal: bool = False,
        limit: int = 10,
    ) -> List[Dict[str, Any]]:
        """
        Get park-level wait times using a rolling N-day window.

        Used for '7days' and '30days' periods (backwards compatibility).
        For calendar periods, use _get_parks_date_range_aggregate() instead.

        Args:
            days: Number of days to look back (inclusive of today)
            filter_disney_universal: Only Disney/Universal parks
            limit: Maximum results
        """
        today = get_today_pacific()
        start_date = today - timedelta(days=days - 1)
        return self._get_parks_date_range_aggregate(start_date, today, filter_disney_universal, limit)
