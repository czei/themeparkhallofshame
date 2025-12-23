"""
ORM Query Abstraction Layer for ThemePark HallOfShame
Provides reusable query primitives for ride status, downtime, and uptime calculations.

Phase 3 (T016) - Migrating from raw SQL to SQLAlchemy 2.0 ORM
Phase 4 (T023) - Hourly aggregation ORM queries
"""

from __future__ import annotations
from datetime import datetime, timedelta
from typing import Optional, List, NamedTuple
from decimal import Decimal

from sqlalchemy import select, func, and_, or_, extract
from sqlalchemy.orm import Session
from sqlalchemy.sql import Select

# Import ORM models from the project
from src.models.orm_ride import Ride
from src.models.orm_park import Park
from src.models.orm_snapshots import RideStatusSnapshot, ParkActivitySnapshot


class RideHourlyMetrics(NamedTuple):
    """
    Hourly metrics for a single ride in a single hour.

    This is the result type for HourlyAggregationQuery.ride_hour_range_metrics().
    """
    hour_start_utc: datetime
    avg_wait_time_minutes: Optional[float]
    uptime_percentage: float
    snapshot_count: int
    downtime_hours: float
    ride_operated: bool


class RideStatusQuery:
    """
    Helper methods for querying ride status snapshots.

    Follows business rules from CLAUDE.md:
    - Park status takes precedence over ride status
    - Rides must have operated to count toward metrics
    """

    @staticmethod
    def rides_that_operated(
        session: Session,
        start_time: datetime,
        end_time: datetime,
        park_id: Optional[int] = None
    ) -> Select:
        """
        Return Select statement for rides that operated in time range.

        Business Rule: A ride "operated" if it had at least one snapshot with:
        - status='OPERATING' OR computed_is_open=TRUE
        - AND park_appears_open=TRUE (park was open)

        Args:
            session: SQLAlchemy session
            start_time: Start of time range (inclusive)
            end_time: End of time range (exclusive)
            park_id: Optional park filter

        Returns:
            SQLAlchemy Select for distinct Ride objects that operated

        Example:
            >>> stmt = RideStatusQuery.rides_that_operated(session, start, end)
            >>> rides = session.execute(stmt).scalars().all()
        """
        # Subquery for rides with operating snapshots
        operating_subquery = (
            select(RideStatusSnapshot.ride_id)
            .where(RideStatusSnapshot.recorded_at >= start_time)
            .where(RideStatusSnapshot.recorded_at < end_time)
            .where(RideStatusSnapshot.park_appears_open == True)
            .where(
                or_(
                    RideStatusSnapshot.status == 'OPERATING',
                    RideStatusSnapshot.computed_is_open == True
                )
            )
            .distinct()
        )

        # Main query
        stmt = select(Ride).where(Ride.ride_id.in_(operating_subquery))

        if park_id:
            stmt = stmt.where(Ride.park_id == park_id)

        return stmt

    @staticmethod
    def snapshots_in_range(
        session: Session,
        start_time: datetime,
        end_time: datetime,
        ride_id: Optional[int] = None,
        park_id: Optional[int] = None
    ) -> Select:
        """
        Return Select statement for ride status snapshots in time range.

        Args:
            session: SQLAlchemy session
            start_time: Start of time range (inclusive)
            end_time: End of time range (exclusive)
            ride_id: Optional ride filter
            park_id: Optional park filter

        Returns:
            SQLAlchemy Select for RideStatusSnapshot objects
        """
        stmt = (
            select(RideStatusSnapshot)
            .where(RideStatusSnapshot.recorded_at >= start_time)
            .where(RideStatusSnapshot.recorded_at < end_time)
        )

        if ride_id:
            stmt = stmt.where(RideStatusSnapshot.ride_id == ride_id)

        if park_id:
            stmt = stmt.join(Ride).where(Ride.park_id == park_id)

        return stmt.order_by(RideStatusSnapshot.recorded_at)


class DowntimeQuery:
    """
    Helper methods for calculating ride downtime.

    Downtime is calculated from snapshots where:
    - Park was open (park_appears_open=TRUE)
    - Ride was down (status='DOWN' or not operating)
    - Ride had operated at some point (to exclude permanently closed rides)
    """

    @staticmethod
    def count_down_snapshots(
        session: Session,
        ride_id: int,
        start_time: datetime,
        end_time: datetime
    ) -> int:
        """
        Count snapshots where ride was down during the period.

        Only counts snapshots where:
        - park_appears_open=TRUE (park was open)
        - Ride was NOT operating

        Args:
            session: SQLAlchemy session
            ride_id: Ride ID
            start_time: Start of time range
            end_time: End of time range

        Returns:
            Count of down snapshots
        """
        result = session.execute(
            select(func.count(RideStatusSnapshot.snapshot_id))
            .where(RideStatusSnapshot.ride_id == ride_id)
            .where(RideStatusSnapshot.recorded_at >= start_time)
            .where(RideStatusSnapshot.recorded_at < end_time)
            .where(RideStatusSnapshot.park_appears_open == True)
            .where(
                and_(
                    RideStatusSnapshot.status != 'OPERATING',
                    RideStatusSnapshot.computed_is_open == False
                )
            )
        ).scalar()

        return int(result or 0)

    @staticmethod
    def rides_down_in_period(
        session: Session,
        start_time: datetime,
        end_time: datetime,
        park_id: Optional[int] = None
    ) -> Select:
        """
        Return Select for rides that were down during the period.

        Args:
            session: SQLAlchemy session
            start_time: Start of time range
            end_time: End of time range
            park_id: Optional park filter

        Returns:
            SQLAlchemy Select for Ride objects with downtime
        """
        # Subquery for rides with down snapshots
        down_subquery = (
            select(RideStatusSnapshot.ride_id)
            .where(RideStatusSnapshot.recorded_at >= start_time)
            .where(RideStatusSnapshot.recorded_at < end_time)
            .where(RideStatusSnapshot.park_appears_open == True)
            .where(
                and_(
                    RideStatusSnapshot.status != 'OPERATING',
                    RideStatusSnapshot.computed_is_open == False
                )
            )
            .distinct()
        )

        stmt = select(Ride).where(Ride.ride_id.in_(down_subquery))

        if park_id:
            stmt = stmt.where(Ride.park_id == park_id)

        return stmt


class UptimeQuery:
    """
    Helper methods for calculating ride uptime.

    Uptime is calculated from snapshots where:
    - Park was open (park_appears_open=TRUE)
    - Ride was operating (status='OPERATING' OR computed_is_open=TRUE)
    """

    @staticmethod
    def count_operating_snapshots(
        session: Session,
        ride_id: int,
        start_time: datetime,
        end_time: datetime
    ) -> int:
        """
        Count snapshots where ride was operating.

        Only counts snapshots where:
        - park_appears_open=TRUE (park was open)
        - Ride was operating

        Args:
            session: SQLAlchemy session
            ride_id: Ride ID
            start_time: Start of time range
            end_time: End of time range

        Returns:
            Count of operating snapshots
        """
        result = session.execute(
            select(func.count(RideStatusSnapshot.snapshot_id))
            .where(RideStatusSnapshot.ride_id == ride_id)
            .where(RideStatusSnapshot.recorded_at >= start_time)
            .where(RideStatusSnapshot.recorded_at < end_time)
            .where(RideStatusSnapshot.park_appears_open == True)
            .where(
                or_(
                    RideStatusSnapshot.status == 'OPERATING',
                    RideStatusSnapshot.computed_is_open == True
                )
            )
        ).scalar()

        return int(result or 0)

    @staticmethod
    def calculate_uptime_percentage(
        session: Session,
        ride_id: int,
        start_time: datetime,
        end_time: datetime
    ) -> Decimal:
        """
        Calculate uptime percentage for a ride.

        Formula: (operating_snapshots / total_snapshots_while_park_open) * 100

        Args:
            session: SQLAlchemy session
            ride_id: Ride ID
            start_time: Start of time range
            end_time: End of time range

        Returns:
            Uptime percentage (0.00 - 100.00)
        """
        # Count total snapshots while park was open
        total = session.execute(
            select(func.count(RideStatusSnapshot.snapshot_id))
            .where(RideStatusSnapshot.ride_id == ride_id)
            .where(RideStatusSnapshot.recorded_at >= start_time)
            .where(RideStatusSnapshot.recorded_at < end_time)
            .where(RideStatusSnapshot.park_appears_open == True)
        ).scalar()

        if not total or total == 0:
            return Decimal('0.00')

        # Count operating snapshots
        operating = UptimeQuery.count_operating_snapshots(
            session, ride_id, start_time, end_time
        )

        # Calculate percentage
        percentage = (Decimal(operating) / Decimal(total)) * Decimal('100.00')

        # Clamp to [0, 100]
        return max(Decimal('0.00'), min(Decimal('100.00'), percentage))


class ParkStatusQuery:
    """
    Helper methods for querying park activity and status.
    """

    @staticmethod
    def parks_open_in_period(
        session: Session,
        start_time: datetime,
        end_time: datetime
    ) -> Select:
        """
        Return Select for parks that were open during the period.

        Args:
            session: SQLAlchemy session
            start_time: Start of time range
            end_time: End of time range

        Returns:
            SQLAlchemy Select for Park objects that were open
        """
        # Subquery for parks with park_appears_open snapshots
        open_parks_subquery = (
            select(RideStatusSnapshot.ride_id)
            .join(Ride)
            .where(RideStatusSnapshot.recorded_at >= start_time)
            .where(RideStatusSnapshot.recorded_at < end_time)
            .where(RideStatusSnapshot.park_appears_open == True)
            .with_only_columns(Ride.park_id)
            .distinct()
        )

        return select(Park).where(Park.park_id.in_(open_parks_subquery))


class HourlyAggregationQuery:
    """
    ORM-based hourly aggregation queries.

    Replaces ride_hourly_stats table with on-the-fly calculations from ride_status_snapshots.
    This is the Single Source of Truth for all hourly metrics (T023).

    Business Rules:
    1. Only count hours where park_appears_open = TRUE
    2. Only count downtime AFTER the ride operated during the Pacific calendar day
    3. Ride "operated" if it had status='OPERATING' OR computed_is_open=TRUE while park was open
    """

    @staticmethod
    def ride_hour_range_metrics(
        session: Session,
        ride_id: int,
        start_utc: datetime,
        end_utc: datetime,
    ) -> List[RideHourlyMetrics]:
        """
        Get hourly metrics for a ride within a UTC time range.

        Computes metrics for each hour from ride_status_snapshots:
        - avg_wait_time_minutes: Average wait time (NULL if no wait data)
        - uptime_percentage: Percentage of snapshots where ride was operating
        - snapshot_count: Total snapshots in the hour
        - downtime_hours: Fractional hours of downtime (1.0 = full hour down)
        - ride_operated: Whether ride operated at any point in the Pacific day

        Args:
            session: SQLAlchemy session
            ride_id: Ride ID
            start_utc: Start of time range (UTC, naive datetime)
            end_utc: End of time range (UTC, naive datetime, exclusive)

        Returns:
            List of RideHourlyMetrics, one per hour with data

        Example:
            >>> metrics = HourlyAggregationQuery.ride_hour_range_metrics(
            ...     session, ride_id=123, start_utc=start, end_utc=end
            ... )
            >>> for m in metrics:
            ...     print(f"Hour {m.hour_start_utc}: {m.uptime_percentage}% uptime")
        """
        # First, determine if ride operated at all in the date range (Pacific day)
        # For simplicity, we check if the ride operated in the entire UTC range
        ride_operated_subquery = (
            select(func.count(RideStatusSnapshot.snapshot_id))
            .where(RideStatusSnapshot.ride_id == ride_id)
            .where(RideStatusSnapshot.recorded_at >= start_utc)
            .where(RideStatusSnapshot.recorded_at < end_utc)
            .where(RideStatusSnapshot.park_appears_open == True)
            .where(
                or_(
                    RideStatusSnapshot.status == 'OPERATING',
                    RideStatusSnapshot.computed_is_open == True
                )
            )
        )
        ride_operated_count = session.execute(ride_operated_subquery).scalar() or 0
        ride_operated = ride_operated_count > 0

        # Generate hourly buckets
        results = []
        current_hour = start_utc.replace(minute=0, second=0, microsecond=0)

        while current_hour < end_utc:
            next_hour = current_hour + timedelta(hours=1)

            # Query snapshots for this hour
            hour_snapshots = (
                select(
                    func.count(RideStatusSnapshot.snapshot_id).label('total_count'),
                    func.avg(RideStatusSnapshot.wait_time).label('avg_wait'),
                    func.sum(
                        func.cast(
                            or_(
                                RideStatusSnapshot.status == 'OPERATING',
                                RideStatusSnapshot.computed_is_open == True
                            ),
                            type_=Decimal
                        )
                    ).label('operating_count')
                )
                .where(RideStatusSnapshot.ride_id == ride_id)
                .where(RideStatusSnapshot.recorded_at >= current_hour)
                .where(RideStatusSnapshot.recorded_at < next_hour)
                .where(RideStatusSnapshot.park_appears_open == True)
            )

            row = session.execute(hour_snapshots).first()

            total_count = int(row.total_count or 0) if row else 0

            if total_count > 0:
                operating_count = int(row.operating_count or 0) if row else 0
                avg_wait = float(row.avg_wait) if row and row.avg_wait is not None else None

                # Calculate uptime percentage
                uptime_pct = (operating_count / total_count) * 100.0 if total_count > 0 else 0.0

                # Calculate downtime hours (fraction of hour)
                # Assume 6 snapshots per hour (10-minute intervals)
                # Downtime = (1 - operating_count/total_count) hours
                down_count = total_count - operating_count
                downtime_hours = (down_count / total_count) if total_count > 0 else 0.0

                results.append(RideHourlyMetrics(
                    hour_start_utc=current_hour,
                    avg_wait_time_minutes=avg_wait,
                    uptime_percentage=uptime_pct,
                    snapshot_count=total_count,
                    downtime_hours=downtime_hours,
                    ride_operated=ride_operated
                ))

            current_hour = next_hour

        return results
