"""
ORM Query Abstraction Layer for ThemePark HallOfShame
Provides reusable query primitives for ride status, downtime, and uptime calculations.

Phase 3 (T016) - Migrating from raw SQL to SQLAlchemy 2.0 ORM
Phase 4 (T023) - Hourly aggregation ORM queries
"""

from __future__ import annotations
from datetime import datetime, date, timedelta
from typing import Optional, List, Dict, Any, NamedTuple
from abc import ABC, abstractmethod
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


class RideStatusExpressions:
    """
    SINGLE SOURCE OF TRUTH for ride status expressions in ORM queries.

    All ride status logic MUST use these expressions to ensure consistency.

    Business Rules:
    1. Disney/Universal parks: Only status='DOWN' counts as downtime
       (CLOSED = scheduled closure, not a breakdown)
    2. Other parks: Both DOWN and CLOSED count as downtime
       (they don't distinguish between the two)
    3. Park must be open (park_appears_open=TRUE) for any status to count
    """

    @staticmethod
    def is_operating_expr():
        """
        SQLAlchemy expression for "ride is operating".

        A ride is operating if:
        - status='OPERATING' OR computed_is_open=TRUE
        """
        return or_(
            RideStatusSnapshot.status == 'OPERATING',
            RideStatusSnapshot.computed_is_open == True
        )

    @staticmethod
    def is_down_disney_universal_expr():
        """
        SQLAlchemy expression for "ride is down" at Disney/Universal parks.

        Disney/Universal properly distinguish:
        - DOWN = unexpected breakdown (counts as downtime)
        - CLOSED = scheduled closure (does NOT count as downtime)

        Returns: Expression that is TRUE when ride is broken
        """
        return RideStatusSnapshot.status == 'DOWN'

    @staticmethod
    def is_down_other_parks_expr():
        """
        SQLAlchemy expression for "ride is down" at non-Disney/Universal parks.

        Most other parks only report CLOSED for all non-operating rides.
        We must include both DOWN and CLOSED.

        Returns: Expression that is TRUE when ride is not operating
        """
        return or_(
            RideStatusSnapshot.status == 'DOWN',
            RideStatusSnapshot.status == 'CLOSED',
            and_(
                RideStatusSnapshot.status == None,
                RideStatusSnapshot.computed_is_open == False
            )
        )

    @staticmethod
    def is_down_for_park(is_disney: bool, is_universal: bool):
        """
        Get the correct "is_down" expression based on park type.

        Args:
            is_disney: True if this is a Disney park
            is_universal: True if this is a Universal park

        Returns:
            SQLAlchemy expression for "ride is down"
        """
        if is_disney or is_universal:
            return RideStatusExpressions.is_down_disney_universal_expr()
        else:
            return RideStatusExpressions.is_down_other_parks_expr()


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

        CRITICAL BUSINESS RULES (from CLAUDE.md):
        1. Only count snapshots when park_appears_open = TRUE
        2. Disney/Universal: Only status='DOWN' counts as downtime (not CLOSED)
        3. Other parks: Both DOWN and CLOSED count as downtime
        4. Ride must have operated while park was open to count toward metrics

        Computes metrics for each hour from ride_status_snapshots:
        - avg_wait_time_minutes: Average wait time (NULL if no wait data)
        - uptime_percentage: Percentage of snapshots where ride was operating
        - snapshot_count: Total snapshots in the hour (while park was open)
        - downtime_hours: Fractional hours of downtime (1.0 = full hour down)
        - ride_operated: Whether ride operated at any point while park was open

        Args:
            session: SQLAlchemy session
            ride_id: Ride ID
            start_utc: Start of time range (UTC, naive datetime)
            end_utc: End of time range (UTC, naive datetime, exclusive)

        Returns:
            List of RideHourlyMetrics, one per hour with data
        """
        from sqlalchemy import literal_column, case

        # First, get the park info to know if it's Disney/Universal
        ride_info = session.execute(
            select(Ride.park_id, Park.is_disney, Park.is_universal)
            .join(Park, Ride.park_id == Park.park_id)
            .where(Ride.ride_id == ride_id)
        ).first()

        if not ride_info:
            return []

        park_id = ride_info.park_id
        is_disney = ride_info.is_disney
        is_universal = ride_info.is_universal

        # Determine if ride operated while park was open
        # CRITICAL: Must join with ParkActivitySnapshot to check park_appears_open
        ride_operated_subquery = (
            select(func.count(RideStatusSnapshot.snapshot_id))
            .select_from(RideStatusSnapshot)
            .join(
                ParkActivitySnapshot,
                and_(
                    ParkActivitySnapshot.park_id == park_id,
                    func.date_format(ParkActivitySnapshot.recorded_at, '%Y-%m-%d %H:%i')
                    == func.date_format(RideStatusSnapshot.recorded_at, '%Y-%m-%d %H:%i')
                )
            )
            .where(RideStatusSnapshot.ride_id == ride_id)
            .where(RideStatusSnapshot.recorded_at >= start_utc)
            .where(RideStatusSnapshot.recorded_at < end_utc)
            .where(ParkActivitySnapshot.park_appears_open == True)  # CRITICAL: Park must be open
            .where(RideStatusExpressions.is_operating_expr())
        )
        ride_operated_count = session.execute(ride_operated_subquery).scalar() or 0
        ride_operated = ride_operated_count > 0

        # Single query with GROUP BY hour
        hour_start_expr = func.date_format(
            RideStatusSnapshot.recorded_at,
            literal_column("'%Y-%m-%d %H:00:00'")
        ).label('hour_start')

        # Operating = status='OPERATING' OR computed_is_open=TRUE
        is_operating_case = case(
            (RideStatusExpressions.is_operating_expr(), 1),
            else_=0
        )

        # Down count: Use park-type-aware logic
        # Disney/Universal: Only DOWN counts
        # Other parks: DOWN, CLOSED, or computed_is_open=FALSE
        if is_disney or is_universal:
            is_down_case = case(
                (RideStatusSnapshot.status == 'DOWN', 1),
                else_=0
            )
        else:
            is_down_case = case(
                (or_(
                    RideStatusSnapshot.status == 'DOWN',
                    RideStatusSnapshot.status == 'CLOSED',
                    and_(
                        RideStatusSnapshot.status == None,
                        RideStatusSnapshot.computed_is_open == False
                    )
                ), 1),
                else_=0
            )

        # CRITICAL: Join with ParkActivitySnapshot to filter by park_appears_open
        hourly_query = (
            select(
                hour_start_expr,
                func.count(RideStatusSnapshot.snapshot_id).label('total_count'),
                func.avg(RideStatusSnapshot.wait_time).label('avg_wait'),
                func.sum(is_operating_case).label('operating_count'),
                func.sum(is_down_case).label('down_count')
            )
            .select_from(RideStatusSnapshot)
            .join(
                ParkActivitySnapshot,
                and_(
                    ParkActivitySnapshot.park_id == park_id,
                    func.date_format(ParkActivitySnapshot.recorded_at, '%Y-%m-%d %H:%i')
                    == func.date_format(RideStatusSnapshot.recorded_at, '%Y-%m-%d %H:%i')
                )
            )
            .where(RideStatusSnapshot.ride_id == ride_id)
            .where(RideStatusSnapshot.recorded_at >= start_utc)
            .where(RideStatusSnapshot.recorded_at < end_utc)
            .where(ParkActivitySnapshot.park_appears_open == True)  # CRITICAL: Only count when park is open
            .group_by(hour_start_expr)
            .order_by(hour_start_expr)
        )

        rows = session.execute(hourly_query).all()

        results = []
        for row in rows:
            total_count = int(row.total_count or 0)
            if total_count > 0:
                operating_count = int(row.operating_count or 0)
                down_count = int(row.down_count or 0)
                avg_wait = float(row.avg_wait) if row.avg_wait is not None else None

                # Calculate uptime percentage (operating / total while park open)
                uptime_pct = (operating_count / total_count) * 100.0

                # Calculate downtime hours using park-type-aware down_count
                downtime_hours = (down_count / total_count)

                # Parse hour_start string back to datetime
                hour_start_dt = datetime.strptime(row.hour_start, '%Y-%m-%d %H:%M:%S')

                results.append(RideHourlyMetrics(
                    hour_start_utc=hour_start_dt,
                    avg_wait_time_minutes=avg_wait,
                    uptime_percentage=uptime_pct,
                    snapshot_count=total_count,
                    downtime_hours=downtime_hours,
                    ride_operated=ride_operated
                ))

        return results


class TimeIntervalHelper:
    """
    Helper methods for timedelta-based date math.

    Use these instead of raw SQL INTERVAL syntax (e.g., text('INTERVAL 30 DAY'))
    for database-agnostic time calculations.

    Phase 9 (T045) - Replace all MySQL-specific INTERVAL patterns.
    """

    @staticmethod
    def days_ago(n: int) -> datetime:
        """
        Return datetime N days in the past from now (UTC).

        Args:
            n: Number of days ago

        Returns:
            datetime N days before current UTC time

        Example:
            >>> TimeIntervalHelper.days_ago(30)  # 30 days ago
        """
        return datetime.utcnow() - timedelta(days=n)

    @staticmethod
    def hours_ago(n: int) -> datetime:
        """
        Return datetime N hours in the past from now (UTC).

        Args:
            n: Number of hours ago

        Returns:
            datetime N hours before current UTC time

        Example:
            >>> TimeIntervalHelper.hours_ago(1)  # 1 hour ago
        """
        return datetime.utcnow() - timedelta(hours=n)

    @staticmethod
    def date_n_days_ago(n: int) -> date:
        """
        Return date N days in the past from today.

        Args:
            n: Number of days ago

        Returns:
            date N days before today

        Example:
            >>> TimeIntervalHelper.date_n_days_ago(30)  # 30 days ago as date
        """
        return (datetime.utcnow() - timedelta(days=n)).date()

    @staticmethod
    def minutes_ago(n: int) -> datetime:
        """
        Return datetime N minutes in the past from now (UTC).

        Args:
            n: Number of minutes ago

        Returns:
            datetime N minutes before current UTC time
        """
        return datetime.utcnow() - timedelta(minutes=n)


class QueryClassBase(ABC):
    """
    Base class for all ORM query handler classes.

    Provides common functionality for Session-based query classes:
    - Consistent session management
    - Result row to dict conversion
    - Standard execute/fetch patterns

    Phase 10 (T046) - Foundation for migrating query classes from Connection to Session.

    Usage:
        class MyQueryClass(QueryClassBase):
            def get_stuff(self) -> List[Dict[str, Any]]:
                stmt = select(...)
                return self.execute_and_fetchall(stmt)
    """

    def __init__(self, session: Session):
        """
        Initialize with SQLAlchemy Session.

        Args:
            session: Active SQLAlchemy session
        """
        self.session = session

    def execute_and_fetchall(self, stmt) -> List[Dict[str, Any]]:
        """
        Execute a SELECT statement and return all rows as dicts.

        Args:
            stmt: SQLAlchemy Select statement

        Returns:
            List of dicts, one per row
        """
        result = self.session.execute(stmt)
        return [self._row_to_dict(row) for row in result]

    def execute_and_fetchone(self, stmt) -> Optional[Dict[str, Any]]:
        """
        Execute a SELECT statement and return first row as dict.

        Args:
            stmt: SQLAlchemy Select statement

        Returns:
            Dict for first row, or None if no results
        """
        result = self.session.execute(stmt)
        row = result.first()
        if row is None:
            return None
        return self._row_to_dict(row)

    def execute_scalar(self, stmt) -> Any:
        """
        Execute a SELECT statement and return single scalar value.

        Args:
            stmt: SQLAlchemy Select statement

        Returns:
            Scalar value from first column of first row
        """
        return self.session.execute(stmt).scalar()

    def _row_to_dict(self, row) -> Dict[str, Any]:
        """
        Convert a SQLAlchemy Row to a dictionary.

        Args:
            row: SQLAlchemy Row (from execute().all() or similar)

        Returns:
            Dict with column names as keys
        """
        if hasattr(row, '_mapping'):
            # SQLAlchemy 2.0 Row objects have _mapping
            return dict(row._mapping)
        elif hasattr(row, '_asdict'):
            # Named tuple style
            return row._asdict()
        elif hasattr(row, '__table__'):
            # ORM model object
            return {c.name: getattr(row, c.name) for c in row.__table__.columns}
        else:
            # Fallback: try direct dict conversion
            return dict(row)
