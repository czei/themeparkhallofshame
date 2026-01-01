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
from models.orm_ride import Ride
from models.orm_park import Park
from models.orm_snapshots import RideStatusSnapshot, ParkActivitySnapshot
from models.orm_stats import RideHourlyStats


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
    operating_snapshots: int  # Number of snapshots where ride was OPERATING
    down_snapshots: int  # Number of snapshots where ride was DOWN


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

    @staticmethod
    def is_down_python(snapshot, park) -> bool:
        """
        Pure Python implementation of is_down logic for unit testing.

        This mirrors the SQL logic but works with Python objects/mocks.

        Args:
            snapshot: Object with status and computed_is_open attributes
            park: Object with is_disney and is_universal attributes

        Returns:
            True if the ride should be counted as "down"
        """
        status = getattr(snapshot, 'status', None)
        computed_is_open = getattr(snapshot, 'computed_is_open', False)
        is_disney = getattr(park, 'is_disney', False)
        is_universal = getattr(park, 'is_universal', False)

        # If ride is operating, it's not down
        if status == 'OPERATING' or computed_is_open:
            return False

        # Disney/Universal: only DOWN counts
        if is_disney or is_universal:
            return status == 'DOWN'

        # Other parks: DOWN or CLOSED counts
        return status in ('DOWN', 'CLOSED') or (status is None and not computed_is_open)


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

        READS FROM PRE-AGGREGATED ride_hourly_stats TABLE.

        This table is populated by the hourly aggregation job and contains
        metrics already filtered by park_appears_open and business rules.
        Raw snapshots (ride_status_snapshots) are purged after aggregation,
        so historical queries MUST use this pre-aggregated table.

        Args:
            session: SQLAlchemy session
            ride_id: Ride ID
            start_utc: Start of time range (UTC, naive datetime)
            end_utc: End of time range (UTC, naive datetime, exclusive)

        Returns:
            List of RideHourlyMetrics, one per hour with data
        """
        # Query pre-aggregated hourly stats
        # This data already has park_appears_open filtering applied during aggregation
        hourly_query = (
            select(RideHourlyStats)
            .where(RideHourlyStats.ride_id == ride_id)
            .where(RideHourlyStats.hour_start_utc >= start_utc)
            .where(RideHourlyStats.hour_start_utc < end_utc)
            .order_by(RideHourlyStats.hour_start_utc)
        )

        rows = session.execute(hourly_query).scalars().all()

        results = []
        for row in rows:
            # Convert ORM model to RideHourlyMetrics namedtuple
            results.append(RideHourlyMetrics(
                hour_start_utc=row.hour_start_utc,
                avg_wait_time_minutes=float(row.avg_wait_time_minutes) if row.avg_wait_time_minutes is not None else None,
                uptime_percentage=float(row.uptime_percentage) if row.uptime_percentage is not None else 0.0,
                snapshot_count=int(row.snapshot_count) if row.snapshot_count is not None else 0,
                downtime_hours=float(row.downtime_hours) if row.downtime_hours is not None else 0.0,
                ride_operated=bool(row.ride_operated),
                operating_snapshots=int(row.operating_snapshots) if row.operating_snapshots is not None else 0,
                down_snapshots=int(row.down_snapshots) if row.down_snapshots is not None else 0
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


class PartitionAwareDateRange:
    """
    Partition-aware date range helpers for MySQL partition pruning.

    Feature 004: The ride_status_snapshots table is partitioned by:
    RANGE (YEAR(recorded_at) * 100 + MONTH(recorded_at))

    Partitions: p_before_2024, p202401...p203012, p_future

    CRITICAL: For MySQL to use partition pruning, queries MUST include:
    - Explicit recorded_at >= start_time bounds
    - Explicit recorded_at < end_time bounds
    - Bounds passed as parameters (not computed in SQL)

    Anti-patterns that PREVENT partition pruning:
    - WHERE YEAR(recorded_at) = 2025 (function on column)
    - WHERE DATE(recorded_at) = '2025-01-01' (function on column)
    - WHERE recorded_at BETWEEN (func.now() - interval) AND func.now() (computed bounds)

    Usage:
        bounds = PartitionAwareDateRange.for_period('yesterday')
        stmt = select(RideStatusSnapshot).where(
            RideStatusSnapshot.recorded_at >= bounds.start,
            RideStatusSnapshot.recorded_at < bounds.end
        )
    """

    class DateBounds(NamedTuple):
        """Start and end bounds for a date range query."""
        start: datetime
        end: datetime
        period_name: str

    @staticmethod
    def for_today(reference_time: Optional[datetime] = None) -> 'PartitionAwareDateRange.DateBounds':
        """
        Get partition-friendly bounds for TODAY period.

        Args:
            reference_time: Reference time (defaults to UTC now)

        Returns:
            DateBounds with start at midnight today, end at midnight tomorrow
        """
        now = reference_time or datetime.utcnow()
        today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        tomorrow_start = today_start + timedelta(days=1)
        return PartitionAwareDateRange.DateBounds(
            start=today_start,
            end=tomorrow_start,
            period_name='today'
        )

    @staticmethod
    def for_yesterday(reference_time: Optional[datetime] = None) -> 'PartitionAwareDateRange.DateBounds':
        """
        Get partition-friendly bounds for YESTERDAY period.

        Args:
            reference_time: Reference time (defaults to UTC now)

        Returns:
            DateBounds with start at midnight yesterday, end at midnight today
        """
        now = reference_time or datetime.utcnow()
        today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        yesterday_start = today_start - timedelta(days=1)
        return PartitionAwareDateRange.DateBounds(
            start=yesterday_start,
            end=today_start,
            period_name='yesterday'
        )

    @staticmethod
    def for_last_week(reference_time: Optional[datetime] = None) -> 'PartitionAwareDateRange.DateBounds':
        """
        Get partition-friendly bounds for LAST_WEEK period (7 days).

        Args:
            reference_time: Reference time (defaults to UTC now)

        Returns:
            DateBounds with start 7 days ago at midnight, end at midnight today
        """
        now = reference_time or datetime.utcnow()
        today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        week_ago = today_start - timedelta(days=7)
        return PartitionAwareDateRange.DateBounds(
            start=week_ago,
            end=today_start,
            period_name='last_week'
        )

    @staticmethod
    def for_last_month(reference_time: Optional[datetime] = None) -> 'PartitionAwareDateRange.DateBounds':
        """
        Get partition-friendly bounds for LAST_MONTH period (30 days).

        Args:
            reference_time: Reference time (defaults to UTC now)

        Returns:
            DateBounds with start 30 days ago at midnight, end at midnight today
        """
        now = reference_time or datetime.utcnow()
        today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        month_ago = today_start - timedelta(days=30)
        return PartitionAwareDateRange.DateBounds(
            start=month_ago,
            end=today_start,
            period_name='last_month'
        )

    @staticmethod
    def for_specific_month(year: int, month: int) -> 'PartitionAwareDateRange.DateBounds':
        """
        Get partition-friendly bounds for a specific calendar month.

        This is optimal for partition pruning as it exactly matches partition boundaries.

        Args:
            year: Year (e.g., 2025)
            month: Month (1-12)

        Returns:
            DateBounds with start at month start, end at next month start
        """
        month_start = datetime(year, month, 1, 0, 0, 0)
        if month == 12:
            next_month_start = datetime(year + 1, 1, 1, 0, 0, 0)
        else:
            next_month_start = datetime(year, month + 1, 1, 0, 0, 0)
        return PartitionAwareDateRange.DateBounds(
            start=month_start,
            end=next_month_start,
            period_name=f'{year}-{month:02d}'
        )

    @staticmethod
    def for_specific_year(year: int) -> 'PartitionAwareDateRange.DateBounds':
        """
        Get partition-friendly bounds for a specific calendar year.

        This accesses exactly 12 partitions (one per month).

        Args:
            year: Year (e.g., 2025)

        Returns:
            DateBounds with start at Jan 1, end at Jan 1 next year
        """
        year_start = datetime(year, 1, 1, 0, 0, 0)
        next_year_start = datetime(year + 1, 1, 1, 0, 0, 0)
        return PartitionAwareDateRange.DateBounds(
            start=year_start,
            end=next_year_start,
            period_name=f'{year}'
        )

    @staticmethod
    def for_period(period: str, reference_time: Optional[datetime] = None) -> 'PartitionAwareDateRange.DateBounds':
        """
        Get partition-friendly bounds for a named period.

        Args:
            period: Period name ('today', 'yesterday', 'last_week', 'last_month')
            reference_time: Reference time (defaults to UTC now)

        Returns:
            DateBounds for the requested period

        Raises:
            ValueError: If period is not recognized
        """
        period_lower = period.lower().replace('-', '_')

        if period_lower in ('today', 'live'):
            return PartitionAwareDateRange.for_today(reference_time)
        elif period_lower == 'yesterday':
            return PartitionAwareDateRange.for_yesterday(reference_time)
        elif period_lower in ('last_week', 'week', '7d'):
            return PartitionAwareDateRange.for_last_week(reference_time)
        elif period_lower in ('last_month', 'month', '30d'):
            return PartitionAwareDateRange.for_last_month(reference_time)
        else:
            raise ValueError(f"Unknown period: {period}. Use 'today', 'yesterday', 'last_week', or 'last_month'")

    @staticmethod
    def for_custom_range(start: datetime, end: datetime, period_name: str = 'custom') -> 'PartitionAwareDateRange.DateBounds':
        """
        Create partition-friendly bounds for a custom date range.

        Args:
            start: Start datetime (inclusive)
            end: End datetime (exclusive)
            period_name: Optional name for this range

        Returns:
            DateBounds for the custom range
        """
        return PartitionAwareDateRange.DateBounds(
            start=start,
            end=end,
            period_name=period_name
        )

    @staticmethod
    def apply_to_query(
        stmt: Select,
        bounds: 'PartitionAwareDateRange.DateBounds',
        column=None
    ) -> Select:
        """
        Apply partition-friendly date bounds to a query.

        Args:
            stmt: SQLAlchemy Select statement
            bounds: DateBounds from any for_* method
            column: Column to apply bounds to (defaults to RideStatusSnapshot.recorded_at)

        Returns:
            Modified Select statement with partition-friendly WHERE clauses
        """
        if column is None:
            column = RideStatusSnapshot.recorded_at

        return stmt.where(
            column >= bounds.start,
            column < bounds.end
        )


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
