"""
Query Filters
=============

Reusable WHERE clause conditions for common filtering patterns.

Replaces: utils/sql_helpers.py RideFilterSQL class

Usage:
    from database.queries.builders import Filters
    from database.schema import parks, rides

    stmt = select(parks).where(Filters.disney_universal(parks))

How to Add a Filter:
1. Add a static method that returns a SQLAlchemy BooleanClauseList
2. Use and_(), or_() from sqlalchemy to combine conditions
3. Accept table references as parameters for flexibility
"""

from datetime import datetime, timedelta
from typing import Optional

from sqlalchemy import and_, or_
from sqlalchemy.sql import ColumnElement
from sqlalchemy.sql.expression import BinaryExpression

from database.schema import parks, rides
from utils.metrics import LIVE_WINDOW_HOURS


# =============================================================================
# CONSTANTS
# =============================================================================
# Time window for "live" data - only consider snapshots from last 2 hours
# Imported from utils.sql_helpers (single source of truth)


class Filters:
    """
    Reusable filter conditions for WHERE clauses.

    All methods return SQLAlchemy expression objects that can be used
    in .where() clauses or combined with and_() / or_().

    Example:
        stmt = (
            select(parks.c.name)
            .where(Filters.disney_universal())
            .where(Filters.active_park())
        )
    """

    # =========================================================================
    # PARK FILTERS
    # =========================================================================

    @staticmethod
    def disney_universal(parks_table=parks) -> BinaryExpression:
        """
        Filter to Disney and Universal parks only.

        Replaces: RideFilterSQL.disney_universal_filter()

        Returns:
            Expression: (is_disney = TRUE OR is_universal = TRUE)
        """
        return or_(
            parks_table.c.is_disney == True,
            parks_table.c.is_universal == True,
        )

    @staticmethod
    def active_park(parks_table=parks) -> BinaryExpression:
        """
        Filter to only active (tracked) parks.

        Returns:
            Expression: is_active = TRUE
        """
        return parks_table.c.is_active == True

    # =========================================================================
    # RIDE FILTERS
    # =========================================================================

    @staticmethod
    def active_ride(rides_table=rides) -> BinaryExpression:
        """
        Filter to only active (tracked) rides.

        Returns:
            Expression: is_active = TRUE
        """
        return rides_table.c.is_active == True

    @staticmethod
    def attractions_only(rides_table=rides) -> BinaryExpression:
        """
        Filter to only ATTRACTION category (excludes MEET_AND_GREET, SHOW, EXPERIENCE).

        Returns:
            Expression: category = 'ATTRACTION'
        """
        return rides_table.c.category == "ATTRACTION"

    @staticmethod
    def active_attractions(
        rides_table=rides, parks_table=parks
    ) -> ColumnElement:
        """
        Filter to active attraction rides at active parks.

        Replaces: RideFilterSQL.active_attractions_filter()

        Returns:
            Expression: rides.is_active AND rides.category = 'ATTRACTION' AND parks.is_active
        """
        return and_(
            rides_table.c.is_active == True,
            rides_table.c.category == "ATTRACTION",
            parks_table.c.is_active == True,
        )

    # =========================================================================
    # TIME WINDOW FILTERS
    # =========================================================================

    @staticmethod
    def within_live_window(recorded_at_column) -> BinaryExpression:
        """
        Filter to snapshots within the live time window (last 2 hours).

        NOTE: This uses func.now() which is NOT partition-friendly.
        For partitioned tables (ride_status_snapshots), prefer:
            Filters.within_partition_aware_live_window()

        Replaces: RideFilterSQL.live_time_window_filter()

        Args:
            recorded_at_column: The recorded_at column to filter on
                               (e.g., ride_status_snapshots.c.recorded_at)

        Returns:
            Expression: recorded_at >= NOW() - 2 hours
        """
        from sqlalchemy import func

        # Use timedelta for database-agnostic date math
        return recorded_at_column >= (func.now() - timedelta(hours=LIVE_WINDOW_HOURS))

    @staticmethod
    def within_partition_aware_live_window(
        recorded_at_column,
        reference_time: Optional[datetime] = None
    ) -> ColumnElement:
        """
        Partition-friendly filter for live time window.

        Feature 004: Uses explicit Python-computed bounds for MySQL partition pruning.
        The ride_status_snapshots table is partitioned by month, and partition
        pruning requires explicit datetime bounds (not SQL function calls).

        Args:
            recorded_at_column: The recorded_at column to filter on
            reference_time: Reference time (defaults to UTC now)

        Returns:
            Expression: recorded_at >= (now - 2 hours) AND recorded_at < now
        """
        now = reference_time or datetime.utcnow()
        start = now - timedelta(hours=LIVE_WINDOW_HOURS)

        return and_(
            recorded_at_column >= start,
            recorded_at_column < now
        )

    @staticmethod
    def within_date_range(
        date_column, start_date: datetime, end_date: datetime
    ) -> ColumnElement:
        """
        Filter to records within a date range (inclusive).

        Args:
            date_column: The date/datetime column to filter on
            start_date: Start of range (inclusive)
            end_date: End of range (inclusive)

        Returns:
            Expression: date_column >= start_date AND date_column <= end_date
        """
        return and_(
            date_column >= start_date,
            date_column <= end_date,
        )

    @staticmethod
    def partition_aware_date_range(
        recorded_at_column,
        start: datetime,
        end: datetime
    ) -> ColumnElement:
        """
        Partition-friendly date range filter for ride_status_snapshots.

        Feature 004: Uses explicit datetime bounds with >= and < operators
        to enable MySQL partition pruning on the partitioned table.

        CRITICAL: Use >= start and < end (exclusive end) pattern.
        This ensures partition boundaries are respected.

        Args:
            recorded_at_column: The recorded_at column to filter on
            start: Start of range (inclusive)
            end: End of range (exclusive)

        Returns:
            Expression: recorded_at >= start AND recorded_at < end
        """
        return and_(
            recorded_at_column >= start,
            recorded_at_column < end
        )

    @staticmethod
    def for_today(recorded_at_column, reference_time: Optional[datetime] = None) -> ColumnElement:
        """
        Partition-friendly filter for TODAY period.

        Uses explicit Python-computed bounds from midnight to midnight.

        Args:
            recorded_at_column: The recorded_at column
            reference_time: Reference time (defaults to UTC now)

        Returns:
            Expression for today's date range
        """
        from utils.query_helpers import PartitionAwareDateRange
        bounds = PartitionAwareDateRange.for_today(reference_time)
        return and_(
            recorded_at_column >= bounds.start,
            recorded_at_column < bounds.end
        )

    @staticmethod
    def for_yesterday(recorded_at_column, reference_time: Optional[datetime] = None) -> ColumnElement:
        """
        Partition-friendly filter for YESTERDAY period.

        Uses explicit Python-computed bounds from yesterday midnight to today midnight.

        Args:
            recorded_at_column: The recorded_at column
            reference_time: Reference time (defaults to UTC now)

        Returns:
            Expression for yesterday's date range
        """
        from utils.query_helpers import PartitionAwareDateRange
        bounds = PartitionAwareDateRange.for_yesterday(reference_time)
        return and_(
            recorded_at_column >= bounds.start,
            recorded_at_column < bounds.end
        )

    @staticmethod
    def for_last_week(recorded_at_column, reference_time: Optional[datetime] = None) -> ColumnElement:
        """
        Partition-friendly filter for LAST_WEEK period (7 days).

        Uses explicit Python-computed bounds.

        Args:
            recorded_at_column: The recorded_at column
            reference_time: Reference time (defaults to UTC now)

        Returns:
            Expression for last week's date range
        """
        from utils.query_helpers import PartitionAwareDateRange
        bounds = PartitionAwareDateRange.for_last_week(reference_time)
        return and_(
            recorded_at_column >= bounds.start,
            recorded_at_column < bounds.end
        )

    @staticmethod
    def for_last_month(recorded_at_column, reference_time: Optional[datetime] = None) -> ColumnElement:
        """
        Partition-friendly filter for LAST_MONTH period (30 days).

        Uses explicit Python-computed bounds.

        Args:
            recorded_at_column: The recorded_at column
            reference_time: Reference time (defaults to UTC now)

        Returns:
            Expression for last month's date range
        """
        from utils.query_helpers import PartitionAwareDateRange
        bounds = PartitionAwareDateRange.for_last_month(reference_time)
        return and_(
            recorded_at_column >= bounds.start,
            recorded_at_column < bounds.end
        )

    @staticmethod
    def for_period(
        recorded_at_column,
        period: str,
        reference_time: Optional[datetime] = None
    ) -> ColumnElement:
        """
        Partition-friendly filter for any named period.

        Convenience method that dispatches to the appropriate period filter.

        Args:
            recorded_at_column: The recorded_at column
            period: 'today', 'yesterday', 'last_week', 'last_month', or 'live'
            reference_time: Reference time (defaults to UTC now)

        Returns:
            Expression for the requested period's date range

        Raises:
            ValueError: If period is not recognized
        """
        from utils.query_helpers import PartitionAwareDateRange
        bounds = PartitionAwareDateRange.for_period(period, reference_time)
        return and_(
            recorded_at_column >= bounds.start,
            recorded_at_column < bounds.end
        )

    # =========================================================================
    # CONDITIONAL FILTER APPLICATION
    # =========================================================================

    @staticmethod
    def maybe_disney_universal(
        apply_filter: bool, parks_table=parks
    ) -> Optional[BinaryExpression]:
        """
        Conditionally apply Disney/Universal filter.

        Args:
            apply_filter: If True, returns the filter. If False, returns None.
            parks_table: Parks table reference

        Returns:
            Expression if apply_filter is True, else None

        Usage:
            conditions = [Filters.active_park()]
            if disney_filter := Filters.maybe_disney_universal(user_wants_disney):
                conditions.append(disney_filter)
            stmt = select(...).where(and_(*conditions))
        """
        if apply_filter:
            return Filters.disney_universal(parks_table)
        return None
