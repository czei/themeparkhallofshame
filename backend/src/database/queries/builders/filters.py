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

from database.schema import parks, rides, ride_classifications


# =============================================================================
# CONSTANTS
# =============================================================================
# Time window for "live" data - only consider snapshots from last 2 hours
# This matches RideStatusSQL.LIVE_WINDOW_HOURS in sql_helpers.py
LIVE_WINDOW_HOURS = 2


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

        Replaces: RideFilterSQL.live_time_window_filter()

        Args:
            recorded_at_column: The recorded_at column to filter on
                               (e.g., ride_status_snapshots.c.recorded_at)

        Returns:
            Expression: recorded_at >= NOW() - 2 hours
        """
        from sqlalchemy import func, text

        # Use text() for MySQL INTERVAL syntax - timedelta doesn't work with date_sub
        return recorded_at_column >= func.now() - text(f"INTERVAL {LIVE_WINDOW_HOURS} HOUR")

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
