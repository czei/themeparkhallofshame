"""
Status Expressions
==================

SQLAlchemy expressions for ride status calculations.

Replaces: utils/sql_helpers.py RideStatusSQL, DowntimeSQL, UptimeSQL classes

Usage:
    from database.queries.builders import StatusExpressions
    from database.schema import ride_status_snapshots as rss

    stmt = select(rss).where(StatusExpressions.is_operating(rss))

Status Logic:
    Status is determined by:
    1. If rss.status is set (ThemeParks.wiki data): Use it directly
    2. If rss.status is NULL (Queue-Times data): Map computed_is_open to OPERATING/DOWN

    COALESCE(status, IF(computed_is_open, 'OPERATING', 'DOWN'))

How to Modify:
1. To add a new status type: Update the status enum in migrations first
2. To change status logic: Update the is_operating/is_down methods
3. After changes: Verify sql_helpers.py has matching logic (for compatibility)
"""

from sqlalchemy import and_, or_, case, func
from sqlalchemy.sql import ColumnElement
from sqlalchemy.sql.expression import BinaryExpression

from database.schema import (
    ride_status_snapshots,
    park_activity_snapshots,
)


class StatusExpressions:
    """
    Expressions for determining ride operational status.

    All methods return SQLAlchemy expression objects.
    """

    # =========================================================================
    # STATUS DETERMINATION
    # =========================================================================

    @staticmethod
    def status_expression(rss=ride_status_snapshots) -> ColumnElement:
        """
        Get the computed status for a ride snapshot.

        Replaces: RideStatusSQL.status_expression()

        Logic:
            - If status is set (ThemeParks.wiki): use it
            - If status is NULL (Queue-Times): map computed_is_open to OPERATING/DOWN

        Returns:
            CASE expression that evaluates to 'OPERATING', 'DOWN', 'CLOSED', or 'REFURBISHMENT'
        """
        return case(
            (rss.c.status.isnot(None), rss.c.status),
            else_=case(
                (rss.c.computed_is_open == True, "OPERATING"),
                else_="DOWN",
            ),
        )

    @staticmethod
    def is_operating(rss=ride_status_snapshots) -> ColumnElement:
        """
        Check if a ride is currently operating.

        Replaces: RideStatusSQL.is_operating()

        Returns:
            Expression: status = 'OPERATING' OR (status IS NULL AND computed_is_open = TRUE)
        """
        return or_(
            rss.c.status == "OPERATING",
            and_(rss.c.status.is_(None), rss.c.computed_is_open == True),
        )

    @staticmethod
    def is_down(rss=ride_status_snapshots) -> ColumnElement:
        """
        Check if a ride is down (unscheduled breakdown).

        Replaces: RideStatusSQL.is_down()

        Note: This is specifically for DOWN status, not CLOSED or REFURBISHMENT.

        Returns:
            Expression: status = 'DOWN' OR (status IS NULL AND computed_is_open = FALSE)
        """
        return or_(
            rss.c.status == "DOWN",
            and_(rss.c.status.is_(None), rss.c.computed_is_open == False),
        )

    @staticmethod
    def is_closed(rss=ride_status_snapshots) -> BinaryExpression:
        """
        Check if a ride has scheduled closure (not breakdown).

        Returns:
            Expression: status = 'CLOSED'
        """
        return rss.c.status == "CLOSED"

    @staticmethod
    def is_refurbishment(rss=ride_status_snapshots) -> BinaryExpression:
        """
        Check if a ride is under refurbishment/extended maintenance.

        Returns:
            Expression: status = 'REFURBISHMENT'
        """
        return rss.c.status == "REFURBISHMENT"

    # =========================================================================
    # PARK STATUS
    # =========================================================================

    @staticmethod
    def park_is_open(pas=park_activity_snapshots) -> BinaryExpression:
        """
        Check if a park appears to be open (has any ride activity).

        Replaces: ParkStatusSQL.park_appears_open_filter()

        Returns:
            Expression: park_appears_open = TRUE
        """
        return pas.c.park_appears_open == True

    # =========================================================================
    # DOWNTIME CALCULATIONS
    # =========================================================================
    # Note: These are used in aggregate queries to sum downtime.
    # Each snapshot represents 5 minutes (SNAPSHOT_INTERVAL_MINUTES).
    # =========================================================================

    SNAPSHOT_INTERVAL_MINUTES = 5  # From metrics.py

    @staticmethod
    def downtime_minutes_case(
        rss=ride_status_snapshots, pas=park_activity_snapshots
    ) -> ColumnElement:
        """
        Case expression for counting downtime minutes.

        Only counts when:
        1. Park is open (park_appears_open = TRUE)
        2. Ride is down (status = DOWN or computed_is_open = FALSE)

        Replaces: DowntimeSQL.downtime_minutes_sum() (partial)

        Returns:
            CASE expression: 5 if down during open hours, else 0
        """
        is_down = StatusExpressions.is_down(rss)
        park_open = StatusExpressions.park_is_open(pas)

        return case(
            (and_(park_open, is_down), StatusExpressions.SNAPSHOT_INTERVAL_MINUTES),
            else_=0,
        )

    @staticmethod
    def weighted_downtime_case(
        rss=ride_status_snapshots,
        pas=park_activity_snapshots,
        tier_weight_expr=None,
    ) -> ColumnElement:
        """
        Case expression for counting weighted downtime minutes.

        Applies tier weight multiplier to downtime.

        Args:
            tier_weight_expr: Expression for tier weight (e.g., rc.c.tier_weight)
                             If None, uses default weight of 2

        Replaces: DowntimeSQL.weighted_downtime_hours() (partial)

        Returns:
            CASE expression: (5 * tier_weight) if down during open hours, else 0
        """
        if tier_weight_expr is None:
            tier_weight_expr = 2  # Default weight

        is_down = StatusExpressions.is_down(rss)
        park_open = StatusExpressions.park_is_open(pas)

        return case(
            (
                and_(park_open, is_down),
                StatusExpressions.SNAPSHOT_INTERVAL_MINUTES * tier_weight_expr,
            ),
            else_=0,
        )

    # =========================================================================
    # UPTIME CALCULATIONS
    # =========================================================================

    @staticmethod
    def uptime_case(
        rss=ride_status_snapshots, pas=park_activity_snapshots
    ) -> ColumnElement:
        """
        Case expression for counting uptime (operating) snapshots.

        Only counts when park is open.

        Returns:
            CASE expression: 1 if operating during open hours, else 0
        """
        is_operating = StatusExpressions.is_operating(rss)
        park_open = StatusExpressions.park_is_open(pas)

        return case(
            (and_(park_open, is_operating), 1),
            else_=0,
        )

    @staticmethod
    def park_open_case(pas=park_activity_snapshots) -> ColumnElement:
        """
        Case expression for counting snapshots when park is open.

        Used as denominator for uptime percentage.

        Returns:
            CASE expression: 1 if park open, else 0
        """
        return case(
            (pas.c.park_appears_open == True, 1),
            else_=0,
        )
