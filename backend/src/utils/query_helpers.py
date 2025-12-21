"""
ORM Query Helpers - Type-Safe Abstractions for Common Queries

This module provides ORM equivalents of sql_helpers.py, replacing raw SQL
with type-safe SQLAlchemy queries. All business rules from sql_helpers.py
are preserved.

SINGLE SOURCE OF TRUTH: Like sql_helpers.py, these classes encapsulate
business logic for ride status, downtime, and uptime calculations.

Usage:
    from utils.query_helpers import RideStatusQuery, DowntimeQuery
    from models.base import db_session

    # Get rides that operated today
    rides = RideStatusQuery.rides_that_operated(
        session=db_session,
        park_id=1,
        start_time=today_start,
        end_time=today_end
    )
"""

from sqlalchemy.orm import Session
from sqlalchemy import and_, or_, case, func, exists, select, text
from datetime import datetime, date
from typing import List, Optional
from decimal import Decimal

# Import ORM models
from src.models.orm_ride import Ride
from src.models.orm_park import Park
from src.models.orm_snapshots import RideStatusSnapshot, ParkActivitySnapshot
from src.models.orm_stats import RideDailyStats, ParkDailyStats

# Import metrics constants
from src.utils.metrics import (
    SNAPSHOT_INTERVAL_MINUTES,
    SHAME_SCORE_MULTIPLIER,
    SHAME_SCORE_PRECISION,
    LIVE_WINDOW_HOURS,
)


class RideStatusQuery:
    """
    ORM queries for ride operating/down status.

    Equivalent to RideStatusSQL in sql_helpers.py with type-safe ORM queries.
    Preserves all business rules:
    - Park-type aware downtime logic (Disney/Universal vs other parks)
    - Park status precedence (ride can only be down if park was open)
    - Timestamp drift handling (minute-level precision)
    """

    # Parks that properly report DOWN status (distinct from CLOSED)
    PARKS_WITH_DOWN_STATUS_EXPR = or_(
        Park.is_disney.is_(True),
        Park.is_universal.is_(True),
        Park.name == 'Dollywood'
    )

    @staticmethod
    def rides_that_operated(
        session: Session,
        park_id: int,
        start_time: datetime,
        end_time: datetime
    ) -> List[Ride]:
        """
        Get all rides that operated during a time period.

        CRITICAL RULE: A ride has "operated" if:
        1. The ride had status='OPERATING' or computed_is_open=TRUE
        2. AND the park was open at that time (park_appears_open=TRUE)

        This filters out seasonal closures and ensures accurate downtime calculations.

        Equivalent to sql_helpers.py RideStatusSQL.has_operated_subquery()

        Args:
            session: SQLAlchemy session
            park_id: Park ID to filter by
            start_time: Period start (UTC)
            end_time: Period end (UTC)

        Returns:
            List of Ride objects that operated during the period
        """
        # Subquery: rides with at least one operating snapshot when park was open
        operated_subquery = (
            select(RideStatusSnapshot.ride_id)
            .distinct()
            .join(
                ParkActivitySnapshot,
                and_(
                    ParkActivitySnapshot.park_id == park_id,
                    # Minute-level timestamp matching (handles 1-2 second drift)
                    func.date_format(RideStatusSnapshot.recorded_at, '%Y-%m-%d %H:%i') ==
                    func.date_format(ParkActivitySnapshot.recorded_at, '%Y-%m-%d %H:%i')
                )
            )
            .where(
                and_(
                    RideStatusSnapshot.recorded_at >= start_time,
                    RideStatusSnapshot.recorded_at < end_time,
                    # Ride was operating (using hybrid method)
                    RideStatusSnapshot.is_operating(),
                    # Park was open
                    ParkActivitySnapshot.park_appears_open.is_(True)
                )
            )
        )

        # Main query: get rides that appear in the operated subquery
        query = (
            select(Ride)
            .where(
                and_(
                    Ride.park_id == park_id,
                    Ride.ride_id.in_(operated_subquery)
                )
            )
        )

        return session.execute(query).scalars().all()

    @staticmethod
    def is_down_condition(park_entity=None):
        """
        Get SQLAlchemy condition for checking if a ride is down.

        PARK-TYPE AWARE LOGIC:
        - Disney/Universal/Dollywood: Only count DOWN status (not CLOSED)
        - Other parks: Include both DOWN and CLOSED

        Equivalent to sql_helpers.py RideStatusSQL.is_down()

        Args:
            park_entity: Optional Park model class or aliased entity for park-type aware logic.
                        Must be a SQLAlchemy entity, NOT an ORM instance.

        Returns:
            SQLAlchemy condition expression
        """
        if park_entity is not None:
            # Park-type aware logic using SQL expressions
            is_premium_park = or_(
                park_entity.is_disney.is_(True),
                park_entity.is_universal.is_(True),
                park_entity.name == 'Dollywood'
            )

            return case(
                (
                    is_premium_park,
                    # Disney/Universal/Dollywood: Only DOWN status
                    RideStatusSnapshot.status == 'DOWN'
                ),
                else_=(
                    # Other parks: DOWN, CLOSED, or computed_is_open=FALSE
                    or_(
                        RideStatusSnapshot.status.in_(['DOWN', 'CLOSED']),
                        and_(
                            RideStatusSnapshot.status.is_(None),
                            RideStatusSnapshot.computed_is_open.is_(False)
                        )
                    )
                )
            )
        else:
            # Legacy behavior (backwards compatibility)
            return or_(
                RideStatusSnapshot.status.in_(['DOWN', 'CLOSED']),
                and_(
                    RideStatusSnapshot.status.is_(None),
                    RideStatusSnapshot.computed_is_open.is_(False)
                )
            )


class ParkStatusQuery:
    """
    ORM queries for park operating status.

    Equivalent to ParkStatusSQL in sql_helpers.py.
    """

    @staticmethod
    def is_park_open(
        session: Session,
        park_id: int,
        timestamp: datetime
    ) -> bool:
        """
        Check if park was open at a specific timestamp.

        Args:
            session: SQLAlchemy session
            park_id: Park ID to check
            timestamp: UTC timestamp

        Returns:
            True if park appears open at timestamp
        """
        snapshot = (
            session.query(ParkActivitySnapshot)
            .filter(
                ParkActivitySnapshot.park_id == park_id,
                ParkActivitySnapshot.recorded_at <= timestamp
            )
            .order_by(ParkActivitySnapshot.recorded_at.desc())
            .first()
        )

        return snapshot.park_appears_open if snapshot else False


class DowntimeQuery:
    """
    ORM queries for downtime calculations.

    Equivalent to DowntimeSQL in sql_helpers.py.
    Preserves Rule 1 (park status precedence) and Rule 2 (rides must have operated).
    """

    @staticmethod
    def calculate_downtime_minutes(
        session: Session,
        ride_id: int,
        start_time: datetime,
        end_time: datetime
    ) -> int:
        """
        Calculate total downtime minutes for a ride during a period.

        Only counts downtime when:
        1. Park was open (park_appears_open=TRUE)
        2. Ride was down (using park-type aware logic)
        3. Ride has operated at least once during the period

        Args:
            session: SQLAlchemy session
            ride_id: Ride ID
            start_time: Period start (UTC)
            end_time: Period end (UTC)

        Returns:
            Total downtime minutes
        """
        # Get the ride and park
        ride = session.query(Ride).filter(Ride.ride_id == ride_id).first()
        if not ride:
            return 0

        # Check if ride operated during period using EXISTS query (avoids O(N) memory check)
        operated = session.execute(
            select(exists().where(
                and_(
                    RideStatusSnapshot.ride_id == ride_id,
                    RideStatusSnapshot.recorded_at >= start_time,
                    RideStatusSnapshot.recorded_at < end_time,
                    RideStatusSnapshot.is_operating(),
                    # Join with park activity to verify park was open
                    exists().where(
                        and_(
                            ParkActivitySnapshot.park_id == ride.park_id,
                            func.date_format(RideStatusSnapshot.recorded_at, '%Y-%m-%d %H:%i') ==
                            func.date_format(ParkActivitySnapshot.recorded_at, '%Y-%m-%d %H:%i'),
                            ParkActivitySnapshot.park_appears_open.is_(True)
                        )
                    )
                )
            ))
        ).scalar()

        if not operated:
            return 0  # Ride never operated, don't count downtime

        # Count down snapshots when park was open
        down_count = (
            session.query(func.count(RideStatusSnapshot.snapshot_id))
            .join(
                ParkActivitySnapshot,
                and_(
                    ParkActivitySnapshot.park_id == ride.park_id,
                    func.date_format(RideStatusSnapshot.recorded_at, '%Y-%m-%d %H:%i') ==
                    func.date_format(ParkActivitySnapshot.recorded_at, '%Y-%m-%d %H:%i')
                )
            )
            .join(Park, Park.park_id == ride.park_id)  # Join Park for park-type aware logic
            .filter(
                and_(
                    RideStatusSnapshot.ride_id == ride_id,
                    RideStatusSnapshot.recorded_at >= start_time,
                    RideStatusSnapshot.recorded_at < end_time,
                    # Ride is down (park-type aware using Park model entity)
                    RideStatusQuery.is_down_condition(Park),
                    # Park was open
                    ParkActivitySnapshot.park_appears_open.is_(True)
                )
            )
            .scalar()
        )

        return (down_count or 0) * SNAPSHOT_INTERVAL_MINUTES


class UptimeQuery:
    """
    ORM queries for uptime percentage calculations.

    Equivalent to UptimeSQL in sql_helpers.py.
    """

    @staticmethod
    def calculate_uptime_percentage(
        session: Session,
        ride_id: int,
        start_time: datetime,
        end_time: datetime
    ) -> Decimal:
        """
        Calculate uptime percentage for a ride during a period.

        Uptime = (Operating snapshots / Total snapshots when park open) * 100

        Args:
            session: SQLAlchemy session
            ride_id: Ride ID
            start_time: Period start (UTC)
            end_time: Period end (UTC)

        Returns:
            Uptime percentage (0.0 to 100.0)
        """
        ride = session.query(Ride).filter(Ride.ride_id == ride_id).first()
        if not ride:
            return Decimal('0.0')

        # Count total and operating snapshots when park was open
        result = (
            session.query(
                func.count(RideStatusSnapshot.snapshot_id).label('total'),
                func.sum(
                    case(
                        (RideStatusSnapshot.is_operating(), 1),
                        else_=0
                    )
                ).label('operating')
            )
            .join(
                ParkActivitySnapshot,
                and_(
                    ParkActivitySnapshot.park_id == ride.park_id,
                    func.date_format(RideStatusSnapshot.recorded_at, '%Y-%m-%d %H:%i') ==
                    func.date_format(ParkActivitySnapshot.recorded_at, '%Y-%m-%d %H:%i')
                )
            )
            .filter(
                and_(
                    RideStatusSnapshot.ride_id == ride_id,
                    RideStatusSnapshot.recorded_at >= start_time,
                    RideStatusSnapshot.recorded_at < end_time,
                    ParkActivitySnapshot.park_appears_open.is_(True)
                )
            )
            .first()
        )

        if not result or not result.total or result.total == 0:
            return Decimal('0.0')

        uptime_pct = (result.operating / result.total) * 100.0
        return Decimal(str(round(uptime_pct, 2)))
