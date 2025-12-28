"""
ShameScoreCalculator - Single Source of Truth for shame score calculations.

This calculator ensures consistency across all UI components:
- Rankings table (TODAY period)
- Breakdown panel (park details modal)
- Chart average display

Key Formula:
    shame_score = AVG(per-snapshot instantaneous shame scores)

    Where instantaneous shame at timestamp T =
        (sum of tier_weights for down rides at T) / total_park_weight * 10

Architecture:
    This calculator generates ORM queries for all shame score calculations,
    ensuring consistent filtering and formulas across all queries.

    The calculator accepts a db_session via dependency injection,
    enabling unit testing with mock sessions.
"""
from datetime import datetime, date, timedelta, timezone
from typing import Optional, List, Dict, Any

from sqlalchemy import select, func, case, and_, or_, literal
from sqlalchemy.orm import Session

from models import (
    Park, Ride, RideClassification, RideStatusSnapshot,
    ParkActivitySnapshot
)
from utils.query_helpers import QueryClassBase
from utils.metrics import SHAME_SCORE_PRECISION, SHAME_SCORE_MULTIPLIER

# Feature flag for 7-day hybrid denominator (allows instant rollback)
import os
USE_7_DAY_HYBRID_DENOMINATOR = os.getenv('USE_7_DAY_HYBRID_DENOMINATOR', 'true').lower() == 'true'


class ShameScoreCalculator(QueryClassBase):
    """
    Single source of truth for shame score ORM query generation.

    Usage:
        calc = ShameScoreCalculator(db_session)
        score = calc.get_average(park_id=1, start=start_dt, end=end_dt)
    """

    def __init__(self, session: Session):
        """
        Initialize the calculator with a database session.

        Args:
            session: SQLAlchemy Session for executing queries
        """
        super().__init__(session)

    @property
    def db(self) -> Session:
        """
        Backward compatibility property for tests.
        Returns the session stored by QueryClassBase.
        """
        return self.session

    def get_effective_park_weight(self, park_id: int, as_of: datetime = None) -> float:
        """
        Get total weight of rides that operated in the last 7 days.
        This is the denominator for shame score calculations.

        Uses the 7-day hybrid denominator approach:
        - Full roster MINUS rides that haven't operated in 7 days
        - Provides stability (no morning volatility)
        - Provides accountability (closed rides don't pad denominator)

        Args:
            park_id: The park to calculate for
            as_of: Reference time (default: now UTC)

        Returns:
            Total tier weight of eligible rides, or 0.0 if none
        """
        if not USE_7_DAY_HYBRID_DENOMINATOR:
            return self.get_park_weight(park_id)  # Rollback path

        # Calculate cutoff time for 7-day window
        # Note: Uses current UTC time for production; as_of parameter is for testing
        cutoff_time = (as_of or datetime.now(timezone.utc)) - timedelta(days=7)

        stmt = (
            select(func.sum(func.coalesce(RideClassification.tier_weight, 2)))
            .select_from(Ride)
            .outerjoin(RideClassification, Ride.ride_id == RideClassification.ride_id)
            .where(Ride.park_id == park_id)
            .where(Ride.is_active == True)
            .where(Ride.category == 'ATTRACTION')
            .where(Ride.last_operated_at >= cutoff_time)
        )

        weight = self.execute_scalar(stmt)

        # Return 0.0 for NULL (no eligible rides) - CRITICAL for division by zero protection
        return float(weight) if weight is not None else 0.0

    def get_park_weight(self, park_id: int) -> float:
        """
        Get full roster weight for a park (all active attractions).
        This is the original denominator before 7-day filtering.

        Used for:
        - Rollback path when USE_7_DAY_HYBRID_DENOMINATOR is False
        - Comparison/validation (effective weight <= full roster weight)

        Args:
            park_id: The park to calculate for

        Returns:
            Total tier weight of all active attractions
        """
        stmt = (
            select(func.sum(func.coalesce(RideClassification.tier_weight, 2)))
            .select_from(Ride)
            .outerjoin(RideClassification, Ride.ride_id == RideClassification.ride_id)
            .where(Ride.park_id == park_id)
            .where(Ride.is_active == True)
            .where(Ride.category == 'ATTRACTION')
        )

        weight = self.execute_scalar(stmt)

        return float(weight) if weight is not None else 0.0

    def calculate_shame_score(self, down_weight: float, effective_park_weight: float) -> float:
        """
        Calculate shame score with zero-denominator protection.

        Formula: (down_weight / effective_park_weight) * 10

        Returns 0.0 if effective_park_weight is 0 (e.g., seasonal closure,
        no rides operated in 7 days). This is CRITICAL for preventing
        ZeroDivisionError.

        Args:
            down_weight: Sum of tier weights for down rides
            effective_park_weight: Sum of tier weights for eligible rides

        Returns:
            Shame score on 0-10 scale, or 0.0 if no eligible rides
        """
        if not effective_park_weight:
            return 0.0  # No eligible rides = no shame (CRITICAL: division by zero protection)

        return round((down_weight / effective_park_weight) * SHAME_SCORE_MULTIPLIER, SHAME_SCORE_PRECISION)

    def get_instantaneous(
        self,
        park_id: int,
        timestamp: datetime
    ) -> Optional[float]:
        """
        DEPRECATED: Use stored shame_score from park_activity_snapshots instead.

        Shame score is now calculated ONCE during data collection and stored in
        park_activity_snapshots.shame_score. All queries should READ that value.

        This method is kept for reference/testing only.

        Args:
            park_id: The park to calculate for
            timestamp: The exact moment to check

        Returns:
            Shame score (0-10 scale) or None if no data
        """
        # Park-type aware downtime logic
        parks_with_down_status = or_(Park.is_disney == True, Park.is_universal == True)
        is_down_expr = case(
            (parks_with_down_status, RideStatusSnapshot.status == 'DOWN'),
            else_=or_(
                RideStatusSnapshot.status.in_(['DOWN', 'CLOSED']),
                and_(RideStatusSnapshot.status.is_(None), RideStatusSnapshot.computed_is_open == False)
            )
        )

        # Subquery: Get total park weight
        park_weight_subq = (
            select(func.sum(func.coalesce(RideClassification.tier_weight, 2)).label('total_weight'))
            .select_from(Ride)
            .outerjoin(RideClassification, Ride.ride_id == RideClassification.ride_id)
            .where(Ride.park_id == park_id)
            .where(Ride.is_active == True)
            .where(Ride.category == 'ATTRACTION')
            .subquery()
        )

        # Subquery: Get total down weight at this timestamp
        down_weight_subq = (
            select(func.sum(func.coalesce(RideClassification.tier_weight, 2)).label('total_down_weight'))
            .select_from(RideStatusSnapshot)
            .join(Ride, RideStatusSnapshot.ride_id == Ride.ride_id)
            .join(Park, Ride.park_id == Park.park_id)
            .outerjoin(RideClassification, Ride.ride_id == RideClassification.ride_id)
            .where(Ride.park_id == park_id)
            .where(Ride.is_active == True)
            .where(Ride.category == 'ATTRACTION')
            .where(RideStatusSnapshot.recorded_at == timestamp)
            .where(is_down_expr)
            .subquery()
        )

        # Main query
        stmt = (
            select(
                park_weight_subq.c.total_weight,
                func.coalesce(down_weight_subq.c.total_down_weight, 0).label('total_down_weight'),
                case(
                    (or_(park_weight_subq.c.total_weight.is_(None), park_weight_subq.c.total_weight == 0), None),
                    else_=func.round(
                        (func.coalesce(down_weight_subq.c.total_down_weight, 0) / park_weight_subq.c.total_weight) * SHAME_SCORE_MULTIPLIER,
                        SHAME_SCORE_PRECISION
                    )
                ).label('shame_score')
            )
            .select_from(park_weight_subq)
            .outerjoin(down_weight_subq, literal(True))
        )

        result = self.execute_and_fetchone(stmt)

        if result is None:
            return None

        return result.get('shame_score')

    def get_average(
        self,
        park_id: int,
        start: datetime,
        end: datetime
    ) -> Optional[float]:
        """
        DEPRECATED: Use AVG(pas.shame_score) from park_activity_snapshots instead.

        Shame score is now calculated ONCE during data collection and stored in
        park_activity_snapshots.shame_score. For averages, use:
            SELECT AVG(shame_score) FROM park_activity_snapshots
            WHERE park_id = :park_id AND recorded_at >= :start AND recorded_at < :end

        This method is kept for reference/testing only.

        Args:
            park_id: The park to calculate for
            start: Start of the time range (inclusive, UTC)
            end: End of the time range (exclusive, UTC)

        Returns:
            Average shame score (0-10 scale) or None if no data
        """
        # Subquery: Rides that operated during the period
        rides_operated_subq = (
            select(Ride.ride_id)
            .join(RideStatusSnapshot, Ride.ride_id == RideStatusSnapshot.ride_id)
            .join(ParkActivitySnapshot,
                  and_(
                      Ride.park_id == ParkActivitySnapshot.park_id,
                      RideStatusSnapshot.recorded_at == ParkActivitySnapshot.recorded_at
                  ))
            .where(Ride.park_id == park_id)
            .where(Ride.is_active == True)
            .where(Ride.category == 'ATTRACTION')
            .where(RideStatusSnapshot.recorded_at >= start)
            .where(RideStatusSnapshot.recorded_at < end)
            .where(ParkActivitySnapshot.park_appears_open == True)
            .where(or_(
                RideStatusSnapshot.status == 'OPERATING',
                and_(RideStatusSnapshot.status.is_(None), RideStatusSnapshot.computed_is_open == True)
            ))
            .distinct()
            .subquery()
        )

        # Subquery: Calculate total park weight (only rides that operated)
        park_weights_subq = (
            select(func.sum(func.coalesce(RideClassification.tier_weight, 2)).label('total_park_weight'))
            .select_from(Ride)
            .outerjoin(RideClassification, Ride.ride_id == RideClassification.ride_id)
            .where(Ride.park_id == park_id)
            .where(Ride.is_active == True)
            .where(Ride.category == 'ATTRACTION')
            .where(Ride.ride_id.in_(select(rides_operated_subq.c.ride_id)))
            .subquery()
        )

        # Park-type aware downtime logic
        parks_with_down_status = or_(Park.is_disney == True, Park.is_universal == True)
        is_down_expr = case(
            (parks_with_down_status, RideStatusSnapshot.status == 'DOWN'),
            else_=or_(
                RideStatusSnapshot.status.in_(['DOWN', 'CLOSED']),
                and_(RideStatusSnapshot.status.is_(None), RideStatusSnapshot.computed_is_open == False)
            )
        )

        # Subquery: Per-snapshot shame scores
        per_snapshot_shame_subq = (
            select(
                RideStatusSnapshot.recorded_at,
                func.sum(
                    case(
                        (is_down_expr, func.coalesce(RideClassification.tier_weight, 2)),
                        else_=0
                    )
                ).label('down_weight')
            )
            .select_from(RideStatusSnapshot)
            .join(Ride, RideStatusSnapshot.ride_id == Ride.ride_id)
            .join(Park, Ride.park_id == Park.park_id)
            .join(ParkActivitySnapshot,
                  and_(
                      Ride.park_id == ParkActivitySnapshot.park_id,
                      RideStatusSnapshot.recorded_at == ParkActivitySnapshot.recorded_at
                  ))
            .outerjoin(RideClassification, Ride.ride_id == RideClassification.ride_id)
            .where(Ride.park_id == park_id)
            .where(Ride.is_active == True)
            .where(Ride.category == 'ATTRACTION')
            .where(RideStatusSnapshot.recorded_at >= start)
            .where(RideStatusSnapshot.recorded_at < end)
            .where(ParkActivitySnapshot.park_appears_open == True)
            .where(Ride.ride_id.in_(select(rides_operated_subq.c.ride_id)))
            .group_by(RideStatusSnapshot.recorded_at)
            .subquery()
        )

        # Main query: Average the per-snapshot shame scores
        stmt = (
            select(
                park_weights_subq.c.total_park_weight,
                func.count(per_snapshot_shame_subq.c.recorded_at).label('total_snapshots'),
                case(
                    (or_(
                        park_weights_subq.c.total_park_weight.is_(None),
                        park_weights_subq.c.total_park_weight == 0
                    ), None),
                    (func.count(per_snapshot_shame_subq.c.recorded_at) == 0, None),
                    else_=func.round(
                        func.avg(per_snapshot_shame_subq.c.down_weight / park_weights_subq.c.total_park_weight) * SHAME_SCORE_MULTIPLIER,
                        SHAME_SCORE_PRECISION
                    )
                ).label('avg_shame_score')
            )
            .select_from(park_weights_subq)
            .outerjoin(per_snapshot_shame_subq, literal(True))
        )

        result = self.execute_and_fetchone(stmt)

        if result is None:
            return None

        return result.get('avg_shame_score')

    def get_hourly_breakdown(
        self,
        park_id: int,
        target_date: date
    ) -> List[Dict[str, Any]]:
        """
        DEPRECATED: Use grouped AVG(pas.shame_score) from park_activity_snapshots instead.

        Shame score is now calculated ONCE during data collection and stored in
        park_activity_snapshots.shame_score. For hourly breakdown, use:
            SELECT HOUR(recorded_at) AS hour, AVG(shame_score) AS shame_score
            FROM park_activity_snapshots
            WHERE park_id = :park_id AND recorded_at >= :start AND recorded_at < :end
            GROUP BY HOUR(recorded_at)

        This method is kept for reference/testing only.

        Args:
            park_id: The park to get data for
            target_date: The date to get hourly data for

        Returns:
            List of dicts with hourly breakdown data
        """
        # Import here to avoid circular dependency
        from utils.timezone import get_pacific_day_range_utc

        start_utc, end_utc = get_pacific_day_range_utc(target_date)

        # Subquery: Rides that operated at any point today
        rides_operated_subq = (
            select(Ride.ride_id)
            .join(RideStatusSnapshot, Ride.ride_id == RideStatusSnapshot.ride_id)
            .join(ParkActivitySnapshot,
                  and_(
                      Ride.park_id == ParkActivitySnapshot.park_id,
                      RideStatusSnapshot.recorded_at == ParkActivitySnapshot.recorded_at
                  ))
            .where(Ride.park_id == park_id)
            .where(Ride.is_active == True)
            .where(Ride.category == 'ATTRACTION')
            .where(RideStatusSnapshot.recorded_at >= start_utc)
            .where(RideStatusSnapshot.recorded_at < end_utc)
            .where(ParkActivitySnapshot.park_appears_open == True)
            .where(or_(
                RideStatusSnapshot.status == 'OPERATING',
                and_(RideStatusSnapshot.status.is_(None), RideStatusSnapshot.computed_is_open == True)
            ))
            .distinct()
            .subquery()
        )

        # Subquery: Total park weight (using only rides that operated)
        park_weight_subq = (
            select(func.sum(func.coalesce(RideClassification.tier_weight, 2)).label('total_weight'))
            .select_from(Ride)
            .outerjoin(RideClassification, Ride.ride_id == RideClassification.ride_id)
            .where(Ride.park_id == park_id)
            .where(Ride.is_active == True)
            .where(Ride.category == 'ATTRACTION')
            .where(Ride.ride_id.in_(select(rides_operated_subq.c.ride_id)))
            .subquery()
        )

        # Park-type aware downtime logic
        parks_with_down_status = or_(Park.is_disney == True, Park.is_universal == True)
        is_down_expr = case(
            (parks_with_down_status, RideStatusSnapshot.status == 'DOWN'),
            else_=or_(
                RideStatusSnapshot.status.in_(['DOWN', 'CLOSED']),
                and_(RideStatusSnapshot.status.is_(None), RideStatusSnapshot.computed_is_open == False)
            )
        )

        # Pacific hour calculation: subtract 8 hours from UTC
        pacific_hour_expr = func.hour(
            func.date_sub(RideStatusSnapshot.recorded_at, literal(8).op('HOUR'))
        )

        # Check if ride operated
        ride_operated_case = case(
            (Ride.ride_id.in_(select(rides_operated_subq.c.ride_id)), True),
            else_=False
        )

        # Downtime and weight calculations
        downtime_minutes_case = case(
            (and_(ride_operated_case, is_down_expr), 5),  # 5-minute intervals
            else_=0
        )

        down_weight_case = case(
            (and_(ride_operated_case, is_down_expr), func.coalesce(RideClassification.tier_weight, 2)),
            else_=0
        )

        # Subquery: Hourly aggregated data
        hourly_data_subq = (
            select(
                pacific_hour_expr.label('hour'),
                func.count(func.distinct(case((ride_operated_case, Ride.ride_id)))).label('total_rides'),
                func.sum(downtime_minutes_case).label('down_minutes'),
                func.sum(down_weight_case).label('down_weight_sum')
            )
            .select_from(RideStatusSnapshot)
            .join(Ride, RideStatusSnapshot.ride_id == Ride.ride_id)
            .join(Park, Ride.park_id == Park.park_id)
            .join(ParkActivitySnapshot,
                  and_(
                      Ride.park_id == ParkActivitySnapshot.park_id,
                      RideStatusSnapshot.recorded_at == ParkActivitySnapshot.recorded_at
                  ))
            .outerjoin(RideClassification, Ride.ride_id == RideClassification.ride_id)
            .where(Ride.park_id == park_id)
            .where(Ride.is_active == True)
            .where(Ride.category == 'ATTRACTION')
            .where(RideStatusSnapshot.recorded_at >= start_utc)
            .where(RideStatusSnapshot.recorded_at < end_utc)
            .where(ParkActivitySnapshot.park_appears_open == True)
            .group_by(pacific_hour_expr)
            .having(func.count(func.distinct(case((ride_operated_case, Ride.ride_id)))) > 0)
            .subquery()
        )

        # Main query
        stmt = (
            select(
                hourly_data_subq.c.hour,
                hourly_data_subq.c.total_rides,
                hourly_data_subq.c.down_minutes,
                case(
                    (or_(park_weight_subq.c.total_weight.is_(None), park_weight_subq.c.total_weight == 0), None),
                    else_=func.round(
                        (hourly_data_subq.c.down_weight_sum / (hourly_data_subq.c.total_rides * 12)) / park_weight_subq.c.total_weight * SHAME_SCORE_MULTIPLIER,
                        SHAME_SCORE_PRECISION
                    )
                ).label('shame_score')
            )
            .select_from(hourly_data_subq)
            .outerjoin(park_weight_subq, literal(True))
            .order_by(hourly_data_subq.c.hour)
        )

        return self.execute_and_fetchall(stmt)

    def get_recent_snapshots(
        self,
        park_id: int,
        minutes: int = 60
    ) -> Dict[str, Any]:
        """
        DEPRECATED: Use pas.shame_score from park_activity_snapshots instead.

        Shame score is now calculated ONCE during data collection and stored in
        park_activity_snapshots.shame_score. For recent snapshots, use:
            SELECT DATE_FORMAT(recorded_at, '%H:%i') AS label, shame_score
            FROM park_activity_snapshots
            WHERE park_id = :park_id AND recorded_at >= :start
            ORDER BY recorded_at

        This method is kept for reference/testing only.

        Args:
            park_id: The park to get data for
            minutes: How many minutes of recent data (default 60)

        Returns:
            Dict with:
                - labels: List of time strings in "HH:MM" format
                - data: List of instantaneous shame scores at each interval
                - granularity: "minutes" to distinguish from hourly charts
        """
        # Calculate time range: now back to (now - minutes)
        now_utc = datetime.now(timezone.utc)
        start_utc = now_utc - timedelta(minutes=minutes)

        # Subquery: Rides that operated in this window
        rides_operated_subq = (
            select(Ride.ride_id)
            .join(RideStatusSnapshot, Ride.ride_id == RideStatusSnapshot.ride_id)
            .join(ParkActivitySnapshot,
                  and_(
                      Ride.park_id == ParkActivitySnapshot.park_id,
                      RideStatusSnapshot.recorded_at == ParkActivitySnapshot.recorded_at
                  ))
            .where(Ride.park_id == park_id)
            .where(Ride.is_active == True)
            .where(Ride.category == 'ATTRACTION')
            .where(RideStatusSnapshot.recorded_at >= start_utc)
            .where(RideStatusSnapshot.recorded_at < now_utc)
            .where(ParkActivitySnapshot.park_appears_open == True)
            .where(or_(
                RideStatusSnapshot.status == 'OPERATING',
                and_(RideStatusSnapshot.status.is_(None), RideStatusSnapshot.computed_is_open == True)
            ))
            .distinct()
            .subquery()
        )

        # Subquery: Total park weight (using only rides that operated)
        park_weight_subq = (
            select(func.sum(func.coalesce(RideClassification.tier_weight, 2)).label('total_weight'))
            .select_from(Ride)
            .outerjoin(RideClassification, Ride.ride_id == RideClassification.ride_id)
            .where(Ride.park_id == park_id)
            .where(Ride.is_active == True)
            .where(Ride.category == 'ATTRACTION')
            .where(Ride.ride_id.in_(select(rides_operated_subq.c.ride_id)))
            .subquery()
        )

        # Park-type aware downtime logic
        parks_with_down_status = or_(Park.is_disney == True, Park.is_universal == True)
        is_down_expr = case(
            (parks_with_down_status, RideStatusSnapshot.status == 'DOWN'),
            else_=or_(
                RideStatusSnapshot.status.in_(['DOWN', 'CLOSED']),
                and_(RideStatusSnapshot.status.is_(None), RideStatusSnapshot.computed_is_open == False)
            )
        )

        # Check if ride operated
        ride_operated_case = case(
            (Ride.ride_id.in_(select(rides_operated_subq.c.ride_id)), True),
            else_=False
        )

        # Time label formatting (Pacific time)
        time_label_expr = func.date_format(
            func.date_sub(RideStatusSnapshot.recorded_at, literal(8).op('HOUR')),
            '%H:%i'
        )

        # Subquery: Get instantaneous shame for each 5-minute snapshot
        snapshot_data_subq = (
            select(
                RideStatusSnapshot.recorded_at,
                time_label_expr.label('time_label'),
                func.sum(
                    case(
                        (and_(ride_operated_case, is_down_expr), func.coalesce(RideClassification.tier_weight, 2)),
                        else_=0
                    )
                ).label('down_weight')
            )
            .select_from(RideStatusSnapshot)
            .join(Ride, RideStatusSnapshot.ride_id == Ride.ride_id)
            .join(Park, Ride.park_id == Park.park_id)
            .join(ParkActivitySnapshot,
                  and_(
                      Ride.park_id == ParkActivitySnapshot.park_id,
                      RideStatusSnapshot.recorded_at == ParkActivitySnapshot.recorded_at
                  ))
            .outerjoin(RideClassification, Ride.ride_id == RideClassification.ride_id)
            .where(Ride.park_id == park_id)
            .where(Ride.is_active == True)
            .where(Ride.category == 'ATTRACTION')
            .where(RideStatusSnapshot.recorded_at >= start_utc)
            .where(RideStatusSnapshot.recorded_at < now_utc)
            .where(ParkActivitySnapshot.park_appears_open == True)
            .group_by(RideStatusSnapshot.recorded_at, time_label_expr)
            .order_by(RideStatusSnapshot.recorded_at)
            .subquery()
        )

        # Main query
        stmt = (
            select(
                snapshot_data_subq.c.recorded_at,
                snapshot_data_subq.c.time_label,
                case(
                    (or_(park_weight_subq.c.total_weight.is_(None), park_weight_subq.c.total_weight == 0), None),
                    else_=func.round(
                        (snapshot_data_subq.c.down_weight / park_weight_subq.c.total_weight) * SHAME_SCORE_MULTIPLIER,
                        SHAME_SCORE_PRECISION
                    )
                ).label('shame_score')
            )
            .select_from(snapshot_data_subq)
            .outerjoin(park_weight_subq, literal(True))
            .order_by(snapshot_data_subq.c.recorded_at)
        )

        rows = self.execute_and_fetchall(stmt)

        # Build labels and data arrays
        labels = []
        data = []
        for row in rows:
            labels.append(row['time_label'])
            # Convert Decimal to float for JSON serialization
            score = row['shame_score']
            data.append(float(score) if score is not None else None)

        # If we have fewer points than expected, that's OK - it means
        # the park wasn't open for the full duration or data is sparse
        return {
            "labels": labels,
            "data": data,
            "granularity": "minutes"
        }
