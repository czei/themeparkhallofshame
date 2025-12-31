"""
Computation Trace Generator (Lightweight Version)
===================================================

Generates step-by-step calculation traces for any displayed number.
This is the core of the user-triggered audit feature.

Uses pre-aggregated daily stats tables for fast queries:
- park_daily_stats: Park-level metrics
- ride_daily_stats: Ride-level metrics
- ride_classifications: Tier information

When a user clicks "Verify this number", they see:
1. Entity identification
2. Pre-computed daily stats
3. Aggregation formula
4. Final result with verification status

Usage:
    from database.audit import ComputationTracer

    tracer = ComputationTracer(session)

    # User clicked on a park shame score
    trace = tracer.trace_park_shame_score(
        park_id=1,
        period="today",
        displayed_value=2.45
    )

    # Returns full computation trace showing how 2.45 was calculated

Created: 2024-11 (Data Accuracy Audit Framework)
Updated: 2024-11 (Rewritten to use pre-aggregated tables)
Updated: 2024-12 (Converted to SQLAlchemy ORM)
"""

from datetime import date, timedelta
from typing import Dict, Any, Optional, List
from dataclasses import dataclass
from sqlalchemy.orm import Session
from sqlalchemy import func

from utils.timezone import get_today_pacific
from models.orm_park import Park
from models.orm_ride import Ride
from models.orm_classification import RideClassification
from models.orm_stats import RideDailyStats, ParkDailyStats
from models.orm_snapshots import RideStatusSnapshot


@dataclass
class ComputationStep:
    """A single step in the computation trace."""

    step_number: int
    name: str
    description: str
    formula: Optional[str]
    inputs: Dict[str, Any]
    output: Any
    output_label: str


@dataclass
class ComputationTrace:
    """Full computation trace for a displayed value."""

    verified: bool
    displayed_value: float
    computed_value: float
    tolerance: float
    entity_type: str  # park, ride
    entity_id: int
    entity_name: str
    period: str
    metric: str
    steps: List[ComputationStep]
    data_quality: Dict[str, Any]
    methodology_url: str


class ComputationTracer:
    """
    Generates computation traces for any displayed statistic.

    Each trace shows the complete calculation path from raw data
    to the final number, allowing users to verify accuracy.
    """

    # Tolerance for matching displayed vs computed (handles rounding)
    DEFAULT_TOLERANCE = 0.05

    def __init__(self, session: Session, tolerance: float = DEFAULT_TOLERANCE):
        """
        Initialize with database session.

        Args:
            session: SQLAlchemy session
            tolerance: Max difference between displayed and computed for "verified"
        """
        self.session = session
        self.tolerance = tolerance

    def trace_park_shame_score(
        self,
        park_id: int,
        period: str,
        displayed_value: float,
        target_date: Optional[date] = None,
    ) -> ComputationTrace:
        """
        Generate computation trace for a park shame score.

        Args:
            park_id: Park to trace
            period: 'today', '7days', or '30days'
            displayed_value: Value shown to user
            target_date: Date to trace (default: today for 'today', yesterday for others)

        Returns:
            ComputationTrace with all calculation steps
        """
        # Determine date range based on period
        if period == "today":
            target_date = target_date or get_today_pacific()
            start_date = target_date
            end_date = target_date
        elif period == "7days":
            target_date = target_date or (get_today_pacific() - timedelta(days=1))
            start_date = target_date - timedelta(days=6)
            end_date = target_date
        else:  # 30days
            target_date = target_date or (get_today_pacific() - timedelta(days=1))
            start_date = target_date - timedelta(days=29)
            end_date = target_date

        steps = []
        step_num = 1

        # Step 1: Get park info
        park_info = self._get_park_info(park_id)
        steps.append(
            ComputationStep(
                step_number=step_num,
                name="Park Identification",
                description="Identify the park being analyzed",
                formula=None,
                inputs={"park_id": park_id},
                output=park_info,
                output_label="park_name",
            )
        )
        step_num += 1

        # Step 2: Count raw snapshots
        snapshot_counts = self._get_snapshot_counts(park_id, start_date, end_date)
        steps.append(
            ComputationStep(
                step_number=step_num,
                name="Raw Snapshot Collection",
                description=f"Count all ride status snapshots from {start_date} to {end_date}",
                formula="SELECT COUNT(*) FROM ride_status_snapshots WHERE park_id = {park_id}",
                inputs={"park_id": park_id, "start_date": str(start_date), "end_date": str(end_date)},
                output=snapshot_counts,
                output_label="total_snapshots",
            )
        )
        step_num += 1

        # Step 3: Get ride-level downtime
        ride_stats = self._get_ride_level_stats(park_id, start_date, end_date)
        steps.append(
            ComputationStep(
                step_number=step_num,
                name="Ride-Level Downtime Calculation",
                description="Calculate downtime for each active ride (ATTRACTION category only)",
                formula="downtime_hours = (down_snapshots * 5 minutes) / 60",
                inputs={
                    "rides_counted": len(ride_stats),
                    "snapshot_interval_minutes": 5,
                },
                output=ride_stats[:5] if len(ride_stats) > 5 else ride_stats,  # Show top 5
                output_label="per_ride_downtime",
            )
        )
        step_num += 1

        # Step 4: Apply tier weights
        weighted_stats = self._get_weighted_stats(park_id, start_date, end_date)
        steps.append(
            ComputationStep(
                step_number=step_num,
                name="Apply Tier Weights",
                description="Multiply each ride's downtime by its tier weight",
                formula="weighted_downtime = downtime_hours * tier_weight (Tier 1=3, Tier 2=2, Tier 3=1)",
                inputs={
                    "tier_weights": {"tier_1": 3, "tier_2": 2, "tier_3": 1, "default": 2}
                },
                output=weighted_stats,
                output_label="weighted_downtime_hours",
            )
        )
        step_num += 1

        # Step 5: Calculate shame score
        shame_calc = self._calculate_shame_score(park_id, start_date, end_date)
        steps.append(
            ComputationStep(
                step_number=step_num,
                name="Calculate Shame Score",
                description="Divide total weighted downtime by total park weight",
                formula="shame_score = weighted_downtime_hours / total_park_weight",
                inputs={
                    "weighted_downtime_hours": shame_calc["weighted_downtime_hours"],
                    "total_park_weight": shame_calc["total_park_weight"],
                },
                output=shame_calc["shame_score"],
                output_label="shame_score",
            )
        )

        # Get data quality metrics
        data_quality = self._get_data_quality(park_id, start_date, end_date)

        # Verify against displayed value
        computed_value = shame_calc["shame_score"] or 0
        verified = abs(displayed_value - computed_value) <= self.tolerance

        return ComputationTrace(
            verified=verified,
            displayed_value=displayed_value,
            computed_value=computed_value,
            tolerance=self.tolerance,
            entity_type="park",
            entity_id=park_id,
            entity_name=park_info.get("park_name", "Unknown"),
            period=period,
            metric="shame_score",
            steps=steps,
            data_quality=data_quality,
            methodology_url="/about#methodology",
        )

    def trace_ride_downtime(
        self,
        ride_id: int,
        period: str,
        displayed_value: float,
        target_date: Optional[date] = None,
    ) -> ComputationTrace:
        """
        Generate computation trace for a ride's downtime hours.

        Args:
            ride_id: Ride to trace
            period: 'today', '7days', or '30days'
            displayed_value: Value shown to user
            target_date: Date to trace

        Returns:
            ComputationTrace with all calculation steps
        """
        # Determine date range based on period
        if period == "today":
            target_date = target_date or get_today_pacific()
            start_date = target_date
            end_date = target_date
        elif period == "7days":
            target_date = target_date or (get_today_pacific() - timedelta(days=1))
            start_date = target_date - timedelta(days=6)
            end_date = target_date
        else:  # 30days
            target_date = target_date or (get_today_pacific() - timedelta(days=1))
            start_date = target_date - timedelta(days=29)
            end_date = target_date

        steps = []
        step_num = 1

        # Step 1: Get ride info
        ride_info = self._get_ride_info(ride_id)
        steps.append(
            ComputationStep(
                step_number=step_num,
                name="Ride Identification",
                description="Identify the ride being analyzed",
                formula=None,
                inputs={"ride_id": ride_id},
                output=ride_info,
                output_label="ride_name",
            )
        )
        step_num += 1

        # Step 2: Count snapshots
        ride_snapshots = self._get_ride_snapshot_breakdown(ride_id, start_date, end_date)
        steps.append(
            ComputationStep(
                step_number=step_num,
                name="Snapshot Breakdown",
                description="Count snapshots by status (only during park open hours)",
                formula="""
                    is_down = status='DOWN' OR (status IS NULL AND computed_is_open=FALSE)
                    Only count when park_appears_open = TRUE
                """,
                inputs={"ride_id": ride_id, "start_date": str(start_date), "end_date": str(end_date)},
                output=ride_snapshots,
                output_label="snapshot_breakdown",
            )
        )
        step_num += 1

        # Step 3: Calculate downtime
        downtime_hours = (ride_snapshots.get("down_snapshots", 0) * 5) / 60.0
        steps.append(
            ComputationStep(
                step_number=step_num,
                name="Convert to Hours",
                description="Convert down snapshots to hours",
                formula="downtime_hours = (down_snapshots × 5 minutes) ÷ 60",
                inputs={
                    "down_snapshots": ride_snapshots.get("down_snapshots", 0),
                    "minutes_per_snapshot": 5,
                },
                output=round(downtime_hours, 2),
                output_label="downtime_hours",
            )
        )

        # Get data quality metrics
        data_quality = {
            "total_snapshots": ride_snapshots.get("total_snapshots", 0),
            "park_open_snapshots": ride_snapshots.get("park_open_snapshots", 0),
            "coverage_percentage": round(
                100.0
                * ride_snapshots.get("park_open_snapshots", 0)
                / max(288 * ((end_date - start_date).days + 1), 1),
                1,
            ),
        }

        # Verify against displayed value
        computed_value = round(downtime_hours, 2)
        verified = abs(displayed_value - computed_value) <= self.tolerance

        return ComputationTrace(
            verified=verified,
            displayed_value=displayed_value,
            computed_value=computed_value,
            tolerance=self.tolerance,
            entity_type="ride",
            entity_id=ride_id,
            entity_name=ride_info.get("ride_name", "Unknown"),
            period=period,
            metric="downtime_hours",
            steps=steps,
            data_quality=data_quality,
            methodology_url="/about#methodology",
        )

    def _get_park_info(self, park_id: int) -> Dict[str, Any]:
        """Get basic park information."""
        park = self.session.query(Park).filter(Park.park_id == park_id).first()
        if not park:
            return {}
        return {
            "park_id": park.park_id,
            "park_name": park.name
        }

    def _get_ride_info(self, ride_id: int) -> Dict[str, Any]:
        """Get basic ride information."""
        result = (
            self.session.query(Ride, Park)
            .join(Park, Ride.park_id == Park.park_id)
            .filter(Ride.ride_id == ride_id)
            .first()
        )
        if not result:
            return {}

        ride, park = result
        return {
            "ride_id": ride.ride_id,
            "ride_name": ride.name,
            "park_name": park.name
        }

    def _get_snapshot_counts(self, park_id: int, start_date: date, end_date: date) -> Dict[str, int]:
        """Get snapshot counts for a park."""
        result = (
            self.session.query(
                func.count(RideStatusSnapshot.snapshot_id).label('total_snapshots'),
                func.count(func.distinct(Ride.ride_id)).label('rides_tracked'),
                func.count(func.distinct(func.date(RideStatusSnapshot.recorded_at))).label('days_with_data')
            )
            .join(Ride, RideStatusSnapshot.ride_id == Ride.ride_id)
            .filter(
                Ride.park_id == park_id,
                func.date(RideStatusSnapshot.recorded_at).between(start_date, end_date),
                Ride.is_active == True,
                Ride.category == 'ATTRACTION'
            )
            .first()
        )

        if not result:
            return {}

        return {
            "total_snapshots": result.total_snapshots or 0,
            "rides_tracked": result.rides_tracked or 0,
            "days_with_data": result.days_with_data or 0
        }

    def _get_ride_level_stats(
        self, park_id: int, start_date: date, end_date: date
    ) -> List[Dict[str, Any]]:
        """Get ride-level stats from pre-aggregated tables."""
        results = (
            self.session.query(
                RideDailyStats.ride_id,
                Ride.name.label('ride_name'),
                func.sum(RideDailyStats.uptime_minutes).label('operating_minutes'),
                func.sum(RideDailyStats.downtime_minutes).label('downtime_minutes'),
                func.round(func.sum(RideDailyStats.downtime_minutes) / 60.0, 2).label('downtime_hours'),
                RideClassification.tier,
                func.coalesce(RideClassification.tier_weight, 2).label('tier_weight')
            )
            .join(Ride, RideDailyStats.ride_id == Ride.ride_id)
            .outerjoin(RideClassification, Ride.ride_id == RideClassification.ride_id)
            .filter(
                Ride.park_id == park_id,
                RideDailyStats.stat_date.between(start_date, end_date)
            )
            .group_by(
                RideDailyStats.ride_id,
                Ride.name,
                RideClassification.tier,
                RideClassification.tier_weight
            )
            .order_by(func.sum(RideDailyStats.downtime_minutes).desc())
            .all()
        )

        return [
            {
                "ride_id": r.ride_id,
                "ride_name": r.ride_name,
                "operating_minutes": r.operating_minutes or 0,
                "downtime_minutes": r.downtime_minutes or 0,
                "downtime_hours": float(r.downtime_hours or 0),
                "tier": r.tier,
                "tier_weight": r.tier_weight or 2
            }
            for r in results
        ]

    def _get_weighted_stats(
        self, park_id: int, start_date: date, end_date: date
    ) -> Dict[str, Any]:
        """Get weighted downtime calculation from pre-aggregated tables."""
        result = (
            self.session.query(
                func.round(
                    func.sum((RideDailyStats.downtime_minutes / 60.0) * func.coalesce(RideClassification.tier_weight, 2)),
                    2
                ).label('weighted_downtime_hours'),
                func.sum(func.coalesce(RideClassification.tier_weight, 2)).label('total_park_weight'),
                func.count(func.distinct(RideDailyStats.ride_id)).label('total_rides')
            )
            .join(Ride, RideDailyStats.ride_id == Ride.ride_id)
            .outerjoin(RideClassification, Ride.ride_id == RideClassification.ride_id)
            .filter(
                Ride.park_id == park_id,
                RideDailyStats.stat_date.between(start_date, end_date)
            )
            .first()
        )

        if not result:
            return {}

        return {
            "weighted_downtime_hours": float(result.weighted_downtime_hours or 0),
            "total_park_weight": int(result.total_park_weight or 0),
            "total_rides": result.total_rides or 0
        }

    def _calculate_shame_score(
        self, park_id: int, start_date: date, end_date: date
    ) -> Dict[str, Any]:
        """Calculate final shame score from pre-aggregated tables."""
        result = (
            self.session.query(
                func.round(
                    func.sum((RideDailyStats.downtime_minutes / 60.0) * func.coalesce(RideClassification.tier_weight, 2)),
                    2
                ).label('weighted_downtime_hours'),
                func.sum(func.coalesce(RideClassification.tier_weight, 2)).label('total_park_weight'),
                func.round(
                    func.sum((RideDailyStats.downtime_minutes / 60.0) * func.coalesce(RideClassification.tier_weight, 2)) /
                    func.nullif(func.sum(func.coalesce(RideClassification.tier_weight, 2)), 0),
                    2
                ).label('shame_score')
            )
            .join(Ride, RideDailyStats.ride_id == Ride.ride_id)
            .outerjoin(RideClassification, Ride.ride_id == RideClassification.ride_id)
            .filter(
                Ride.park_id == park_id,
                RideDailyStats.stat_date.between(start_date, end_date)
            )
            .first()
        )

        if not result:
            return {}

        return {
            "weighted_downtime_hours": float(result.weighted_downtime_hours or 0),
            "total_park_weight": int(result.total_park_weight or 0),
            "shame_score": float(result.shame_score or 0)
        }

    def _get_ride_snapshot_breakdown(
        self, ride_id: int, start_date: date, end_date: date
    ) -> Dict[str, Any]:
        """Get ride stats breakdown from pre-aggregated tables."""
        # Note: Pre-aggregated tables have minutes, not snapshot counts
        # We convert to equivalent metrics
        result = (
            self.session.query(
                func.sum(RideDailyStats.operating_hours_minutes).label('operating_minutes'),
                func.sum(RideDailyStats.uptime_minutes).label('uptime_minutes'),
                func.sum(RideDailyStats.downtime_minutes).label('downtime_minutes'),
                # Convert to snapshot equivalents (5 min intervals)
                func.round(func.sum(RideDailyStats.operating_hours_minutes) / 5).label('park_open_snapshots'),
                func.round(func.sum(RideDailyStats.uptime_minutes) / 5).label('operating_snapshots'),
                func.round(func.sum(RideDailyStats.downtime_minutes) / 5).label('down_snapshots'),
                func.round(
                    (func.sum(RideDailyStats.operating_hours_minutes) -
                     func.sum(RideDailyStats.uptime_minutes) -
                     func.sum(RideDailyStats.downtime_minutes)) / 5
                ).label('other_snapshots')
            )
            .filter(
                RideDailyStats.ride_id == ride_id,
                RideDailyStats.stat_date.between(start_date, end_date)
            )
            .first()
        )

        if not result:
            return {}

        return {
            "operating_minutes": result.operating_minutes or 0,
            "uptime_minutes": result.uptime_minutes or 0,
            "downtime_minutes": result.downtime_minutes or 0,
            "park_open_snapshots": int(result.park_open_snapshots or 0),
            "operating_snapshots": int(result.operating_snapshots or 0),
            "down_snapshots": int(result.down_snapshots or 0),
            "other_snapshots": int(result.other_snapshots or 0),
            "total_snapshots": int(result.park_open_snapshots or 0)
        }

    def _get_data_quality(
        self, park_id: int, start_date: date, end_date: date
    ) -> Dict[str, Any]:
        """Get data quality metrics for a park from pre-aggregated tables."""
        result = (
            self.session.query(
                func.avg(ParkDailyStats.total_rides_tracked).label('avg_rides_tracked'),
                func.sum(ParkDailyStats.total_downtime_hours).label('total_downtime_hours'),
                func.avg(ParkDailyStats.avg_uptime_percentage).label('avg_uptime_percentage'),
                func.count(func.distinct(ParkDailyStats.stat_date)).label('days_with_data')
            )
            .filter(
                ParkDailyStats.park_id == park_id,
                ParkDailyStats.stat_date.between(start_date, end_date)
            )
            .first()
        )

        if not result:
            return {}

        days = (end_date - start_date).days + 1

        return {
            "avg_rides_tracked": float(result.avg_rides_tracked or 0),
            "total_downtime_hours": float(result.total_downtime_hours or 0),
            "avg_uptime_percentage": round(float(result.avg_uptime_percentage or 0), 1),
            "days_with_data": result.days_with_data or 0,
            "expected_days": days,
            "data_completeness": round(
                100.0 * (result.days_with_data or 0) / max(days, 1), 1
            ),
        }

    def to_dict(self, trace: ComputationTrace) -> Dict[str, Any]:
        """Convert trace to JSON-serializable dict."""
        return {
            "verified": trace.verified,
            "displayed_value": trace.displayed_value,
            "computed_value": trace.computed_value,
            "tolerance": trace.tolerance,
            "entity_type": trace.entity_type,
            "entity_id": trace.entity_id,
            "entity_name": trace.entity_name,
            "period": trace.period,
            "metric": trace.metric,
            "computation_trace": [
                {
                    "step": s.step_number,
                    "name": s.name,
                    "description": s.description,
                    "formula": s.formula,
                    "inputs": s.inputs,
                    "output": s.output,
                    "output_label": s.output_label,
                }
                for s in trace.steps
            ],
            "data_quality": trace.data_quality,
            "methodology_url": trace.methodology_url,
        }
