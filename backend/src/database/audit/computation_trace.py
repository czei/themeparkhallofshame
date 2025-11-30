"""
Computation Trace Generator
===========================

Generates step-by-step calculation traces for any displayed number.
This is the core of the user-triggered audit feature.

When a user clicks "Verify this number", they see:
1. Raw data counts (snapshots)
2. Each calculation step with formula
3. Final result with verification status
4. Data quality metrics

Usage:
    from database.audit import ComputationTracer

    tracer = ComputationTracer(conn)

    # User clicked on a park shame score
    trace = tracer.trace_park_shame_score(
        park_id=1,
        period="today",
        displayed_value=2.45
    )

    # Returns full computation trace showing how 2.45 was calculated

Created: 2024-11 (Data Accuracy Audit Framework)
"""

from datetime import date, datetime, timedelta
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, asdict
from sqlalchemy import text
from sqlalchemy.engine import Connection

from utils.logger import logger
from utils.timezone import get_today_pacific


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

    def __init__(self, conn: Connection, tolerance: float = DEFAULT_TOLERANCE):
        """
        Initialize with database connection.

        Args:
            conn: SQLAlchemy connection
            tolerance: Max difference between displayed and computed for "verified"
        """
        self.conn = conn
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
        query = text("SELECT park_id, name AS park_name FROM parks WHERE park_id = :park_id")
        result = self.conn.execute(query, {"park_id": park_id}).fetchone()
        return dict(result._mapping) if result else {}

    def _get_ride_info(self, ride_id: int) -> Dict[str, Any]:
        """Get basic ride information."""
        query = text("""
            SELECT r.ride_id, r.name AS ride_name, p.name AS park_name
            FROM rides r
            JOIN parks p ON r.park_id = p.park_id
            WHERE r.ride_id = :ride_id
        """)
        result = self.conn.execute(query, {"ride_id": ride_id}).fetchone()
        return dict(result._mapping) if result else {}

    def _get_snapshot_counts(self, park_id: int, start_date: date, end_date: date) -> Dict[str, int]:
        """Get snapshot counts for a park."""
        query = text("""
            SELECT
                COUNT(*) AS total_snapshots,
                COUNT(DISTINCT r.ride_id) AS rides_tracked,
                COUNT(DISTINCT DATE(rss.recorded_at)) AS days_with_data
            FROM ride_status_snapshots rss
            JOIN rides r ON rss.ride_id = r.ride_id
            WHERE r.park_id = :park_id
            AND DATE(rss.recorded_at) BETWEEN :start_date AND :end_date
            AND r.is_active = 1
            AND r.category = 'ATTRACTION'
        """)
        result = self.conn.execute(
            query, {"park_id": park_id, "start_date": start_date, "end_date": end_date}
        ).fetchone()
        return dict(result._mapping) if result else {}

    def _get_ride_level_stats(
        self, park_id: int, start_date: date, end_date: date
    ) -> List[Dict[str, Any]]:
        """Get ride-level stats from audit view."""
        query = text("""
            SELECT
                ride_id,
                ride_name,
                SUM(total_snapshots) AS total_snapshots,
                SUM(down_snapshots) AS down_snapshots,
                ROUND(SUM(downtime_hours), 2) AS downtime_hours,
                tier,
                tier_weight
            FROM v_audit_ride_daily
            WHERE park_id = :park_id
            AND stat_date BETWEEN :start_date AND :end_date
            GROUP BY ride_id, ride_name, tier, tier_weight
            ORDER BY downtime_hours DESC
        """)
        result = self.conn.execute(
            query, {"park_id": park_id, "start_date": start_date, "end_date": end_date}
        )
        return [dict(row._mapping) for row in result.fetchall()]

    def _get_weighted_stats(
        self, park_id: int, start_date: date, end_date: date
    ) -> Dict[str, Any]:
        """Get weighted downtime calculation."""
        query = text("""
            SELECT
                ROUND(SUM(downtime_hours * tier_weight), 2) AS weighted_downtime_hours,
                SUM(tier_weight) AS total_park_weight,
                COUNT(DISTINCT ride_id) AS total_rides
            FROM v_audit_ride_daily
            WHERE park_id = :park_id
            AND stat_date BETWEEN :start_date AND :end_date
        """)
        result = self.conn.execute(
            query, {"park_id": park_id, "start_date": start_date, "end_date": end_date}
        ).fetchone()
        return dict(result._mapping) if result else {}

    def _calculate_shame_score(
        self, park_id: int, start_date: date, end_date: date
    ) -> Dict[str, Any]:
        """Calculate final shame score."""
        query = text("""
            SELECT
                ROUND(SUM(downtime_hours * tier_weight), 2) AS weighted_downtime_hours,
                SUM(tier_weight) AS total_park_weight,
                ROUND(
                    SUM(downtime_hours * tier_weight) / NULLIF(SUM(tier_weight), 0),
                    2
                ) AS shame_score
            FROM v_audit_ride_daily
            WHERE park_id = :park_id
            AND stat_date BETWEEN :start_date AND :end_date
        """)
        result = self.conn.execute(
            query, {"park_id": park_id, "start_date": start_date, "end_date": end_date}
        ).fetchone()
        return dict(result._mapping) if result else {}

    def _get_ride_snapshot_breakdown(
        self, ride_id: int, start_date: date, end_date: date
    ) -> Dict[str, Any]:
        """Get snapshot breakdown for a single ride."""
        query = text("""
            SELECT
                SUM(total_snapshots) AS total_snapshots,
                SUM(park_open_snapshots) AS park_open_snapshots,
                SUM(operating_snapshots) AS operating_snapshots,
                SUM(down_snapshots) AS down_snapshots,
                SUM(closed_snapshots) AS closed_snapshots,
                SUM(refurbishment_snapshots) AS refurbishment_snapshots
            FROM v_audit_ride_daily
            WHERE ride_id = :ride_id
            AND stat_date BETWEEN :start_date AND :end_date
        """)
        result = self.conn.execute(
            query, {"ride_id": ride_id, "start_date": start_date, "end_date": end_date}
        ).fetchone()
        return dict(result._mapping) if result else {}

    def _get_data_quality(
        self, park_id: int, start_date: date, end_date: date
    ) -> Dict[str, Any]:
        """Get data quality metrics for a park."""
        query = text("""
            SELECT
                SUM(total_rides) AS total_rides,
                SUM(total_ride_snapshots) AS total_snapshots,
                SUM(total_park_open_snapshots) AS park_open_snapshots,
                COUNT(DISTINCT stat_date) AS days_with_data
            FROM v_audit_park_daily
            WHERE park_id = :park_id
            AND stat_date BETWEEN :start_date AND :end_date
        """)
        result = self.conn.execute(
            query, {"park_id": park_id, "start_date": start_date, "end_date": end_date}
        ).fetchone()

        if not result:
            return {}

        data = dict(result._mapping)
        days = (end_date - start_date).days + 1
        expected_snapshots = (data.get("total_rides") or 0) * 288 * days

        return {
            "total_snapshots": data.get("total_snapshots", 0),
            "park_open_snapshots": data.get("park_open_snapshots", 0),
            "expected_snapshots": expected_snapshots,
            "coverage_percentage": round(
                100.0 * (data.get("park_open_snapshots") or 0) / max(expected_snapshots, 1), 1
            ),
            "days_with_data": data.get("days_with_data", 0),
            "expected_days": days,
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
