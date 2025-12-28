"""
Aggregate Data Verification System
==================================

Verifies that aggregate table values match raw snapshot calculations.
This catches bugs like timezone issues or incorrect interval multipliers.

Usage:
    verifier = AggregateVerifier(session)
    summary = verifier.audit_date(date(2025, 12, 17))

    if not summary.overall_passed:
        print(f"Verification failed: {summary.issues_found}")

Verification Process:
1. Calculate expected values from raw snapshots using correct Pacific timezone
2. Compare against stored values in aggregate tables
3. Flag any discrepancies above tolerance thresholds
"""

from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import List, Dict, Any, Optional

from sqlalchemy.orm import Session
from sqlalchemy import func, case, and_, or_, distinct

from models.orm_park import Park
from models.orm_ride import Ride
from models.orm_snapshots import RideStatusSnapshot, ParkActivitySnapshot
from models.orm_stats import RideDailyStats, ParkDailyStats, RideHourlyStats, ParkHourlyStats
from utils.timezone import get_pacific_day_range_utc
from utils.metrics import SNAPSHOT_INTERVAL_MINUTES


@dataclass
class AggregateAuditResult:
    """Result of verifying a single aggregate table."""

    table_name: str
    target_date: date
    total_records_checked: int
    records_matching: int
    records_mismatched: int
    records_missing_from_aggregate: int
    records_missing_from_raw: int

    # Match rate (0.0 - 1.0)
    match_rate: float

    # Deviation statistics by column
    max_deviation: Dict[str, float] = field(default_factory=dict)
    avg_deviation: Dict[str, float] = field(default_factory=dict)

    # Worst mismatches for debugging (top 10)
    worst_mismatches: List[Dict[str, Any]] = field(default_factory=list)

    # Verdict
    passed: bool = True
    severity: str = "INFO"  # INFO, WARNING, CRITICAL
    message: str = ""


@dataclass
class DisneyDownCheckResult:
    """Result of checking Disney/Universal DOWN status is counted correctly.

    This catches the bug where Disney/Universal rides with status='DOWN' were
    excluded from downtime calculations because they didn't have status='OPERATING'
    first. For Disney/Universal parks, DOWN status IS reliable - it's a real
    breakdown signal that should always count.
    """

    parks_checked: int
    rides_with_down_status: int
    rides_incorrectly_excluded: int
    examples: List[Dict[str, Any]] = field(default_factory=list)
    passed: bool = True
    message: str = ""


@dataclass
class IntervalConsistencyResult:
    """Result of checking snapshot interval is used consistently.

    This catches bugs where SNAPSHOT_INTERVAL_MINUTES is hardcoded instead of
    imported from utils/metrics.py, causing downtime calculations to be wrong.
    """

    expected_interval: int
    calculated_interval: float  # From actual snapshot timing
    is_consistent: bool = True
    message: str = ""


@dataclass
class AuditSummary:
    """Overall audit summary across all tables."""

    audit_timestamp: datetime
    target_date: date

    # Per-table results
    ride_daily_result: Optional[AggregateAuditResult] = None
    park_daily_result: Optional[AggregateAuditResult] = None
    ride_hourly_results: List[AggregateAuditResult] = field(default_factory=list)
    park_hourly_results: List[AggregateAuditResult] = field(default_factory=list)

    # Special checks
    disney_down_check_result: Optional[DisneyDownCheckResult] = None
    interval_check_result: Optional[IntervalConsistencyResult] = None

    # Overall status
    overall_passed: bool = True
    critical_failures: int = 0
    warnings: int = 0

    # Summary
    issues_found: List[str] = field(default_factory=list)
    recommended_actions: List[str] = field(default_factory=list)


class AggregateVerifier:
    """
    Verifies aggregate table values match raw snapshot calculations.

    Tolerances are set to account for:
    - Timing drift between snapshot inserts (1 snapshot = ~10 minutes)
    - Rounding accumulation in percentages
    - Tier weighting amplification
    """

    # Tolerance thresholds
    TOLERANCES = {
        'ride_daily': {
            'uptime_minutes': 10,      # 1 snapshot
            'downtime_minutes': 10,    # 1 snapshot
            'operating_hours_minutes': 10,
        },
        'park_daily': {
            'total_downtime_hours': 0.17,  # ~10 minutes
            'shame_score': 0.2,            # 2% on 0-10 scale
            'rides_with_downtime': 1,      # 1 ride tolerance
        },
        'ride_hourly': {
            'downtime_hours': 0.1,         # ~6 minutes
            'uptime_percentage': 2.0,      # 2%
        },
        'park_hourly': {
            'shame_score': 0.3,            # 3% on 0-10 scale
            'total_downtime_hours': 0.25,  # 15 minutes
        }
    }

    def __init__(self, session: Session):
        self.session = session
        self.snapshot_interval = SNAPSHOT_INTERVAL_MINUTES

    def audit_date(self, target_date: date) -> AuditSummary:
        """
        Run full verification for a specific date.

        Args:
            target_date: Pacific date to verify

        Returns:
            AuditSummary with results for all tables
        """
        summary = AuditSummary(
            audit_timestamp=datetime.utcnow(),
            target_date=target_date
        )

        # Verify ride daily stats
        summary.ride_daily_result = self.verify_ride_daily_stats(target_date)
        if not summary.ride_daily_result.passed:
            if summary.ride_daily_result.severity == "CRITICAL":
                summary.critical_failures += 1
            else:
                summary.warnings += 1
            summary.issues_found.append(summary.ride_daily_result.message)

        # Verify park daily stats
        summary.park_daily_result = self.verify_park_daily_stats(target_date)
        if not summary.park_daily_result.passed:
            if summary.park_daily_result.severity == "CRITICAL":
                summary.critical_failures += 1
            else:
                summary.warnings += 1
            summary.issues_found.append(summary.park_daily_result.message)

        # Set overall status
        summary.overall_passed = summary.critical_failures == 0

        # Add recommendations
        if summary.critical_failures > 0:
            summary.recommended_actions.append(
                "Re-run daily aggregation for this date: "
                f"python -m scripts.aggregate_daily --date {target_date}"
            )

        return summary

    def verify_ride_daily_stats(self, target_date: date) -> AggregateAuditResult:
        """
        Verify ride_daily_stats against raw snapshots.

        Uses correct Pacific timezone conversion for date range.
        """
        day_start_utc, day_end_utc = get_pacific_day_range_utc(target_date)
        tolerances = self.TOLERANCES['ride_daily']

        # CTE: Rides that operated at least once during the Pacific day
        rides_operated_today = (
            self.session.query(distinct(RideStatusSnapshot.ride_id).label('ride_id'))
            .join(Ride, RideStatusSnapshot.ride_id == Ride.ride_id)
            .join(
                ParkActivitySnapshot,
                and_(
                    Ride.park_id == ParkActivitySnapshot.park_id,
                    func.date_format(ParkActivitySnapshot.recorded_at, '%Y-%m-%d %H:%i') ==
                    func.date_format(RideStatusSnapshot.recorded_at, '%Y-%m-%d %H:%i')
                )
            )
            .filter(
                and_(
                    RideStatusSnapshot.recorded_at >= day_start_utc,
                    RideStatusSnapshot.recorded_at < day_end_utc,
                    ParkActivitySnapshot.park_appears_open.is_(True),
                    or_(
                        RideStatusSnapshot.status == 'OPERATING',
                        and_(
                            RideStatusSnapshot.status.is_(None),
                            RideStatusSnapshot.computed_is_open.is_(True)
                        )
                    )
                )
            )
            .subquery()
        )

        # Calculate expected values from raw snapshots
        raw_calc = (
            self.session.query(
                Ride.ride_id,
                Ride.name.label('ride_name'),
                Park.name.label('park_name'),

                # Uptime minutes (when ride was open and park was open)
                func.coalesce(
                    func.sum(
                        case(
                            (
                                and_(
                                    ParkActivitySnapshot.park_appears_open == 1,
                                    RideStatusSnapshot.computed_is_open.is_(True)
                                ),
                                self.snapshot_interval
                            ),
                            else_=0
                        )
                    ),
                    0
                ).label('calc_uptime_minutes'),

                # Downtime minutes (using same logic as aggregate_daily.py)
                case(
                    (
                        Ride.ride_id.in_(self.session.query(rides_operated_today.c.ride_id)),
                        func.coalesce(
                            func.sum(
                                case(
                                    (
                                        and_(
                                            ParkActivitySnapshot.park_appears_open == 1,
                                            or_(
                                                and_(
                                                    RideStatusSnapshot.status.isnot(None),
                                                    RideStatusSnapshot.status == 'DOWN'
                                                ),
                                                and_(
                                                    RideStatusSnapshot.status.is_(None),
                                                    RideStatusSnapshot.computed_is_open.is_(False)
                                                )
                                            )
                                        ),
                                        self.snapshot_interval
                                    ),
                                    else_=0
                                )
                            ),
                            0
                        )
                    ),
                    else_=0
                ).label('calc_downtime_minutes'),

                # Operating hours minutes (park open time)
                func.coalesce(
                    func.sum(
                        case(
                            (ParkActivitySnapshot.park_appears_open == 1, self.snapshot_interval),
                            else_=0
                        )
                    ),
                    0
                ).label('calc_operating_hours_minutes')
            )
            .select_from(RideStatusSnapshot)
            .join(Ride, RideStatusSnapshot.ride_id == Ride.ride_id)
            .join(Park, Ride.park_id == Park.park_id)
            .join(
                ParkActivitySnapshot,
                and_(
                    Ride.park_id == ParkActivitySnapshot.park_id,
                    ParkActivitySnapshot.recorded_at == RideStatusSnapshot.recorded_at
                )
            )
            .filter(
                and_(
                    RideStatusSnapshot.recorded_at >= day_start_utc,
                    RideStatusSnapshot.recorded_at < day_end_utc,
                    Ride.is_active.is_(True),
                    Ride.category == 'ATTRACTION'
                )
            )
            .group_by(Ride.ride_id, Ride.name, Park.name)
            .subquery()
        )

        # Join with stored values and calculate deltas
        results = (
            self.session.query(
                raw_calc.c.ride_id,
                raw_calc.c.ride_name,
                raw_calc.c.park_name,

                # Stored values
                func.coalesce(RideDailyStats.uptime_minutes, 0).label('stored_uptime_minutes'),
                func.coalesce(RideDailyStats.downtime_minutes, 0).label('stored_downtime_minutes'),
                func.coalesce(RideDailyStats.operating_hours_minutes, 0).label('stored_operating_hours_minutes'),

                # Calculated values
                raw_calc.c.calc_uptime_minutes,
                raw_calc.c.calc_downtime_minutes,
                raw_calc.c.calc_operating_hours_minutes,

                # Deltas
                func.abs(func.coalesce(RideDailyStats.uptime_minutes, 0) - raw_calc.c.calc_uptime_minutes).label('uptime_delta'),
                func.abs(func.coalesce(RideDailyStats.downtime_minutes, 0) - raw_calc.c.calc_downtime_minutes).label('downtime_delta'),
                func.abs(func.coalesce(RideDailyStats.operating_hours_minutes, 0) - raw_calc.c.calc_operating_hours_minutes).label('operating_hours_delta'),

                # Is aggregate missing?
                case((RideDailyStats.ride_id.is_(None), 1), else_=0).label('missing_from_aggregate')
            )
            .select_from(raw_calc)
            .outerjoin(
                RideDailyStats,
                and_(
                    raw_calc.c.ride_id == RideDailyStats.ride_id,
                    RideDailyStats.stat_date == target_date
                )
            )
            .filter(
                or_(
                    raw_calc.c.calc_uptime_minutes > 0,
                    raw_calc.c.calc_downtime_minutes > 0
                )
            )
            .order_by(
                case((RideDailyStats.ride_id.is_(None), 1), else_=0).desc(),
                func.greatest(
                    func.abs(func.coalesce(RideDailyStats.uptime_minutes, 0) - raw_calc.c.calc_uptime_minutes),
                    func.abs(func.coalesce(RideDailyStats.downtime_minutes, 0) - raw_calc.c.calc_downtime_minutes)
                ).desc()
            )
            .all()
        )

        # Convert to list of dicts for analysis
        rows = [
            {
                'ride_id': row.ride_id,
                'ride_name': row.ride_name,
                'park_name': row.park_name,
                'stored_uptime_minutes': row.stored_uptime_minutes,
                'stored_downtime_minutes': row.stored_downtime_minutes,
                'stored_operating_hours_minutes': row.stored_operating_hours_minutes,
                'calc_uptime_minutes': row.calc_uptime_minutes,
                'calc_downtime_minutes': row.calc_downtime_minutes,
                'calc_operating_hours_minutes': row.calc_operating_hours_minutes,
                'uptime_delta': row.uptime_delta,
                'downtime_delta': row.downtime_delta,
                'operating_hours_delta': row.operating_hours_delta,
                'missing_from_aggregate': row.missing_from_aggregate
            }
            for row in results
        ]

        # Analyze results
        total_checked = len(rows)
        mismatches = []
        missing_count = 0

        uptime_deltas = []
        downtime_deltas = []

        for row in rows:
            if row['missing_from_aggregate']:
                missing_count += 1
                mismatches.append(row)
                continue

            uptime_deltas.append(row['uptime_delta'])
            downtime_deltas.append(row['downtime_delta'])

            # Check tolerances
            if (row['uptime_delta'] > tolerances['uptime_minutes'] or
                row['downtime_delta'] > tolerances['downtime_minutes']):
                mismatches.append(row)

        match_count = total_checked - len(mismatches)
        match_rate = match_count / total_checked if total_checked > 0 else 1.0

        # Build result
        audit_result = AggregateAuditResult(
            table_name='ride_daily_stats',
            target_date=target_date,
            total_records_checked=total_checked,
            records_matching=match_count,
            records_mismatched=len(mismatches) - missing_count,
            records_missing_from_aggregate=missing_count,
            records_missing_from_raw=0,
            match_rate=match_rate,
            max_deviation={
                'uptime_minutes': max(uptime_deltas) if uptime_deltas else 0,
                'downtime_minutes': max(downtime_deltas) if downtime_deltas else 0,
            },
            avg_deviation={
                'uptime_minutes': sum(uptime_deltas) / len(uptime_deltas) if uptime_deltas else 0,
                'downtime_minutes': sum(downtime_deltas) / len(downtime_deltas) if downtime_deltas else 0,
            },
            worst_mismatches=mismatches[:10]
        )

        # Determine severity
        if len(mismatches) > 0:
            audit_result.passed = False
            if len(mismatches) > 10 or missing_count > 5:
                audit_result.severity = "CRITICAL"
                audit_result.message = (
                    f"ride_daily_stats: {len(mismatches)} mismatches "
                    f"({missing_count} missing, {len(mismatches) - missing_count} wrong values)"
                )
            else:
                audit_result.severity = "WARNING"
                audit_result.message = (
                    f"ride_daily_stats: {len(mismatches)} minor discrepancies"
                )
        else:
            audit_result.message = f"ride_daily_stats: All {total_checked} records verified"

        return audit_result

    def verify_park_daily_stats(self, target_date: date) -> AggregateAuditResult:
        """
        Verify park_daily_stats against raw snapshots.

        Calculates park-level metrics from ride_daily_stats (which should be verified first).
        """
        tolerances = self.TOLERANCES['park_daily']

        # Calculate park stats from ride_daily_stats
        park_raw_calc = (
            self.session.query(
                Park.park_id,
                Park.name.label('park_name'),
                func.count(distinct(RideDailyStats.ride_id)).label('calc_total_rides'),
                func.round(func.sum(RideDailyStats.downtime_minutes) / 60.0, 2).label('calc_total_downtime_hours'),
                func.sum(case((RideDailyStats.downtime_minutes > 0, 1), else_=0)).label('calc_rides_with_downtime'),
                func.round(func.avg(RideDailyStats.uptime_percentage), 2).label('calc_avg_uptime')
            )
            .select_from(Park)
            .join(
                RideDailyStats,
                Park.park_id == self.session.query(Ride.park_id).filter(Ride.ride_id == RideDailyStats.ride_id).scalar_subquery()
            )
            .filter(
                and_(
                    RideDailyStats.stat_date == target_date,
                    Park.is_active.is_(True)
                )
            )
            .group_by(Park.park_id, Park.name)
            .subquery()
        )

        # Join with stored values and calculate deltas
        results = (
            self.session.query(
                park_raw_calc.c.park_id,
                park_raw_calc.c.park_name,

                # Stored values
                func.coalesce(ParkDailyStats.total_rides_tracked, 0).label('stored_total_rides'),
                func.coalesce(ParkDailyStats.total_downtime_hours, 0).label('stored_total_downtime_hours'),
                func.coalesce(ParkDailyStats.rides_with_downtime, 0).label('stored_rides_with_downtime'),
                func.coalesce(ParkDailyStats.shame_score, 0).label('stored_shame_score'),

                # Calculated values
                park_raw_calc.c.calc_total_rides,
                park_raw_calc.c.calc_total_downtime_hours,
                park_raw_calc.c.calc_rides_with_downtime,

                # Deltas
                func.abs(func.coalesce(ParkDailyStats.total_downtime_hours, 0) - park_raw_calc.c.calc_total_downtime_hours).label('downtime_hours_delta'),
                func.abs(func.coalesce(ParkDailyStats.rides_with_downtime, 0) - park_raw_calc.c.calc_rides_with_downtime).label('rides_with_downtime_delta'),

                # Is aggregate missing?
                case((ParkDailyStats.park_id.is_(None), 1), else_=0).label('missing_from_aggregate')
            )
            .select_from(park_raw_calc)
            .outerjoin(
                ParkDailyStats,
                and_(
                    park_raw_calc.c.park_id == ParkDailyStats.park_id,
                    ParkDailyStats.stat_date == target_date
                )
            )
            .filter(park_raw_calc.c.calc_total_rides > 0)
            .order_by(
                case((ParkDailyStats.park_id.is_(None), 1), else_=0).desc(),
                func.abs(func.coalesce(ParkDailyStats.total_downtime_hours, 0) - park_raw_calc.c.calc_total_downtime_hours).desc()
            )
            .all()
        )

        # Convert to list of dicts
        rows = [
            {
                'park_id': row.park_id,
                'park_name': row.park_name,
                'stored_total_rides': row.stored_total_rides,
                'stored_total_downtime_hours': float(row.stored_total_downtime_hours),
                'stored_rides_with_downtime': row.stored_rides_with_downtime,
                'stored_shame_score': float(row.stored_shame_score),
                'calc_total_rides': row.calc_total_rides,
                'calc_total_downtime_hours': float(row.calc_total_downtime_hours),
                'calc_rides_with_downtime': row.calc_rides_with_downtime,
                'downtime_hours_delta': float(row.downtime_hours_delta),
                'rides_with_downtime_delta': row.rides_with_downtime_delta,
                'missing_from_aggregate': row.missing_from_aggregate
            }
            for row in results
        ]

        # Analyze results
        total_checked = len(rows)
        mismatches = []
        missing_count = 0

        downtime_deltas = []

        for row in rows:
            if row['missing_from_aggregate']:
                missing_count += 1
                mismatches.append(row)
                continue

            downtime_deltas.append(row['downtime_hours_delta'])

            # Check tolerances
            if (row['downtime_hours_delta'] > tolerances['total_downtime_hours'] or
                row['rides_with_downtime_delta'] > tolerances['rides_with_downtime']):
                mismatches.append(row)

        match_count = total_checked - len(mismatches)
        match_rate = match_count / total_checked if total_checked > 0 else 1.0

        audit_result = AggregateAuditResult(
            table_name='park_daily_stats',
            target_date=target_date,
            total_records_checked=total_checked,
            records_matching=match_count,
            records_mismatched=len(mismatches) - missing_count,
            records_missing_from_aggregate=missing_count,
            records_missing_from_raw=0,
            match_rate=match_rate,
            max_deviation={
                'total_downtime_hours': max(downtime_deltas) if downtime_deltas else 0,
            },
            avg_deviation={
                'total_downtime_hours': sum(downtime_deltas) / len(downtime_deltas) if downtime_deltas else 0,
            },
            worst_mismatches=mismatches[:10]
        )

        # Determine severity
        if len(mismatches) > 0:
            audit_result.passed = False
            if len(mismatches) > 5 or missing_count > 2:
                audit_result.severity = "CRITICAL"
                audit_result.message = (
                    f"park_daily_stats: {len(mismatches)} mismatches "
                    f"({missing_count} missing, {len(mismatches) - missing_count} wrong values)"
                )
            else:
                audit_result.severity = "WARNING"
                audit_result.message = f"park_daily_stats: {len(mismatches)} minor discrepancies"
        else:
            audit_result.message = f"park_daily_stats: All {total_checked} records verified"

        return audit_result

    def get_summary_report(self, summary: AuditSummary) -> str:
        """
        Generate a human-readable summary report.
        """
        lines = [
            "=" * 60,
            f"AGGREGATE VERIFICATION REPORT - {summary.target_date}",
            f"Timestamp: {summary.audit_timestamp.isoformat()}",
            "=" * 60,
            ""
        ]

        # Overall status
        status = "PASS" if summary.overall_passed else "FAIL"
        lines.append(f"Overall Status: {status}")
        lines.append(f"Critical Failures: {summary.critical_failures}")
        lines.append(f"Warnings: {summary.warnings}")
        lines.append("")

        # ride_daily_stats
        if summary.ride_daily_result:
            r = summary.ride_daily_result
            lines.append(f"ride_daily_stats: {r.message}")
            lines.append(f"  - Records checked: {r.total_records_checked}")
            lines.append(f"  - Match rate: {r.match_rate:.1%}")
            if r.max_deviation:
                lines.append(f"  - Max uptime delta: {r.max_deviation.get('uptime_minutes', 0)} min")
                lines.append(f"  - Max downtime delta: {r.max_deviation.get('downtime_minutes', 0)} min")
            lines.append("")

        # park_daily_stats
        if summary.park_daily_result:
            r = summary.park_daily_result
            lines.append(f"park_daily_stats: {r.message}")
            lines.append(f"  - Records checked: {r.total_records_checked}")
            lines.append(f"  - Match rate: {r.match_rate:.1%}")
            if r.max_deviation:
                lines.append(f"  - Max downtime delta: {r.max_deviation.get('total_downtime_hours', 0):.2f} hours")
            lines.append("")

        # Issues and recommendations
        if summary.issues_found:
            lines.append("Issues Found:")
            for issue in summary.issues_found:
                lines.append(f"  - {issue}")
            lines.append("")

        if summary.recommended_actions:
            lines.append("Recommended Actions:")
            for action in summary.recommended_actions:
                lines.append(f"  - {action}")

        # Disney DOWN check
        if summary.disney_down_check_result:
            r = summary.disney_down_check_result
            lines.append("")
            lines.append(f"Disney/Universal DOWN Status Check: {r.message}")
            lines.append(f"  - Parks checked: {r.parks_checked}")
            lines.append(f"  - Rides with DOWN status: {r.rides_with_down_status}")
            if not r.passed:
                lines.append(f"  - Rides incorrectly excluded: {r.rides_incorrectly_excluded}")
                for ex in r.examples[:3]:
                    lines.append(f"    * {ex.get('ride_name', 'Unknown')}: {ex.get('issue', 'no detail')}")

        # Interval check
        if summary.interval_check_result:
            r = summary.interval_check_result
            lines.append("")
            lines.append(f"Interval Consistency Check: {r.message}")
            lines.append(f"  - Expected interval: {r.expected_interval} min")
            lines.append(f"  - Calculated interval: {r.calculated_interval:.1f} min")

        # Hourly results
        if summary.ride_hourly_results:
            failed_hours = [r for r in summary.ride_hourly_results if not r.passed]
            lines.append("")
            lines.append(f"Ride Hourly Stats: {len(summary.ride_hourly_results)} hours checked")
            if failed_hours:
                lines.append(f"  - Failed hours: {len(failed_hours)}")
                for r in failed_hours[:3]:
                    lines.append(f"    * {r.target_date}: {r.message}")
            else:
                lines.append("  - All hours passed")

        if summary.park_hourly_results:
            failed_hours = [r for r in summary.park_hourly_results if not r.passed]
            lines.append("")
            lines.append(f"Park Hourly Stats: {len(summary.park_hourly_results)} hours checked")
            if failed_hours:
                lines.append(f"  - Failed hours: {len(failed_hours)}")
                for r in failed_hours[:3]:
                    lines.append(f"    * {r.target_date}: {r.message}")
            else:
                lines.append("  - All hours passed")

        return "\n".join(lines)

    def verify_disney_down_status(self, target_date: date) -> DisneyDownCheckResult:
        """
        Verify that Disney/Universal rides with DOWN status are counted correctly.

        This catches the bug where rides with status='DOWN' but no prior 'OPERATING'
        status were excluded from downtime calculations due to the 'ride_operated'
        filter. For Disney/Universal parks, DOWN status is reliable and should
        always count as downtime.

        Args:
            target_date: Pacific date to check

        Returns:
            DisneyDownCheckResult with verification results
        """
        day_start_utc, day_end_utc = get_pacific_day_range_utc(target_date)

        # Find Disney/Universal rides that had DOWN status during the day
        disney_down_rides = (
            self.session.query(
                distinct(RideStatusSnapshot.ride_id).label('ride_id'),
                Ride.name.label('ride_name'),
                Park.park_id,
                Park.name.label('park_name'),
                func.date_format(RideStatusSnapshot.recorded_at, '%Y-%m-%d %H:00:00').label('hour_start')
            )
            .join(Ride, RideStatusSnapshot.ride_id == Ride.ride_id)
            .join(Park, Ride.park_id == Park.park_id)
            .join(
                ParkActivitySnapshot,
                and_(
                    Park.park_id == ParkActivitySnapshot.park_id,
                    ParkActivitySnapshot.recorded_at == RideStatusSnapshot.recorded_at
                )
            )
            .filter(
                and_(
                    RideStatusSnapshot.recorded_at >= day_start_utc,
                    RideStatusSnapshot.recorded_at < day_end_utc,
                    RideStatusSnapshot.status == 'DOWN',
                    ParkActivitySnapshot.park_appears_open.is_(True),
                    or_(Park.is_disney.is_(True), Park.is_universal.is_(True))
                )
            )
            .subquery()
        )

        # Check hourly ride status
        hourly_results = (
            self.session.query(
                disney_down_rides.c.ride_id,
                disney_down_rides.c.ride_name,
                disney_down_rides.c.park_name,
                disney_down_rides.c.hour_start,
                func.coalesce(RideHourlyStats.ride_operated, 0).label('ride_operated'),
                func.coalesce(RideHourlyStats.downtime_hours, 0).label('stored_downtime_hours'),
                func.coalesce(RideHourlyStats.down_snapshots, 0).label('stored_down_snapshots'),
                case(
                    (
                        or_(
                            RideHourlyStats.ride_operated == 0,
                            RideHourlyStats.ride_operated.is_(None)
                        ),
                        'excluded'
                    ),
                    (
                        or_(
                            RideHourlyStats.downtime_hours == 0,
                            RideHourlyStats.downtime_hours.is_(None)
                        ),
                        'zero_downtime'
                    ),
                    else_='ok'
                ).label('status')
            )
            .select_from(disney_down_rides)
            .outerjoin(
                RideHourlyStats,
                and_(
                    disney_down_rides.c.ride_id == RideHourlyStats.ride_id,
                    RideHourlyStats.hour_start_utc == disney_down_rides.c.hour_start
                )
            )
            .filter(
                or_(
                    RideHourlyStats.ride_operated == 0,
                    RideHourlyStats.ride_operated.is_(None),
                    RideHourlyStats.downtime_hours == 0,
                    RideHourlyStats.downtime_hours.is_(None)
                )
            )
            .order_by(disney_down_rides.c.park_name, disney_down_rides.c.ride_name, disney_down_rides.c.hour_start)
            .all()
        )

        # Convert to list of dicts
        rows = [
            {
                'ride_id': row.ride_id,
                'ride_name': row.ride_name,
                'park_name': row.park_name,
                'hour_start': row.hour_start,
                'ride_operated': row.ride_operated,
                'stored_downtime_hours': float(row.stored_downtime_hours),
                'stored_down_snapshots': row.stored_down_snapshots,
                'status': row.status
            }
            for row in hourly_results
        ]

        # Count parks checked
        parks_checked = (
            self.session.query(func.count(distinct(Park.park_id)))
            .filter(or_(Park.is_disney.is_(True), Park.is_universal.is_(True)))
            .scalar() or 0
        )

        # Count rides with DOWN status
        rides_with_down = (
            self.session.query(func.count(distinct(RideStatusSnapshot.ride_id)))
            .join(Ride, RideStatusSnapshot.ride_id == Ride.ride_id)
            .join(Park, Ride.park_id == Park.park_id)
            .filter(
                and_(
                    RideStatusSnapshot.recorded_at >= day_start_utc,
                    RideStatusSnapshot.recorded_at < day_end_utc,
                    RideStatusSnapshot.status == 'DOWN',
                    or_(Park.is_disney.is_(True), Park.is_universal.is_(True))
                )
            )
            .scalar() or 0
        )

        # Build result
        check_result = DisneyDownCheckResult(
            parks_checked=parks_checked,
            rides_with_down_status=rides_with_down,
            rides_incorrectly_excluded=len(rows)
        )

        if rows:
            check_result.passed = False
            check_result.examples = rows[:10]
            check_result.message = (
                f"FAIL: {len(rows)} Disney/Universal ride-hour combinations "
                f"have DOWN status but ride_operated=0 or zero downtime"
            )
        else:
            check_result.message = (
                f"PASS: All {rides_with_down} Disney/Universal rides with DOWN status "
                f"are correctly counted in hourly stats"
            )

        return check_result

    def verify_interval_consistency(self, target_date: date) -> IntervalConsistencyResult:
        """
        Verify that the snapshot interval used in calculations matches reality.

        Compares SNAPSHOT_INTERVAL_MINUTES constant against actual timing between
        consecutive snapshots. If there's a significant mismatch, it indicates
        the interval constant may be wrong.

        Args:
            target_date: Pacific date to check

        Returns:
            IntervalConsistencyResult with verification results
        """
        day_start_utc, day_end_utc = get_pacific_day_range_utc(target_date)

        # Get distinct snapshot times
        distinct_times = (
            self.session.query(distinct(RideStatusSnapshot.recorded_at).label('recorded_at'))
            .filter(
                and_(
                    RideStatusSnapshot.recorded_at >= day_start_utc,
                    RideStatusSnapshot.recorded_at < day_end_utc
                )
            )
            .subquery()
        )

        # Calculate time between consecutive snapshots
        snapshot_times = (
            self.session.query(
                distinct_times.c.recorded_at,
                func.lag(distinct_times.c.recorded_at).over(order_by=distinct_times.c.recorded_at).label('prev_time')
            )
            .subquery()
        )

        # Calculate average interval
        result = (
            self.session.query(
                (func.avg(func.timestampdiff('SECOND', snapshot_times.c.prev_time, snapshot_times.c.recorded_at)) / 60.0).label('avg_interval_minutes'),
                (func.min(func.timestampdiff('SECOND', snapshot_times.c.prev_time, snapshot_times.c.recorded_at)) / 60.0).label('min_interval_minutes'),
                (func.max(func.timestampdiff('SECOND', snapshot_times.c.prev_time, snapshot_times.c.recorded_at)) / 60.0).label('max_interval_minutes'),
                func.count().label('sample_count')
            )
            .select_from(snapshot_times)
            .filter(snapshot_times.c.prev_time.isnot(None))
            .first()
        )

        if not result or result.avg_interval_minutes is None:
            return IntervalConsistencyResult(
                expected_interval=self.snapshot_interval,
                calculated_interval=0.0,
                is_consistent=True,
                message="No snapshot data to verify interval"
            )

        avg_interval = float(result.avg_interval_minutes)
        expected = self.snapshot_interval

        # Allow 20% tolerance for timing drift
        tolerance = expected * 0.20
        is_consistent = abs(avg_interval - expected) <= tolerance

        check_result = IntervalConsistencyResult(
            expected_interval=expected,
            calculated_interval=avg_interval,
            is_consistent=is_consistent
        )

        if is_consistent:
            check_result.message = (
                f"PASS: Actual interval ({avg_interval:.1f} min) matches "
                f"expected ({expected} min)"
            )
        else:
            check_result.message = (
                f"FAIL: Actual interval ({avg_interval:.1f} min) differs from "
                f"expected ({expected} min) - check SNAPSHOT_INTERVAL_MINUTES"
            )

        return check_result

    def verify_ride_hourly_stats(self, hour_start_utc: datetime) -> AggregateAuditResult:
        """
        Verify ride_hourly_stats for a specific hour against raw snapshots.

        Recalculates downtime from raw snapshots and compares against stored values.

        Args:
            hour_start_utc: UTC hour start time to verify

        Returns:
            AggregateAuditResult with verification results
        """
        hour_end_utc = hour_start_utc + timedelta(hours=1)
        tolerances = self.TOLERANCES['ride_hourly']
        interval = self.snapshot_interval

        # Calculate from raw snapshots
        raw_calc = (
            self.session.query(
                RideStatusSnapshot.ride_id,
                Ride.name.label('ride_name'),
                Park.name.label('park_name'),
                Park.is_disney,
                Park.is_universal,
                func.count().label('total_snapshots'),

                func.sum(
                    case(
                        (
                            and_(
                                ParkActivitySnapshot.park_appears_open.is_(True),
                                or_(
                                    RideStatusSnapshot.status == 'DOWN',
                                    and_(
                                        RideStatusSnapshot.status == 'CLOSED',
                                        Park.is_disney.is_(False),
                                        Park.is_universal.is_(False)
                                    )
                                )
                            ),
                            1
                        ),
                        else_=0
                    )
                ).label('down_count'),

                func.sum(
                    case(
                        (
                            or_(
                                RideStatusSnapshot.status == 'OPERATING',
                                RideStatusSnapshot.computed_is_open.is_(True)
                            ),
                            1
                        ),
                        else_=0
                    )
                ).label('operating_count'),

                # Expected downtime hours
                func.round(
                    func.sum(
                        case(
                            (
                                and_(
                                    ParkActivitySnapshot.park_appears_open.is_(True),
                                    or_(
                                        RideStatusSnapshot.status == 'DOWN',
                                        and_(
                                            RideStatusSnapshot.status == 'CLOSED',
                                            Park.is_disney.is_(False),
                                            Park.is_universal.is_(False)
                                        )
                                    )
                                ),
                                interval / 60.0
                            ),
                            else_=0
                        )
                    ),
                    2
                ).label('calc_downtime_hours')
            )
            .join(Ride, RideStatusSnapshot.ride_id == Ride.ride_id)
            .join(Park, Ride.park_id == Park.park_id)
            .join(
                ParkActivitySnapshot,
                and_(
                    Park.park_id == ParkActivitySnapshot.park_id,
                    ParkActivitySnapshot.recorded_at == RideStatusSnapshot.recorded_at
                )
            )
            .filter(
                and_(
                    RideStatusSnapshot.recorded_at >= hour_start_utc,
                    RideStatusSnapshot.recorded_at < hour_end_utc
                )
            )
            .group_by(RideStatusSnapshot.ride_id, Ride.name, Park.name, Park.is_disney, Park.is_universal)
            .having(or_(func.sum(case((and_(ParkActivitySnapshot.park_appears_open.is_(True), or_(RideStatusSnapshot.status == 'DOWN', and_(RideStatusSnapshot.status == 'CLOSED', Park.is_disney.is_(False), Park.is_universal.is_(False)))), 1), else_=0)) > 0, func.sum(case((or_(RideStatusSnapshot.status == 'OPERATING', RideStatusSnapshot.computed_is_open.is_(True)), 1), else_=0)) > 0))
            .subquery()
        )

        # Join with stored values
        results = (
            self.session.query(
                raw_calc.c.ride_id,
                raw_calc.c.ride_name,
                raw_calc.c.park_name,
                raw_calc.c.down_count.label('calc_down_snapshots'),
                raw_calc.c.calc_downtime_hours,

                func.coalesce(RideHourlyStats.down_snapshots, 0).label('stored_down_snapshots'),
                func.coalesce(RideHourlyStats.downtime_hours, 0).label('stored_downtime_hours'),
                func.coalesce(RideHourlyStats.ride_operated, 0).label('ride_operated'),

                func.abs(func.coalesce(RideHourlyStats.downtime_hours, 0) - raw_calc.c.calc_downtime_hours).label('downtime_delta'),
                case((RideHourlyStats.ride_id.is_(None), 1), else_=0).label('missing_from_aggregate')
            )
            .select_from(raw_calc)
            .outerjoin(
                RideHourlyStats,
                and_(
                    raw_calc.c.ride_id == RideHourlyStats.ride_id,
                    RideHourlyStats.hour_start_utc == hour_start_utc
                )
            )
            .filter(
                or_(
                    func.abs(func.coalesce(RideHourlyStats.downtime_hours, 0) - raw_calc.c.calc_downtime_hours) > tolerances['downtime_hours'],
                    RideHourlyStats.ride_id.is_(None)
                )
            )
            .order_by(func.abs(func.coalesce(RideHourlyStats.downtime_hours, 0) - raw_calc.c.calc_downtime_hours).desc())
            .limit(20)
            .all()
        )

        # Convert to list of dicts
        mismatches = [
            {
                'ride_id': row.ride_id,
                'ride_name': row.ride_name,
                'park_name': row.park_name,
                'calc_down_snapshots': row.calc_down_snapshots,
                'calc_downtime_hours': float(row.calc_downtime_hours),
                'stored_down_snapshots': row.stored_down_snapshots,
                'stored_downtime_hours': float(row.stored_downtime_hours),
                'ride_operated': row.ride_operated,
                'downtime_delta': float(row.downtime_delta),
                'missing_from_aggregate': row.missing_from_aggregate
            }
            for row in results
        ]

        # Count total records checked
        total_checked = (
            self.session.query(func.count(distinct(RideHourlyStats.ride_id)))
            .filter(RideHourlyStats.hour_start_utc == hour_start_utc)
            .scalar() or 0
        )

        missing_count = sum(1 for m in mismatches if m['missing_from_aggregate'])
        match_count = total_checked - len(mismatches)
        match_rate = match_count / total_checked if total_checked > 0 else 1.0

        audit_result = AggregateAuditResult(
            table_name='ride_hourly_stats',
            target_date=hour_start_utc.date(),
            total_records_checked=total_checked,
            records_matching=match_count,
            records_mismatched=len(mismatches) - missing_count,
            records_missing_from_aggregate=missing_count,
            records_missing_from_raw=0,
            match_rate=match_rate,
            max_deviation={
                'downtime_hours': max((m['downtime_delta'] for m in mismatches), default=0)
            },
            avg_deviation={},
            worst_mismatches=mismatches[:10]
        )

        if mismatches:
            audit_result.passed = False
            if len(mismatches) > 10 or missing_count > 5:
                audit_result.severity = "CRITICAL"
            else:
                audit_result.severity = "WARNING"
            audit_result.message = (
                f"ride_hourly_stats {hour_start_utc}: {len(mismatches)} mismatches"
            )
        else:
            audit_result.message = (
                f"ride_hourly_stats {hour_start_utc}: All {total_checked} records verified"
            )

        return audit_result

    def verify_park_hourly_stats(self, hour_start_utc: datetime) -> AggregateAuditResult:
        """
        Verify park_hourly_stats for a specific hour against ride_hourly_stats.

        Park hourly stats should be the sum of ride hourly stats for that hour.

        Args:
            hour_start_utc: UTC hour start time to verify

        Returns:
            AggregateAuditResult with verification results
        """
        tolerances = self.TOLERANCES['park_hourly']

        # Calculate from ride_hourly_stats
        ride_sums = (
            self.session.query(
                Ride.park_id,
                Park.name.label('park_name'),
                func.sum(RideHourlyStats.downtime_hours).label('calc_total_downtime_hours'),
                func.sum(case((RideHourlyStats.downtime_hours > 0, 1), else_=0)).label('calc_rides_down')
            )
            .select_from(RideHourlyStats)
            .join(Ride, RideHourlyStats.ride_id == Ride.ride_id)
            .join(Park, Ride.park_id == Park.park_id)
            .filter(
                and_(
                    RideHourlyStats.hour_start_utc == hour_start_utc,
                    RideHourlyStats.ride_operated == 1
                )
            )
            .group_by(Ride.park_id, Park.name)
            .subquery()
        )

        # Join with stored values
        results = (
            self.session.query(
                ride_sums.c.park_id,
                ride_sums.c.park_name,
                ride_sums.c.calc_total_downtime_hours,
                ride_sums.c.calc_rides_down,

                func.coalesce(ParkHourlyStats.total_downtime_hours, 0).label('stored_total_downtime_hours'),
                func.coalesce(ParkHourlyStats.rides_down, 0).label('stored_rides_down'),
                func.coalesce(ParkHourlyStats.shame_score, 0).label('stored_shame_score'),

                func.abs(func.coalesce(ParkHourlyStats.total_downtime_hours, 0) - ride_sums.c.calc_total_downtime_hours).label('downtime_delta'),
                case((ParkHourlyStats.park_id.is_(None), 1), else_=0).label('missing_from_aggregate')
            )
            .select_from(ride_sums)
            .outerjoin(
                ParkHourlyStats,
                and_(
                    ride_sums.c.park_id == ParkHourlyStats.park_id,
                    ParkHourlyStats.hour_start_utc == hour_start_utc
                )
            )
            .filter(
                or_(
                    func.abs(func.coalesce(ParkHourlyStats.total_downtime_hours, 0) - ride_sums.c.calc_total_downtime_hours) > tolerances['total_downtime_hours'],
                    ParkHourlyStats.park_id.is_(None)
                )
            )
            .order_by(func.abs(func.coalesce(ParkHourlyStats.total_downtime_hours, 0) - ride_sums.c.calc_total_downtime_hours).desc())
            .all()
        )

        # Convert to list of dicts
        mismatches = [
            {
                'park_id': row.park_id,
                'park_name': row.park_name,
                'calc_total_downtime_hours': float(row.calc_total_downtime_hours),
                'calc_rides_down': row.calc_rides_down,
                'stored_total_downtime_hours': float(row.stored_total_downtime_hours),
                'stored_rides_down': row.stored_rides_down,
                'stored_shame_score': float(row.stored_shame_score),
                'downtime_delta': float(row.downtime_delta),
                'missing_from_aggregate': row.missing_from_aggregate
            }
            for row in results
        ]

        # Count total records checked
        total_checked = (
            self.session.query(func.count(distinct(ParkHourlyStats.park_id)))
            .filter(ParkHourlyStats.hour_start_utc == hour_start_utc)
            .scalar() or 0
        )

        missing_count = sum(1 for m in mismatches if m['missing_from_aggregate'])
        match_count = total_checked - len(mismatches)
        match_rate = match_count / total_checked if total_checked > 0 else 1.0

        audit_result = AggregateAuditResult(
            table_name='park_hourly_stats',
            target_date=hour_start_utc.date(),
            total_records_checked=total_checked,
            records_matching=match_count,
            records_mismatched=len(mismatches) - missing_count,
            records_missing_from_aggregate=missing_count,
            records_missing_from_raw=0,
            match_rate=match_rate,
            max_deviation={
                'total_downtime_hours': max((m['downtime_delta'] for m in mismatches), default=0)
            },
            avg_deviation={},
            worst_mismatches=mismatches[:10]
        )

        if mismatches:
            audit_result.passed = False
            if len(mismatches) > 5 or missing_count > 2:
                audit_result.severity = "CRITICAL"
            else:
                audit_result.severity = "WARNING"
            audit_result.message = (
                f"park_hourly_stats {hour_start_utc}: {len(mismatches)} mismatches"
            )
        else:
            audit_result.message = (
                f"park_hourly_stats {hour_start_utc}: All {total_checked} records verified"
            )

        return audit_result

    def audit_hourly(self, target_date: date) -> AuditSummary:
        """
        Run hourly verification for all hours in a Pacific date.

        Args:
            target_date: Pacific date to verify

        Returns:
            AuditSummary with hourly verification results
        """
        summary = AuditSummary(
            audit_timestamp=datetime.utcnow(),
            target_date=target_date
        )

        day_start_utc, day_end_utc = get_pacific_day_range_utc(target_date)

        # Get all hours that have data
        hours = (
            self.session.query(distinct(ParkHourlyStats.hour_start_utc))
            .filter(
                and_(
                    ParkHourlyStats.hour_start_utc >= day_start_utc,
                    ParkHourlyStats.hour_start_utc < day_end_utc
                )
            )
            .order_by(ParkHourlyStats.hour_start_utc)
            .all()
        )

        # Verify each hour
        for (hour,) in hours:
            ride_result = self.verify_ride_hourly_stats(hour)
            summary.ride_hourly_results.append(ride_result)
            if not ride_result.passed:
                if ride_result.severity == "CRITICAL":
                    summary.critical_failures += 1
                else:
                    summary.warnings += 1
                summary.issues_found.append(ride_result.message)

            park_result = self.verify_park_hourly_stats(hour)
            summary.park_hourly_results.append(park_result)
            if not park_result.passed:
                if park_result.severity == "CRITICAL":
                    summary.critical_failures += 1
                else:
                    summary.warnings += 1
                summary.issues_found.append(park_result.message)

        # Run Disney DOWN status check
        summary.disney_down_check_result = self.verify_disney_down_status(target_date)
        if not summary.disney_down_check_result.passed:
            summary.critical_failures += 1
            summary.issues_found.append(summary.disney_down_check_result.message)

        # Run interval consistency check
        summary.interval_check_result = self.verify_interval_consistency(target_date)
        if not summary.interval_check_result.is_consistent:
            summary.critical_failures += 1
            summary.issues_found.append(summary.interval_check_result.message)

        # Set overall status
        summary.overall_passed = summary.critical_failures == 0

        # Add recommendations
        if summary.critical_failures > 0:
            summary.recommended_actions.append(
                "Re-run hourly aggregation for this date: "
                f"python -m scripts.aggregate_hourly --backfill --date {target_date}"
            )

        return summary

    def full_audit(self, target_date: date) -> AuditSummary:
        """
        Run complete verification including both daily and hourly stats.

        This is the comprehensive audit that should be run daily after aggregation.

        Args:
            target_date: Pacific date to verify

        Returns:
            AuditSummary with all verification results
        """
        # Start with daily audit
        summary = self.audit_date(target_date)

        # Add hourly verification
        hourly_summary = self.audit_hourly(target_date)

        # Merge hourly results into main summary
        summary.ride_hourly_results = hourly_summary.ride_hourly_results
        summary.park_hourly_results = hourly_summary.park_hourly_results
        summary.disney_down_check_result = hourly_summary.disney_down_check_result
        summary.interval_check_result = hourly_summary.interval_check_result

        # Update counts
        summary.critical_failures += hourly_summary.critical_failures
        summary.warnings += hourly_summary.warnings
        summary.issues_found.extend(hourly_summary.issues_found)
        summary.recommended_actions.extend(hourly_summary.recommended_actions)
        summary.overall_passed = summary.critical_failures == 0

        return summary
