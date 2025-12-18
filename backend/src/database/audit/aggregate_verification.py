"""
Aggregate Data Verification System
==================================

Verifies that aggregate table values match raw snapshot calculations.
This catches bugs like timezone issues or incorrect interval multipliers.

Usage:
    verifier = AggregateVerifier(conn)
    summary = verifier.audit_date(date(2025, 12, 17))

    if not summary.overall_passed:
        print(f"Verification failed: {summary.issues_found}")

Verification Process:
1. Calculate expected values from raw snapshots using correct Pacific timezone
2. Compare against stored values in aggregate tables
3. Flag any discrepancies above tolerance thresholds
"""

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import List, Dict, Any, Optional

from sqlalchemy import text
from sqlalchemy.engine import Connection

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

    def __init__(self, conn: Connection):
        self.conn = conn
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

        # Query that recalculates from raw snapshots with correct timezone
        sql = text("""
            WITH rides_operated_today AS (
                -- Rides that operated at least once during the Pacific day
                SELECT DISTINCT rss.ride_id
                FROM ride_status_snapshots rss
                JOIN rides r ON rss.ride_id = r.ride_id
                JOIN park_activity_snapshots pas ON r.park_id = pas.park_id
                    AND DATE_FORMAT(pas.recorded_at, '%Y-%m-%d %H:%i') = DATE_FORMAT(rss.recorded_at, '%Y-%m-%d %H:%i')
                WHERE rss.recorded_at >= :day_start_utc
                  AND rss.recorded_at < :day_end_utc
                  AND pas.park_appears_open = TRUE
                  AND (rss.status = 'OPERATING' OR (rss.status IS NULL AND rss.computed_is_open = TRUE))
            ),
            raw_calculation AS (
                SELECT
                    r.ride_id,
                    r.name AS ride_name,
                    p.name AS park_name,

                    -- Uptime minutes (when ride was open and park was open)
                    COALESCE(SUM(CASE
                        WHEN pas.park_appears_open = 1 AND rss.computed_is_open = TRUE
                        THEN :snapshot_interval
                        ELSE 0
                    END), 0) AS calc_uptime_minutes,

                    -- Downtime minutes (using same logic as aggregate_daily.py)
                    CASE
                        WHEN r.ride_id IN (SELECT ride_id FROM rides_operated_today)
                        THEN COALESCE(SUM(CASE
                            WHEN pas.park_appears_open = 1 AND (
                                (rss.status IS NOT NULL AND rss.status = 'DOWN') OR
                                (rss.status IS NULL AND NOT rss.computed_is_open)
                            )
                            THEN :snapshot_interval
                            ELSE 0
                        END), 0)
                        ELSE 0
                    END AS calc_downtime_minutes,

                    -- Operating hours minutes (park open time)
                    COALESCE(SUM(CASE
                        WHEN pas.park_appears_open = 1
                        THEN :snapshot_interval
                        ELSE 0
                    END), 0) AS calc_operating_hours_minutes

                FROM ride_status_snapshots rss
                JOIN rides r ON rss.ride_id = r.ride_id
                JOIN parks p ON r.park_id = p.park_id
                JOIN park_activity_snapshots pas ON r.park_id = pas.park_id
                    AND pas.recorded_at = rss.recorded_at
                WHERE rss.recorded_at >= :day_start_utc
                  AND rss.recorded_at < :day_end_utc
                  AND r.is_active = TRUE
                  AND r.category = 'ATTRACTION'
                GROUP BY r.ride_id, r.name, p.name
            )
            SELECT
                rc.ride_id,
                rc.ride_name,
                rc.park_name,

                -- Stored values
                COALESCE(rds.uptime_minutes, 0) AS stored_uptime_minutes,
                COALESCE(rds.downtime_minutes, 0) AS stored_downtime_minutes,
                COALESCE(rds.operating_hours_minutes, 0) AS stored_operating_hours_minutes,

                -- Calculated values
                rc.calc_uptime_minutes,
                rc.calc_downtime_minutes,
                rc.calc_operating_hours_minutes,

                -- Deltas
                ABS(COALESCE(rds.uptime_minutes, 0) - rc.calc_uptime_minutes) AS uptime_delta,
                ABS(COALESCE(rds.downtime_minutes, 0) - rc.calc_downtime_minutes) AS downtime_delta,
                ABS(COALESCE(rds.operating_hours_minutes, 0) - rc.calc_operating_hours_minutes) AS operating_hours_delta,

                -- Is aggregate missing?
                CASE WHEN rds.ride_id IS NULL THEN 1 ELSE 0 END AS missing_from_aggregate

            FROM raw_calculation rc
            LEFT JOIN ride_daily_stats rds
                ON rc.ride_id = rds.ride_id
                AND rds.stat_date = :stat_date
            WHERE rc.calc_uptime_minutes > 0 OR rc.calc_downtime_minutes > 0
            ORDER BY
                CASE WHEN rds.ride_id IS NULL THEN 1 ELSE 0 END DESC,
                GREATEST(
                    ABS(COALESCE(rds.uptime_minutes, 0) - rc.calc_uptime_minutes),
                    ABS(COALESCE(rds.downtime_minutes, 0) - rc.calc_downtime_minutes)
                ) DESC
        """)

        result = self.conn.execute(sql, {
            'day_start_utc': day_start_utc,
            'day_end_utc': day_end_utc,
            'stat_date': target_date,
            'snapshot_interval': self.snapshot_interval
        })

        rows = [dict(row._mapping) for row in result]

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

        # Query that calculates park stats from ride_daily_stats
        sql = text("""
            WITH park_raw_calc AS (
                SELECT
                    p.park_id,
                    p.name AS park_name,
                    COUNT(DISTINCT rds.ride_id) AS calc_total_rides,
                    ROUND(SUM(rds.downtime_minutes) / 60.0, 2) AS calc_total_downtime_hours,
                    SUM(CASE WHEN rds.downtime_minutes > 0 THEN 1 ELSE 0 END) AS calc_rides_with_downtime,
                    ROUND(AVG(rds.uptime_percentage), 2) AS calc_avg_uptime
                FROM parks p
                JOIN ride_daily_stats rds ON p.park_id = (
                    SELECT r.park_id FROM rides r WHERE r.ride_id = rds.ride_id
                )
                WHERE rds.stat_date = :stat_date
                  AND p.is_active = TRUE
                GROUP BY p.park_id, p.name
            )
            SELECT
                prc.park_id,
                prc.park_name,

                -- Stored values
                COALESCE(pds.total_rides_tracked, 0) AS stored_total_rides,
                COALESCE(pds.total_downtime_hours, 0) AS stored_total_downtime_hours,
                COALESCE(pds.rides_with_downtime, 0) AS stored_rides_with_downtime,
                COALESCE(pds.shame_score, 0) AS stored_shame_score,

                -- Calculated values
                prc.calc_total_rides,
                prc.calc_total_downtime_hours,
                prc.calc_rides_with_downtime,

                -- Deltas
                ABS(COALESCE(pds.total_downtime_hours, 0) - prc.calc_total_downtime_hours) AS downtime_hours_delta,
                ABS(COALESCE(pds.rides_with_downtime, 0) - prc.calc_rides_with_downtime) AS rides_with_downtime_delta,

                -- Is aggregate missing?
                CASE WHEN pds.park_id IS NULL THEN 1 ELSE 0 END AS missing_from_aggregate

            FROM park_raw_calc prc
            LEFT JOIN park_daily_stats pds
                ON prc.park_id = pds.park_id
                AND pds.stat_date = :stat_date
            WHERE prc.calc_total_rides > 0
            ORDER BY
                CASE WHEN pds.park_id IS NULL THEN 1 ELSE 0 END DESC,
                ABS(COALESCE(pds.total_downtime_hours, 0) - prc.calc_total_downtime_hours) DESC
        """)

        result = self.conn.execute(sql, {'stat_date': target_date})
        rows = [dict(row._mapping) for row in result]

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
        # and check if they're marked as ride_operated in hourly stats
        sql = text("""
            WITH disney_down_rides AS (
                -- All Disney/Universal rides with DOWN status during this day
                SELECT DISTINCT
                    rss.ride_id,
                    r.name AS ride_name,
                    p.park_id,
                    p.name AS park_name,
                    DATE_FORMAT(rss.recorded_at, '%%Y-%%m-%%d %%H:00:00') AS hour_start
                FROM ride_status_snapshots rss
                JOIN rides r ON rss.ride_id = r.ride_id
                JOIN parks p ON r.park_id = p.park_id
                JOIN park_activity_snapshots pas ON p.park_id = pas.park_id
                    AND pas.recorded_at = rss.recorded_at
                WHERE rss.recorded_at >= :day_start
                  AND rss.recorded_at < :day_end
                  AND rss.status = 'DOWN'
                  AND pas.park_appears_open = TRUE
                  AND (p.is_disney = TRUE OR p.is_universal = TRUE)
            ),
            hourly_ride_status AS (
                -- Check ride_hourly_stats for these ride/hour combos
                SELECT
                    ddr.ride_id,
                    ddr.ride_name,
                    ddr.park_name,
                    ddr.hour_start,
                    rhs.ride_operated,
                    rhs.downtime_hours,
                    rhs.down_snapshots
                FROM disney_down_rides ddr
                LEFT JOIN ride_hourly_stats rhs
                    ON ddr.ride_id = rhs.ride_id
                    AND rhs.hour_start_utc = ddr.hour_start
            )
            SELECT
                ride_id,
                ride_name,
                park_name,
                hour_start,
                COALESCE(ride_operated, 0) AS ride_operated,
                COALESCE(downtime_hours, 0) AS stored_downtime_hours,
                COALESCE(down_snapshots, 0) AS stored_down_snapshots,
                CASE
                    WHEN ride_operated = 0 OR ride_operated IS NULL THEN 'excluded'
                    WHEN downtime_hours = 0 OR downtime_hours IS NULL THEN 'zero_downtime'
                    ELSE 'ok'
                END AS status
            FROM hourly_ride_status
            WHERE ride_operated = 0 OR ride_operated IS NULL
               OR downtime_hours = 0 OR downtime_hours IS NULL
            ORDER BY park_name, ride_name, hour_start
        """)

        result = self.conn.execute(sql, {
            'day_start': day_start_utc,
            'day_end': day_end_utc
        })
        rows = [dict(row._mapping) for row in result]

        # Count parks and rides checked
        parks_count_sql = text("""
            SELECT COUNT(DISTINCT p.park_id) as cnt
            FROM parks p
            WHERE p.is_disney = TRUE OR p.is_universal = TRUE
        """)
        parks_checked = self.conn.execute(parks_count_sql).scalar() or 0

        rides_count_sql = text("""
            SELECT COUNT(DISTINCT rss.ride_id) as cnt
            FROM ride_status_snapshots rss
            JOIN rides r ON rss.ride_id = r.ride_id
            JOIN parks p ON r.park_id = p.park_id
            WHERE rss.recorded_at >= :day_start
              AND rss.recorded_at < :day_end
              AND rss.status = 'DOWN'
              AND (p.is_disney = TRUE OR p.is_universal = TRUE)
        """)
        rides_with_down = self.conn.execute(rides_count_sql, {
            'day_start': day_start_utc,
            'day_end': day_end_utc
        }).scalar() or 0

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

        # Calculate average time between consecutive snapshots
        sql = text("""
            WITH snapshot_times AS (
                SELECT
                    recorded_at,
                    LAG(recorded_at) OVER (ORDER BY recorded_at) AS prev_time
                FROM (
                    SELECT DISTINCT recorded_at
                    FROM ride_status_snapshots
                    WHERE recorded_at >= :day_start
                      AND recorded_at < :day_end
                ) distinct_times
            )
            SELECT
                AVG(TIMESTAMPDIFF(SECOND, prev_time, recorded_at)) / 60.0 AS avg_interval_minutes,
                MIN(TIMESTAMPDIFF(SECOND, prev_time, recorded_at)) / 60.0 AS min_interval_minutes,
                MAX(TIMESTAMPDIFF(SECOND, prev_time, recorded_at)) / 60.0 AS max_interval_minutes,
                COUNT(*) AS sample_count
            FROM snapshot_times
            WHERE prev_time IS NOT NULL
        """)

        result = self.conn.execute(sql, {
            'day_start': day_start_utc,
            'day_end': day_end_utc
        }).fetchone()

        if not result or result[0] is None:
            return IntervalConsistencyResult(
                expected_interval=self.snapshot_interval,
                calculated_interval=0.0,
                is_consistent=True,
                message="No snapshot data to verify interval"
            )

        avg_interval = float(result[0])
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
        hour_end_utc = hour_start_utc.replace(minute=0, second=0, microsecond=0)
        from datetime import timedelta
        hour_end_utc = hour_start_utc + timedelta(hours=1)
        tolerances = self.TOLERANCES['ride_hourly']
        interval = self.snapshot_interval

        # Calculate from raw snapshots and compare to stored
        sql = text(f"""
            WITH raw_calc AS (
                SELECT
                    rss.ride_id,
                    r.name AS ride_name,
                    p.name AS park_name,
                    p.is_disney,
                    p.is_universal,
                    COUNT(*) AS total_snapshots,
                    SUM(CASE
                        WHEN pas.park_appears_open = TRUE AND (
                            rss.status = 'DOWN'
                            OR (rss.status = 'CLOSED'
                                AND p.is_disney = FALSE
                                AND p.is_universal = FALSE)
                        )
                        THEN 1 ELSE 0
                    END) AS down_count,
                    SUM(CASE
                        WHEN rss.status = 'OPERATING'
                            OR rss.computed_is_open = TRUE
                        THEN 1 ELSE 0
                    END) AS operating_count,
                    -- Expected downtime hours
                    ROUND(SUM(CASE
                        WHEN pas.park_appears_open = TRUE AND (
                            rss.status = 'DOWN'
                            OR (rss.status = 'CLOSED'
                                AND p.is_disney = FALSE
                                AND p.is_universal = FALSE)
                        )
                        THEN {interval} / 60.0 ELSE 0
                    END), 2) AS calc_downtime_hours
                FROM ride_status_snapshots rss
                JOIN rides r ON rss.ride_id = r.ride_id
                JOIN parks p ON r.park_id = p.park_id
                JOIN park_activity_snapshots pas
                    ON p.park_id = pas.park_id
                    AND pas.recorded_at = rss.recorded_at
                WHERE rss.recorded_at >= :hour_start
                  AND rss.recorded_at < :hour_end
                GROUP BY rss.ride_id, r.name, p.name, p.is_disney, p.is_universal
                HAVING down_count > 0 OR operating_count > 0
            )
            SELECT
                rc.ride_id,
                rc.ride_name,
                rc.park_name,
                rc.down_count AS calc_down_snapshots,
                rc.calc_downtime_hours,

                COALESCE(rhs.down_snapshots, 0) AS stored_down_snapshots,
                COALESCE(rhs.downtime_hours, 0) AS stored_downtime_hours,
                COALESCE(rhs.ride_operated, 0) AS ride_operated,

                ABS(COALESCE(rhs.downtime_hours, 0) - rc.calc_downtime_hours) AS downtime_delta,
                CASE WHEN rhs.ride_id IS NULL THEN 1 ELSE 0 END AS missing_from_aggregate

            FROM raw_calc rc
            LEFT JOIN ride_hourly_stats rhs
                ON rc.ride_id = rhs.ride_id
                AND rhs.hour_start_utc = :hour_start
            WHERE ABS(COALESCE(rhs.downtime_hours, 0) - rc.calc_downtime_hours) > :tolerance
               OR rhs.ride_id IS NULL
            ORDER BY ABS(COALESCE(rhs.downtime_hours, 0) - rc.calc_downtime_hours) DESC
            LIMIT 20
        """)

        result = self.conn.execute(sql, {
            'hour_start': hour_start_utc,
            'hour_end': hour_end_utc,
            'tolerance': tolerances['downtime_hours']
        })
        mismatches = [dict(row._mapping) for row in result]

        # Count total records checked
        count_sql = text("""
            SELECT COUNT(DISTINCT ride_id)
            FROM ride_hourly_stats
            WHERE hour_start_utc = :hour_start
        """)
        total_checked = self.conn.execute(count_sql, {'hour_start': hour_start_utc}).scalar() or 0

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

        # Calculate from ride_hourly_stats and compare to stored park_hourly_stats
        sql = text("""
            WITH ride_sums AS (
                SELECT
                    r.park_id,
                    p.name AS park_name,
                    SUM(rhs.downtime_hours) AS calc_total_downtime_hours,
                    SUM(CASE WHEN rhs.downtime_hours > 0 THEN 1 ELSE 0 END) AS calc_rides_down
                FROM ride_hourly_stats rhs
                JOIN rides r ON rhs.ride_id = r.ride_id
                JOIN parks p ON r.park_id = p.park_id
                WHERE rhs.hour_start_utc = :hour_start
                  AND rhs.ride_operated = 1
                GROUP BY r.park_id, p.name
            )
            SELECT
                rs.park_id,
                rs.park_name,
                rs.calc_total_downtime_hours,
                rs.calc_rides_down,

                COALESCE(phs.total_downtime_hours, 0) AS stored_total_downtime_hours,
                COALESCE(phs.rides_down, 0) AS stored_rides_down,
                COALESCE(phs.shame_score, 0) AS stored_shame_score,

                ABS(COALESCE(phs.total_downtime_hours, 0) - rs.calc_total_downtime_hours) AS downtime_delta,
                CASE WHEN phs.park_id IS NULL THEN 1 ELSE 0 END AS missing_from_aggregate

            FROM ride_sums rs
            LEFT JOIN park_hourly_stats phs
                ON rs.park_id = phs.park_id
                AND phs.hour_start_utc = :hour_start
            WHERE ABS(COALESCE(phs.total_downtime_hours, 0) - rs.calc_total_downtime_hours) > :tolerance
               OR phs.park_id IS NULL
            ORDER BY ABS(COALESCE(phs.total_downtime_hours, 0) - rs.calc_total_downtime_hours) DESC
        """)

        result = self.conn.execute(sql, {
            'hour_start': hour_start_utc,
            'tolerance': tolerances['total_downtime_hours']
        })
        mismatches = [dict(row._mapping) for row in result]

        # Count total records checked
        count_sql = text("""
            SELECT COUNT(DISTINCT park_id)
            FROM park_hourly_stats
            WHERE hour_start_utc = :hour_start
        """)
        total_checked = self.conn.execute(count_sql, {'hour_start': hour_start_utc}).scalar() or 0

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
        hours_sql = text("""
            SELECT DISTINCT hour_start_utc
            FROM park_hourly_stats
            WHERE hour_start_utc >= :day_start
              AND hour_start_utc < :day_end
            ORDER BY hour_start_utc
        """)
        result = self.conn.execute(hours_sql, {
            'day_start': day_start_utc,
            'day_end': day_end_utc
        })
        hours = [row[0] for row in result]

        # Verify each hour
        for hour in hours:
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
