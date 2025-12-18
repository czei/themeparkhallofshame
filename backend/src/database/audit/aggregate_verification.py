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
class AuditSummary:
    """Overall audit summary across all tables."""

    audit_timestamp: datetime
    target_date: date

    # Per-table results
    ride_daily_result: Optional[AggregateAuditResult] = None
    park_daily_result: Optional[AggregateAuditResult] = None
    ride_hourly_results: List[AggregateAuditResult] = field(default_factory=list)
    park_hourly_results: List[AggregateAuditResult] = field(default_factory=list)

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

        return "\n".join(lines)
