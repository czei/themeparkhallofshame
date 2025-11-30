"""
Data Validation Rules (Lightweight Version)
============================================

Validation rules that query pre-aggregated daily stats tables.
These are fast because they don't touch raw snapshot tables.

Tables used:
- park_daily_stats: Pre-aggregated park-level stats
- ride_daily_stats: Pre-aggregated ride-level stats

Severity Levels:
- CRITICAL: Data cannot be published (impossible values)
- WARNING: Data flagged for review
- INFO: Logged for monitoring

Usage:
    from database.audit import ValidationChecker

    checker = ValidationChecker(conn)
    results = checker.run_all_checks(target_date)

Created: 2024-11 (Data Accuracy Audit Framework)
Updated: 2024-11 (Rewritten to use pre-aggregated tables)
"""

from datetime import date, timedelta
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from sqlalchemy import text
from sqlalchemy.engine import Connection

from utils.logger import logger


@dataclass
class ValidationResult:
    """Result of a single validation check."""

    check_name: str
    severity: str  # CRITICAL, WARNING, INFO
    passed: bool
    violations: int
    max_allowed: int
    message: str
    sample: List[Dict[str, Any]]  # First N violations for debugging


# =============================================================================
# VALIDATION RULES - Using pre-aggregated daily stats tables
# =============================================================================

VALIDATION_RULES: Dict[str, Dict[str, Any]] = {
    # =========================================================================
    # IMPOSSIBLE VALUES (ride_daily_stats)
    # =========================================================================
    "downtime_exceeds_24h": {
        "query": """
            SELECT rds.ride_id, r.name AS ride_name, p.name AS park_name,
                   rds.stat_date, ROUND(rds.downtime_minutes / 60.0, 2) AS downtime_hours
            FROM ride_daily_stats rds
            JOIN rides r ON rds.ride_id = r.ride_id
            JOIN parks p ON r.park_id = p.park_id
            WHERE rds.downtime_minutes > 1440
            AND rds.stat_date = :target_date
        """,
        "max_rows": 0,
        "severity": "CRITICAL",
        "message": "Ride has more than 24 hours of downtime in a single day",
    },
    "uptime_percentage_out_of_bounds": {
        "query": """
            SELECT rds.ride_id, r.name AS ride_name, p.name AS park_name,
                   rds.stat_date, rds.uptime_percentage
            FROM ride_daily_stats rds
            JOIN rides r ON rds.ride_id = r.ride_id
            JOIN parks p ON r.park_id = p.park_id
            WHERE (rds.uptime_percentage > 100 OR rds.uptime_percentage < 0)
            AND rds.stat_date = :target_date
        """,
        "max_rows": 0,
        "severity": "CRITICAL",
        "message": "Uptime percentage is outside valid range (0-100)",
    },
    "negative_downtime": {
        "query": """
            SELECT rds.ride_id, r.name AS ride_name, p.name AS park_name,
                   rds.stat_date, rds.downtime_minutes
            FROM ride_daily_stats rds
            JOIN rides r ON rds.ride_id = r.ride_id
            JOIN parks p ON r.park_id = p.park_id
            WHERE rds.downtime_minutes < 0
            AND rds.stat_date = :target_date
        """,
        "max_rows": 0,
        "severity": "CRITICAL",
        "message": "Negative downtime detected (calculation error)",
    },
    # =========================================================================
    # IMPOSSIBLE VALUES (park_daily_stats)
    # =========================================================================
    "shame_score_negative": {
        "query": """
            SELECT pds.park_id, p.name AS park_name, pds.stat_date, pds.shame_score
            FROM park_daily_stats pds
            JOIN parks p ON pds.park_id = p.park_id
            WHERE pds.shame_score < 0
            AND pds.stat_date = :target_date
        """,
        "max_rows": 0,
        "severity": "CRITICAL",
        "message": "Negative shame score detected (calculation error)",
    },
    "park_uptime_out_of_bounds": {
        "query": """
            SELECT pds.park_id, p.name AS park_name, pds.stat_date, pds.avg_uptime_percentage
            FROM park_daily_stats pds
            JOIN parks p ON pds.park_id = p.park_id
            WHERE (pds.avg_uptime_percentage > 100 OR pds.avg_uptime_percentage < 0)
            AND pds.stat_date = :target_date
        """,
        "max_rows": 0,
        "severity": "CRITICAL",
        "message": "Park avg uptime percentage is outside valid range (0-100)",
    },
    # =========================================================================
    # DATA COMPLETENESS
    # =========================================================================
    "no_ride_data_for_date": {
        "query": """
            SELECT p.park_id, p.name AS park_name, :target_date AS stat_date
            FROM parks p
            WHERE p.is_active = 1
            AND NOT EXISTS (
                SELECT 1 FROM ride_daily_stats rds
                JOIN rides r ON rds.ride_id = r.ride_id
                WHERE r.park_id = p.park_id
                AND rds.stat_date = :target_date
            )
        """,
        "max_rows": 50,  # Allow some parks to have no data (closed parks)
        "severity": "WARNING",
        "message": "Active park has no ride data for this date",
    },
    "rides_with_zero_operating_time": {
        "query": """
            SELECT rds.ride_id, r.name AS ride_name, p.name AS park_name,
                   rds.stat_date, rds.operating_hours_minutes
            FROM ride_daily_stats rds
            JOIN rides r ON rds.ride_id = r.ride_id
            JOIN parks p ON r.park_id = p.park_id
            WHERE rds.operating_hours_minutes = 0
            AND rds.uptime_minutes = 0
            AND rds.downtime_minutes = 0
            AND rds.stat_date = :target_date
        """,
        "max_rows": 100,  # Many rides may be closed
        "severity": "INFO",
        "message": "Ride has zero operating time (may be closed)",
    },
    # =========================================================================
    # LOGICAL CONSISTENCY
    # =========================================================================
    "downtime_exceeds_operating_hours": {
        "query": """
            SELECT rds.ride_id, r.name AS ride_name, p.name AS park_name,
                   rds.stat_date, rds.downtime_minutes, rds.operating_hours_minutes
            FROM ride_daily_stats rds
            JOIN rides r ON rds.ride_id = r.ride_id
            JOIN parks p ON r.park_id = p.park_id
            WHERE rds.downtime_minutes > rds.operating_hours_minutes
            AND rds.operating_hours_minutes > 0
            AND rds.stat_date = :target_date
        """,
        "max_rows": 0,
        "severity": "CRITICAL",
        "message": "Downtime exceeds operating hours (impossible)",
    },
    "rides_with_downtime_exceeds_total": {
        "query": """
            SELECT pds.park_id, p.name AS park_name, pds.stat_date,
                   pds.rides_with_downtime, pds.total_rides_tracked
            FROM park_daily_stats pds
            JOIN parks p ON pds.park_id = p.park_id
            WHERE pds.rides_with_downtime > pds.total_rides_tracked
            AND pds.stat_date = :target_date
        """,
        "max_rows": 0,
        "severity": "CRITICAL",
        "message": "Rides with downtime exceeds total rides tracked",
    },
    # =========================================================================
    # CROSS-TABLE CONSISTENCY
    # =========================================================================
    "weekly_vs_daily_sum_mismatch": {
        "query": """
            SELECT
                pws.park_id,
                p.name AS park_name,
                pws.year,
                pws.week_number,
                pws.total_downtime_hours AS weekly_total,
                ROUND(SUM(pds.total_downtime_hours), 2) AS daily_sum,
                ABS(pws.total_downtime_hours - SUM(pds.total_downtime_hours)) AS diff
            FROM park_weekly_stats pws
            JOIN parks p ON pws.park_id = p.park_id
            JOIN park_daily_stats pds
                ON pws.park_id = pds.park_id
                AND YEARWEEK(pds.stat_date, 1) = YEARWEEK(:target_date, 1)
            WHERE pws.year = YEAR(:target_date)
            AND pws.week_number = WEEK(:target_date, 1)
            GROUP BY pws.park_id, p.name, pws.year, pws.week_number, pws.total_downtime_hours
            HAVING ABS(pws.total_downtime_hours - SUM(pds.total_downtime_hours)) > 0.5
        """,
        "max_rows": 0,
        "severity": "WARNING",
        "message": "Weekly total doesn't match sum of daily stats",
    },
}


class ValidationChecker:
    """
    Runs validation checks against pre-aggregated daily stats tables.

    Usage:
        checker = ValidationChecker(conn)
        results = checker.run_all_checks(date.today() - timedelta(days=1))
    """

    def __init__(self, conn: Connection):
        self.conn = conn

    def run_check(
        self, check_name: str, target_date: date, sample_limit: int = 5
    ) -> ValidationResult:
        """Run a single validation check."""
        if check_name not in VALIDATION_RULES:
            raise ValueError(f"Unknown validation check: {check_name}")

        rule = VALIDATION_RULES[check_name]

        try:
            result = self.conn.execute(
                text(rule["query"]), {"target_date": target_date}
            )
            rows = result.fetchall()

            sample = []
            for row in rows[:sample_limit]:
                sample.append(dict(row._mapping))

            passed = len(rows) <= rule["max_rows"]

            return ValidationResult(
                check_name=check_name,
                severity=rule["severity"],
                passed=passed,
                violations=len(rows),
                max_allowed=rule["max_rows"],
                message=rule["message"],
                sample=sample,
            )

        except Exception as e:
            logger.error(f"Validation check '{check_name}' failed with error: {e}")
            return ValidationResult(
                check_name=check_name,
                severity="CRITICAL",
                passed=False,
                violations=0,
                max_allowed=rule["max_rows"],
                message=f"Check failed with error: {str(e)}",
                sample=[],
            )

    def run_all_checks(
        self, target_date: date, sample_limit: int = 5
    ) -> List[ValidationResult]:
        """Run all validation checks for a date."""
        results = []
        for check_name in VALIDATION_RULES:
            result = self.run_check(check_name, target_date, sample_limit)
            results.append(result)

        return results

    def run_critical_checks(
        self, target_date: date, sample_limit: int = 5
    ) -> List[ValidationResult]:
        """Run only CRITICAL severity checks."""
        critical_checks = [
            name for name, rule in VALIDATION_RULES.items() if rule["severity"] == "CRITICAL"
        ]
        results = []
        for check_name in critical_checks:
            results.append(self.run_check(check_name, target_date, sample_limit))
        return results

    def get_failed_checks(
        self, results: List[ValidationResult]
    ) -> List[ValidationResult]:
        """Filter to only failed checks."""
        return [r for r in results if not r.passed]

    def get_critical_failures(
        self, results: List[ValidationResult]
    ) -> List[ValidationResult]:
        """Filter to only CRITICAL failures."""
        return [r for r in results if not r.passed and r.severity == "CRITICAL"]

    def to_dict(self, results: List[ValidationResult]) -> List[Dict[str, Any]]:
        """Convert results to JSON-serializable dicts."""
        return [
            {
                "check_name": r.check_name,
                "severity": r.severity,
                "passed": r.passed,
                "violations": r.violations,
                "max_allowed": r.max_allowed,
                "message": r.message,
                "sample": r.sample,
            }
            for r in results
        ]


def run_hourly_audit(conn: Connection, target_date: Optional[date] = None) -> Dict[str, Any]:
    """
    Run audit after data updates.

    Args:
        conn: Database connection
        target_date: Date to audit (default: yesterday)

    Returns:
        Audit summary with pass/fail status and any failures
    """
    if target_date is None:
        target_date = date.today() - timedelta(days=1)

    checker = ValidationChecker(conn)
    results = checker.run_all_checks(target_date)

    critical_failures = checker.get_critical_failures(results)
    all_failures = checker.get_failed_checks(results)

    summary = {
        "target_date": target_date.isoformat(),
        "total_checks": len(results),
        "passed": len(results) - len(all_failures),
        "failed": len(all_failures),
        "critical_failures": len(critical_failures),
        "status": "FAIL" if critical_failures else "WARN" if all_failures else "PASS",
        "failures": checker.to_dict(all_failures),
    }

    return summary
