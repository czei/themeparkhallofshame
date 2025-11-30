"""
Data Validation Rules
=====================

Hard validation rules that detect data quality issues.
These run after aggregation to catch impossible values and inconsistencies.

Severity Levels:
- CRITICAL: Pipeline should halt; data cannot be published
- WARNING: Data flagged for review; published with annotation
- INFO: Logged for monitoring; no user-visible impact

Usage:
    from database.audit import ValidationChecker

    checker = ValidationChecker(conn)
    results = checker.run_all_checks(target_date)

    critical = [r for r in results if r['severity'] == 'CRITICAL']
    if critical:
        # Don't publish, alert admin
        send_alert(critical)

How to Add Rules:
1. Add rule definition to VALIDATION_RULES dict
2. Rule query should return rows that VIOLATE the rule
3. max_rows = 0 means any violation is a failure
4. Add tests in tests/unit/test_validation_checks.py

Created: 2024-11 (Data Accuracy Audit Framework)
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
# VALIDATION RULES
# =============================================================================
# Each rule is a SQL query that returns rows VIOLATING the rule.
# If len(results) > max_rows, the check fails.
# =============================================================================

VALIDATION_RULES: Dict[str, Dict[str, Any]] = {
    # =========================================================================
    # IMPOSSIBLE VALUES
    # =========================================================================
    "downtime_exceeds_24h": {
        "query": """
            SELECT ride_id, ride_name, park_name, stat_date, downtime_hours
            FROM v_audit_ride_daily
            WHERE downtime_hours > 24
            AND stat_date = :target_date
        """,
        "max_rows": 0,
        "severity": "CRITICAL",
        "message": "Ride has more than 24 hours of downtime in a single day",
    },
    "uptime_percentage_out_of_bounds": {
        "query": """
            SELECT ride_id, ride_name, park_name, stat_date, uptime_percentage
            FROM v_audit_ride_daily
            WHERE (uptime_percentage > 100 OR uptime_percentage < 0)
            AND stat_date = :target_date
        """,
        "max_rows": 0,
        "severity": "CRITICAL",
        "message": "Uptime percentage is outside valid range (0-100)",
    },
    "negative_downtime": {
        "query": """
            SELECT ride_id, ride_name, park_name, stat_date, downtime_hours
            FROM v_audit_ride_daily
            WHERE downtime_hours < 0
            AND stat_date = :target_date
        """,
        "max_rows": 0,
        "severity": "CRITICAL",
        "message": "Negative downtime detected (calculation error)",
    },
    "shame_score_negative": {
        "query": """
            SELECT park_id, park_name, stat_date, shame_score
            FROM v_audit_park_daily
            WHERE shame_score < 0
            AND stat_date = :target_date
        """,
        "max_rows": 0,
        "severity": "CRITICAL",
        "message": "Negative shame score detected (calculation error)",
    },
    # =========================================================================
    # DATA COMPLETENESS
    # =========================================================================
    "insufficient_snapshots": {
        "query": """
            SELECT ride_id, ride_name, park_name, stat_date, total_snapshots
            FROM v_audit_ride_daily
            WHERE total_snapshots < 12
            AND stat_date = :target_date
        """,
        "max_rows": 20,  # Allow some rides with missing data
        "severity": "WARNING",
        "message": "Ride has less than 1 hour of snapshot data (< 12 snapshots)",
    },
    "low_park_coverage": {
        "query": """
            SELECT park_id, park_name, stat_date, total_rides,
                   total_park_open_snapshots,
                   ROUND(100.0 * total_park_open_snapshots / NULLIF(total_rides * 288, 0), 1) AS coverage_pct
            FROM v_audit_park_daily
            WHERE total_park_open_snapshots < total_rides * 144
            AND stat_date = :target_date
        """,
        "max_rows": 5,  # Allow a few parks with low coverage
        "severity": "WARNING",
        "message": "Park has less than 50% expected snapshot coverage",
    },
    # =========================================================================
    # CROSS-TABLE CONSISTENCY
    # =========================================================================
    "daily_vs_aggregated_mismatch": {
        "query": """
            SELECT
                v.ride_id,
                v.ride_name,
                v.stat_date,
                v.downtime_hours AS view_downtime_hours,
                ROUND(rds.downtime_minutes / 60.0, 2) AS table_downtime_hours,
                ABS(v.downtime_hours - ROUND(rds.downtime_minutes / 60.0, 2)) AS diff_hours
            FROM v_audit_ride_daily v
            JOIN ride_daily_stats rds
                ON v.ride_id = rds.ride_id
                AND v.stat_date = rds.stat_date
            WHERE ABS(v.downtime_hours - ROUND(rds.downtime_minutes / 60.0, 2)) > 0.1
            AND v.stat_date = :target_date
        """,
        "max_rows": 0,
        "severity": "CRITICAL",
        "message": "Audit view calculation doesn't match ride_daily_stats table",
    },
    "weekly_vs_daily_sum_mismatch": {
        "query": """
            SELECT
                pws.park_id,
                p.name AS park_name,
                pws.year,
                pws.week_number,
                pws.total_downtime_hours AS weekly_total,
                ROUND(SUM(pds.total_downtime_hours), 2) AS daily_sum,
                ABS(pws.total_downtime_hours - ROUND(SUM(pds.total_downtime_hours), 2)) AS diff
            FROM park_weekly_stats pws
            JOIN parks p ON pws.park_id = p.park_id
            JOIN park_daily_stats pds
                ON pws.park_id = pds.park_id
                AND YEARWEEK(pds.stat_date, 1) = YEARWEEK(:target_date, 1)
            WHERE pws.year = YEAR(:target_date)
            AND pws.week_number = WEEK(:target_date, 1)
            GROUP BY pws.park_id, p.name, pws.year, pws.week_number, pws.total_downtime_hours
            HAVING ABS(pws.total_downtime_hours - ROUND(SUM(pds.total_downtime_hours), 2)) > 0.5
        """,
        "max_rows": 0,
        "severity": "CRITICAL",
        "message": "Weekly total doesn't match sum of daily stats",
    },
    # =========================================================================
    # TIER WEIGHT CONSISTENCY
    # =========================================================================
    "invalid_tier_weight": {
        "query": """
            SELECT ride_id, ride_name, park_name, stat_date, tier, tier_weight
            FROM v_audit_ride_daily
            WHERE tier_weight NOT IN (1, 2, 3)
            AND stat_date = :target_date
        """,
        "max_rows": 0,
        "severity": "CRITICAL",
        "message": "Invalid tier weight (must be 1, 2, or 3)",
    },
    "tier_weight_mismatch": {
        "query": """
            SELECT
                v.ride_id,
                v.ride_name,
                v.tier AS view_tier,
                v.tier_weight AS view_weight,
                rc.tier AS rc_tier,
                rc.tier_weight AS rc_weight
            FROM v_audit_ride_daily v
            JOIN ride_classifications rc ON v.ride_id = rc.ride_id
            WHERE v.tier_weight != rc.tier_weight
            AND v.stat_date = :target_date
        """,
        "max_rows": 0,
        "severity": "WARNING",
        "message": "Tier weight in view doesn't match ride_classifications table",
    },
    # =========================================================================
    # LOGICAL CONSISTENCY
    # =========================================================================
    "more_down_than_open": {
        "query": """
            SELECT ride_id, ride_name, park_name, stat_date,
                   down_snapshots, park_open_snapshots
            FROM v_audit_ride_daily
            WHERE down_snapshots > park_open_snapshots
            AND stat_date = :target_date
        """,
        "max_rows": 0,
        "severity": "CRITICAL",
        "message": "Down snapshots exceeds park open snapshots (impossible)",
    },
    "snapshot_sum_mismatch": {
        "query": """
            SELECT ride_id, ride_name, park_name, stat_date,
                   operating_snapshots, down_snapshots, closed_snapshots, refurbishment_snapshots,
                   park_open_snapshots,
                   (operating_snapshots + down_snapshots + closed_snapshots + refurbishment_snapshots) AS sum_statuses
            FROM v_audit_ride_daily
            WHERE (operating_snapshots + down_snapshots + closed_snapshots + refurbishment_snapshots) != park_open_snapshots
            AND park_open_snapshots > 0
            AND stat_date = :target_date
        """,
        "max_rows": 10,  # Allow some discrepancy due to timing
        "severity": "WARNING",
        "message": "Sum of status snapshots doesn't equal park_open_snapshots",
    },
}


class ValidationChecker:
    """
    Runs validation checks against audit views.

    Usage:
        checker = ValidationChecker(conn)
        results = checker.run_all_checks(date.today() - timedelta(days=1))

        for result in results:
            if result.severity == 'CRITICAL' and not result.passed:
                alert_admin(result)
    """

    def __init__(self, conn: Connection):
        """
        Initialize with database connection.

        Args:
            conn: SQLAlchemy connection
        """
        self.conn = conn

    def run_check(
        self, check_name: str, target_date: date, sample_limit: int = 5
    ) -> ValidationResult:
        """
        Run a single validation check.

        Args:
            check_name: Name of check from VALIDATION_RULES
            target_date: Date to validate
            sample_limit: Max violations to include in sample

        Returns:
            ValidationResult with pass/fail status and details
        """
        if check_name not in VALIDATION_RULES:
            raise ValueError(f"Unknown validation check: {check_name}")

        rule = VALIDATION_RULES[check_name]

        try:
            result = self.conn.execute(
                text(rule["query"]), {"target_date": target_date}
            )
            rows = result.fetchall()

            # Convert rows to dicts for sample
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
        """
        Run all validation checks for a date.

        Args:
            target_date: Date to validate
            sample_limit: Max violations per check to include in sample

        Returns:
            List of ValidationResult objects
        """
        results = []
        for check_name in VALIDATION_RULES:
            result = self.run_check(check_name, target_date, sample_limit)
            results.append(result)
            log_level = (
                logger.error
                if not result.passed and result.severity == "CRITICAL"
                else logger.warning
                if not result.passed
                else logger.info
            )
            log_level(
                f"Validation '{check_name}': {'PASS' if result.passed else 'FAIL'} "
                f"({result.violations} violations, max {result.max_allowed})"
            )

        return results

    def run_critical_checks(
        self, target_date: date, sample_limit: int = 5
    ) -> List[ValidationResult]:
        """
        Run only CRITICAL severity checks.

        Use this for fast validation before publishing.
        """
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
    Run hourly audit after data updates.

    This is the main entry point called by the aggregation service.

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

    if critical_failures:
        logger.error(
            f"CRITICAL validation failures for {target_date}: {len(critical_failures)} checks failed"
        )
    elif all_failures:
        logger.warning(
            f"Validation warnings for {target_date}: {len(all_failures)} checks failed"
        )
    else:
        logger.info(f"All validation checks passed for {target_date}")

    return summary
