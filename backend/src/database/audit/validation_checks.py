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

    checker = ValidationChecker(session)
    results = checker.run_all_checks(target_date)

Created: 2024-11 (Data Accuracy Audit Framework)
Updated: 2024-11 (Rewritten to use pre-aggregated tables)
Updated: 2025-12 (Converted from raw SQL to SQLAlchemy ORM)
"""

from datetime import date, timedelta
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from sqlalchemy import func, and_, or_
from sqlalchemy.orm import Session

from utils.logger import logger
from models import (
    Park,
    Ride,
    RideDailyStats,
    ParkDailyStats,
    ParkWeeklyStats
)


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

# Rule metadata (max_rows, severity, message)
VALIDATION_RULES_METADATA: Dict[str, Dict[str, Any]] = {
    "downtime_exceeds_24h": {
        "max_rows": 0,
        "severity": "CRITICAL",
        "message": "Ride has more than 24 hours of downtime in a single day",
    },
    "uptime_percentage_out_of_bounds": {
        "max_rows": 0,
        "severity": "CRITICAL",
        "message": "Uptime percentage is outside valid range (0-100)",
    },
    "negative_downtime": {
        "max_rows": 0,
        "severity": "CRITICAL",
        "message": "Negative downtime detected (calculation error)",
    },
    "shame_score_negative": {
        "max_rows": 0,
        "severity": "CRITICAL",
        "message": "Negative shame score detected (calculation error)",
    },
    "park_uptime_out_of_bounds": {
        "max_rows": 0,
        "severity": "CRITICAL",
        "message": "Park avg uptime percentage is outside valid range (0-100)",
    },
    "no_ride_data_for_date": {
        "max_rows": 50,  # Allow some parks to have no data (closed parks)
        "severity": "WARNING",
        "message": "Active park has no ride data for this date",
    },
    "rides_with_zero_operating_time": {
        "max_rows": 100,  # Many rides may be closed
        "severity": "INFO",
        "message": "Ride has zero operating time (may be closed)",
    },
    "downtime_exceeds_operating_hours": {
        "max_rows": 0,
        "severity": "CRITICAL",
        "message": "Downtime exceeds operating hours (impossible)",
    },
    "rides_with_downtime_exceeds_total": {
        "max_rows": 0,
        "severity": "CRITICAL",
        "message": "Rides with downtime exceeds total rides tracked",
    },
    "weekly_vs_daily_sum_mismatch": {
        "max_rows": 0,
        "severity": "WARNING",
        "message": "Weekly total doesn't match sum of daily stats",
    },
}


class ValidationChecker:
    """
    Runs validation checks against pre-aggregated daily stats tables.

    Usage:
        checker = ValidationChecker(session)
        results = checker.run_all_checks(date.today() - timedelta(days=1))
    """

    def __init__(self, session: Session):
        self.session = session

    # =========================================================================
    # ORM QUERY METHODS - One method per validation rule
    # =========================================================================

    def _query_downtime_exceeds_24h(self, target_date: date) -> List[Dict[str, Any]]:
        """Check for rides with more than 24 hours of downtime in a day."""
        results = (
            self.session.query(
                RideDailyStats.ride_id,
                Ride.name.label('ride_name'),
                Park.name.label('park_name'),
                RideDailyStats.stat_date,
                func.round(RideDailyStats.downtime_minutes / 60.0, 2).label('downtime_hours')
            )
            .join(Ride, RideDailyStats.ride_id == Ride.ride_id)
            .join(Park, Ride.park_id == Park.park_id)
            .filter(
                and_(
                    RideDailyStats.downtime_minutes > 1440,
                    RideDailyStats.stat_date == target_date
                )
            )
            .all()
        )
        return [dict(zip(['ride_id', 'ride_name', 'park_name', 'stat_date', 'downtime_hours'], row)) for row in results]

    def _query_uptime_percentage_out_of_bounds(self, target_date: date) -> List[Dict[str, Any]]:
        """Check for uptime percentage outside valid range (0-100)."""
        results = (
            self.session.query(
                RideDailyStats.ride_id,
                Ride.name.label('ride_name'),
                Park.name.label('park_name'),
                RideDailyStats.stat_date,
                RideDailyStats.uptime_percentage
            )
            .join(Ride, RideDailyStats.ride_id == Ride.ride_id)
            .join(Park, Ride.park_id == Park.park_id)
            .filter(
                and_(
                    or_(
                        RideDailyStats.uptime_percentage > 100,
                        RideDailyStats.uptime_percentage < 0
                    ),
                    RideDailyStats.stat_date == target_date
                )
            )
            .all()
        )
        return [dict(zip(['ride_id', 'ride_name', 'park_name', 'stat_date', 'uptime_percentage'], row)) for row in results]

    def _query_negative_downtime(self, target_date: date) -> List[Dict[str, Any]]:
        """Check for negative downtime values."""
        results = (
            self.session.query(
                RideDailyStats.ride_id,
                Ride.name.label('ride_name'),
                Park.name.label('park_name'),
                RideDailyStats.stat_date,
                RideDailyStats.downtime_minutes
            )
            .join(Ride, RideDailyStats.ride_id == Ride.ride_id)
            .join(Park, Ride.park_id == Park.park_id)
            .filter(
                and_(
                    RideDailyStats.downtime_minutes < 0,
                    RideDailyStats.stat_date == target_date
                )
            )
            .all()
        )
        return [dict(zip(['ride_id', 'ride_name', 'park_name', 'stat_date', 'downtime_minutes'], row)) for row in results]

    def _query_shame_score_negative(self, target_date: date) -> List[Dict[str, Any]]:
        """Check for negative shame scores."""
        results = (
            self.session.query(
                ParkDailyStats.park_id,
                Park.name.label('park_name'),
                ParkDailyStats.stat_date,
                ParkDailyStats.shame_score
            )
            .join(Park, ParkDailyStats.park_id == Park.park_id)
            .filter(
                and_(
                    ParkDailyStats.shame_score < 0,
                    ParkDailyStats.stat_date == target_date
                )
            )
            .all()
        )
        return [dict(zip(['park_id', 'park_name', 'stat_date', 'shame_score'], row)) for row in results]

    def _query_park_uptime_out_of_bounds(self, target_date: date) -> List[Dict[str, Any]]:
        """Check for park uptime percentage outside valid range (0-100)."""
        results = (
            self.session.query(
                ParkDailyStats.park_id,
                Park.name.label('park_name'),
                ParkDailyStats.stat_date,
                ParkDailyStats.avg_uptime_percentage
            )
            .join(Park, ParkDailyStats.park_id == Park.park_id)
            .filter(
                and_(
                    or_(
                        ParkDailyStats.avg_uptime_percentage > 100,
                        ParkDailyStats.avg_uptime_percentage < 0
                    ),
                    ParkDailyStats.stat_date == target_date
                )
            )
            .all()
        )
        return [dict(zip(['park_id', 'park_name', 'stat_date', 'avg_uptime_percentage'], row)) for row in results]

    def _query_no_ride_data_for_date(self, target_date: date) -> List[Dict[str, Any]]:
        """Check for active parks with no ride data."""
        # Subquery for parks that have ride data on target_date
        parks_with_data = (
            self.session.query(Park.park_id)
            .join(Ride, Park.park_id == Ride.park_id)
            .join(RideDailyStats, Ride.ride_id == RideDailyStats.ride_id)
            .filter(RideDailyStats.stat_date == target_date)
            .distinct()
            .subquery()
        )

        # Get active parks NOT in the subquery
        results = (
            self.session.query(
                Park.park_id,
                Park.name.label('park_name')
            )
            .filter(
                and_(
                    Park.is_active.is_(True),
                    ~Park.park_id.in_(parks_with_data)
                )
            )
            .all()
        )
        return [{'park_id': row.park_id, 'park_name': row.park_name, 'stat_date': target_date} for row in results]

    def _query_rides_with_zero_operating_time(self, target_date: date) -> List[Dict[str, Any]]:
        """Check for rides with zero operating time."""
        results = (
            self.session.query(
                RideDailyStats.ride_id,
                Ride.name.label('ride_name'),
                Park.name.label('park_name'),
                RideDailyStats.stat_date,
                RideDailyStats.operating_hours_minutes
            )
            .join(Ride, RideDailyStats.ride_id == Ride.ride_id)
            .join(Park, Ride.park_id == Park.park_id)
            .filter(
                and_(
                    RideDailyStats.operating_hours_minutes == 0,
                    RideDailyStats.uptime_minutes == 0,
                    RideDailyStats.downtime_minutes == 0,
                    RideDailyStats.stat_date == target_date
                )
            )
            .all()
        )
        return [dict(zip(['ride_id', 'ride_name', 'park_name', 'stat_date', 'operating_hours_minutes'], row)) for row in results]

    def _query_downtime_exceeds_operating_hours(self, target_date: date) -> List[Dict[str, Any]]:
        """Check for downtime exceeding operating hours."""
        results = (
            self.session.query(
                RideDailyStats.ride_id,
                Ride.name.label('ride_name'),
                Park.name.label('park_name'),
                RideDailyStats.stat_date,
                RideDailyStats.downtime_minutes,
                RideDailyStats.operating_hours_minutes
            )
            .join(Ride, RideDailyStats.ride_id == Ride.ride_id)
            .join(Park, Ride.park_id == Park.park_id)
            .filter(
                and_(
                    RideDailyStats.downtime_minutes > RideDailyStats.operating_hours_minutes,
                    RideDailyStats.operating_hours_minutes > 0,
                    RideDailyStats.stat_date == target_date
                )
            )
            .all()
        )
        return [dict(zip(['ride_id', 'ride_name', 'park_name', 'stat_date', 'downtime_minutes', 'operating_hours_minutes'], row)) for row in results]

    def _query_rides_with_downtime_exceeds_total(self, target_date: date) -> List[Dict[str, Any]]:
        """Check for rides_with_downtime exceeding total_rides_tracked."""
        results = (
            self.session.query(
                ParkDailyStats.park_id,
                Park.name.label('park_name'),
                ParkDailyStats.stat_date,
                ParkDailyStats.rides_with_downtime,
                ParkDailyStats.total_rides_tracked
            )
            .join(Park, ParkDailyStats.park_id == Park.park_id)
            .filter(
                and_(
                    ParkDailyStats.rides_with_downtime > ParkDailyStats.total_rides_tracked,
                    ParkDailyStats.stat_date == target_date
                )
            )
            .all()
        )
        return [dict(zip(['park_id', 'park_name', 'stat_date', 'rides_with_downtime', 'total_rides_tracked'], row)) for row in results]

    def _query_weekly_vs_daily_sum_mismatch(self, target_date: date) -> List[Dict[str, Any]]:
        """Check for weekly total not matching sum of daily stats."""
        # Get year and week number for target_date
        year = target_date.year
        # Python's isocalendar() provides ISO week number (week starts Monday)
        week_number = target_date.isocalendar()[1]

        # Get daily stats sum grouped by park
        daily_sums = (
            self.session.query(
                ParkDailyStats.park_id,
                func.sum(ParkDailyStats.total_downtime_hours).label('daily_sum')
            )
            .filter(
                and_(
                    func.year(ParkDailyStats.stat_date) == year,
                    func.weekofyear(ParkDailyStats.stat_date) == week_number
                )
            )
            .group_by(ParkDailyStats.park_id)
            .subquery()
        )

        # Join with weekly stats to compare
        results = (
            self.session.query(
                ParkWeeklyStats.park_id,
                Park.name.label('park_name'),
                ParkWeeklyStats.year,
                ParkWeeklyStats.week_number,
                ParkWeeklyStats.total_downtime_hours.label('weekly_total'),
                func.round(daily_sums.c.daily_sum, 2).label('daily_sum'),
                func.abs(ParkWeeklyStats.total_downtime_hours - daily_sums.c.daily_sum).label('diff')
            )
            .join(Park, ParkWeeklyStats.park_id == Park.park_id)
            .join(daily_sums, ParkWeeklyStats.park_id == daily_sums.c.park_id)
            .filter(
                and_(
                    ParkWeeklyStats.year == year,
                    ParkWeeklyStats.week_number == week_number,
                    func.abs(ParkWeeklyStats.total_downtime_hours - daily_sums.c.daily_sum) > 0.5
                )
            )
            .all()
        )
        return [dict(zip(['park_id', 'park_name', 'year', 'week_number', 'weekly_total', 'daily_sum', 'diff'], row)) for row in results]

    def run_check(
        self, check_name: str, target_date: date, sample_limit: int = 5
    ) -> ValidationResult:
        """Run a single validation check."""
        if check_name not in VALIDATION_RULES_METADATA:
            raise ValueError(f"Unknown validation check: {check_name}")

        rule = VALIDATION_RULES_METADATA[check_name]

        # Map check_name to query method
        query_method_name = f"_query_{check_name}"
        if not hasattr(self, query_method_name):
            raise ValueError(f"Query method {query_method_name} not implemented")

        try:
            query_method = getattr(self, query_method_name)
            rows = query_method(target_date)

            sample = rows[:sample_limit]
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
        for check_name in VALIDATION_RULES_METADATA:
            result = self.run_check(check_name, target_date, sample_limit)
            results.append(result)

        return results

    def run_critical_checks(
        self, target_date: date, sample_limit: int = 5
    ) -> List[ValidationResult]:
        """Run only CRITICAL severity checks."""
        critical_checks = [
            name for name, rule in VALIDATION_RULES_METADATA.items() if rule["severity"] == "CRITICAL"
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


def run_hourly_audit(session: Session, target_date: Optional[date] = None) -> Dict[str, Any]:
    """
    Run audit after data updates.

    Args:
        session: SQLAlchemy session
        target_date: Date to audit (default: yesterday)

    Returns:
        Audit summary with pass/fail status and any failures
    """
    if target_date is None:
        target_date = date.today() - timedelta(days=1)

    checker = ValidationChecker(session)
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
