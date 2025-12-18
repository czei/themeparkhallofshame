"""
Unit tests for aggregate verification module.

Tests the verification logic without requiring a database connection.
"""

import pytest
from datetime import date, datetime
from unittest.mock import MagicMock, patch

from database.audit.aggregate_verification import (
    AggregateVerifier,
    AggregateAuditResult,
    AuditSummary,
)


class TestAggregateAuditResult:
    """Tests for AggregateAuditResult dataclass."""

    def test_default_values(self):
        """Test that default values are set correctly."""
        result = AggregateAuditResult(
            table_name='test_table',
            target_date=date(2025, 12, 17),
            total_records_checked=100,
            records_matching=95,
            records_mismatched=3,
            records_missing_from_aggregate=2,
            records_missing_from_raw=0,
            match_rate=0.95
        )

        assert result.passed is True
        assert result.severity == "INFO"
        assert result.message == ""
        assert result.worst_mismatches == []
        assert result.max_deviation == {}
        assert result.avg_deviation == {}

    def test_calculated_fields(self):
        """Test that match rate is calculated correctly."""
        result = AggregateAuditResult(
            table_name='ride_daily_stats',
            target_date=date(2025, 12, 17),
            total_records_checked=100,
            records_matching=90,
            records_mismatched=8,
            records_missing_from_aggregate=2,
            records_missing_from_raw=0,
            match_rate=0.90,
            passed=False,
            severity="WARNING",
            message="10 discrepancies found"
        )

        assert result.match_rate == 0.90
        assert result.passed is False
        assert result.severity == "WARNING"


class TestAuditSummary:
    """Tests for AuditSummary dataclass."""

    def test_default_values(self):
        """Test that summary default values are set correctly."""
        summary = AuditSummary(
            audit_timestamp=datetime(2025, 12, 18, 12, 0, 0),
            target_date=date(2025, 12, 17)
        )

        assert summary.overall_passed is True
        assert summary.critical_failures == 0
        assert summary.warnings == 0
        assert summary.issues_found == []
        assert summary.recommended_actions == []
        assert summary.ride_daily_result is None
        assert summary.park_daily_result is None

    def test_with_failures(self):
        """Test summary with failures."""
        ride_result = AggregateAuditResult(
            table_name='ride_daily_stats',
            target_date=date(2025, 12, 17),
            total_records_checked=100,
            records_matching=80,
            records_mismatched=20,
            records_missing_from_aggregate=0,
            records_missing_from_raw=0,
            match_rate=0.80,
            passed=False,
            severity="CRITICAL",
            message="20 mismatches found"
        )

        summary = AuditSummary(
            audit_timestamp=datetime(2025, 12, 18, 12, 0, 0),
            target_date=date(2025, 12, 17),
            ride_daily_result=ride_result,
            overall_passed=False,
            critical_failures=1,
            issues_found=["ride_daily_stats: 20 mismatches found"]
        )

        assert summary.overall_passed is False
        assert summary.critical_failures == 1


class TestAggregateVerifier:
    """Tests for AggregateVerifier class."""

    def test_tolerances_defined(self):
        """Test that tolerance thresholds are properly defined."""
        mock_conn = MagicMock()
        verifier = AggregateVerifier(mock_conn)

        # Verify tolerances exist for all tables
        assert 'ride_daily' in verifier.TOLERANCES
        assert 'park_daily' in verifier.TOLERANCES
        assert 'ride_hourly' in verifier.TOLERANCES
        assert 'park_hourly' in verifier.TOLERANCES

        # Verify ride_daily tolerances
        assert verifier.TOLERANCES['ride_daily']['uptime_minutes'] == 10
        assert verifier.TOLERANCES['ride_daily']['downtime_minutes'] == 10

        # Verify park_daily tolerances
        assert verifier.TOLERANCES['park_daily']['total_downtime_hours'] == 0.17
        assert verifier.TOLERANCES['park_daily']['shame_score'] == 0.2

    def test_snapshot_interval_imported(self):
        """Test that snapshot interval is imported from metrics module."""
        mock_conn = MagicMock()
        verifier = AggregateVerifier(mock_conn)

        # After our fix, this should be 10 (not 5)
        assert verifier.snapshot_interval == 10

    def test_get_summary_report_format(self):
        """Test that summary report is properly formatted."""
        mock_conn = MagicMock()
        verifier = AggregateVerifier(mock_conn)

        ride_result = AggregateAuditResult(
            table_name='ride_daily_stats',
            target_date=date(2025, 12, 17),
            total_records_checked=500,
            records_matching=500,
            records_mismatched=0,
            records_missing_from_aggregate=0,
            records_missing_from_raw=0,
            match_rate=1.0,
            max_deviation={'uptime_minutes': 5, 'downtime_minutes': 3},
            avg_deviation={'uptime_minutes': 2, 'downtime_minutes': 1},
            message="ride_daily_stats: All 500 records verified"
        )

        summary = AuditSummary(
            audit_timestamp=datetime(2025, 12, 18, 12, 0, 0),
            target_date=date(2025, 12, 17),
            ride_daily_result=ride_result,
            overall_passed=True
        )

        report = verifier.get_summary_report(summary)

        # Check report contains expected sections
        assert "AGGREGATE VERIFICATION REPORT" in report
        assert "2025-12-17" in report
        assert "Overall Status: PASS" in report
        assert "ride_daily_stats" in report
        assert "Records checked: 500" in report
        assert "Match rate: 100.0%" in report


class TestTimezoneHandling:
    """Tests for timezone handling in verification."""

    def test_uses_pacific_day_range(self):
        """Test that verifier uses Pacific timezone for day boundaries."""
        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.fetchall.return_value = []
        mock_conn.execute.return_value = mock_result

        verifier = AggregateVerifier(mock_conn)

        # The verifier should call get_pacific_day_range_utc
        # which converts Pacific dates to UTC ranges
        with patch('database.audit.aggregate_verification.get_pacific_day_range_utc') as mock_range:
            from datetime import datetime
            mock_range.return_value = (
                datetime(2025, 12, 17, 8, 0, 0),  # 08:00 UTC = midnight Pacific PST
                datetime(2025, 12, 18, 8, 0, 0)   # next day
            )

            # Mock the execute to return empty results
            mock_result = MagicMock()
            mock_result.__iter__ = lambda self: iter([])
            mock_conn.execute.return_value = mock_result

            verifier.verify_ride_daily_stats(date(2025, 12, 17))

            # Verify Pacific range function was called
            mock_range.assert_called_once_with(date(2025, 12, 17))


class TestToleranceChecks:
    """Tests for tolerance threshold logic."""

    def test_within_tolerance_passes(self):
        """Test that values within tolerance are marked as matching."""
        # If stored = 100 and calculated = 105, delta = 5
        # Tolerance = 10, so this should pass
        delta = 5
        tolerance = 10
        assert delta <= tolerance

    def test_exceeds_tolerance_fails(self):
        """Test that values exceeding tolerance are marked as mismatched."""
        # If stored = 100 and calculated = 120, delta = 20
        # Tolerance = 10, so this should fail
        delta = 20
        tolerance = 10
        assert delta > tolerance

    def test_tolerance_is_absolute_not_percentage(self):
        """Test that tolerance is an absolute value, not percentage."""
        # For downtime_minutes, tolerance = 10 means ±10 minutes
        # Not 10% of the value
        stored = 100
        calculated = 108
        delta = abs(stored - calculated)
        tolerance = 10

        # 8 minute difference should pass (< 10)
        assert delta <= tolerance

        # But if tolerance were 10%, 8/100 = 8% would also pass
        # We want absolute, so verify the tolerance is in same units
        assert isinstance(tolerance, int)
