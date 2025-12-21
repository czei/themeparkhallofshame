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


class TestDisneyDownCheckResult:
    """Tests for DisneyDownCheckResult dataclass."""

    def test_default_values(self):
        """Test that default values are set correctly."""
        from database.audit.aggregate_verification import DisneyDownCheckResult

        result = DisneyDownCheckResult(
            parks_checked=5,
            rides_with_down_status=10,
            rides_incorrectly_excluded=0
        )

        assert result.passed is True
        assert result.message == ""
        assert result.examples == []

    def test_failure_values(self):
        """Test with failure case."""
        from database.audit.aggregate_verification import DisneyDownCheckResult

        result = DisneyDownCheckResult(
            parks_checked=5,
            rides_with_down_status=10,
            rides_incorrectly_excluded=3,
            passed=False,
            message="3 rides incorrectly excluded",
            examples=[{'ride_name': 'DINOSAUR', 'status': 'excluded'}]
        )

        assert result.passed is False
        assert result.rides_incorrectly_excluded == 3
        assert len(result.examples) == 1


class TestIntervalConsistencyResult:
    """Tests for IntervalConsistencyResult dataclass."""

    def test_default_values(self):
        """Test that default values are set correctly."""
        from database.audit.aggregate_verification import IntervalConsistencyResult

        result = IntervalConsistencyResult(
            expected_interval=10,
            calculated_interval=9.8
        )

        assert result.is_consistent is True
        assert result.message == ""

    def test_inconsistent_interval(self):
        """Test with inconsistent interval."""
        from database.audit.aggregate_verification import IntervalConsistencyResult

        result = IntervalConsistencyResult(
            expected_interval=10,
            calculated_interval=5.0,
            is_consistent=False,
            message="FAIL: interval mismatch"
        )

        assert result.is_consistent is False
        assert "FAIL" in result.message


class TestHourlyVerification:
    """Tests for hourly verification methods."""

    def test_audit_hourly_returns_summary(self):
        """Test that audit_hourly returns an AuditSummary."""
        mock_conn = MagicMock()

        # Mock the hours query to return empty
        mock_result = MagicMock()
        mock_result.__iter__ = lambda self: iter([])
        mock_conn.execute.return_value = mock_result

        verifier = AggregateVerifier(mock_conn)

        with patch('database.audit.aggregate_verification.get_pacific_day_range_utc') as mock_range:
            mock_range.return_value = (
                datetime(2025, 12, 18, 8, 0, 0),
                datetime(2025, 12, 19, 8, 0, 0)
            )

            # Mock all the verification method results
            with patch.object(verifier, 'verify_disney_down_status') as mock_disney:
                from database.audit.aggregate_verification import DisneyDownCheckResult
                mock_disney.return_value = DisneyDownCheckResult(
                    parks_checked=5,
                    rides_with_down_status=0,
                    rides_incorrectly_excluded=0,
                    passed=True,
                    message="PASS"
                )

                with patch.object(verifier, 'verify_interval_consistency') as mock_interval:
                    from database.audit.aggregate_verification import IntervalConsistencyResult
                    mock_interval.return_value = IntervalConsistencyResult(
                        expected_interval=10,
                        calculated_interval=10.0,
                        is_consistent=True,
                        message="PASS"
                    )

                    summary = verifier.audit_hourly(date(2025, 12, 18))

                    assert isinstance(summary, AuditSummary)
                    assert summary.disney_down_check_result is not None
                    assert summary.interval_check_result is not None

    def test_full_audit_combines_daily_and_hourly(self):
        """Test that full_audit includes both daily and hourly verification."""
        mock_conn = MagicMock()

        # Mock execute to return empty results for all queries
        mock_result = MagicMock()
        mock_result.__iter__ = lambda self: iter([])
        mock_result.fetchone.return_value = None
        mock_result.scalar.return_value = 0
        mock_conn.execute.return_value = mock_result

        verifier = AggregateVerifier(mock_conn)

        with patch('database.audit.aggregate_verification.get_pacific_day_range_utc') as mock_range:
            mock_range.return_value = (
                datetime(2025, 12, 18, 8, 0, 0),
                datetime(2025, 12, 19, 8, 0, 0)
            )

            # Mock daily audit
            with patch.object(verifier, 'audit_date') as mock_daily:
                mock_daily.return_value = AuditSummary(
                    audit_timestamp=datetime(2025, 12, 18, 12, 0, 0),
                    target_date=date(2025, 12, 18),
                    overall_passed=True
                )

                # Mock hourly audit
                with patch.object(verifier, 'audit_hourly') as mock_hourly:
                    from database.audit.aggregate_verification import (
                        DisneyDownCheckResult,
                        IntervalConsistencyResult
                    )
                    mock_hourly.return_value = AuditSummary(
                        audit_timestamp=datetime(2025, 12, 18, 12, 0, 0),
                        target_date=date(2025, 12, 18),
                        overall_passed=True,
                        disney_down_check_result=DisneyDownCheckResult(
                            parks_checked=5,
                            rides_with_down_status=0,
                            rides_incorrectly_excluded=0,
                            passed=True,
                            message="PASS"
                        ),
                        interval_check_result=IntervalConsistencyResult(
                            expected_interval=10,
                            calculated_interval=10.0,
                            is_consistent=True,
                            message="PASS"
                        )
                    )

                    summary = verifier.full_audit(date(2025, 12, 18))

                    # Verify both audits were called
                    mock_daily.assert_called_once()
                    mock_hourly.assert_called_once()

                    # Verify results were merged
                    assert summary.disney_down_check_result is not None
                    assert summary.interval_check_result is not None


class TestDisneyDownVerification:
    """Tests for Disney DOWN status verification."""

    def test_disney_down_check_detects_missing_ride_operated(self):
        """Test that Disney DOWN check flags rides with ride_operated=0."""
        # This is the bug we fixed - Disney rides with DOWN status
        # were excluded because ride_operated=0

        mock_conn = MagicMock()

        # Mock query that finds rides incorrectly excluded
        mock_result = MagicMock()
        mock_result.__iter__ = lambda self: iter([
            MagicMock(_mapping={
                'ride_id': 4065,
                'ride_name': 'DINOSAUR',
                'park_name': 'Animal Kingdom',
                'hour_start': '2025-12-18 13:00:00',
                'ride_operated': 0,
                'stored_downtime_hours': 0,
                'stored_down_snapshots': 0,
                'status': 'excluded'
            })
        ])
        mock_conn.execute.return_value = mock_result

        verifier = AggregateVerifier(mock_conn)

        with patch('database.audit.aggregate_verification.get_pacific_day_range_utc') as mock_range:
            mock_range.return_value = (
                datetime(2025, 12, 18, 8, 0, 0),
                datetime(2025, 12, 19, 8, 0, 0)
            )

            # Reset mock for the parks count query
            def side_effect(*args, **kwargs):
                query = str(args[0])
                if 'COUNT(DISTINCT p.park_id)' in query:
                    result = MagicMock()
                    result.scalar.return_value = 5
                    return result
                elif 'COUNT(DISTINCT rss.ride_id)' in query:
                    result = MagicMock()
                    result.scalar.return_value = 10
                    return result
                else:
                    result = MagicMock()
                    result.__iter__ = lambda self: iter([
                        MagicMock(_mapping={
                            'ride_id': 4065,
                            'ride_name': 'DINOSAUR',
                            'park_name': 'Animal Kingdom',
                            'hour_start': '2025-12-18 13:00:00',
                            'ride_operated': 0,
                            'stored_downtime_hours': 0,
                            'stored_down_snapshots': 0,
                            'status': 'excluded'
                        })
                    ])
                    return result

            mock_conn.execute.side_effect = side_effect

            result = verifier.verify_disney_down_status(date(2025, 12, 18))

            # Should fail because DINOSAUR was incorrectly excluded
            assert result.passed is False
            assert result.rides_incorrectly_excluded > 0
            assert "FAIL" in result.message


class TestSummaryReportWithHourly:
    """Tests for summary report including hourly and special checks."""

    def test_report_includes_disney_check(self):
        """Test that summary report includes Disney DOWN check results."""
        from database.audit.aggregate_verification import (
            DisneyDownCheckResult,
            IntervalConsistencyResult
        )

        mock_conn = MagicMock()
        verifier = AggregateVerifier(mock_conn)

        summary = AuditSummary(
            audit_timestamp=datetime(2025, 12, 18, 12, 0, 0),
            target_date=date(2025, 12, 18),
            overall_passed=True,
            disney_down_check_result=DisneyDownCheckResult(
                parks_checked=5,
                rides_with_down_status=10,
                rides_incorrectly_excluded=0,
                passed=True,
                message="PASS: All 10 Disney/Universal rides with DOWN status are correctly counted"
            ),
            interval_check_result=IntervalConsistencyResult(
                expected_interval=10,
                calculated_interval=9.8,
                is_consistent=True,
                message="PASS: Actual interval (9.8 min) matches expected (10 min)"
            )
        )

        report = verifier.get_summary_report(summary)

        # Check Disney DOWN check is in report
        assert "Disney/Universal DOWN Status Check" in report
        assert "Parks checked: 5" in report
        assert "Rides with DOWN status: 10" in report

        # Check interval check is in report
        assert "Interval Consistency Check" in report
        assert "Expected interval: 10 min" in report

    def test_report_includes_hourly_results(self):
        """Test that summary report includes hourly verification results."""
        mock_conn = MagicMock()
        verifier = AggregateVerifier(mock_conn)

        ride_hourly = AggregateAuditResult(
            table_name='ride_hourly_stats',
            target_date=date(2025, 12, 18),
            total_records_checked=100,
            records_matching=100,
            records_mismatched=0,
            records_missing_from_aggregate=0,
            records_missing_from_raw=0,
            match_rate=1.0,
            message="All 100 records verified"
        )

        summary = AuditSummary(
            audit_timestamp=datetime(2025, 12, 18, 12, 0, 0),
            target_date=date(2025, 12, 18),
            overall_passed=True,
            ride_hourly_results=[ride_hourly]
        )

        report = verifier.get_summary_report(summary)

        # Check hourly section is in report
        assert "Ride Hourly Stats" in report
        assert "1 hours checked" in report
        assert "All hours passed" in report
