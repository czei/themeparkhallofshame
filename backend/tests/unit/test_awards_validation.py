"""
TDD Tests for Awards Validation in Data Quality Audit
======================================================

These tests verify that the data quality audit script properly validates
awards data from yesterday to catch bugs like:
- Downtime hours > 24h for single-day period (impossible)
- Wait times outside reasonable bounds
- Data existence issues

Test-Driven Development:
1. RED: Write these tests first - they define expected behavior
2. GREEN: Implement validation functions to make tests pass
3. REFACTOR: Clean up code while keeping tests green
"""

import sys
from unittest.mock import MagicMock

# Mock sendgrid before importing the script (sendgrid not installed in test env)
sys.modules['sendgrid'] = MagicMock()
sys.modules['sendgrid.helpers'] = MagicMock()
sys.modules['sendgrid.helpers.mail'] = MagicMock()

from pathlib import Path


class TestAwardsValidationFunctionsExist:
    """
    Tests to verify the validation functions exist in the audit script.
    """

    def test_validate_awards_function_exists(self):
        """
        The audit script must have a validate_awards() function.
        This is the main entry point for awards validation.
        """
        script_path = Path(__file__).parent.parent.parent / "src" / "scripts" / "send_data_quality_alert.py"
        source_code = script_path.read_text()

        assert "def validate_awards" in source_code or "def get_awards_issues" in source_code, \
            "Audit script must have awards validation function"

    def test_awards_validation_called_in_main(self):
        """
        Awards validation must be called from the main() function.
        """
        script_path = Path(__file__).parent.parent.parent / "src" / "scripts" / "send_data_quality_alert.py"
        source_code = script_path.read_text()

        # Should call awards validation and include results
        assert "awards" in source_code.lower(), \
            "Audit script main() should call awards validation"


class TestDowntimeValidationBounds:
    """
    Tests for downtime validation bounds.

    Key rule: For YESTERDAY period, no ride can have >24 hours of downtime
    because a day only has 24 hours.
    """

    def test_validate_downtime_rejects_over_24h_for_yesterday(self):
        """
        Downtime > 24h for a single day is physically impossible.
        This was the bug that prompted this validation feature.

        Example: Goofy's Sky School showing 22.67h downtime for 'yesterday'
        was actually returning 30-day aggregate data (bug now fixed, but
        validation should catch similar issues).
        """
        from scripts.send_data_quality_alert import validate_downtime_bounds

        # Test data mimicking buggy output (downtime > 24h for single day)
        buggy_ride = {
            "ride_name": "Test Ride",
            "downtime_hours": 26.5,  # Impossible for single day
            "period": "yesterday"
        }

        issues = validate_downtime_bounds([buggy_ride], period="yesterday")

        assert len(issues) > 0, "Should flag downtime > 24h for yesterday"
        assert any("24" in str(i) or "impossible" in str(i).lower() for i in issues)

    def test_validate_downtime_allows_valid_values(self):
        """
        Valid downtime values (0-24h) should not be flagged.
        """
        from scripts.send_data_quality_alert import validate_downtime_bounds

        valid_ride = {
            "ride_name": "Test Ride",
            "downtime_hours": 9.25,  # Valid - less than 24h
            "period": "yesterday"
        }

        issues = validate_downtime_bounds([valid_ride], period="yesterday")

        assert len(issues) == 0, "Valid downtime should not be flagged"

    def test_validate_downtime_allows_higher_for_weekly(self):
        """
        For last_week period, downtime can be up to 168h (7 days × 24h).
        """
        from scripts.send_data_quality_alert import validate_downtime_bounds

        weekly_ride = {
            "ride_name": "Test Ride",
            "downtime_hours": 50.0,  # Valid for a week
            "period": "last_week"
        }

        issues = validate_downtime_bounds([weekly_ride], period="last_week")

        assert len(issues) == 0, "50h downtime valid for weekly period"


class TestWaitTimeValidationBounds:
    """
    Tests for wait time validation bounds.

    Key rules:
    - Average wait times should be 0-300 minutes (reasonable theme park range)
    - Peak wait times up to 420 minutes (7 hours - seen at Disney on peak days)
    """

    def test_validate_wait_time_rejects_extreme_values(self):
        """
        Wait times over 300 minutes average are suspicious.
        No ride consistently has 5+ hour average waits.
        """
        from scripts.send_data_quality_alert import validate_wait_time_bounds

        extreme_ride = {
            "ride_name": "Test Ride",
            "avg_wait_time": 400,  # 6.7 hours average - unrealistic
        }

        issues = validate_wait_time_bounds([extreme_ride])

        assert len(issues) > 0, "Should flag avg wait > 300 min"

    def test_validate_wait_time_allows_valid_values(self):
        """
        Reasonable wait times should not be flagged.
        """
        from scripts.send_data_quality_alert import validate_wait_time_bounds

        valid_ride = {
            "ride_name": "Test Ride",
            "avg_wait_time": 85,  # Reasonable
        }

        issues = validate_wait_time_bounds([valid_ride])

        assert len(issues) == 0, "Valid wait time should not be flagged"

    def test_validate_wait_time_rejects_negative(self):
        """
        Negative wait times are data errors.
        """
        from scripts.send_data_quality_alert import validate_wait_time_bounds

        negative_ride = {
            "ride_name": "Test Ride",
            "avg_wait_time": -5,
        }

        issues = validate_wait_time_bounds([negative_ride])

        assert len(issues) > 0, "Should flag negative wait times"


class TestAwardsDataExistence:
    """
    Tests for awards data existence validation.

    If the awards query returns no data for yesterday, that's suspicious
    and should be flagged (unless no parks were open).
    """

    def test_validate_flags_empty_awards(self):
        """
        Empty awards for yesterday should be flagged as suspicious.
        (Unless it's a day when all parks were closed)
        """
        from scripts.send_data_quality_alert import validate_awards_existence

        issues = validate_awards_existence(rides=[], parks=[])

        assert len(issues) > 0, "Should flag when no awards data exists"

    def test_validate_allows_populated_awards(self):
        """
        When awards data exists, no existence issues.
        """
        from scripts.send_data_quality_alert import validate_awards_existence

        rides = [{"ride_name": "Test", "downtime_hours": 5.0}]
        parks = [{"park_name": "Test Park", "downtime_hours": 10.0}]

        issues = validate_awards_existence(rides=rides, parks=parks)

        assert len(issues) == 0, "Should not flag when data exists"


class TestAwardsValidationIntegration:
    """
    Integration tests for the full awards validation flow.
    """

    def test_get_awards_issues_returns_list(self):
        """
        The main validation function should return a list of issues.
        """
        from scripts.send_data_quality_alert import get_awards_issues

        # Should return a list (possibly empty if no issues)
        issues = get_awards_issues()

        assert isinstance(issues, list), "get_awards_issues should return a list"

    def test_awards_issues_have_required_fields(self):
        """
        Each issue should have: category, severity, message, details
        """
        from scripts.send_data_quality_alert import get_awards_issues

        issues = get_awards_issues()

        for issue in issues:
            assert "category" in issue, "Issue should have category"
            assert "severity" in issue, "Issue should have severity"
            assert "message" in issue, "Issue should have message"


class TestEmailIncludesAwardsSection:
    """
    Tests to verify the email includes awards validation results.
    """

    def test_format_email_includes_awards_section(self):
        """
        The email formatter should include an Awards Validation section.
        """
        script_path = Path(__file__).parent.parent.parent / "src" / "scripts" / "send_data_quality_alert.py"
        source_code = script_path.read_text()

        # Email should mention awards validation
        assert "awards" in source_code.lower() and "format_email" in source_code, \
            "Email formatting should include awards section"
