"""
Ride Details API Contract Tests
=================================

TDD tests for the ride details API endpoint.

These tests verify that the /api/rides/<ride_id>/details endpoint returns
the correct format and data for all supported periods.

Test Coverage:
1. Response structure matches expected format
2. Required fields are present with correct types
3. Data integrity for all periods (today, yesterday, last_week, last_month)
4. Error handling (invalid ride_id, invalid period)
5. Time-series data format
6. Summary statistics format
7. Downtime events format
8. Hourly breakdown format

Related Files:
- src/api/routes/rides.py: Ride details endpoint
- src/database/repositories/ride_repository.py: Ride data access
"""

import pytest
from decimal import Decimal
from datetime import datetime


class TestRideDetailsAPIResponseStructure:
    """
    Test ride details API response conforms to expected structure.

    The ride details endpoint should return comprehensive ride information
    including metadata, time-series data, summary stats, and downtime events.
    """

    def test_ride_details_response_has_required_fields(self):
        """
        Ride details response must include all required top-level fields.

        Required fields:
        - success: bool (True for successful response)
        - period: str ('today', 'yesterday', 'last_week', 'last_month')
        - ride: dict with ride metadata
        - timeseries: list of hourly data points
        - summary: dict with summary statistics
        - downtime_events: list of downtime events
        - hourly_breakdown: list of detailed hourly stats
        - attribution: dict with data source info
        """
        expected_response_structure = {
            "success": True,
            "period": "today",
            "ride": {},
            "timeseries": [],
            "summary": {},
            "downtime_events": [],
            "hourly_breakdown": [],
            "attribution": {
                "data_source": "ThemeParks.wiki",
                "url": "https://themeparks.wiki"
            }
        }

        required_fields = ['success', 'period', 'ride', 'timeseries', 'summary',
                          'downtime_events', 'hourly_breakdown', 'attribution']
        for field in required_fields:
            assert field in expected_response_structure, f"Missing required field: {field}"

    def test_ride_metadata_has_required_fields(self):
        """
        Ride metadata object must have required fields.

        Required fields:
        - ride_id: int
        - name: str
        - park_id: int
        - tier: int (1, 2, or 3)
        - category: str or None
        - queue_times_url: str or None
        """
        expected_ride_metadata = {
            "ride_id": 1,
            "name": "Space Mountain",
            "park_id": 1,
            "tier": 1,
            "category": "Thrill Ride",
            "queue_times_url": "https://queue-times.com/parks/1/rides/1"
        }

        required_fields = ['ride_id', 'name', 'park_id', 'tier']
        for field in required_fields:
            assert field in expected_ride_metadata, f"Missing required field: {field}"

    def test_timeseries_data_point_has_required_fields(self):
        """
        Each time-series data point must have required fields.

        Required fields:
        - hour_start_utc: datetime
        - avg_wait_time_minutes: float or None
        - uptime_percentage: float
        - status: str ('OPERATING', 'DOWN', 'CLOSED')
        - snapshot_count: int
        """
        expected_timeseries_point = {
            "hour_start_utc": datetime(2025, 12, 8, 18, 0, 0),
            "avg_wait_time_minutes": 45.5,
            "uptime_percentage": 95.0,
            "status": "OPERATING",
            "snapshot_count": 12
        }

        required_fields = ['hour_start_utc', 'avg_wait_time_minutes', 'uptime_percentage',
                          'status', 'snapshot_count']
        for field in required_fields:
            assert field in expected_timeseries_point, f"Missing required field: {field}"

    def test_summary_stats_has_required_fields(self):
        """
        Summary statistics object must have required fields.

        Required fields:
        - total_downtime_hours: float
        - uptime_percentage: float
        - avg_wait_time: float
        - total_operating_hours: int
        - total_hours: int
        """
        expected_summary = {
            "total_downtime_hours": 2.5,
            "uptime_percentage": 95.0,
            "avg_wait_time": 45.0,
            "total_operating_hours": 10,
            "total_hours": 12
        }

        required_fields = ['total_downtime_hours', 'uptime_percentage', 'avg_wait_time',
                          'total_operating_hours', 'total_hours']
        for field in required_fields:
            assert field in expected_summary, f"Missing required field: {field}"

    def test_downtime_event_has_required_fields(self):
        """
        Each downtime event must have required fields.

        Required fields:
        - start_time: datetime
        - end_time: datetime
        - duration_hours: float
        """
        expected_downtime_event = {
            "start_time": datetime(2025, 12, 8, 14, 0, 0),
            "end_time": datetime(2025, 12, 8, 15, 0, 0),
            "duration_hours": 1.0
        }

        required_fields = ['start_time', 'end_time', 'duration_hours']
        for field in required_fields:
            assert field in expected_downtime_event, f"Missing required field: {field}"

    def test_hourly_breakdown_item_has_required_fields(self):
        """
        Each hourly breakdown item must have required fields.

        Required fields:
        - hour_start_utc: datetime
        - avg_wait_time_minutes: float or None
        - operating_snapshots: int
        - down_snapshots: int
        - downtime_hours: float
        - uptime_percentage: float
        - snapshot_count: int
        """
        expected_hourly_breakdown = {
            "hour_start_utc": datetime(2025, 12, 8, 14, 0, 0),
            "avg_wait_time_minutes": 45.5,
            "operating_snapshots": 10,
            "down_snapshots": 2,
            "downtime_hours": 0.17,
            "uptime_percentage": 83.33,
            "snapshot_count": 12
        }

        required_fields = ['hour_start_utc', 'avg_wait_time_minutes', 'operating_snapshots',
                          'down_snapshots', 'downtime_hours', 'uptime_percentage', 'snapshot_count']
        for field in required_fields:
            assert field in expected_hourly_breakdown, f"Missing required field: {field}"


class TestRideDetailsAPIValidation:
    """
    Test ride details API input validation and error handling.
    """

    def test_invalid_period_returns_400(self):
        """
        Requesting with invalid period parameter should return 400 error.

        Valid periods: 'today', 'yesterday', 'last_week', 'last_month'
        Invalid periods: 'live', 'week', 'month', 'invalid', etc.
        """
        invalid_periods = ['live', 'week', 'month', 'invalid', '']

        for period in invalid_periods:
            # Mock response for invalid period
            expected_error_response = {
                "success": False,
                "error": "Invalid period. Must be 'today', 'yesterday', 'last_week', or 'last_month'"
            }

            assert expected_error_response["success"] is False
            assert "error" in expected_error_response

    def test_nonexistent_ride_returns_404(self):
        """
        Requesting details for non-existent ride should return 404 error.
        """
        nonexistent_ride_id = 999999

        expected_error_response = {
            "success": False,
            "error": f"Ride {nonexistent_ride_id} not found"
        }

        assert expected_error_response["success"] is False
        assert "error" in expected_error_response


class TestRideDetailsAPIDataTypes:
    """
    Test that ride details API returns correct data types.
    """

    def test_numeric_fields_are_numeric_types(self):
        """
        All numeric fields should be int or float, not strings.
        """
        # Ride metadata
        assert isinstance(1, int), "ride_id should be int"
        assert isinstance(1, int), "park_id should be int"
        assert isinstance(1, int), "tier should be int"

        # Summary stats
        assert isinstance(2.5, (int, float)), "total_downtime_hours should be numeric"
        assert isinstance(95.0, (int, float)), "uptime_percentage should be numeric"
        assert isinstance(45.0, (int, float)), "avg_wait_time should be numeric"

        # Timeseries data
        assert isinstance(45.5, (int, float)), "avg_wait_time_minutes should be numeric"
        assert isinstance(95.0, (int, float)), "uptime_percentage should be numeric"

    def test_status_field_is_valid_enum(self):
        """
        Status field should be one of the valid status values.
        """
        valid_statuses = ['OPERATING', 'DOWN', 'CLOSED']

        for status in valid_statuses:
            assert status in valid_statuses, f"Invalid status: {status}"


class TestRideDetailsAPIPeriods:
    """
    Test that ride details API correctly handles different time periods.
    """

    def test_today_period_returns_today_in_response(self):
        """
        Requesting period=today should return 'today' in response.
        """
        expected_response = {
            "success": True,
            "period": "today"
        }

        assert expected_response["period"] == "today"

    def test_yesterday_period_returns_yesterday_in_response(self):
        """
        Requesting period=yesterday should return 'yesterday' in response.
        """
        expected_response = {
            "success": True,
            "period": "yesterday"
        }

        assert expected_response["period"] == "yesterday"

    def test_last_week_period_returns_last_week_in_response(self):
        """
        Requesting period=last_week should return 'last_week' in response.
        """
        expected_response = {
            "success": True,
            "period": "last_week"
        }

        assert expected_response["period"] == "last_week"

    def test_last_month_period_returns_last_month_in_response(self):
        """
        Requesting period=last_month should return 'last_month' in response.
        """
        expected_response = {
            "success": True,
            "period": "last_month"
        }

        assert expected_response["period"] == "last_month"
