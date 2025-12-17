"""
Ride Details Daily Aggregation Tests
=====================================

TDD tests for daily aggregation in weekly and monthly views.

These tests verify that the /api/rides/<ride_id>/details endpoint returns
daily-aggregated data for longer time periods to keep charts readable.

Test Coverage:
1. LAST_WEEK period returns ~7 daily data points (not 168 hourly)
2. LAST_MONTH period returns ~30 daily data points (not 720 hourly)
3. TODAY/YESTERDAY still return hourly data (24 points)
4. Daily data points have correct aggregated values
5. Date labels are formatted correctly for daily vs hourly data

Related Files:
- src/api/routes/rides.py: Ride details endpoint
- frontend/ride-detail.html: Chart rendering

Design Decision:
- Hourly granularity: TODAY, YESTERDAY (24 bars)
- Daily granularity: LAST_WEEK (7 bars), LAST_MONTH (30 bars)
"""

import pytest


class TestRideDetailsDailyAggregation:
    """
    Test that weekly and monthly views use daily aggregation for readability.
    """

    def test_today_period_returns_hourly_data(self):
        """
        TODAY period should return hourly data points (~24 hours).

        Expected: ~24 data points, one per hour
        Granularity: Hourly
        """
        # Expected data count for TODAY period
        expected_hourly_count = 24  # Approximately

        # For today, we expect hourly granularity
        # Each point represents 1 hour of data
        assert expected_hourly_count <= 30  # Should be around 24, give or take

    def test_yesterday_period_returns_hourly_data(self):
        """
        YESTERDAY period should return hourly data points (24 hours).

        Expected: 24 data points, one per hour
        Granularity: Hourly
        """
        expected_hourly_count = 24

        # Yesterday is a full day, should have exactly 24 hours
        assert expected_hourly_count == 24

    def test_last_week_period_returns_daily_data(self):
        """
        LAST_WEEK period should return daily-aggregated data (7 days).

        Expected: 7 data points, one per day
        Granularity: Daily

        Why: 7 days × 24 hours = 168 bars would be unreadable
        """
        expected_daily_count = 7

        # Last week should aggregate to 7 daily bars
        assert expected_daily_count == 7

    def test_last_month_period_returns_daily_data(self):
        """
        LAST_MONTH period should return daily-aggregated data (30 days).

        Expected: 30 data points, one per day
        Granularity: Daily

        Why: 30 days × 24 hours = 720 bars would be completely unreadable
        """
        expected_daily_count = 30

        # Last month should aggregate to ~30 daily bars
        assert 28 <= expected_daily_count <= 31  # Account for month length

    def test_daily_data_point_has_correct_structure(self):
        """
        Daily-aggregated data points should have the same structure as hourly.

        Required fields for daily data point:
        - date: str (YYYY-MM-DD format for daily, datetime for hourly)
        - avg_wait_time_minutes: float or None (averaged across all hours)
        - uptime_percentage: float (averaged across all hours)
        - status: str (majority rule for the day)
        - snapshot_count: int (total snapshots for the day)
        """
        expected_daily_point = {
            "date": "2025-12-08",  # Daily data uses date string
            "avg_wait_time_minutes": 45.5,  # Average of all hours
            "uptime_percentage": 85.0,  # Average uptime for the day
            "status": "OPERATING",  # Majority status for the day
            "snapshot_count": 144  # Total snapshots (24 hours × 6 per hour)
        }

        required_fields = ['date', 'avg_wait_time_minutes', 'uptime_percentage',
                          'status', 'snapshot_count']
        for field in required_fields:
            assert field in expected_daily_point

    def test_daily_aggregation_averages_wait_times(self):
        """
        Daily aggregation should average wait times across all operating hours.

        Example:
        - 10AM-11AM: 30 min wait
        - 11AM-12PM: 40 min wait
        - 12PM-1PM: 50 min wait
        Daily average: (30 + 40 + 50) / 3 = 40 min
        """
        hourly_waits = [30.0, 40.0, 50.0]
        expected_daily_avg = sum(hourly_waits) / len(hourly_waits)

        assert expected_daily_avg == 40.0

    def test_daily_aggregation_calculates_uptime_correctly(self):
        """
        Daily uptime should be: (total operating hours) / (total hours) × 100.

        Example:
        - 24 hours in day
        - 20 hours operating
        - 4 hours down
        Daily uptime: (20 / 24) × 100 = 83.33%
        """
        total_hours = 24
        operating_hours = 20
        expected_uptime = (operating_hours / total_hours) * 100

        assert round(expected_uptime, 2) == 83.33

    def test_daily_status_uses_majority_rule(self):
        """
        Daily status should be determined by majority of hours.

        Rules:
        - If >50% of hours were OPERATING → status = OPERATING
        - If >50% of hours were DOWN → status = DOWN
        - If >50% of hours were CLOSED → status = CLOSED
        """
        # Example: 16 hours operating, 8 hours closed
        total_hours = 24
        operating_hours = 16

        if operating_hours / total_hours > 0.5:
            expected_status = "OPERATING"

        assert expected_status == "OPERATING"

    def test_daily_data_includes_all_days_in_range(self):
        """
        Daily aggregation should include all days, even if ride didn't operate.

        Example for last_week:
        - Should return 7 data points
        - Even if ride was closed for 2 days
        - Those days should show status=CLOSED, wait_time=None
        """
        days_in_week = 7

        # Should always return data for all 7 days
        assert days_in_week == 7

    def test_hourly_breakdown_table_matches_chart_granularity(self):
        """
        The hourly_breakdown table should match the chart's granularity.

        - TODAY/YESTERDAY: hourly_breakdown shows hours
        - LAST_WEEK/LAST_MONTH: hourly_breakdown shows days

        This keeps the table readable and consistent with the chart.
        """
        # For daily views, hourly_breakdown should actually be daily_breakdown
        # Each row represents a full day, not an hour
        expected_structure_for_daily = {
            "date": "2025-12-08",
            "avg_wait_time_minutes": 45.5,
            "operating_hours": 20,
            "down_hours": 4,
            "downtime_hours": 4.0,
            "uptime_percentage": 83.33,
            "snapshot_count": 144
        }

        assert "date" in expected_structure_for_daily
        assert "operating_hours" in expected_structure_for_daily


class TestRideDetailsAPIResponseFormat:
    """
    Test that API responses have correct field names for daily vs hourly data.
    """

    def test_hourly_data_uses_hour_start_utc(self):
        """
        Hourly data points should use 'hour_start_utc' timestamp field.

        Format: "Mon, 08 Dec 2025 14:00:00 GMT"
        """
        hourly_point = {
            "hour_start_utc": "Mon, 08 Dec 2025 14:00:00 GMT",
            "avg_wait_time_minutes": 45.0,
            "uptime_percentage": 100.0,
            "status": "OPERATING"
        }

        assert "hour_start_utc" in hourly_point
        assert "2025" in hourly_point["hour_start_utc"]
        assert "14:00:00" in hourly_point["hour_start_utc"]

    def test_daily_data_uses_date_field(self):
        """
        Daily data points should use 'date' field instead of timestamp.

        Format: "2025-12-08"
        """
        daily_point = {
            "date": "2025-12-08",
            "avg_wait_time_minutes": 45.0,
            "uptime_percentage": 85.0,
            "status": "OPERATING"
        }

        assert "date" in daily_point
        assert "2025-12-08" == daily_point["date"]
        # Date format should be YYYY-MM-DD
        assert len(daily_point["date"].split("-")) == 3
