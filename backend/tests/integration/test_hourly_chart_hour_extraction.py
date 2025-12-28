# backend/tests/integration/test_hourly_chart_hour_extraction.py
"""
Integration test for hourly chart hour extraction bug.

Bug: _query_hourly_tables was using Python timedelta to calculate Pacific time,
which SQLAlchemy cannot translate to SQL. This caused func.hour() to return NULL
for all rows, resulting in all data being filtered out.

Fix: Use MySQL DATE_SUB() function instead of Python timedelta subtraction.

This is a TDD regression test - written RED first to expose the bug.
"""

from datetime import datetime, timedelta, timezone, date
import pytest
from sqlalchemy import text


@pytest.fixture
def hourly_chart_test_data(mysql_session):
    """
    Insert test data into park_hourly_stats for a single park.

    The test verifies that hour extraction works correctly for UTC->Pacific
    time conversion.
    """
    conn = mysql_session

    # Use high IDs to avoid conflicts
    park_id = 990001

    # Clean up any existing test data
    conn.execute(text("DELETE FROM park_hourly_stats WHERE park_id = :park_id"), {"park_id": park_id})
    conn.execute(text("DELETE FROM parks WHERE park_id = :park_id"), {"park_id": park_id})

    # Create test park
    conn.execute(text("""
        INSERT INTO parks (park_id, queue_times_id, name, city, state_province, country, timezone, operator, is_active)
        VALUES (:park_id, 990001, 'Hour Extraction Test Park', 'Test City', 'TC', 'US', 'America/Los_Angeles', 'Test Operator', TRUE)
    """), {"park_id": park_id})

    # Insert hourly stats for a full day (Dec 27, 2025)
    # UTC times: 08:00 to 07:00 next day (Pacific midnight to midnight)
    # This represents Pacific Dec 27, 2025: 00:00 to 23:00

    hours_utc = [
        # UTC time -> Pacific time -> Expected hour
        ("2025-12-27 08:00:00", 0.0, 0),    # 00:00 Pacific
        ("2025-12-27 09:00:00", 0.0, 1),    # 01:00 Pacific
        ("2025-12-27 10:00:00", 0.0, 2),    # 02:00 Pacific
        ("2025-12-27 15:00:00", 0.0, 7),    # 07:00 Pacific
        ("2025-12-27 16:00:00", 1.5, 8),    # 08:00 Pacific - park opens
        ("2025-12-27 17:00:00", 2.0, 9),    # 09:00 Pacific
        ("2025-12-27 18:00:00", 3.5, 10),   # 10:00 Pacific
        ("2025-12-27 19:00:00", 7.5, 11),   # 11:00 Pacific
        ("2025-12-27 20:00:00", 5.0, 12),   # 12:00 Pacific
        ("2025-12-27 21:00:00", 4.0, 13),   # 13:00 Pacific
        ("2025-12-28 00:00:00", 2.5, 16),   # 16:00 Pacific
        ("2025-12-28 01:00:00", 1.0, 17),   # 17:00 Pacific
    ]

    for utc_time, downtime_hours, expected_pacific_hour in hours_utc:
        conn.execute(text("""
            INSERT INTO park_hourly_stats (
                park_id, hour_start_utc, total_downtime_hours, rides_down,
                avg_wait_time_minutes, park_was_open, snapshot_count
            )
            VALUES (
                :park_id, :hour_start_utc, :total_downtime_hours, :rides_down,
                :avg_wait_minutes, :park_was_open, :snapshot_count
            )
        """), {
            "park_id": park_id,
            "hour_start_utc": utc_time,
            "total_downtime_hours": downtime_hours,
            "rides_down": 5 if downtime_hours > 0 else 0,
            "avg_wait_minutes": 30.0,
            "park_was_open": True,
            "snapshot_count": 12
        })

    conn.commit()

    return {
        "park_id": park_id,
        "target_date": date(2025, 12, 27),
        "expected_hours_with_downtime": [8, 9, 10, 11, 12, 13, 16, 17],  # Pacific hours with downtime > 0
        "expected_shame_scores": {
            8: 1.5,   # 08:00 Pacific
            9: 2.0,   # 09:00 Pacific
            10: 3.5,  # 10:00 Pacific
            11: 7.5,  # 11:00 Pacific
            12: 5.0,  # 12:00 Pacific
            13: 4.0,  # 13:00 Pacific
            16: 2.5,  # 16:00 Pacific
            17: 1.0,  # 17:00 Pacific
        }
    }


class TestHourlyChartHourExtraction:
    """
    Test that _query_hourly_tables correctly extracts Pacific hours from UTC timestamps.

    Bug: Using Python timedelta subtraction on a SQLAlchemy column doesn't translate
    to SQL properly, causing func.hour() to return NULL.
    """

    def test_hour_extraction_returns_non_null_hours(self, mysql_session, hourly_chart_test_data):
        """
        TDD RED: Verify hour extraction produces valid Pacific hours, not NULL.

        The bug caused hour to be NULL for all rows, which filtered out all data.
        """
        from database.queries.charts.park_shame_history import ParkShameHistoryQuery

        park_id = hourly_chart_test_data["park_id"]
        target_date = hourly_chart_test_data["target_date"]

        # Create query instance with use_hourly_tables=True (the buggy code path)
        query = ParkShameHistoryQuery(mysql_session, use_hourly_tables=True)

        # Get chart data
        result = query.get_single_park_hourly(
            park_id=park_id,
            target_date=target_date,
            is_today=False
        )

        # The chart should have data (not all zeros/None)
        assert result is not None, "Result should not be None"
        assert "data" in result, "Result should contain 'data' key"
        assert "labels" in result, "Result should contain 'labels' key"

        # CRITICAL: Data should not be empty or all None
        # The bug caused this to be an empty list because hour was NULL
        non_null_data = [d for d in result["data"] if d is not None and d > 0]
        assert len(non_null_data) > 0, (
            f"Chart data should contain non-zero values. "
            f"Got data: {result['data']}, labels: {result['labels']}. "
            f"If data is empty or all zeros, hour extraction is broken (returning NULL)."
        )

    def test_shame_scores_match_expected_values(self, mysql_session, hourly_chart_test_data):
        """
        TDD RED: Verify shame scores match the expected values from test data.
        """
        from database.queries.charts.park_shame_history import ParkShameHistoryQuery

        park_id = hourly_chart_test_data["park_id"]
        target_date = hourly_chart_test_data["target_date"]
        expected_scores = hourly_chart_test_data["expected_shame_scores"]

        query = ParkShameHistoryQuery(mysql_session, use_hourly_tables=True)
        result = query.get_single_park_hourly(
            park_id=park_id,
            target_date=target_date,
            is_today=False
        )

        # Build a mapping of hour -> shame_score from the result
        actual_scores = {}
        for i, label in enumerate(result["labels"]):
            # Labels are like "8:00", "9:00", etc.
            hour = int(label.split(":")[0])
            if result["data"][i] is not None:
                actual_scores[hour] = result["data"][i]

        # Verify at least some of the expected hours are present
        matching_hours = set(actual_scores.keys()) & set(expected_scores.keys())
        assert len(matching_hours) >= 3, (
            f"Should have at least 3 matching hours. "
            f"Expected hours: {list(expected_scores.keys())}, "
            f"Actual hours: {list(actual_scores.keys())}"
        )

        # Verify scores match for matching hours
        for hour in matching_hours:
            expected = expected_scores[hour]
            actual = actual_scores[hour]
            assert abs(actual - expected) < 0.01, (
                f"Shame score mismatch at hour {hour}: expected {expected}, got {actual}"
            )

    def test_rides_down_data_present(self, mysql_session, hourly_chart_test_data):
        """
        TDD RED: Verify rides_down data is present in chart response.
        """
        from database.queries.charts.park_shame_history import ParkShameHistoryQuery

        park_id = hourly_chart_test_data["park_id"]
        target_date = hourly_chart_test_data["target_date"]

        query = ParkShameHistoryQuery(mysql_session, use_hourly_tables=True)
        result = query.get_single_park_hourly(
            park_id=park_id,
            target_date=target_date,
            is_today=False
        )

        assert "rides_down" in result, "Result should contain 'rides_down' key"

        # Should have non-zero rides_down values
        non_zero_rides = [r for r in result["rides_down"] if r is not None and r > 0]
        assert len(non_zero_rides) > 0, (
            f"rides_down should contain non-zero values. Got: {result['rides_down']}"
        )
