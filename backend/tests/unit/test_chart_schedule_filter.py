"""
Unit tests for chart query schedule-based filtering.

TDD RED PHASE: These tests verify that chart queries filter hours
by official park schedule times, not by park_appears_open or park_was_open.

Bug Report:
- Chart showed 23 rides "down" during hours 0-7 and 22-23
- Root cause: park_was_open = 1 for all hours due to rides_open > 0 fallback
- Fix: Filter by actual schedule opening/closing times

Expected behavior:
- Chart should only show hours within the park's official operating schedule
- Hours outside schedule should have no data (not 0, not included at all)
"""

import pytest
from unittest.mock import MagicMock, patch
from datetime import date, datetime, timedelta
from decimal import Decimal


class TestChartScheduleFilter:
    """Test that chart queries filter by schedule times."""

    def test_hourly_chart_excludes_hours_before_opening(self):
        """
        FAILING TEST: Chart should not include hours before park opens.

        If park opens at 8am, hours 0-7 should not appear in chart data.
        """
        from src.database.queries.charts.park_shame_history import ParkShameHistoryQuery

        # Create a mock connection
        mock_conn = MagicMock()

        # Create mock schedule data: park opens 8am, closes 10pm Pacific
        # In UTC: 16:00 - 06:00 next day
        mock_schedule = {
            'opening_time': datetime(2025, 12, 25, 16, 0, 0),  # 8am Pacific
            'closing_time': datetime(2025, 12, 26, 6, 0, 0),   # 10pm Pacific
        }

        # Mock the query to return data for ALL 24 hours (simulating the bug)
        mock_all_hours_data = [
            {'hour': h, 'shame_score': 0.0 if h < 8 or h > 21 else 1.5,
             'rides_down': 23 if h < 8 or h > 21 else 5,
             'avg_wait_time_minutes': None if h < 8 or h > 21 else 30}
            for h in range(24)
        ]

        mock_result = MagicMock()
        mock_result.__iter__ = lambda self: iter([
            MagicMock(_mapping=row) for row in mock_all_hours_data
        ])
        mock_conn.execute.return_value = mock_result

        query = ParkShameHistoryQuery(mock_conn, use_hourly_tables=False)

        # This should filter by schedule and return only operating hours
        with patch.object(query, '_get_schedule_for_date', return_value=mock_schedule):
            result = query.get_single_park_hourly(
                park_id=194,
                target_date=date(2025, 12, 25),
                is_today=False
            )

        # Hours before opening (0-7) should NOT be in labels
        labels = result.get('labels', [])
        for hour in range(8):
            assert f"{hour}:00" not in labels, \
                f"Hour {hour}:00 should not appear - park doesn't open until 8am"

        # Hours during operation (8-21) SHOULD be in labels
        for hour in range(8, 22):
            assert f"{hour}:00" in labels, \
                f"Hour {hour}:00 should appear - park is open"

    def test_hourly_chart_excludes_hours_after_closing(self):
        """
        FAILING TEST: Chart should not include hours after park closes.

        If park closes at 10pm, hours 22-23 should not appear in chart data.
        """
        from src.database.queries.charts.park_shame_history import ParkShameHistoryQuery

        mock_conn = MagicMock()

        # Schedule: park closes at 10pm Pacific (22:00)
        mock_schedule = {
            'opening_time': datetime(2025, 12, 25, 16, 0, 0),  # 8am Pacific
            'closing_time': datetime(2025, 12, 26, 6, 0, 0),   # 10pm Pacific
        }

        mock_all_hours_data = [
            {'hour': h, 'shame_score': 1.5, 'rides_down': 5, 'avg_wait_time_minutes': 30}
            for h in range(24)
        ]

        mock_result = MagicMock()
        mock_result.__iter__ = lambda self: iter([
            MagicMock(_mapping=row) for row in mock_all_hours_data
        ])
        mock_conn.execute.return_value = mock_result

        query = ParkShameHistoryQuery(mock_conn, use_hourly_tables=False)

        with patch.object(query, '_get_schedule_for_date', return_value=mock_schedule):
            result = query.get_single_park_hourly(
                park_id=194,
                target_date=date(2025, 12, 25),
                is_today=False
            )

        labels = result.get('labels', [])

        # Hours after closing (22-23) should NOT be in labels
        assert "22:00" not in labels, "Hour 22:00 should not appear - park closed at 10pm"
        assert "23:00" not in labels, "Hour 23:00 should not appear - park closed at 10pm"

    def test_no_schedule_returns_empty_chart(self):
        """
        When no schedule data exists, chart should return empty data.

        This prevents showing "all rides down" for parks with missing schedules.
        """
        from src.database.queries.charts.park_shame_history import ParkShameHistoryQuery

        mock_conn = MagicMock()

        query = ParkShameHistoryQuery(mock_conn, use_hourly_tables=False)

        # No schedule available
        with patch.object(query, '_get_schedule_for_date', return_value=None):
            result = query.get_single_park_hourly(
                park_id=194,
                target_date=date(2025, 12, 25),
                is_today=False
            )

        # Should return empty or minimal chart data
        assert len(result.get('labels', [])) == 0 or \
               all(d is None for d in result.get('data', [])), \
               "Chart should be empty when no schedule data exists"


class TestAggregationParkWasOpen:
    """Test that hourly aggregation correctly sets park_was_open."""

    def test_park_was_open_uses_schedule_not_rides_open(self):
        """
        park_was_open should be based on schedule, not rides_open > 0.

        Bug: A few rides showing as "open" during closed hours caused
        park_was_open = 1, which then showed 23 rides as "down".
        """
        # This test validates the SQL in aggregate_hourly.py
        # The fix should remove: OR pas.rides_open > 0

        # For now, just verify the expected behavior
        # The actual SQL fix will be in aggregate_hourly.py
        pass  # Will be implemented with the aggregation fix
