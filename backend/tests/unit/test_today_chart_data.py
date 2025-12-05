"""
Test TODAY Period Chart Data
=============================

Tests that the chart_data field in park details API returns populated data
for the TODAY period, not all None values.

This test verifies the bug fix where TODAY period charts were showing blank
(all None values) even when shame score data exists.
"""

import pytest
from datetime import datetime, date, timedelta
from unittest.mock import MagicMock, patch
from sqlalchemy import text

from database.repositories.stats_repository import StatsRepository


class TestTodayChartData:
    """Test TODAY period chart data is populated correctly."""

    @pytest.fixture
    def mock_conn(self):
        """Create a mock database connection."""
        return MagicMock()

    @pytest.fixture
    def stats_repo(self, mock_conn):
        """Create a StatsRepository with mock connection."""
        return StatsRepository(mock_conn)

    def test_today_chart_data_should_not_be_all_none(self, stats_repo, mock_conn):
        """
        FAILING TEST: Chart data for TODAY period should have actual values, not all None.

        Given: A park with shame_score data in park_activity_snapshots for today
        When: get_park_today_shame_breakdown() is called
        Then: chart_data should contain actual shame score values, not all None
        """
        # Mock the shame score query to return a valid score
        shame_score_result = MagicMock()
        shame_score_result.fetchone.return_value = MagicMock(avg_shame_score=1.5)

        # Mock the chart data query to return hourly data
        # Simulating park_activity_snapshots with shame_score values
        chart_rows = []
        for hour in range(6, 24):  # 6:00 to 23:00
            row = MagicMock()
            row.hour = f"{hour:02d}:00"
            row.shame_score = 1.5 if hour < 12 else 0.5  # Some variation
            chart_rows.append(row)

        chart_result = MagicMock()
        chart_result.fetchall.return_value = chart_rows

        # Configure mock to return different results for different queries
        mock_conn.execute.side_effect = [shame_score_result, chart_result]

        # Call the method with TODAY period
        with patch('utils.timezone.get_today_range_to_now_utc') as mock_time:
            mock_time.return_value = (datetime.now().replace(hour=0, minute=0, second=0), datetime.now())

            result = stats_repo.get_park_today_shame_breakdown(park_id=196)

        # Assertions
        assert 'chart_data' in result, "chart_data should be present in result"
        chart = result['chart_data']

        assert chart is not None, "chart_data should not be None"
        assert 'data' in chart, "chart_data should have 'data' field"
        assert len(chart['data']) > 0, "chart_data should have data points"

        # CRITICAL: Not all values should be None
        non_none_values = [v for v in chart['data'] if v is not None]
        assert len(non_none_values) > 0, f"Chart data should have actual values, not all None. Got: {chart['data']}"

        # Verify we have hourly granularity
        assert chart.get('granularity') == 'hourly', "TODAY period should have hourly granularity"

        # Verify labels are present
        assert 'labels' in chart, "chart_data should have labels"
        assert len(chart['labels']) > 0, "chart_data should have hour labels"

    def test_today_chart_average_matches_shame_score(self, stats_repo, mock_conn):
        """
        Chart average should match the shame_score value.

        The chart_data.average field should equal shame_breakdown.shame_score.
        """
        shame_score_result = MagicMock()
        shame_score_result.fetchone.return_value = MagicMock(avg_shame_score=1.5)

        chart_rows = []
        for hour in range(6, 24):
            row = MagicMock()
            row.hour = f"{hour:02d}:00"
            row.shame_score = 1.5
            chart_rows.append(row)

        chart_result = MagicMock()
        chart_result.fetchall.return_value = chart_rows

        mock_conn.execute.side_effect = [shame_score_result, chart_result]

        with patch('utils.timezone.get_today_range_to_now_utc') as mock_time:
            mock_time.return_value = (datetime.now().replace(hour=0, minute=0, second=0), datetime.now())

            result = stats_repo.get_park_today_shame_breakdown(park_id=196)

        shame_score = result['shame_breakdown']['shame_score']
        chart_average = result['chart_data']['average']

        assert chart_average == shame_score, \
            f"Chart average ({chart_average}) should match shame_score ({shame_score})"
