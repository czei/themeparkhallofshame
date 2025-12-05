"""
Test Park Details API Chart Data
=================================

Tests that the park details API endpoint passes the correct is_today parameter
to get_single_park_hourly() for different time periods.

This test verifies the bug fix where TODAY period wasn't passing is_today=True,
causing charts to query for full 24-hour days instead of midnight-to-now.
"""

import pytest
from datetime import datetime, date
from unittest.mock import MagicMock, patch, call
from flask import Flask

from api.routes.parks import parks_bp


class TestParksAPIChartData:
    """Test park details API chart data parameter passing."""

    @pytest.fixture
    def app(self):
        """Create a Flask test app."""
        app = Flask(__name__)
        app.register_blueprint(parks_bp, url_prefix='/api/parks')
        app.config['TESTING'] = True
        return app

    @pytest.fixture
    def client(self, app):
        """Create a test client."""
        return app.test_client()

    def test_today_period_passes_is_today_true(self, client):
        """
        CRITICAL BUG FIX TEST: TODAY period must pass is_today=True to get_single_park_hourly().

        Bug: parks.py line 501 was calling get_single_park_hourly() without is_today=True
        Result: Chart queried full 24-hour day including future hours, returning all None
        Fix: Add is_today=True parameter for TODAY period

        This test verifies the parameter is passed correctly.
        """
        with patch('api.routes.parks.get_db_connection') as mock_db:
            mock_conn = MagicMock()
            mock_db.return_value.__enter__.return_value = mock_conn

            # Mock the query results
            mock_conn.execute.return_value.fetchone.return_value = MagicMock(
                park_id=139,
                name="Knott's Berry Farm",
                city="Buena Park",
                state_province="CA",
                country="US",
                operator="Cedar Fair"
            )

            with patch('api.routes.parks.ParkShameHistoryQuery') as mock_chart_query:
                mock_chart_instance = MagicMock()
                mock_chart_query.return_value = mock_chart_instance

                # Mock chart data return value
                mock_chart_instance.get_single_park_hourly.return_value = {
                    'labels': [f'{h}:00' for h in range(6, 24)],
                    'data': [1.5] * 18,  # Non-None values
                    'average': 1.5,
                    'granularity': 'hourly'
                }

                with patch('api.routes.parks.StatsRepository') as mock_stats:
                    mock_stats_instance = MagicMock()
                    mock_stats.return_value = mock_stats_instance

                    # Mock shame breakdown
                    mock_stats_instance.get_park_today_shame_breakdown.return_value = {
                        'shame_breakdown': {
                            'shame_score': 1.5,
                            'rides_down': [],
                            'total_park_weight': 45.0,
                            'park_is_open': True
                        }
                    }

                    # Make the request
                    response = client.get('/api/parks/139/details?period=today')

                    # CRITICAL ASSERTION: Verify is_today=True was passed
                    assert mock_chart_instance.get_single_park_hourly.called, \
                        "get_single_park_hourly should have been called"

                    call_args = mock_chart_instance.get_single_park_hourly.call_args
                    assert call_args is not None, "get_single_park_hourly was not called"

                    # Check that is_today=True was passed
                    assert 'is_today' in call_args.kwargs, \
                        "is_today parameter should be passed"
                    assert call_args.kwargs['is_today'] is True, \
                        "TODAY period must pass is_today=True to avoid querying future hours"

    def test_yesterday_period_passes_is_today_false(self, client):
        """
        YESTERDAY period should pass is_today=False to query full 24-hour day.

        Unlike TODAY (which should only query midnight to NOW), YESTERDAY should
        query the complete 24-hour day since it's in the past.
        """
        with patch('api.routes.parks.get_db_connection') as mock_db:
            mock_conn = MagicMock()
            mock_db.return_value.__enter__.return_value = mock_conn

            # Mock the query results
            mock_conn.execute.return_value.fetchone.return_value = MagicMock(
                park_id=139,
                name="Knott's Berry Farm",
                city="Buena Park",
                state_province="CA",
                country="US",
                operator="Cedar Fair"
            )

            with patch('api.routes.parks.ParkShameHistoryQuery') as mock_chart_query:
                mock_chart_instance = MagicMock()
                mock_chart_query.return_value = mock_chart_instance

                # Mock chart data return value
                mock_chart_instance.get_single_park_hourly.return_value = {
                    'labels': [f'{h}:00' for h in range(6, 24)],
                    'data': [1.5] * 18,
                    'average': 1.5,
                    'granularity': 'hourly'
                }

                with patch('api.routes.parks.StatsRepository') as mock_stats:
                    mock_stats_instance = MagicMock()
                    mock_stats.return_value = mock_stats_instance

                    # Mock shame breakdown
                    mock_stats_instance.get_park_yesterday_shame_breakdown.return_value = {
                        'shame_breakdown': {
                            'shame_score': 1.5,
                            'rides_down': [],
                            'total_park_weight': 45.0,
                            'park_is_open': True
                        }
                    }

                    # Make the request
                    response = client.get('/api/parks/139/details?period=yesterday')

                    # ASSERTION: Verify is_today=False was passed
                    assert mock_chart_instance.get_single_park_hourly.called, \
                        "get_single_park_hourly should have been called"

                    call_args = mock_chart_instance.get_single_park_hourly.call_args
                    assert call_args is not None, "get_single_park_hourly was not called"

                    # Check that is_today=False was passed
                    assert 'is_today' in call_args.kwargs, \
                        "is_today parameter should be passed"
                    assert call_args.kwargs['is_today'] is False, \
                        "YESTERDAY period should pass is_today=False to query full 24-hour day"
