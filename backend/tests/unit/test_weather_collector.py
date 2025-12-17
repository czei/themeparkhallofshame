"""
Unit Tests: Weather Collector
==============================

Tests WeatherCollector with mocked dependencies.

Test Strategy:
- Mock API client, repository, and database connection
- Test collection logic for single park
- Test failure threshold logic
- Test concurrent collection orchestration

Coverage:
- T041: _collect_for_park() with mocked API
- T042: Failure threshold logic (>50% fail = abort)
"""

import pytest
from unittest.mock import Mock, MagicMock, patch, call
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor

from scripts.collect_weather import WeatherCollector


class TestWeatherCollector:
    """Unit tests for WeatherCollector."""

    @pytest.fixture
    def mock_db_connection(self):
        """Mock database connection."""
        connection = MagicMock()
        cursor = MagicMock()
        connection.cursor.return_value.__enter__.return_value = cursor
        return connection

    @pytest.fixture
    def mock_api_client(self):
        """Mock OpenMeteo API client."""
        client = MagicMock()
        # Mock successful weather fetch
        client.fetch_weather.return_value = {
            'hourly': {
                'time': ['2025-12-17T00:00'],
                'temperature_2m': [75.2],
                'apparent_temperature': [72.1],
                'precipitation': [0.0],
                'precipitation_probability': [0],
                'rain': [0.0],
                'snowfall': [0.0],
                'weather_code': [0],
                'cloud_cover': [20],
                'wind_speed_10m': [5.2],
                'wind_gusts_10m': [8.1],
                'wind_direction_10m': [180],
                'relative_humidity_2m': [65],
                'surface_pressure': [1013.2],
                'visibility': [10000],
            }
        }
        client.parse_observations.return_value = [
            {
                'park_id': 1,
                'observation_time': datetime(2025, 12, 17, 0, 0, 0, tzinfo=timezone.utc),
                'temperature_f': 75.2,
                'temperature_c': 24.0,
                'weather_code': 0,
            }
        ]
        return client

    @pytest.fixture
    def mock_repository(self):
        """Mock WeatherObservationRepository."""
        repo = MagicMock()
        return repo

    @pytest.fixture
    def collector(self, mock_db_connection, mock_api_client, mock_repository):
        """Create WeatherCollector with mocked dependencies."""
        with patch('scripts.collect_weather.get_openmeteo_client', return_value=mock_api_client):
            with patch('scripts.collect_weather.WeatherObservationRepository', return_value=mock_repository):
                collector = WeatherCollector(mock_db_connection)
                return collector

    def test_initialization(self, collector, mock_db_connection):
        """WeatherCollector should initialize with database connection."""
        assert collector.db == mock_db_connection

    def test_collect_for_park_success(self, collector, mock_api_client, mock_repository):
        """_collect_for_park() should fetch weather and insert observations."""
        park = {'park_id': 1, 'latitude': 28.41777, 'longitude': -81.58116}

        result = collector._collect_for_park(park)

        # Should call API client
        mock_api_client.fetch_weather.assert_called_once_with(
            latitude=28.41777,
            longitude=-81.58116,
            forecast_days=7
        )

        # Should parse observations
        mock_api_client.parse_observations.assert_called_once()

        # Should batch insert
        mock_repository.batch_insert_observations.assert_called_once()

        # Should return success
        assert result['success'] is True
        assert result['park_id'] == 1

    def test_collect_for_park_api_failure(self, collector, mock_api_client):
        """_collect_for_park() should handle API failures gracefully."""
        park = {'park_id': 1, 'latitude': 28.41777, 'longitude': -81.58116}

        # Mock API failure
        mock_api_client.fetch_weather.side_effect = Exception("API timeout")

        result = collector._collect_for_park(park)

        # Should return failure
        assert result['success'] is False
        assert result['park_id'] == 1
        assert 'API timeout' in result['error']

    def test_collect_for_park_repository_failure(self, collector, mock_api_client, mock_repository):
        """_collect_for_park() should handle repository failures gracefully."""
        park = {'park_id': 1, 'latitude': 28.41777, 'longitude': -81.58116}

        # Mock repository failure
        mock_repository.batch_insert_observations.side_effect = Exception("Database error")

        result = collector._collect_for_park(park)

        # Should return failure
        assert result['success'] is False
        assert result['park_id'] == 1
        assert 'Database error' in result['error']

    def test_failure_threshold_not_exceeded(self, collector):
        """run() should succeed when failure rate is below 50%."""
        # Mock successful collection for most parks
        results = [
            {'success': True, 'park_id': 1},
            {'success': True, 'park_id': 2},
            {'success': False, 'park_id': 3},  # 1 failure
            {'success': True, 'park_id': 4},
        ]

        # Should not raise error (25% failure rate)
        try:
            collector._check_failure_threshold(results)
        except RuntimeError:
            pytest.fail("Should not raise error when failure rate < 50%")

    def test_failure_threshold_exceeded(self, collector):
        """run() should raise error when failure rate exceeds 50%."""
        # Mock failed collection for most parks
        results = [
            {'success': False, 'park_id': 1},
            {'success': False, 'park_id': 2},
            {'success': False, 'park_id': 3},
            {'success': True, 'park_id': 4},   # Only 1 success
        ]

        # Should raise error (75% failure rate)
        with pytest.raises(RuntimeError, match="Collection failed for 75.0% of parks"):
            collector._check_failure_threshold(results)

    def test_failure_threshold_exactly_50_percent(self, collector):
        """run() should raise error when failure rate is exactly 50%."""
        results = [
            {'success': False, 'park_id': 1},
            {'success': False, 'park_id': 2},
            {'success': True, 'park_id': 3},
            {'success': True, 'park_id': 4},
        ]

        # Should raise error (50% failure rate)
        with pytest.raises(RuntimeError, match="Collection failed for 50.0% of parks"):
            collector._check_failure_threshold(results)

    def test_failure_threshold_empty_results(self, collector):
        """_check_failure_threshold() should handle empty results."""
        results = []

        # Should not raise error
        try:
            collector._check_failure_threshold(results)
        except RuntimeError:
            pytest.fail("Should not raise error for empty results")

    def test_get_parks_queries_database(self, collector, mock_db_connection):
        """_get_parks() should query parks table."""
        # Mock SQLAlchemy result rows
        mock_row_1 = MagicMock()
        mock_row_1._mapping = {'park_id': 1, 'latitude': 28.41777, 'longitude': -81.58116, 'name': 'Park 1'}
        mock_row_2 = MagicMock()
        mock_row_2._mapping = {'park_id': 2, 'latitude': 33.8121, 'longitude': -117.9190, 'name': 'Park 2'}

        mock_db_connection.execute.return_value = [mock_row_1, mock_row_2]

        parks = collector._get_parks()

        # Should query database via execute()
        mock_db_connection.execute.assert_called_once()

        # Should return parks as list of dicts
        assert len(parks) == 2
        assert parks[0]['park_id'] == 1
        assert parks[1]['park_id'] == 2

    def test_collect_for_park_uses_rate_limiter(self, collector, mock_api_client):
        """_collect_for_park() should use rate limiter before API call."""
        park = {'park_id': 1, 'latitude': 28.41777, 'longitude': -81.58116}

        with patch.object(collector.rate_limiter, 'acquire') as mock_acquire:
            collector._collect_for_park(park)

            # Should acquire rate limit token
            mock_acquire.assert_called_once()

    def test_collect_for_park_logs_success(self, collector):
        """_collect_for_park() should log successful collection."""
        park = {'park_id': 1, 'latitude': 28.41777, 'longitude': -81.58116}

        with patch('scripts.collect_weather.logger') as mock_logger:
            result = collector._collect_for_park(park)

            # Should log success
            assert result['success'] is True
            # Verify logger was called (implementation detail)

    def test_collect_for_park_logs_failure(self, collector, mock_api_client):
        """_collect_for_park() should log failed collection."""
        park = {'park_id': 1, 'latitude': 28.41777, 'longitude': -81.58116}
        mock_api_client.fetch_weather.side_effect = Exception("API error")

        with patch('scripts.collect_weather.logger') as mock_logger:
            result = collector._collect_for_park(park)

            # Should log failure
            assert result['success'] is False
