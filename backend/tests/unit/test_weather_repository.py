"""
Unit Tests: Weather Repository
================================

Tests weather repositories with mocked database connections.

Test Strategy:
- Mock database connection (no real DB)
- Test SQL query construction
- Test parameter binding
- Test error handling

Coverage:
- T028: WeatherObservationRepository insert
- T029: WeatherForecastRepository insert
"""

import pytest
from unittest.mock import Mock, MagicMock, call
from datetime import datetime, timezone

from database.repositories.weather_repository import (
    WeatherObservationRepository,
    WeatherForecastRepository
)


@pytest.fixture
def mock_db_connection():
    """Mock database connection for unit tests."""
    connection = MagicMock()
    cursor = MagicMock()
    connection.cursor.return_value.__enter__.return_value = cursor
    return connection


class TestWeatherObservationRepository:
    """Unit tests for WeatherObservationRepository."""

    def test_initialization(self, mock_db_connection):
        """Repository should initialize with database connection."""
        repo = WeatherObservationRepository(mock_db_connection)
        assert repo.db == mock_db_connection

    def test_insert_observation_basic(self, mock_db_connection):
        """insert_observation() should execute INSERT with ON DUPLICATE KEY UPDATE."""
        repo = WeatherObservationRepository(mock_db_connection)

        observation = {
            'park_id': 42,
            'observation_time': datetime(2025, 12, 17, 0, 0, 0, tzinfo=timezone.utc),
            'temperature_c': 24.0,
            'temperature_f': 75.2,
            'wind_speed_kmh': 8.37,
            'wind_speed_mph': 5.2,
            'weather_code': 0,
        }

        repo.insert_observation(observation)

        # Verify cursor.execute was called
        cursor = mock_db_connection.cursor.return_value.__enter__.return_value
        cursor.execute.assert_called_once()

        # Verify SQL contains INSERT and ON DUPLICATE KEY UPDATE
        sql = cursor.execute.call_args[0][0]
        assert 'INSERT INTO weather_observations' in sql
        assert 'ON DUPLICATE KEY UPDATE' in sql

    def test_insert_observation_params(self, mock_db_connection):
        """insert_observation() should bind parameters correctly."""
        repo = WeatherObservationRepository(mock_db_connection)

        observation = {
            'park_id': 42,
            'observation_time': datetime(2025, 12, 17, 0, 0, 0, tzinfo=timezone.utc),
            'temperature_c': 24.0,
            'temperature_f': 75.2,
        }

        repo.insert_observation(observation)

        cursor = mock_db_connection.cursor.return_value.__enter__.return_value
        params = cursor.execute.call_args[0][1]

        assert params['park_id'] == 42
        assert params['observation_time'] == datetime(2025, 12, 17, 0, 0, 0, tzinfo=timezone.utc)
        assert params['temperature_c'] == 24.0
        assert params['temperature_f'] == 75.2

    def test_batch_insert_observations_uses_executemany(self, mock_db_connection):
        """batch_insert_observations() should use executemany() for efficiency."""
        repo = WeatherObservationRepository(mock_db_connection)

        observations = [
            {
                'park_id': 1,
                'observation_time': datetime(2025, 12, 17, 0, 0, 0, tzinfo=timezone.utc),
                'temperature_c': 24.0,
                'temperature_f': 75.2,
            },
            {
                'park_id': 1,
                'observation_time': datetime(2025, 12, 17, 1, 0, 0, tzinfo=timezone.utc),
                'temperature_c': 23.5,
                'temperature_f': 74.3,
            },
        ]

        repo.batch_insert_observations(observations)

        # Verify executemany was called (not execute)
        cursor = mock_db_connection.cursor.return_value.__enter__.return_value
        cursor.executemany.assert_called_once()

    def test_batch_insert_observations_empty_list(self, mock_db_connection):
        """batch_insert_observations() should handle empty list gracefully."""
        repo = WeatherObservationRepository(mock_db_connection)

        repo.batch_insert_observations([])

        # Should not call executemany for empty list
        cursor = mock_db_connection.cursor.return_value.__enter__.return_value
        cursor.executemany.assert_not_called()

    def test_get_latest_observation_query(self, mock_db_connection):
        """get_latest_observation() should query for most recent observation."""
        repo = WeatherObservationRepository(mock_db_connection)

        # Mock cursor.fetchone() to return a result
        cursor = mock_db_connection.cursor.return_value.__enter__.return_value
        cursor.fetchone.return_value = {
            'observation_id': 123,
            'park_id': 42,
            'temperature_f': 75.2,
        }

        result = repo.get_latest_observation(park_id=42)

        # Verify SELECT query was executed
        cursor.execute.assert_called_once()
        sql = cursor.execute.call_args[0][0]
        assert 'SELECT' in sql
        assert 'ORDER BY observation_time DESC' in sql
        assert 'LIMIT 1' in sql

    def test_get_latest_observation_no_data(self, mock_db_connection):
        """get_latest_observation() should return None when no data exists."""
        repo = WeatherObservationRepository(mock_db_connection)

        cursor = mock_db_connection.cursor.return_value.__enter__.return_value
        cursor.fetchone.return_value = None

        result = repo.get_latest_observation(park_id=42)

        assert result is None


class TestWeatherForecastRepository:
    """Unit tests for WeatherForecastRepository."""

    def test_initialization(self, mock_db_connection):
        """Repository should initialize with database connection."""
        repo = WeatherForecastRepository(mock_db_connection)
        assert repo.db == mock_db_connection

    def test_insert_forecast_basic(self, mock_db_connection):
        """insert_forecast() should execute INSERT with ON DUPLICATE KEY UPDATE."""
        repo = WeatherForecastRepository(mock_db_connection)

        forecast = {
            'park_id': 42,
            'issued_at': datetime(2025, 12, 17, 0, 0, 0, tzinfo=timezone.utc),
            'forecast_time': datetime(2025, 12, 18, 0, 0, 0, tzinfo=timezone.utc),
            'temperature_c': 24.0,
            'temperature_f': 75.2,
            'precipitation_probability': 30,
        }

        repo.insert_forecast(forecast)

        # Verify cursor.execute was called
        cursor = mock_db_connection.cursor.return_value.__enter__.return_value
        cursor.execute.assert_called_once()

        # Verify SQL contains INSERT and ON DUPLICATE KEY UPDATE
        sql = cursor.execute.call_args[0][0]
        assert 'INSERT INTO weather_forecasts' in sql
        assert 'ON DUPLICATE KEY UPDATE' in sql

    def test_insert_forecast_params(self, mock_db_connection):
        """insert_forecast() should bind parameters correctly."""
        repo = WeatherForecastRepository(mock_db_connection)

        forecast = {
            'park_id': 42,
            'issued_at': datetime(2025, 12, 17, 0, 0, 0, tzinfo=timezone.utc),
            'forecast_time': datetime(2025, 12, 18, 0, 0, 0, tzinfo=timezone.utc),
            'temperature_c': 24.0,
            'precipitation_probability': 30,
        }

        repo.insert_forecast(forecast)

        cursor = mock_db_connection.cursor.return_value.__enter__.return_value
        params = cursor.execute.call_args[0][1]

        assert params['park_id'] == 42
        assert params['issued_at'] == datetime(2025, 12, 17, 0, 0, 0, tzinfo=timezone.utc)
        assert params['forecast_time'] == datetime(2025, 12, 18, 0, 0, 0, tzinfo=timezone.utc)
        assert params['precipitation_probability'] == 30

    def test_batch_insert_forecasts_uses_executemany(self, mock_db_connection):
        """batch_insert_forecasts() should use executemany() for efficiency."""
        repo = WeatherForecastRepository(mock_db_connection)

        forecasts = [
            {
                'park_id': 1,
                'issued_at': datetime(2025, 12, 17, 0, 0, 0, tzinfo=timezone.utc),
                'forecast_time': datetime(2025, 12, 18, 0, 0, 0, tzinfo=timezone.utc),
                'temperature_c': 24.0,
            },
            {
                'park_id': 1,
                'issued_at': datetime(2025, 12, 17, 0, 0, 0, tzinfo=timezone.utc),
                'forecast_time': datetime(2025, 12, 18, 1, 0, 0, tzinfo=timezone.utc),
                'temperature_c': 23.5,
            },
        ]

        repo.batch_insert_forecasts(forecasts)

        # Verify executemany was called
        cursor = mock_db_connection.cursor.return_value.__enter__.return_value
        cursor.executemany.assert_called_once()

    def test_batch_insert_forecasts_empty_list(self, mock_db_connection):
        """batch_insert_forecasts() should handle empty list gracefully."""
        repo = WeatherForecastRepository(mock_db_connection)

        repo.batch_insert_forecasts([])

        # Should not call executemany for empty list
        cursor = mock_db_connection.cursor.return_value.__enter__.return_value
        cursor.executemany.assert_not_called()
