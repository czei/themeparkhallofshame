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
from unittest.mock import Mock, MagicMock, call, ANY
from datetime import datetime, timezone

from database.repositories.weather_repository import (
    WeatherObservationRepository,
    WeatherForecastRepository
)


@pytest.fixture
def mock_db_connection():
    """Mock SQLAlchemy database connection for unit tests.

    The repository uses SQLAlchemy's connection.execute(text(sql), params).
    """
    connection = MagicMock()
    # For queries that return results (like get_latest_observation)
    mock_result = MagicMock()
    mock_result.fetchone.return_value = None
    connection.execute.return_value = mock_result
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

        # Verify connection.execute was called
        mock_db_connection.execute.assert_called_once()

        # Get the text() object passed to execute
        call_args = mock_db_connection.execute.call_args
        sql_text = str(call_args[0][0])

        # Verify SQL contains INSERT and ON DUPLICATE KEY UPDATE
        assert 'INSERT INTO weather_observations' in sql_text
        assert 'ON DUPLICATE KEY UPDATE' in sql_text

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

        # Get the parameters passed to execute
        call_args = mock_db_connection.execute.call_args
        params = call_args[0][1]  # Second positional arg is parameters

        assert params['park_id'] == 42
        assert params['observation_time'] == datetime(2025, 12, 17, 0, 0, 0, tzinfo=timezone.utc)
        assert params['temperature_c'] == 24.0
        assert params['temperature_f'] == 75.2

    def test_batch_insert_observations_iterates(self, mock_db_connection):
        """batch_insert_observations() should call execute for each observation."""
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

        # Verify execute was called twice (once per observation)
        assert mock_db_connection.execute.call_count == 2

    def test_batch_insert_observations_empty_list(self, mock_db_connection):
        """batch_insert_observations() should handle empty list gracefully."""
        repo = WeatherObservationRepository(mock_db_connection)

        repo.batch_insert_observations([])

        # Should not call execute for empty list
        mock_db_connection.execute.assert_not_called()

    def test_get_latest_observation_query(self, mock_db_connection):
        """get_latest_observation() should query for most recent observation."""
        repo = WeatherObservationRepository(mock_db_connection)

        # Mock the result to return a row
        mock_result = MagicMock()
        mock_row = MagicMock()
        mock_row._mapping = {
            'observation_id': 123,
            'park_id': 42,
            'temperature_f': 75.2,
        }
        mock_result.fetchone.return_value = mock_row
        mock_db_connection.execute.return_value = mock_result

        result = repo.get_latest_observation(park_id=42)

        # Verify execute was called
        mock_db_connection.execute.assert_called_once()

        # Get the SQL from the text() object
        sql_text = str(mock_db_connection.execute.call_args[0][0])
        assert 'SELECT' in sql_text
        assert 'ORDER BY observation_time DESC' in sql_text
        assert 'LIMIT 1' in sql_text

    def test_get_latest_observation_no_data(self, mock_db_connection):
        """get_latest_observation() should return None when no data exists."""
        repo = WeatherObservationRepository(mock_db_connection)

        mock_result = MagicMock()
        mock_result.fetchone.return_value = None
        mock_db_connection.execute.return_value = mock_result

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

        # Verify connection.execute was called
        mock_db_connection.execute.assert_called_once()

        # Get the text() object passed to execute
        sql_text = str(mock_db_connection.execute.call_args[0][0])

        # Verify SQL contains INSERT and ON DUPLICATE KEY UPDATE
        assert 'INSERT INTO weather_forecasts' in sql_text
        assert 'ON DUPLICATE KEY UPDATE' in sql_text

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

        # Get the parameters passed to execute
        call_args = mock_db_connection.execute.call_args
        params = call_args[0][1]

        assert params['park_id'] == 42
        assert params['issued_at'] == datetime(2025, 12, 17, 0, 0, 0, tzinfo=timezone.utc)
        assert params['forecast_time'] == datetime(2025, 12, 18, 0, 0, 0, tzinfo=timezone.utc)
        assert params['precipitation_probability'] == 30

    def test_batch_insert_forecasts_iterates(self, mock_db_connection):
        """batch_insert_forecasts() should call execute for each forecast."""
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

        # Verify execute was called twice (once per forecast)
        assert mock_db_connection.execute.call_count == 2

    def test_batch_insert_forecasts_empty_list(self, mock_db_connection):
        """batch_insert_forecasts() should handle empty list gracefully."""
        repo = WeatherForecastRepository(mock_db_connection)

        repo.batch_insert_forecasts([])

        # Should not call execute for empty list
        mock_db_connection.execute.assert_not_called()
